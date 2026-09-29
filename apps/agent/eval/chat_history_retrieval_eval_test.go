package eval

import (
	"NexusGo/apps/agent/dao"
	"NexusGo/apps/agent/models"
	"NexusGo/apps/agent/retrieval"
	"NexusGo/apps/pkg/config"
	"NexusGo/apps/pkg/db"
	"NexusGo/apps/pkg/utils"
	vdb "NexusGo/apps/pkg/vector_db"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	milvusret "github.com/cloudwego/eino-ext/components/retriever/milvus2"
	"github.com/cloudwego/eino/schema"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

const chatHistoryEvalTopK = 5

type chatHistoryDataset struct {
	DatasetVersion string                    `json:"dataset_version"`
	CurrentUser    chatHistoryCurrentUser    `json:"current_user"`
	Conversations  []chatHistoryConversation `json:"conversations"`
	Corpus         []chatHistoryMessage      `json:"corpus"`
	Queries        []chatHistoryQuery        `json:"queries"`
}

type chatHistoryCurrentUser struct {
	UserID int64 `json:"user_id"`
}

type chatHistoryConversation struct {
	ID            string `json:"id"`
	Type          string `json:"type"`
	TargetUser    string `json:"target_user"`
	TargetUserID  int64  `json:"target_user_id"`
	TargetGroup   string `json:"target_group"`
	TargetGroupID int64  `json:"target_group_id"`
}

type chatHistoryMessage struct {
	MsgID          int64  `json:"msg_id"`
	ConversationID string `json:"conversation_id"`
	Sender         string `json:"sender"`
	SendTime       string `json:"send_time"`
	Content        string `json:"content"`
}

type chatHistoryToolRequest struct {
	TargetUser  string `json:"target_user"`
	TargetGroup string `json:"target_group"`
	StartTime   string `json:"start_time"`
	EndTime     string `json:"end_time"`
	Keywords    string `json:"keywords"`
}

type chatHistoryQuery struct {
	ID             string                 `json:"id"`
	Category       string                 `json:"category"`
	Query          string                 `json:"query"`
	ToolRequest    chatHistoryToolRequest `json:"tool_request"`
	ExpectedMsgIDs []int64                `json:"expected_msg_ids"`
	LabelType      string                 `json:"label_type"`
}

type chatHistoryMetrics struct {
	RecallAt1         float64
	RecallAt3         float64
	RecallAt5         float64
	MRR               float64
	NDCGAt5           float64
	HitRate           float64
	NoAnswerCount     int
	NoAnswerFalseHits int
	ScopeViolations   int
	TimeViolations    int
	latencies         []time.Duration
}

// TestChatHistoryRetrievalEvaluation exercises the same filtered MySQL/Milvus
// retrieval used by chat-history search. It is opt-in because it calls the
// configured embedding provider and writes a disposable, ID-isolated corpus.
func TestChatHistoryRetrievalEvaluation(t *testing.T) {
	if os.Getenv("NEXUSGO_RUN_CHAT_HISTORY_EVAL") != "1" {
		t.Skip("set NEXUSGO_RUN_CHAT_HISTORY_EVAL=1 to run the frozen chat-history evaluation")
	}
	if db.GetDB() == nil {
		t.Fatal("MySQL is not initialized")
	}
	if err := db.EnsureMessagesFullTextIndex(); err != nil {
		t.Fatalf("ensure FULLTEXT index: %v", err)
	}

	datasetPath := os.Getenv("NEXUSGO_CHAT_HISTORY_EVAL_DATASET")
	dataset := loadChatHistoryDataset(t, datasetPath)
	if dataset.DatasetVersion == "" {
		t.Fatal("chat-history dataset must declare dataset_version")
	}
	if datasetPath == "" && dataset.DatasetVersion != "v1.0" {
		t.Fatalf("default dataset must be frozen v1.0, got %q", dataset.DatasetVersion)
	}
	t.Logf("dataset=%s semantic score threshold: %.2f", dataset.DatasetVersion, chatHistoryEvaluationMinScore(t))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ids := chatHistoryMessageIDs(dataset.Corpus)
	cleanupChatHistoryCorpus(t, ctx, ids)
	t.Cleanup(func() { cleanupChatHistoryCorpus(t, context.Background(), ids) })
	conversationByID := chatHistoryConversationMap(dataset.Conversations)
	messageByID := chatHistoryMessageMap(dataset.Corpus)
	validateChatHistoryDataset(t, dataset, conversationByID, messageByID)

	if err := seedChatHistoryMySQL(dataset, conversationByID); err != nil {
		t.Fatalf("seed MySQL corpus: %v", err)
	}
	if err := seedChatHistoryMilvus(ctx, dataset, conversationByID); err != nil {
		t.Fatalf("seed Milvus corpus: %v", err)
	}
	flushTask, err := vdb.GetMilvus().Flush(ctx, milvusclient.NewFlushOption(vdb.MessageCollection))
	if err != nil {
		t.Fatalf("flush Milvus corpus: %v", err)
	}
	if err := flushTask.Await(ctx); err != nil {
		t.Fatalf("await Milvus flush: %v", err)
	}

	strategies := []struct {
		name   string
		search func(chatHistoryQuery, chatHistoryConversation) []int64
	}{
		{"fulltext_only", func(q chatHistoryQuery, c chatHistoryConversation) []int64 {
			return chatHistoryFulltextSearch(t, dataset.CurrentUser.UserID, q, c)
		}},
		{"semantic_only", func(q chatHistoryQuery, c chatHistoryConversation) []int64 {
			return chatHistorySemanticSearch(t, ctx, dataset.CurrentUser.UserID, q, c)
		}},
		{"semantic_first_fallback", func(q chatHistoryQuery, c chatHistoryConversation) []int64 {
			semantic := chatHistorySemanticSearch(t, ctx, dataset.CurrentUser.UserID, q, c)
			if len(semantic) > 0 {
				return semantic
			}
			return chatHistoryFulltextSearch(t, dataset.CurrentUser.UserID, q, c)
		}},
	}

	for _, strategy := range strategies {
		metrics := chatHistoryMetrics{}
		positiveCount := 0
		for _, query := range dataset.Queries {
			conversation := chatHistoryConversationForQuery(t, dataset.Conversations, query)
			startedAt := time.Now()
			returned := strategy.search(query, conversation)
			metrics.latencies = append(metrics.latencies, time.Since(startedAt))
			chatHistoryRecordFilterViolations(&metrics, returned, query, conversation, messageByID)

			if query.LabelType == "no_result" {
				metrics.NoAnswerCount++
				if len(returned) > 0 {
					metrics.NoAnswerFalseHits++
				}
				continue
			}

			positiveCount++
			mrr := computeMRR(returned, query.ExpectedMsgIDs)
			metrics.RecallAt1 += computeRecall(returned, query.ExpectedMsgIDs, 1)
			metrics.RecallAt3 += computeRecall(returned, query.ExpectedMsgIDs, 3)
			metrics.RecallAt5 += computeRecall(returned, query.ExpectedMsgIDs, chatHistoryEvalTopK)
			metrics.MRR += mrr
			metrics.NDCGAt5 += computeNDCG(returned, query.ExpectedMsgIDs, chatHistoryEvalTopK)
			if mrr > 0 {
				metrics.HitRate++
			}
			t.Logf("%s %s (%s): %v", strategy.name, query.ID, query.Category, returned)
		}
		chatHistoryFinalizeMetrics(&metrics, positiveCount)
		noAnswerFPR := chatHistoryRate(metrics.NoAnswerFalseHits, metrics.NoAnswerCount)
		latencyP50 := chatHistoryLatencyPercentile(metrics.latencies, 0.50)
		latencyP95 := chatHistoryLatencyPercentile(metrics.latencies, 0.95)
		fmt.Printf("chat_history_v1 strategy=%-24s R@1=%.3f R@3=%.3f R@5=%.3f MRR=%.3f NDCG@5=%.3f HitRate=%.3f no_answer_FPR=%.3f scope_violations=%d time_violations=%d latency_p50_ms=%.1f latency_p95_ms=%.1f\n",
			strategy.name, metrics.RecallAt1, metrics.RecallAt3, metrics.RecallAt5, metrics.MRR, metrics.NDCGAt5, metrics.HitRate,
			noAnswerFPR, metrics.ScopeViolations, metrics.TimeViolations, float64(latencyP50.Microseconds())/1000, float64(latencyP95.Microseconds())/1000)
		if metrics.ScopeViolations != 0 || metrics.TimeViolations != 0 {
			t.Fatalf("%s returned records outside the requested scope/time window", strategy.name)
		}
		chatHistoryAssertAcceptance(t, strategy.name, metrics, noAnswerFPR)
	}
}

func chatHistoryLatencyPercentile(latencies []time.Duration, percentile float64) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	index := int(math.Ceil(float64(len(sorted))*percentile)) - 1
	if index < 0 {
		index = 0
	}
	return sorted[index]
}

func chatHistoryAssertAcceptance(t *testing.T, strategy string, metrics chatHistoryMetrics, noAnswerFPR float64) {
	t.Helper()
	if strategy != "semantic_first_fallback" {
		return
	}
	if metrics.RecallAt5 < 0.75 {
		t.Errorf("%s failed Recall@5 gate: got %.3f, want >= 0.750", strategy, metrics.RecallAt5)
	}
	if metrics.MRR < 0.75 {
		t.Errorf("%s failed MRR gate: got %.3f, want >= 0.750", strategy, metrics.MRR)
	}
	if noAnswerFPR != 0 {
		t.Errorf("%s failed no-answer gate: got %.3f, want 0", strategy, noAnswerFPR)
	}
}

func loadChatHistoryDataset(t *testing.T, requestedPath string) chatHistoryDataset {
	t.Helper()
	paths := []string{requestedPath}
	if requestedPath == "" {
		paths = []string{
			filepath.Join("testdata", "chat_history_retrieval_review_v1.json"),
			filepath.Join("apps", "agent", "eval", "testdata", "chat_history_retrieval_review_v1.json"),
		}
	} else if !filepath.IsAbs(requestedPath) {
		paths = append(paths, filepath.Join("..", "..", "..", requestedPath))
	}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var dataset chatHistoryDataset
		if err := json.Unmarshal(data, &dataset); err != nil {
			t.Fatalf("parse dataset: %v", err)
		}
		return dataset
	}
	t.Fatalf("read chat-history dataset %q", requestedPath)
	return chatHistoryDataset{}
}

func chatHistoryConversationMap(conversations []chatHistoryConversation) map[string]chatHistoryConversation {
	result := make(map[string]chatHistoryConversation, len(conversations))
	for _, conversation := range conversations {
		result[conversation.ID] = conversation
	}
	return result
}

func chatHistoryMessageMap(messages []chatHistoryMessage) map[int64]chatHistoryMessage {
	result := make(map[int64]chatHistoryMessage, len(messages))
	for _, message := range messages {
		result[message.MsgID] = message
	}
	return result
}

func validateChatHistoryDataset(t *testing.T, dataset chatHistoryDataset, conversations map[string]chatHistoryConversation, messages map[int64]chatHistoryMessage) {
	t.Helper()
	for _, message := range dataset.Corpus {
		if _, ok := conversations[message.ConversationID]; !ok {
			t.Fatalf("message %d references unknown conversation %q", message.MsgID, message.ConversationID)
		}
		if _, err := time.Parse(time.RFC3339, message.SendTime); err != nil {
			t.Fatalf("message %d has invalid send_time: %v", message.MsgID, err)
		}
	}
	for _, query := range dataset.Queries {
		conversation := chatHistoryConversationForQuery(t, dataset.Conversations, query)
		for _, msgID := range query.ExpectedMsgIDs {
			message, ok := messages[msgID]
			if !ok || message.ConversationID != conversation.ID {
				t.Fatalf("query %s has invalid expected message %d", query.ID, msgID)
			}
		}
	}
}

func chatHistoryConversationForQuery(t *testing.T, conversations []chatHistoryConversation, query chatHistoryQuery) chatHistoryConversation {
	t.Helper()
	for _, conversation := range conversations {
		if query.ToolRequest.TargetUser != "" && conversation.TargetUser == query.ToolRequest.TargetUser {
			return conversation
		}
		if query.ToolRequest.TargetGroup != "" && conversation.TargetGroup == query.ToolRequest.TargetGroup {
			return conversation
		}
	}
	t.Fatalf("query %s has no matching conversation", query.ID)
	return chatHistoryConversation{}
}

func seedChatHistoryMySQL(dataset chatHistoryDataset, conversations map[string]chatHistoryConversation) error {
	messages := make([]models.Messages, 0, len(dataset.Corpus))
	for _, item := range dataset.Corpus {
		conversation := conversations[item.ConversationID]
		createdAt, err := time.Parse(time.RFC3339, item.SendTime)
		if err != nil {
			return err
		}
		senderID := chatHistorySenderID(item.Sender)
		receiverID, groupID := chatHistoryReceiverAndGroup(dataset.CurrentUser.UserID, senderID, conversation)
		messages = append(messages, models.Messages{
			MsgId: item.MsgID, SenderId: senderID, ReceiverId: receiverID, GroupId: groupID,
			Content: item.Content, CreateTime: createdAt,
		})
	}
	return db.GetDB().Create(&messages).Error
}

func seedChatHistoryMilvus(ctx context.Context, dataset chatHistoryDataset, conversations map[string]chatHistoryConversation) error {
	embedder, err := vdb.GetEmbedder(config.GlobalConfig.Milvus)
	if err != nil {
		return err
	}
	indexer, err := vdb.GetIndxer(ctx, vdb.MessageCollection, embedder, vdb.MessageDocumentConverter)
	if err != nil {
		return err
	}
	for start := 0; start < len(dataset.Corpus); start += 10 {
		end := start + 10
		if end > len(dataset.Corpus) {
			end = len(dataset.Corpus)
		}
		docs := make([]*schema.Document, 0, end-start)
		for _, item := range dataset.Corpus[start:end] {
			conversation := conversations[item.ConversationID]
			createdAt, err := time.Parse(time.RFC3339, item.SendTime)
			if err != nil {
				return err
			}
			docs = append(docs, &schema.Document{
				ID: itemID(item.MsgID), Content: item.Content,
				MetaData: map[string]any{
					"msg_id": itemID(item.MsgID), "conv_id": chatHistoryConvID(dataset.CurrentUser.UserID, conversation),
					"sender_id": chatHistorySenderID(item.Sender), "send_time": createdAt.Unix(),
				},
			})
		}
		if _, err := indexer.Store(ctx, docs); err != nil {
			return err
		}
	}
	return nil
}

func chatHistoryFulltextSearch(t *testing.T, currentUserID int64, query chatHistoryQuery, conversation chatHistoryConversation) []int64 {
	t.Helper()
	start, end := chatHistoryTimeBounds(t, query.ToolRequest)
	peerID, groupID := int64(0), int64(0)
	if conversation.Type == "group" {
		groupID = conversation.TargetGroupID
	} else {
		peerID = conversation.TargetUserID
	}
	messages, err := dao.SearchHistoryMessages(currentUserID, peerID, groupID, strings.Fields(query.ToolRequest.Keywords), start, end, chatHistoryEvalTopK)
	if err != nil {
		t.Fatalf("fulltext %s: %v", query.ID, err)
	}
	return chatHistoryModelMessageIDs(messages)
}

func chatHistorySemanticSearch(t *testing.T, ctx context.Context, currentUserID int64, query chatHistoryQuery, conversation chatHistoryConversation) []int64 {
	t.Helper()
	embedder, err := vdb.GetEmbedder(config.GlobalConfig.Milvus)
	if err != nil {
		t.Fatalf("embedder %s: %v", query.ID, err)
	}
	filter := fmt.Sprintf("conv_id == \"%s\"", chatHistoryConvID(currentUserID, conversation))
	start, end := chatHistoryTimeBounds(t, query.ToolRequest)
	if query.ToolRequest.StartTime != "" {
		filter += fmt.Sprintf(" && send_time >= %d", start.Unix())
	}
	if query.ToolRequest.EndTime != "" {
		filter += fmt.Sprintf(" && send_time <= %d", end.Unix())
	}
	retriever, err := vdb.GetRetriever(ctx, vdb.MessageCollection, "embedding", []string{"msg_id", "sender_id", "send_time"}, chatHistoryEvalTopK, embedder)
	if err != nil {
		t.Fatalf("retriever %s: %v", query.ID, err)
	}
	docs, err := retriever.Retrieve(ctx, query.ToolRequest.Keywords, milvusret.WithFilter(filter))
	if err != nil {
		t.Fatalf("semantic %s: %v", query.ID, err)
	}
	if query.LabelType == "no_result" {
		scores := make([]float64, 0, len(docs))
		for _, doc := range docs {
			scores = append(scores, doc.Score())
		}
		t.Logf("semantic raw scores %s: %v", query.ID, scores)
	}
	docs = retrieval.FilterDocumentsByScore(docs, chatHistoryEvaluationMinScore(t))
	if len(docs) == 0 {
		return nil
	}
	ids := make([]int64, 0, len(docs))
	for _, doc := range docs {
		if id, err := strconv.ParseInt(doc.ID, 10, 64); err == nil {
			ids = append(ids, id)
		}
	}
	messages, err := dao.GetMessagesByIDs(ids)
	if err != nil {
		t.Fatalf("semantic source lookup %s: %v", query.ID, err)
	}
	contents := make([]string, 0, len(messages))
	for _, message := range messages {
		contents = append(contents, message.Content)
	}
	if !retrieval.HasContentKeywordCoverage(contents, strings.Fields(query.ToolRequest.Keywords)) {
		return nil
	}
	return chatHistoryModelMessageIDs(retrieval.OrderMessagesByIDs(messages, ids))
}

// chatHistoryEvaluationMinScore permits an offline threshold sweep without
// changing the production threshold. An unset value reproduces production.
func chatHistoryEvaluationMinScore(t *testing.T) float64 {
	t.Helper()
	value := os.Getenv("NEXUSGO_CHAT_HISTORY_EVAL_MIN_SCORE")
	if value == "" {
		return retrieval.MinMessageSemanticScore
	}
	threshold, err := strconv.ParseFloat(value, 64)
	if err != nil || threshold < 0 || threshold > 1 {
		t.Fatalf("invalid NEXUSGO_CHAT_HISTORY_EVAL_MIN_SCORE %q", value)
	}
	return threshold
}

func chatHistoryRecordFilterViolations(metrics *chatHistoryMetrics, returned []int64, query chatHistoryQuery, conversation chatHistoryConversation, messages map[int64]chatHistoryMessage) {
	for _, id := range returned {
		message, ok := messages[id]
		if !ok || message.ConversationID != conversation.ID {
			metrics.ScopeViolations++
			continue
		}
		messageTime, err := time.Parse(time.RFC3339, message.SendTime)
		if err != nil {
			metrics.TimeViolations++
			continue
		}
		if query.ToolRequest.StartTime != "" {
			start, _ := time.Parse(time.RFC3339, query.ToolRequest.StartTime)
			if messageTime.Before(start) {
				metrics.TimeViolations++
			}
		}
		if query.ToolRequest.EndTime != "" {
			end, _ := time.Parse(time.RFC3339, query.ToolRequest.EndTime)
			if messageTime.After(end) {
				metrics.TimeViolations++
			}
		}
	}
}

func chatHistoryTimeBounds(t *testing.T, request chatHistoryToolRequest) (time.Time, time.Time) {
	t.Helper()
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	var err error
	if request.StartTime != "" {
		start, err = time.Parse(time.RFC3339, request.StartTime)
		if err != nil {
			t.Fatalf("invalid start_time %q: %v", request.StartTime, err)
		}
	}
	if request.EndTime != "" {
		end, err = time.Parse(time.RFC3339, request.EndTime)
		if err != nil {
			t.Fatalf("invalid end_time %q: %v", request.EndTime, err)
		}
	}
	return start, end
}

func chatHistorySenderID(sender string) int64 {
	ids := map[string]int64{
		"我": 991001, "林夏": 991002, "陈默": 991003, "赵宁": 991004, "方遥": 991005,
		"小周": 991011, "阿敏": 991012, "阿杰": 991013,
		"小叶": 991021, "老杜": 991022, "小吴": 991023, "小贺": 991024, "阿乔": 991025,
	}
	return ids[sender]
}

func chatHistoryReceiverAndGroup(currentUserID, senderID int64, conversation chatHistoryConversation) (int64, int64) {
	if conversation.Type == "group" {
		return currentUserID, conversation.TargetGroupID
	}
	if senderID == currentUserID {
		return conversation.TargetUserID, 0
	}
	return currentUserID, 0
}

func chatHistoryConvID(currentUserID int64, conversation chatHistoryConversation) string {
	if conversation.Type == "group" {
		return utils.GroupConvID(conversation.TargetGroupID)
	}
	return utils.SingleChatConvID(currentUserID, conversation.TargetUserID)
}

func chatHistoryMessageIDs(messages []chatHistoryMessage) []int64 {
	ids := make([]int64, 0, len(messages))
	for _, message := range messages {
		ids = append(ids, message.MsgID)
	}
	return ids
}

func chatHistoryModelMessageIDs(messages []models.Messages) []int64 {
	ids := make([]int64, 0, len(messages))
	for _, message := range messages {
		ids = append(ids, message.MsgId)
	}
	return ids
}

func cleanupChatHistoryCorpus(t *testing.T, ctx context.Context, ids []int64) {
	t.Helper()
	if db.GetDB() != nil {
		if err := db.GetDB().Where("msg_id IN ?", ids).Delete(&models.Messages{}).Error; err != nil {
			t.Errorf("cleanup MySQL seed corpus: %v", err)
		}
	}
	stringIDs := make([]string, 0, len(ids))
	for _, id := range ids {
		stringIDs = append(stringIDs, itemID(id))
	}
	if _, err := vdb.GetMilvus().Delete(ctx, milvusclient.NewDeleteOption(vdb.MessageCollection).WithStringIDs("msg_id", stringIDs)); err != nil {
		t.Errorf("cleanup Milvus seed corpus: %v", err)
	}
}

func chatHistoryFinalizeMetrics(metrics *chatHistoryMetrics, positiveCount int) {
	if positiveCount == 0 {
		return
	}
	count := float64(positiveCount)
	metrics.RecallAt1 /= count
	metrics.RecallAt3 /= count
	metrics.RecallAt5 /= count
	metrics.MRR /= count
	metrics.NDCGAt5 /= count
	metrics.HitRate /= count
}

func chatHistoryRate(numerator, denominator int) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func itemID(id int64) string {
	return strconv.FormatInt(id, 10)
}
