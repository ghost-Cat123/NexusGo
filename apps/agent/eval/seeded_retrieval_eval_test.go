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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	milvusret "github.com/cloudwego/eino-ext/components/retriever/milvus2"
	"github.com/cloudwego/eino/schema"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

const (
	seededEvalUserID = int64(991001)
	seededEvalPeerID = int64(991002)
	seededEvalTopK   = 5
)

type seededCorpusMessage struct {
	MsgID   int64  `json:"msg_id"`
	Content string `json:"content"`
}

type seededQuery struct {
	ID             string  `json:"id"`
	Category       string  `json:"category"`
	Query          string  `json:"query"`
	Keywords       string  `json:"keywords"`
	ExpectedMsgIDs []int64 `json:"expected_msg_ids"`
}

type seededDataset struct {
	Corpus  []seededCorpusMessage `json:"corpus"`
	Queries []seededQuery         `json:"queries"`
}

type seededStrategyMetrics struct {
	RecallAt1 float64
	RecallAt3 float64
	RecallAt5 float64
	MRR       float64
	NDCGAt5   float64
	HitRate   float64
}

// TestSeededRetrievalEvaluation is opt-in because it calls the configured
// embedding provider. It uses a separate conversation and cleans up both MySQL
// rows and Milvus vectors after the report is generated.
func TestSeededRetrievalEvaluation(t *testing.T) {
	if os.Getenv("NEXUSGO_RUN_LIVE_EVAL") != "1" {
		t.Skip("set NEXUSGO_RUN_LIVE_EVAL=1 to run the live embedding evaluation")
	}
	if db.GetDB() == nil {
		t.Fatal("MySQL is not initialized")
	}
	if err := db.EnsureMessagesFullTextIndex(); err != nil {
		t.Fatalf("ensure FULLTEXT index: %v", err)
	}

	dataset := loadSeededDataset(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	ids := make([]int64, 0, len(dataset.Corpus))
	for _, message := range dataset.Corpus {
		ids = append(ids, message.MsgID)
	}
	cleanupSeededCorpus(t, ctx, ids)
	t.Cleanup(func() { cleanupSeededCorpus(t, context.Background(), ids) })

	seededAt := time.Now().Add(-time.Minute).Truncate(time.Second)
	if err := seedMySQLCorpus(dataset.Corpus, seededAt); err != nil {
		t.Fatalf("seed MySQL corpus: %v", err)
	}
	if err := seedMilvusCorpus(ctx, dataset.Corpus, seededAt); err != nil {
		t.Fatalf("seed Milvus corpus: %v", err)
	}
	flushTask, err := vdb.GetMilvus().Flush(ctx, milvusclient.NewFlushOption(vdb.MessageCollection))
	if err != nil {
		t.Fatalf("flush seeded Milvus corpus: %v", err)
	}
	if err := flushTask.Await(ctx); err != nil {
		t.Fatalf("await seeded Milvus flush: %v", err)
	}
	assertSeededMilvusVisible(t, ctx, len(dataset.Corpus))

	serialRows := make([]StrategyResult, 0, len(dataset.Queries))
	semanticRows := make([]StrategyResult, 0, len(dataset.Queries))
	semanticFirstRows := make([]StrategyResult, 0, len(dataset.Queries))
	rrfRows := make([]StrategyResult, 0, len(dataset.Queries))
	for _, query := range dataset.Queries {
		fulltextIDs := seededFulltextSearch(t, query)
		milvusIDs := seededMilvusSearch(t, ctx, query)
		serialIDs := fulltextIDs
		if len(serialIDs) == 0 {
			serialIDs = milvusIDs
		}
		semanticFirstIDs := milvusIDs
		if len(semanticFirstIDs) == 0 {
			semanticFirstIDs = fulltextIDs
		}
		rrfIDs := fusedIDs(fulltextIDs, milvusIDs)

		serialRows = append(serialRows, scoredSeededResult("serial_fallback", query, serialIDs))
		semanticRows = append(semanticRows, scoredSeededResult("semantic_only", query, milvusIDs))
		semanticFirstRows = append(semanticFirstRows, scoredSeededResult("semantic_first_fallback", query, semanticFirstIDs))
		rrfRows = append(rrfRows, scoredSeededResult("rrf", query, rrfIDs))
		t.Logf("%s %-14s ft=%v mv=%v serial=%v semantic_first=%v rrf=%v", query.ID, query.Category, fulltextIDs, milvusIDs, serialIDs, semanticFirstIDs, rrfIDs)
	}

	serial := aggregateSeededMetrics(serialRows)
	semantic := aggregateSeededMetrics(semanticRows)
	semanticFirst := aggregateSeededMetrics(semanticFirstRows)
	rrf := aggregateSeededMetrics(rrfRows)
	fmt.Printf("\nSeeded retrieval evaluation (%d labeled queries)\n", len(dataset.Queries))
	fmt.Printf("strategy          R@1   R@3   R@5   MRR   NDCG@5 HitRate\n")
	fmt.Printf("serial_fallback   %.2f  %.2f  %.2f  %.2f  %.2f   %.2f\n", serial.RecallAt1, serial.RecallAt3, serial.RecallAt5, serial.MRR, serial.NDCGAt5, serial.HitRate)
	fmt.Printf("semantic_only     %.2f  %.2f  %.2f  %.2f  %.2f   %.2f\n", semantic.RecallAt1, semantic.RecallAt3, semantic.RecallAt5, semantic.MRR, semantic.NDCGAt5, semantic.HitRate)
	fmt.Printf("semantic_first    %.2f  %.2f  %.2f  %.2f  %.2f   %.2f\n", semanticFirst.RecallAt1, semanticFirst.RecallAt3, semanticFirst.RecallAt5, semanticFirst.MRR, semanticFirst.NDCGAt5, semanticFirst.HitRate)
	fmt.Printf("rrf               %.2f  %.2f  %.2f  %.2f  %.2f   %.2f\n", rrf.RecallAt1, rrf.RecallAt3, rrf.RecallAt5, rrf.MRR, rrf.NDCGAt5, rrf.HitRate)

	if semantic.HitRate < 0.80 {
		t.Fatalf("acceptance failed: semantic-only HitRate %.2f is below 0.80", semantic.HitRate)
	}
	if semanticFirst.HitRate <= serial.HitRate {
		t.Fatalf("acceptance failed: semantic-first HitRate %.2f must exceed serial fallback %.2f", semanticFirst.HitRate, serial.HitRate)
	}
	if rrf.RecallAt5 < serial.RecallAt5 {
		t.Fatalf("acceptance failed: RRF R@5 %.2f is below serial fallback %.2f", rrf.RecallAt5, serial.RecallAt5)
	}
	if rrf.NDCGAt5 < serial.NDCGAt5 {
		t.Fatalf("acceptance failed: RRF NDCG@5 %.2f is below serial fallback %.2f", rrf.NDCGAt5, serial.NDCGAt5)
	}
}

func loadSeededDataset(t *testing.T) seededDataset {
	t.Helper()
	paths := []string{
		filepath.Join("testdata", "seeded_retrieval_cases.json"),
		filepath.Join("apps", "agent", "eval", "testdata", "seeded_retrieval_cases.json"),
	}
	var data []byte
	var err error
	for _, path := range paths {
		data, err = os.ReadFile(path)
		if err == nil {
			break
		}
	}
	if err != nil {
		t.Fatalf("read seeded dataset: %v", err)
	}
	var dataset seededDataset
	if err := json.Unmarshal(data, &dataset); err != nil {
		t.Fatalf("parse seeded dataset: %v", err)
	}
	if len(dataset.Corpus) == 0 || len(dataset.Queries) == 0 {
		t.Fatal("seeded dataset must contain corpus and labeled queries")
	}
	return dataset
}

func seedMySQLCorpus(corpus []seededCorpusMessage, createdAt time.Time) error {
	messages := make([]models.Messages, 0, len(corpus))
	for _, item := range corpus {
		messages = append(messages, models.Messages{
			MsgId:      item.MsgID,
			SenderId:   seededEvalPeerID,
			ReceiverId: seededEvalUserID,
			Content:    item.Content,
			CreateTime: createdAt,
		})
	}
	return db.GetDB().Create(&messages).Error
}

func seedMilvusCorpus(ctx context.Context, corpus []seededCorpusMessage, createdAt time.Time) error {
	embedder, err := vdb.GetEmbedder(config.GlobalConfig.Milvus)
	if err != nil {
		return err
	}
	indexer, err := vdb.GetIndxer(ctx, vdb.MessageCollection, embedder, vdb.MessageDocumentConverter)
	if err != nil {
		return err
	}
	convID := seededConversationID()
	for start := 0; start < len(corpus); start += 10 {
		end := start + 10
		if end > len(corpus) {
			end = len(corpus)
		}
		docs := make([]*schema.Document, 0, end-start)
		for _, item := range corpus[start:end] {
			docs = append(docs, &schema.Document{
				ID:      strconv.FormatInt(item.MsgID, 10),
				Content: item.Content,
				MetaData: map[string]any{
					"msg_id":    strconv.FormatInt(item.MsgID, 10),
					"conv_id":   convID,
					"sender_id": seededEvalPeerID,
					"send_time": createdAt.Unix(),
				},
			})
		}
		if _, err := indexer.Store(ctx, docs); err != nil {
			return err
		}
	}
	return nil
}

func seededFulltextSearch(t *testing.T, query seededQuery) []int64 {
	t.Helper()
	start := time.Unix(0, 0)
	end := time.Now().Add(time.Minute)
	messages, err := dao.SearchHistoryMessages(seededEvalUserID, seededEvalPeerID, 0,
		strings.Fields(query.Keywords), start, end, 20)
	if err != nil {
		t.Fatalf("fulltext %s: %v", query.ID, err)
	}
	return messageIDs(messages)
}

func seededMilvusSearch(t *testing.T, ctx context.Context, query seededQuery) []int64 {
	t.Helper()
	embedder, err := vdb.GetEmbedder(config.GlobalConfig.Milvus)
	if err != nil {
		t.Fatalf("embedder %s: %v", query.ID, err)
	}
	retriever, err := vdb.GetRetriever(ctx, vdb.MessageCollection, "embedding",
		[]string{"msg_id", "sender_id", "send_time"}, 20, embedder)
	if err != nil {
		t.Fatalf("retriever %s: %v", query.ID, err)
	}
	filter := fmt.Sprintf("conv_id == \"%s\"", seededConversationID())
	docs, err := retriever.Retrieve(ctx, query.Query, milvusret.WithFilter(filter))
	if err != nil {
		t.Fatalf("milvus %s: %v", query.ID, err)
	}
	ids := make([]int64, 0, len(docs))
	for _, doc := range docs {
		id, err := strconv.ParseInt(doc.ID, 10, 64)
		if err == nil {
			ids = append(ids, id)
		}
	}
	return ids
}

func assertSeededMilvusVisible(t *testing.T, ctx context.Context, expected int) {
	t.Helper()
	rows, err := vdb.GetMilvus().Query(ctx,
		milvusclient.NewQueryOption(vdb.MessageCollection).
			WithFilter(fmt.Sprintf("conv_id == \"%s\"", seededConversationID())).
			WithOutputFields("msg_id", "conv_id").
			WithLimit(expected).
			WithConsistencyLevel(entity.ClStrong))
	if err != nil {
		t.Fatalf("query seeded Milvus corpus: %v", err)
	}
	if rows.ResultCount != expected {
		t.Fatalf("seeded Milvus corpus visibility: got %d rows, want %d", rows.ResultCount, expected)
	}
}

func seededConversationID() string {
	return utils.SingleChatConvID(seededEvalUserID, seededEvalPeerID)
}

func cleanupSeededCorpus(t *testing.T, ctx context.Context, ids []int64) {
	t.Helper()
	if db.GetDB() != nil {
		if err := db.GetDB().Where("msg_id IN ?", ids).Delete(&models.Messages{}).Error; err != nil {
			t.Errorf("cleanup MySQL seed corpus: %v", err)
		}
	}
	if len(ids) == 0 {
		return
	}
	stringIDs := make([]string, 0, len(ids))
	for _, id := range ids {
		stringIDs = append(stringIDs, strconv.FormatInt(id, 10))
	}
	if _, err := vdb.GetMilvus().Delete(ctx,
		milvusclient.NewDeleteOption(vdb.MessageCollection).WithStringIDs("msg_id", stringIDs)); err != nil {
		t.Errorf("cleanup Milvus seed corpus: %v", err)
	}
}

func fusedIDs(fulltext, milvus []int64) []int64 {
	fused := retrieval.FuseRankedIDs(retrieval.DefaultRRFK,
		retrieval.RankedIDs{Source: "fulltext", IDs: fulltext},
		retrieval.RankedIDs{Source: "milvus", IDs: milvus},
	)
	ids := make([]int64, 0, len(fused))
	for _, result := range fused {
		ids = append(ids, result.ID)
	}
	if len(ids) > seededEvalTopK {
		ids = ids[:seededEvalTopK]
	}
	return ids
}

func scoredSeededResult(strategy string, query seededQuery, returned []int64) StrategyResult {
	mrr := computeMRR(returned, query.ExpectedMsgIDs)
	return StrategyResult{
		QueryID:     query.ID,
		Strategy:    strategy,
		ExpectedIDs: query.ExpectedMsgIDs,
		ReturnedIDs: returned,
		RecallAt1:   computeRecall(returned, query.ExpectedMsgIDs, 1),
		RecallAt3:   computeRecall(returned, query.ExpectedMsgIDs, 3),
		RecallAt5:   computeRecall(returned, query.ExpectedMsgIDs, 5),
		MRR:         mrr,
		Hit:         mrr > 0,
	}
}

func aggregateSeededMetrics(results []StrategyResult) seededStrategyMetrics {
	metrics := seededStrategyMetrics{}
	for _, result := range results {
		metrics.RecallAt1 += result.RecallAt1
		metrics.RecallAt3 += result.RecallAt3
		metrics.RecallAt5 += result.RecallAt5
		metrics.MRR += result.MRR
		metrics.NDCGAt5 += computeNDCG(result.ReturnedIDs, result.ExpectedIDs, seededEvalTopK)
		if result.Hit {
			metrics.HitRate++
		}
	}
	count := float64(len(results))
	metrics.RecallAt1 /= count
	metrics.RecallAt3 /= count
	metrics.RecallAt5 /= count
	metrics.MRR /= count
	metrics.NDCGAt5 /= count
	metrics.HitRate /= count
	return metrics
}

func messageIDs(messages []models.Messages) []int64 {
	ids := make([]int64, 0, len(messages))
	for _, message := range messages {
		ids = append(ids, message.MsgId)
	}
	return ids
}
