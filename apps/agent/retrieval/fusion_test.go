package retrieval

import (
	"NexusGo/apps/agent/models"
	"testing"
)

func TestFuseMessagesRewardsAgreementAcrossRetrievers(t *testing.T) {
	fulltext := []models.Messages{{MsgId: 1}, {MsgId: 2}, {MsgId: 3}}
	milvus := []models.Messages{{MsgId: 3}, {MsgId: 2}, {MsgId: 4}}

	fused := FuseMessages(fulltext, milvus, 60)
	if len(fused) != 4 {
		t.Fatalf("result count = %d, want 4", len(fused))
	}
	if fused[0].Message.MsgId != 3 {
		t.Fatalf("first result = %d, want shared result 3", fused[0].Message.MsgId)
	}
	if fused[0].SourceLabel() != "fulltext+milvus" {
		t.Fatalf("source = %q", fused[0].SourceLabel())
	}
}

func TestOrderMessagesByIDsPreservesRetrieverOrder(t *testing.T) {
	messages := []models.Messages{{MsgId: 2}, {MsgId: 3}, {MsgId: 1}}
	ordered := OrderMessagesByIDs(messages, []int64{1, 2, 3})
	for index, want := range []int64{1, 2, 3} {
		if ordered[index].MsgId != want {
			t.Fatalf("result[%d] = %d, want %d", index, ordered[index].MsgId, want)
		}
	}
}

func TestFuseRankedIDsRewardsAgreementAcrossLists(t *testing.T) {
	fused := FuseRankedIDs(60,
		RankedIDs{Source: "fulltext", IDs: []int64{1, 2, 3}},
		RankedIDs{Source: "milvus", IDs: []int64{3, 2, 4}},
	)
	if fused[0].ID != 3 {
		t.Fatalf("first result = %d, want 3", fused[0].ID)
	}
}
