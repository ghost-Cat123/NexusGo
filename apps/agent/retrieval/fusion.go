// Package retrieval contains deterministic first-stage ranking helpers shared
// by the Agent tool and MCP server.
package retrieval

import (
	"NexusGo/apps/agent/models"
	"sort"
	"strings"
)

const DefaultRRFK = 60

// FusedMessage is a message ordered by reciprocal-rank-fusion score.
type FusedMessage struct {
	Message models.Messages
	Score   float64
	Sources []string
}

// RankedIDs keeps the ordered result list emitted by one retriever.
type RankedIDs struct {
	Source string
	IDs    []int64
}

// FusedID is the deterministic RRF result used by offline evaluation.
type FusedID struct {
	ID      int64
	Score   float64
	Sources []string
}

type fusionEntry struct {
	message  models.Messages
	score    float64
	sources  map[string]struct{}
	bestRank int
}

// FuseMessages combines independently ranked FULLTEXT and Milvus results.
// RRF only relies on rank positions, so incomparable database and vector scores
// do not need to be normalized before fusion.
func FuseMessages(fulltext, milvus []models.Messages, k int) []FusedMessage {
	if k <= 0 {
		k = DefaultRRFK
	}

	entries := make(map[int64]*fusionEntry, len(fulltext)+len(milvus))
	add := func(messages []models.Messages, source string) {
		seenInList := make(map[int64]struct{}, len(messages))
		for index, message := range messages {
			if _, seen := seenInList[message.MsgId]; seen {
				continue
			}
			seenInList[message.MsgId] = struct{}{}

			rank := index + 1
			entry, ok := entries[message.MsgId]
			if !ok {
				entry = &fusionEntry{
					message:  message,
					sources:  make(map[string]struct{}),
					bestRank: rank,
				}
				entries[message.MsgId] = entry
			}
			entry.score += 1.0 / float64(k+rank)
			entry.sources[source] = struct{}{}
			if rank < entry.bestRank {
				entry.bestRank = rank
			}
		}
	}

	add(fulltext, "fulltext")
	add(milvus, "milvus")

	ordered := make([]*fusionEntry, 0, len(entries))
	for _, entry := range entries {
		ordered = append(ordered, entry)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].score != ordered[j].score {
			return ordered[i].score > ordered[j].score
		}
		if ordered[i].bestRank != ordered[j].bestRank {
			return ordered[i].bestRank < ordered[j].bestRank
		}
		return ordered[i].message.MsgId < ordered[j].message.MsgId
	})

	result := make([]FusedMessage, 0, len(ordered))
	for _, entry := range ordered {
		sources := make([]string, 0, len(entry.sources))
		for source := range entry.sources {
			sources = append(sources, source)
		}
		sort.Strings(sources)
		result = append(result, FusedMessage{
			Message: entry.message,
			Score:   entry.score,
			Sources: sources,
		})
	}
	return result
}

// FuseRankedIDs fuses any number of independently ranked result lists.
func FuseRankedIDs(k int, lists ...RankedIDs) []FusedID {
	if k <= 0 {
		k = DefaultRRFK
	}
	type entry struct {
		score    float64
		sources  map[string]struct{}
		bestRank int
	}
	entries := make(map[int64]*entry)
	for _, list := range lists {
		seenInList := make(map[int64]struct{}, len(list.IDs))
		for index, id := range list.IDs {
			if _, seen := seenInList[id]; seen {
				continue
			}
			seenInList[id] = struct{}{}
			rank := index + 1
			item, ok := entries[id]
			if !ok {
				item = &entry{sources: make(map[string]struct{}), bestRank: rank}
				entries[id] = item
			}
			item.score += 1.0 / float64(k+rank)
			item.sources[list.Source] = struct{}{}
			if rank < item.bestRank {
				item.bestRank = rank
			}
		}
	}

	type sortable struct {
		id int64
		entry
	}
	ordered := make([]sortable, 0, len(entries))
	for id, item := range entries {
		ordered = append(ordered, sortable{id: id, entry: *item})
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].score != ordered[j].score {
			return ordered[i].score > ordered[j].score
		}
		if ordered[i].bestRank != ordered[j].bestRank {
			return ordered[i].bestRank < ordered[j].bestRank
		}
		return ordered[i].id < ordered[j].id
	})

	result := make([]FusedID, 0, len(ordered))
	for _, item := range ordered {
		sources := make([]string, 0, len(item.sources))
		for source := range item.sources {
			sources = append(sources, source)
		}
		sort.Strings(sources)
		result = append(result, FusedID{ID: item.id, Score: item.score, Sources: sources})
	}
	return result
}

// SourceLabel returns a stable provenance label for logs and RAG candidates.
func (m FusedMessage) SourceLabel() string {
	return strings.Join(m.Sources, "+")
}

// OrderMessagesByIDs restores the vector retriever's rank after MySQL source
// lookup, whose default ordering is by create_time rather than retrieval score.
func OrderMessagesByIDs(messages []models.Messages, ids []int64) []models.Messages {
	byID := make(map[int64]models.Messages, len(messages))
	for _, message := range messages {
		byID[message.MsgId] = message
	}

	ordered := make([]models.Messages, 0, len(messages))
	for _, id := range ids {
		if message, ok := byID[id]; ok {
			ordered = append(ordered, message)
			delete(byID, id)
		}
	}
	return ordered
}
