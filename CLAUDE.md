# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

NexusGo is a distributed real-time IM system built in Go with three microservices (Gateway, Logic, Agent) connected via RabbitMQ and a custom RPC framework. It supports single/group chat, AI streaming via SSE, offline message sync, and vector-based semantic search.

## Build & run commands

```bash
# Build all three services
go build ./apps/gateway
go build ./apps/logic
go build ./apps/agent

# Run locally (requires MySQL, Redis, RabbitMQ, Milvus infrastructure)
go run ./apps/logic/main.go      # start Logic first (RPC server + MQ consumer)
go run ./apps/agent/main.go      # then Agent (SSE endpoint)
go run ./apps/gateway/main.go    # then Gateway (HTTP/WS entry)

# Docker (full stack: 3 services + 6 infrastructure containers)
docker-compose up -d --build

# Run all tests
go test ./...

# Run tests for a specific package
go test ./apps/agent/middleware/
go test ./apps/agent/tools/
go test ./apps/pkg/GrowRPC/

# Run a single test
go test ./apps/agent/tools/ -run TestSchMessageReq_GroupFieldPresent -v

# Run k6 performance tests
k6 run perf/k6/ws_online_chat.js -e VUS=200 -e DURATION=30s
```

`go.work` declares a multi-module workspace: the root module `NexusGo` and `./apps/pkg/GrowRPC`.

`apps/config.yaml` is **gitignored** (contains secrets). The Docker Compose services pass configuration via environment variables.

## Architecture: message flow (advanced)

The README covers the basic flow. Here are the details you need to be productive:

### MQ routing (2 independent exchanges)

```
Gateway ──PublishUpload("upload.all")──▶ message.upload (exchange)
                                           └── logic.upload.queue (shared, all Logic instances compete)

Logic ──PublishDown(gatewayAddr)───────▶ gateway.exchange
                                           └── gateway.queue.<addr> (exclusive per Gateway instance)
```

- `PublishUpload` routing key is always `"upload.all"` — single shared queue.
- `PublishDown` routing key is the **target gateway address** — 1:1 routing to the gateway instance where the recipient is connected.
- Downstream consumption runs **50 goroutines** per gateway instance.

### Batch insert worker (upload_consumer.go)

When Logic receives upload messages, 50 consumer goroutines feed into a **batch pipeline**:
- 4 parallel `batchInsertWorker` goroutines each accumulate messages.
- **Dual trigger**: flush when buffer reaches 50 messages OR 30ms timer fires.
- **Batch Redis lookup**: `MGet` all 50 recipient gateway addresses in one round-trip.
- **Batch MySQL insert**: `CreateInBatches` for the 50 messages.
- **Graceful degradation**: if batch INSERT fails, falls back to per-message single INSERT; treats MySQL 1062 (duplicate key) as idempotent success.

Group chat uses **write diffusion**: each group message is inserted once per member, each with a unique MsgID (via Snowflake).

### Dual ID system (reliability.go)

- **MsgID**: globally unique Snowflake int64 (41-bit timestamp + 10-bit node + 12-bit sequence). Each service gets its own node ID (Gateway=1, Logic=2, Agent=3).
- **SeqID**: per-conversation monotonic counter via Redis `INCR seq:<convID>:<userID>`. Used for client-side ordering/gap detection.
- Retry framework with Redis ZSet-based pending queue and dedup is scaffolded but **commented out**.

### Write diffusion (group messages)

When a group message arrives at Logic:
1. Fetch all group member IDs from DB.
2. For each member, insert one row in `messages` (sender gets the original MsgID; others get new Snowflake IDs).
3. For each member, upsert their `conversations` row and publish to downstream MQ for their gateway.
4. Invalidate group history cache.

## Agent service architecture

### Middleware stack (execution order)

Registered in `engine/runner.go` `buildAgent()`:

1. `patchToolCalls` — Eino built-in, fixes tool call ID consistency.
2. `AutoContinueMiddleware` — detects `finish_reason=length` truncation, injects a synthetic `continue_output` tool call. Max 3 continuation loops.
3. `TrimResultMiddleWare` — crops overly long historical tool results before they reach the LLM context window. Targets `search_chat_history` results specifically.
4. `RateLimitMiddleware` — Redis ZSET sliding window, 5 requests/min per user. Key: `rate_limit:ai:<userID>`.
5. `ApprovalMiddleware` — intercepts destructive tools (currently only `schedule_message`) via `StatefulInterrupt`. Waits for human approve/reject before executing. Uses `gob.Register` for serialization across checkpoint state.
6. `SafeAgentMiddleware` — global error catch-all for both model and tool calls, wraps errors as `[model error] ...` / `[tool error] ...` messages.
7. `ToolFixMiddleware` — a `compose.ToolMiddleware` at the tool-node level; attempts to repair malformed JSON from the LLM before passing to the actual tool handler (multi-strategy: extract valid object → strip artifacts → `jsonrepair` library → hard fallback).

### Three-layer memory system

- **Short-term** (Redis): `memory:<sessionID>` list with 24h TTL. Size 4 window; when full, triggers background compression via Lua scripts (atomic RPUSH + EXPIRE, atomic LTRIM + LPUSH).
- **Medium-term** (MySQL): `agent_summaries` table. Background goroutine sends oldest 2 messages to LLM for structured summary (JSON: summary, type, tags, importance 0-5). Injected as system message before short-term history.
- **Long-term** (Milvus): summaries are embedded (DashScope text-embedding-v3, 1024-dim) and stored in `memory_vectors` collection. At query time, `RecallMemory` does semantic search filtered by `user_id` with `minScore=0.5` threshold.

All three layers are assembled in `engine.PrepareAgentContext()` → `[Long-term] + [Medium-term] + [Short-term messages]`.

### Tool security classification (metadata.go)

Tools are labeled at registration time:
- `im:readOnly` — `search_chat_history`, `summarize_group`
- `im:destructive` — `schedule_message`

The `ApprovalMiddleware` gates destructive tools by `StatefulInterrupt`; the frontend receives an `interrupt` SSE event and must POST to `/agent/chat/approve`.

### SSE checkpoint / approval flow

```
Client → GET /agent/chat/sse → handler/chat_sse.go
  → engine.PrepareAgentContext → runner.Run(ctx, history)
  → [if interrupt] → push SSE "interrupt" event with checkpoint_id
    → RegisterSessionChan(checkpointID) → block on channel
    → POST /agent/chat/approve → PushSessionDecision(checkpointID, result)
    → runner.ResumeWithParams(tool.WithResumeContext(...))
  → stream chunks as SSE "stream_chunk" / "reasoning_chunk" events
  → done → async: append to Redis session + insert AI reply into MySQL
```

Checkpoint store is **in-memory** (does not survive process restart). Approval channel is a global `map[string]chan *ApprovalResult` with buffered (1) channels.

### Eino integration

Uses `adk.ChatModelAgent` (not raw `compose.NewGraph`). The `graph/` directory is empty — the ReAct loop is abstracted by the ADK. Configuration:
- `MaxIterations: 5`
- `ModelRetryConfig`: retry on 429 / "Too Many Requests" / "qpm limit" (max 5 retries)
- MCP tools loaded optionally from external MCP servers; graceful degradation if unavailable
- Runner cached as global singleton; cleared on config reload (`ClearCachedRunner`)

### Trace/telemetry callbacks

`TraceLoggerCallback` tracks TTFT (time-to-first-token), total stream time, token usage (input/output/total), and tool execution latency. It spawns a side goroutine to read a copy of the stream without blocking the main event loop.

## RPC framework (GrowRPC)

Located at `apps/pkg/GrowRPC/`, a **separate Go module** linked via `go.work`. This is a custom Layer-7 RPC framework:

- **Generic service registration**: `RegisterMethod[Req, Resp any]` uses Go generics to capture type parameters at registration time — zero reflection at call time.
- **Wire protocol**: JSON option header (magic number + codec type) + 4-byte big-endian length prefix + protobuf binary body.
- **Service discovery**: etcd with lease + keepalive (`/services/{serviceName}/{addr}`), or static `MultiServersDiscovery` for local dev.
- **Load balancing**: random, round-robin, or **consistent hash** (CRC32 with configurable replicas). Gateway uses consistent hash with `routingKey=<userID>` to pin user requests to the same Logic instance.
- **Connection pool**: LIFO hot-stack, max idle/active limits, idle/lifetime expiry with background cleaner.

Gateway initializes 4 service clients via GrowRPC: `UserServiceClient`, `MsgServiceClient`, `FriendServiceClient`, `GroupServiceClient`.

## Key patterns

### Static singleton initialization

All infrastructure clients (Redis, MySQL, Milvus, logger, Snowflake) are `sync.Once` singletons accessed via `Get*()` functions. Config uses `atomic.Pointer` for lock-free hot reload.

### Config hot reload

`pkg/config/hotreload.go`: Viper watches `apps/config.yaml` changes, calls registered `OnReload` callbacks. Agent clears cached runner + summary chat model on reload.

### Offline message handling

When a recipient is offline (not in `GlobalCliMap`), the downstream MQ consumer **NACKs without requeue** — the message is already in MySQL. On reconnect, `ws/handler.go` calls `SyncUnread` RPC to pull all undelivered messages.

### Conversation ID convention

- Single chat: `utils.SingleChatConvID(a, b)` → `"<smallerID>_<largerID>"` (order-independent)
- Group chat: `"group_<groupID>"`

### Dual transport delivery notification

After WS push succeeds, `NotifyDeliveredRPC` updates `send_status` from 0→1 in MySQL. `AckMessage` updates 0→1→2 (confirmed).

### Test patterns

- Agent middleware tests use a `mockChatModel` implementing `model.ToolCallingChatModel` with a `generate` closure.
- Tool tests are pure unit tests (no external dependencies) with `init()` bypassing Snowflake init.
- GrowRPC has its own test suite (`client_test.go`, `service_test.go`, `pool/pool_test.go`, benchmark).
- Agent `eval/` is an offline RAG evaluation framework that computes Recall@K, MRR, NDCG.
