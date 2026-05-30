package event

type EventType string

const (
	EventStreamChunk    EventType = "stream_chunk"
	EventReasoningChunk EventType = "reasoning_chunk"
	EventToolCall       EventType = "tool_call"
	EventToolResult     EventType = "tool_result"
	EventInterrupt      EventType = "interrupt"
	EventError          EventType = "error"
	EventDone           EventType = "done"
)
