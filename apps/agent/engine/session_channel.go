package engine

import (
	"go-im-system/apps/agent/middleware"
	"sync"
)

var (
	// 用于传递审批结果的channel
	sessionChans = make(map[string]chan *middleware.ApprovalResult)
	// map的互斥锁
	sessionChanMu sync.Mutex
)

// RegisterSessionChan 通过checkpoint将结果channel注册到map中
func RegisterSessionChan(checkpointID string) chan *middleware.ApprovalResult {
	sessionChanMu.Lock()
	defer sessionChanMu.Unlock()
	ch := make(chan *middleware.ApprovalResult, 1)
	sessionChans[checkpointID] = ch
	return ch
}

// PushSessionDecision 将结果放入ch中 删除旧结果 送入新结果
func PushSessionDecision(checkpointID string, result *middleware.ApprovalResult) bool {
	sessionChanMu.Lock()
	ch, ok := sessionChans[checkpointID]
	delete(sessionChans, checkpointID)
	sessionChanMu.Unlock()
	if !ok {
		return false
	}
	ch <- result
	return true
}
