package utils

import "fmt"

// SingleChatConvID 单聊会话 ID，按用户 ID 有序拼接保证唯一性
// 无论 a、b 谁先谁后，同一对用户的 ConvID 始终相同
func SingleChatConvID(a, b int64) string {
	if a > b {
		return fmt.Sprintf("%d_%d", b, a)
	}
	return fmt.Sprintf("%d_%d", a, b)
}

// GroupConvID 群聊会话 ID
func GroupConvID(groupID int64) string {
	return fmt.Sprintf("group_%d", groupID)
}
