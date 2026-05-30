package task

import (
	"go-im-system/apps/agent/models"
	"testing"
	"time"
)

func TestHandleScheduledMessages(t *testing.T) {
	// 这个测试需要数据库中有定时消息数据
	// 实际测试时需要准备测试数据

	// 调用处理函数
	handleScheduledMessages()

	// 这里可以根据实际情况验证处理结果
	t.Log("定时消息处理测试（需要准备测试数据）")
}

func TestSendSchMessages(t *testing.T) {
	// 这个测试需要数据库中有定时消息数据
	// 实际测试时需要准备测试数据

	// 创建一个定时消息任务
	task := &models.ScheduledMessages{
		CreatorId:         1001,
		ReceiverId:        1002,
		Content:           "这是一条定时消息",
		ScheduledSendTime: time.Now().Add(time.Minute),
		Status:            0,
	}

	// 调用发送函数
	SendSchMessages(task)

	// 这里可以根据实际情况验证发送结果
	t.Log("定时消息发送测试（需要准备测试数据）")
}
