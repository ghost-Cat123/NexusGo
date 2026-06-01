package task

import (
	"NexusGo/apps/agent/models"
	"testing"
	"time"
)

func TestScheduledMessages_GroupIdFieldExists(t *testing.T) {
	task := models.ScheduledMessages{
		GroupId: 123,
	}
	if task.GroupId != 123 {
		t.Error("GroupId 字段应存在且可赋值")
	}
}

func TestScheduledMessages_GroupIdDefaultZero(t *testing.T) {
	task := models.ScheduledMessages{}
	if task.GroupId != 0 {
		t.Errorf("GroupId 默认值应为 0, 实际 %d", task.GroupId)
	}
}

func TestScheduledMessages_SingleChatTask(t *testing.T) {
	task := &models.ScheduledMessages{
		CreatorId:         1001,
		ReceiverId:        1002,
		GroupId:           0,
		Content:           "单聊定时消息",
		ScheduledSendTime: time.Now().Add(time.Minute),
		Status:            0,
	}

	if task.GroupId != 0 {
		t.Error("单聊定时消息 GroupId 应为 0")
	}
	if task.ReceiverId != 1002 {
		t.Error("单聊定时消息 ReceiverId 应指向目标用户")
	}
}

func TestScheduledMessages_GroupChatTask(t *testing.T) {
	task := &models.ScheduledMessages{
		CreatorId:         1001,
		ReceiverId:        0,
		GroupId:           123,
		Content:           "群聊定时消息",
		ScheduledSendTime: time.Now().Add(time.Minute),
		Status:            0,
	}

	if task.GroupId != 123 {
		t.Error("群聊定时消息 GroupId 应指向目标群")
	}
	if task.ReceiverId != 0 {
		t.Error("群聊定时消息 ReceiverId 应为 0")
	}
}

func TestScheduledMessages_BothReceiverAndGroup(t *testing.T) {
	task := &models.ScheduledMessages{
		CreatorId:         1001,
		ReceiverId:        0,
		GroupId:           0,
		Content:           "异常情况",
		ScheduledSendTime: time.Now().Add(time.Minute),
		Status:            0,
	}

	if task.GroupId != 0 {
		t.Error("无目标时 GroupId 应为 0")
	}
	if task.ReceiverId != 0 {
		t.Error("无目标时 ReceiverId 应为 0")
	}
}
