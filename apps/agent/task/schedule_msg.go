package task

import (
	"NexusGo/apps/agent/dao"
	"NexusGo/apps/agent/models"
	"NexusGo/apps/pkg/logger"
	"fmt"
	"github.com/robfig/cron/v3"
)

const batch = 100

func StartCronJobs() *cron.Cron {
	c := cron.New(cron.WithSeconds())
	// 每分钟的第 0 秒执行一次
	_, err := c.AddFunc("0 * * * * *", handleScheduledMessages)
	if err != nil {
		logger.Log.Fatalf("启动定时任务失败: %v", err)
	}
	c.Start()
	return c
}

func handleScheduledMessages() {
	tasks, err := dao.GetPendingScheduledTasks(batch)
	if err != nil || len(tasks) == 0 {
		return
	}

	for _, task := range tasks {
		// 抢占锁：只有把 status 从 0 更新为 1 才算抢到
		rowsAffected, err := dao.ClaimTask(task.SchMsgId)
		if err != nil {
			return
		}
		if rowsAffected == 0 {
			continue // 已经被其他机器抢走了，跳过
		}

		// 3. 抢占成功，开协程异步发送
		go SendSchMessages(&task)
	}
}

func SendSchMessages(task *models.ScheduledMessages) {
	var err error
	var feedback string

	if task.GroupId != 0 {
		err = sendGroupSchMessage(task)
	} else {
		err = sendSingleSchMessage(task)
	}

	if err != nil {
		dao.FailedTask(task.SchMsgId, err.Error())
		target := fmt.Sprintf("用户 [%d]", task.ReceiverId)
		if task.GroupId != 0 {
			target = fmt.Sprintf("群 [%d]", task.GroupId)
		}
		feedback = fmt.Sprintf("❌ 抱歉，您预定发给 %s 的消息发送失败。原因：%v", target, err)
	} else {
		feedback = fmt.Sprintf("✅ 任务完成！您预定的消息已成功为您发出：\n『%s』", task.Content)
	}

	feedbackMsg := models.NewMessages(-1, task.CreatorId, 0, feedback, false)
	_ = dao.InsertMessage(feedbackMsg)

	logger.Log.Infof("[Task] 定时消息已发送: taskID=%d creator=%d receiver=%d group=%d",
		task.SchMsgId, task.CreatorId, task.ReceiverId, task.GroupId)
}

func sendSingleSchMessage(task *models.ScheduledMessages) error {
	msg := models.NewMessages(task.CreatorId, task.ReceiverId, 0, task.Content, false)
	return dao.ExecuteSchSend(msg, task.SchMsgId)
}

func sendGroupSchMessage(task *models.ScheduledMessages) error {
	members, err := dao.GetGroupMembers(task.GroupId)
	if err != nil {
		return fmt.Errorf("查询群成员失败: %w", err)
	}
	if len(members) == 0 {
		return fmt.Errorf("群成员为空")
	}

	for _, memberID := range members {
		if memberID == task.CreatorId {
			continue
		}
		msg := models.NewMessages(task.CreatorId, memberID, task.GroupId, task.Content, false)
		if err := dao.InsertMessage(msg); err != nil {
			return fmt.Errorf("群成员 %d 落库失败: %w", memberID, err)
		}
	}

	return dao.MarkSchTaskDone(task.SchMsgId)
}
