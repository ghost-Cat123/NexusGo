package task

import (
	"fmt"
	"github.com/robfig/cron/v3"
	"go-im-system/apps/agent/dao"
	"go-im-system/apps/agent/models"
	"go-im-system/apps/pkg/logger"
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
	// 将定时任务消息变成普通消息送入消息列表，接收者上线自动推送
	schMessage := models.NewMessages(task.CreatorId, task.ReceiverId, task.Content, false)
	err := dao.ExecuteSchSend(schMessage, task.SchMsgId)
	var feedback string

	if err != nil {
		// 发送失败 事务回滚 修改事务状态为3
		dao.FailedTask(task.SchMsgId, err.Error())
		feedback = fmt.Sprintf("❌ 抱歉，您预定发给 [%d] 的消息发送失败。原因：%v", task.ReceiverId, err)
	} else {
		// 发送成功 事务状态为2
		feedback = fmt.Sprintf("✅ 任务完成！您预定的消息已成功为您发出：\n『%s』", task.Content)
	}
	// 通知网关推送AI消息给创建者
	feedbackMsg := models.NewMessages(-1, task.CreatorId, feedback, false)
	_ = dao.InsertMessage(feedbackMsg)

	logger.Log.Infof("[Task] 定时消息已发送: taskID=%d creator=%d receiver=%d",
		task.SchMsgId, task.CreatorId, task.ReceiverId)
}
