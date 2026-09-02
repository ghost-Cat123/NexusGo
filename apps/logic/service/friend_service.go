package service

import (
	"context"
	"encoding/json"
	"strconv"

	"NexusGo/apps/logic/dao"
	"NexusGo/apps/pkg/cache"
	"NexusGo/apps/pkg/mq"
	"NexusGo/apps/pkg/proto/pb_friend"
)

func (s *LogicService) ApplyFriend(ctx context.Context, req *pb_friend.ApplyFriendArgs, resp *pb_friend.ApplyFriendReply) error {
	err := dao.ApplyFriend(req.UserId, req.FriendId, req.ApplyMsg)
	if err != nil {
		return err
	}
	resp.Success = true

	// 推送给接收方：如果对方在线，通过 MQ → WS 实时通知
	redisKey := "route:user:" + strconv.FormatInt(req.FriendId, 10)
	gatewayAddr, rErr := cache.GetCache().Get(context.Background(), redisKey).Result()
	if rErr == nil && gatewayAddr != "" {
		downPayload := &mq.DownPayload{
			SenderID:   req.UserId,
			ReceiverID: req.FriendId,
			Content:    req.ApplyMsg,
			ChatType:   mq.ChatTypeFriendRequest,
		}
		body, _ := json.Marshal(downPayload)
		if body != nil {
			_ = mq.PublishDown(context.Background(), gatewayAddr, body)
		}
	}
	return nil
}

func (s *LogicService) ResolveFriend(ctx context.Context, req *pb_friend.ResolveFriendArgs, resp *pb_friend.ResolveFriendReply) error {
	var status int8 = 2
	if req.Action == "accept" {
		status = 1
	}
	err := dao.ResolveFriend(req.UserId, req.FriendId, status)
	if err != nil {
		return err
	}
	resp.Success = true

	// 通知申请人（req.FriendId）处理结果
	redisKey := "route:user:" + strconv.FormatInt(req.FriendId, 10)
	gatewayAddr, rErr := cache.GetCache().Get(context.Background(), redisKey).Result()
	if rErr == nil && gatewayAddr != "" {
		downPayload := &mq.DownPayload{
			SenderID:   req.UserId,
			ReceiverID: req.FriendId,
			Content:    req.Action,
			ChatType:   mq.ChatTypeFriendResolved,
		}
		body, _ := json.Marshal(downPayload)
		if body != nil {
			_ = mq.PublishDown(context.Background(), gatewayAddr, body)
		}
	}
	return nil
}

func (s *LogicService) GetFriendList(ctx context.Context, req *pb_friend.GetFriendListArgs, resp *pb_friend.GetFriendListReply) error {
	friends, err := dao.GetFriendList(req.UserId)
	if err != nil {
		return err
	}

	// 收集所有对方用户ID，批量查询避免 N+1
	ids := make([]int64, 0, len(friends))
	for _, f := range friends {
		targetID := f.FriendID
		if f.FriendID == req.UserId {
			targetID = f.UserID
		}
		ids = append(ids, targetID)
	}

	userMap, err := dao.GetUsersByIDs(ids)
	if err != nil {
		return err
	}

	for i, f := range friends {
		targetID := f.FriendID
		if f.FriendID == req.UserId {
			targetID = f.UserID
		}
		u := userMap[targetID]
		resp.Friends = append(resp.Friends, &pb_friend.FriendItem{
			FriendId: targetID,
			Nickname: u.Nickname,
			Avatar:   u.Avatar,
			Status:   int32(f.Status),
		})
		_ = i // suppress unused warning if any
	}
	return nil
}

func (s *LogicService) DeleteFriend(ctx context.Context, req *pb_friend.DeleteFriendArgs, resp *pb_friend.DeleteFriendReply) error {
	err := dao.DeleteFriend(req.UserId, req.FriendId)
	if err != nil {
		return err
	}
	// 同时删除双方会话
	dao.DeleteConversation(req.UserId, req.FriendId, 1)
	// 同时删除双方聊天记录
	dao.DeleteMessagesBetween(req.UserId, req.FriendId)

	// 清除双方的聊天记录缓存
	invalidateChatCache(req.UserId, req.FriendId)

	// 通知对方被删除
	redisKey := "route:user:" + strconv.FormatInt(req.FriendId, 10)
	gatewayAddr, rErr := cache.GetCache().Get(context.Background(), redisKey).Result()
	if rErr == nil && gatewayAddr != "" {
		downPayload := &mq.DownPayload{
			SenderID:   req.UserId,
			ReceiverID: req.FriendId,
			ChatType:   mq.ChatTypeFriendDeleted,
		}
		body, _ := json.Marshal(downPayload)
		if body != nil {
			_ = mq.PublishDown(context.Background(), gatewayAddr, body)
		}
	}
	resp.Success = true
	return nil
}

func (s *LogicService) GetPendingRequests(ctx context.Context, req *pb_friend.GetPendingRequestsArgs, resp *pb_friend.GetPendingRequestsReply) error {
	requests, err := dao.GetPendingFriendRequests(req.UserId)
	if err != nil {
		return err
	}
	for _, r := range requests {
		resp.Requests = append(resp.Requests, &pb_friend.PendingRequestItem{
			UserId:    r.UserID,
			Username:  r.Username,
			Nickname:  r.Nickname,
			Avatar:    r.Avatar,
			ApplyMsg:  r.Remark,
			CreatedAt: r.CreatedAt.Format("2006-01-02 15:04:05"),
		})
	}
	return nil
}
