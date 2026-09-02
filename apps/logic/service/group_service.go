package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"NexusGo/apps/logic/dao"
	"NexusGo/apps/logic/models"
	"NexusGo/apps/pkg/cache"
	"NexusGo/apps/pkg/mq"
	"NexusGo/apps/pkg/proto/pb_group"
)

func (s *LogicService) CreateGroup(ctx context.Context, req *pb_group.CreateGroupArgs, resp *pb_group.CreateGroupReply) error {
	group := &models.Group{
		GroupName: req.GroupName,
		OwnerID:   req.UserId,
		CreatorID: req.UserId,
	}
	
	members := req.MemberIds
	// 把自己加进群
	found := false
	for _, id := range members {
		if id == req.UserId {
			found = true
			break
		}
	}
	if !found {
		members = append(members, req.UserId)
	}

	err := dao.CreateGroup(group, members)
	if err != nil {
		return err
	}
	resp.GroupId = group.GroupID

	// 为所有成员创建群会话
	for _, memberID := range members {
		dao.UpsertConversation(memberID, group.GroupID, 2, 0, 0)
	}
	// 通过 WS 通知所有群成员
	for _, memberID := range members {
		if memberID == req.UserId {
			continue
		}
		redisKey := "route:user:" + strconv.FormatInt(memberID, 10)
		gatewayAddr, rErr := cache.GetCache().Get(context.Background(), redisKey).Result()
		if rErr == nil && gatewayAddr != "" {
			dp := &mq.DownPayload{
				SenderID:   req.UserId,
				ReceiverID: memberID,
				Content:    req.GroupName,
				ChatType:   mq.ChatTypeGroupCreated,
				GroupID:    group.GroupID,
			}
			body, _ := json.Marshal(dp)
			if body != nil {
				_ = mq.PublishDown(context.Background(), gatewayAddr, body)
			}
		}
	}
	return nil
}

func (s *LogicService) GetGroupList(ctx context.Context, req *pb_group.GetGroupListArgs, resp *pb_group.GetGroupListReply) error {
	members, err := dao.GetUserGroups(req.UserId)
	if err != nil {
		return err
	}
	var groupIDs []int64
	for _, m := range members {
		groupIDs = append(groupIDs, m.GroupID)
	}
	
	groups, err := dao.GetGroupsByIDs(groupIDs)
	if err != nil {
		return err
	}
	
	for _, g := range groups {
		resp.Groups = append(resp.Groups, &pb_group.GroupItem{
			GroupId:   g.GroupID,
			GroupName: g.GroupName,
			OwnerId:   g.OwnerID,
		})
	}
	return nil
}

func (s *LogicService) GetGroupInfo(ctx context.Context, req *pb_group.GetGroupInfoArgs, resp *pb_group.GetGroupInfoReply) error {
	g, err := dao.GetGroupByID(req.GroupId)
	if err != nil {
		return err
	}
	resp.GroupId = g.GroupID
	resp.GroupName = g.GroupName
	resp.OwnerId = g.OwnerID
	return nil
}

func (s *LogicService) GetGroupMembers(ctx context.Context, req *pb_group.GetGroupMembersArgs, resp *pb_group.GetGroupMembersReply) error {
	members, err := dao.GetGroupMemberModels(req.GroupId)
	if err != nil {
		return err
	}

	// 批量查询所有成员的用户信息，避免 N+1
	ids := make([]int64, len(members))
	for i, m := range members {
		ids[i] = m.UserID
	}
	userMap, err := dao.GetUsersByIDs(ids)
	if err != nil {
		return err
	}

	for _, m := range members {
		u := userMap[m.UserID]
		resp.Members = append(resp.Members, &pb_group.GroupMemberItem{
			UserId:   m.UserID,
			Role:     int32(m.Role),
			Nickname: u.Nickname,
		})
	}
	return nil
}

func (s *LogicService) JoinGroup(ctx context.Context, req *pb_group.JoinGroupArgs, resp *pb_group.JoinGroupReply) error {
	err := dao.InsertGroupMember(&models.GroupMember{
		GroupID: req.GroupId,
		UserID:  req.UserId,
	})
	if err != nil {
		return err
	}
	// 创建群会话，让新成员能看到群聊
	dao.UpsertConversation(req.UserId, req.GroupId, 2, 0, 0)
	resp.Success = true
	// 通知群内所有人成员已变更
	broadcastMemberChanged(req.GroupId, req.UserId)
	return nil
}

func (s *LogicService) LeaveGroup(ctx context.Context, req *pb_group.LeaveGroupArgs, resp *pb_group.LeaveGroupReply) error {
	err := dao.DeleteGroupMember(req.GroupId, req.UserId)
	if err != nil {
		return err
	}
	// 删除退群人的会话
	dao.DeleteConversation(req.UserId, req.GroupId, 2)
	resp.Success = true
	// 通知群内剩余成员
	broadcastMemberChanged(req.GroupId, req.UserId)
	return nil
}

func broadcastMemberChanged(groupID, excludeUserID int64) {
	members, err := dao.GetGroupMembers(groupID)
	if err != nil {
		return
	}
	for _, memberID := range members {
		if memberID == excludeUserID {
			continue
		}
		redisKey := "route:user:" + strconv.FormatInt(memberID, 10)
		gatewayAddr, rErr := cache.GetCache().Get(context.Background(), redisKey).Result()
		if rErr == nil && gatewayAddr != "" {
			dp := &mq.DownPayload{
				SenderID:   excludeUserID,
				ReceiverID: memberID,
				Content:    strconv.FormatInt(groupID, 10),
				ChatType:   mq.ChatTypeGroupMemberChanged,
				GroupID:    groupID,
			}
			body, _ := json.Marshal(dp)
			if body != nil {
				_ = mq.PublishDown(context.Background(), gatewayAddr, body)
			}
		}
	}
}

func (s *LogicService) DissolveGroup(ctx context.Context, req *pb_group.DissolveGroupArgs, resp *pb_group.DissolveGroupReply) error {
	members, err := dao.GetGroupMembers(req.GroupId)
	if err != nil {
		return err
	}
	// 通知所有成员群被解散
	for _, memberID := range members {
		if memberID == req.UserId {
			continue
		}
		redisKey := "route:user:" + strconv.FormatInt(memberID, 10)
		gatewayAddr, rErr := cache.GetCache().Get(context.Background(), redisKey).Result()
		if rErr == nil && gatewayAddr != "" {
			dp := &mq.DownPayload{
				SenderID:   req.UserId,
				ReceiverID: memberID,
				Content:    "",
				ChatType:   mq.ChatTypeGroupDissolved,
				GroupID:    req.GroupId,
			}
			body, _ := json.Marshal(dp)
			if body != nil {
				_ = mq.PublishDown(context.Background(), gatewayAddr, body)
			}
		}
	}
	// 删除所有群成员、群消息、群会话、群本身
	dao.DeleteAllGroupMembers(req.GroupId)
	dao.DeleteGroupMessages(req.GroupId)
	dao.DeleteGroupConversations(req.GroupId)
	dao.DeleteGroup(req.GroupId)
	resp.Success = true
	return nil
}

func (s *LogicService) GetGroupHistory(ctx context.Context, req *pb_group.GetGroupHistoryArgs, resp *pb_group.GetGroupHistoryReply) error {
	// 仅对首屏做 Redis 缓存
	if req.Cursor == 0 {
		cacheKey := fmt.Sprintf("chat:history:group:%d:%d:%d", req.GroupId, req.Cursor, req.Limit)
		if cached, err := cache.GetCache().Get(ctx, cacheKey).Result(); err == nil && cached != "" {
			return json.Unmarshal([]byte(cached), resp)
		}
	}

	msgs, err := dao.GetGroupHistory(req.GroupId, req.UserId, req.Cursor, req.Limit)
	if err != nil {
		return err
	}
	for _, msg := range msgs {
		resp.Messages = append(resp.Messages, &pb_group.GroupMsgItem{
			MsgId:      msg.MsgId,
			SenderId:   msg.SenderId,
			Content:    msg.Content,
			CreateTime: msg.CreateTime.Format("2006-01-02 15:04:05"),
		})
	}
	if len(msgs) == int(req.Limit) {
		resp.HasMore = true
		resp.NextCursor = msgs[len(msgs)-1].MsgId
	}

	if req.Cursor == 0 && len(msgs) > 0 {
		if data, err := json.Marshal(resp); err == nil {
			cacheKey := fmt.Sprintf("chat:history:group:%d:%d:%d", req.GroupId, 0, req.Limit)
			cache.GetCache().Set(ctx, cacheKey, data, 5*time.Minute)
		}
	}
	return nil
}

func (s *LogicService) SearchGroup(ctx context.Context, req *pb_group.SearchGroupArgs, resp *pb_group.SearchGroupReply) error {
	groups, err := dao.SearchGroups(req.Keyword)
	if err != nil {
		return err
	}
	for _, g := range groups {
		resp.Groups = append(resp.Groups, &pb_group.GroupItem{
			GroupId:   g.GroupID,
			GroupName: g.GroupName,
			OwnerId:   g.OwnerID,
		})
	}
	return nil
}
