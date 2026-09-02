package main

import (
	"GrowRPC"
	"GrowRPC/codec"
	"NexusGo/apps/pkg/proto/pb_friend"
	"NexusGo/apps/pkg/proto/pb_group"
	"NexusGo/apps/pkg/proto/pb_msg"
	"NexusGo/apps/pkg/proto/pb_user"
	"context"
	"errors"
	"fmt"
	"net"
	"time"
)

type mockLogicService struct{}

func (m *mockLogicService) UserLogin(ctx context.Context, req *pb_user.UserLoginArgs, resp *pb_user.UserLoginReply) error {
	if req.UserName == "alice" && req.Password == "secret" {
		resp.UserId = 1
		resp.Token = "mock-token-abc123"
		return nil
	}
	return errors.New("用户不存在或密码错误")
}

func (m *mockLogicService) UserRegister(ctx context.Context, req *pb_user.UserRegisterArgs, resp *pb_user.UserRegisterReply) error {
	resp.Success = true
	return nil
}

func (m *mockLogicService) SyncUnread(ctx context.Context, req *pb_msg.SyncUnreadArgs, resp *pb_msg.SyncUnreadReply) error {
	resp.Messages = []*pb_msg.MessageItem{
		{MsgId: 1, SenderId: 2, Content: "hello", SeqId: 1, SendStatus: 1},
		{MsgId: 2, SenderId: 3, Content: "world", SeqId: 2, SendStatus: 1},
	}
	return nil
}

func (m *mockLogicService) NotifyDelivered(ctx context.Context, req *pb_msg.NotifyDeliveredArgs, resp *pb_msg.NotifyDeliveredReply) error {
	resp.Success = true
	return nil
}

func (m *mockLogicService) AckMessage(ctx context.Context, req *pb_msg.AckMessageArgs, resp *pb_msg.AckMessageReply) error {
	resp.Success = true
	return nil
}

func (m *mockLogicService) ReadMessages(ctx context.Context, req *pb_msg.ReadMessagesArgs, resp *pb_msg.ReadMessagesReply) error {
	resp.Success = true
	return nil
}

func (m *mockLogicService) GetUserInfo(ctx context.Context, req *pb_user.GetUserInfoArgs, resp *pb_user.GetUserInfoReply) error {
	resp.UserId = 1
	resp.Username = "alice"
	resp.Nickname = "Alice"
	resp.Avatar = ""
	return nil
}

func (m *mockLogicService) SearchUser(ctx context.Context, req *pb_user.SearchUserArgs, resp *pb_user.SearchUserReply) error {
	resp.Users = append(resp.Users, &pb_user.SearchUserItem{
		UserId: 1, Username: req.Keyword, Nickname: req.Keyword, Avatar: "",
	})
	return nil
}

func (m *mockLogicService) GetConversations(ctx context.Context, req *pb_msg.GetConversationsArgs, resp *pb_msg.GetConversationsReply) error {
	resp.Conversations = append(resp.Conversations, &pb_msg.ConversationItem{
		TargetId: 2, SessionType: 1, UnreadCount: 3, OwnerId: req.UserId,
	})
	return nil
}

func (m *mockLogicService) GetChatHistory(ctx context.Context, req *pb_msg.GetChatHistoryArgs, resp *pb_msg.GetChatHistoryReply) error {
	resp.TargetId = req.TargetId
	resp.Messages = append(resp.Messages, &pb_msg.MessageItem{
		MsgId: 100, SenderId: req.UserId, Content: "cached history", CreateTime: "2026-01-01 12:00:00",
	})
	if req.Limit > 1 {
		resp.HasMore = true
		resp.NextCursor = 99
	}
	return nil
}

func (m *mockLogicService) MarkMessageRead(ctx context.Context, req *pb_msg.MarkReadArgs, resp *pb_msg.MarkReadReply) error {
	resp.Success = true
	return nil
}

// ─── FriendService mock ───

func (m *mockLogicService) ApplyFriend(ctx context.Context, req *pb_friend.ApplyFriendArgs, resp *pb_friend.ApplyFriendReply) error {
	resp.Success = true
	return nil
}

func (m *mockLogicService) ResolveFriend(ctx context.Context, req *pb_friend.ResolveFriendArgs, resp *pb_friend.ResolveFriendReply) error {
	resp.Success = true
	return nil
}

func (m *mockLogicService) GetFriendList(ctx context.Context, req *pb_friend.GetFriendListArgs, resp *pb_friend.GetFriendListReply) error {
	resp.Friends = append(resp.Friends, &pb_friend.FriendItem{
		FriendId: 2, Nickname: "Bob", Avatar: "", Status: 1,
	})
	return nil
}

func (m *mockLogicService) DeleteFriend(ctx context.Context, req *pb_friend.DeleteFriendArgs, resp *pb_friend.DeleteFriendReply) error {
	resp.Success = true
	return nil
}

func (m *mockLogicService) UpdateUserInfo(ctx context.Context, req *pb_user.UpdateUserInfoArgs, resp *pb_user.UpdateUserInfoReply) error {
	resp.Success = true
	return nil
}

func (m *mockLogicService) GetPendingRequests(ctx context.Context, req *pb_friend.GetPendingRequestsArgs, resp *pb_friend.GetPendingRequestsReply) error {
	resp.Requests = append(resp.Requests, &pb_friend.PendingRequestItem{
		UserId: 3, Username: "charlie", Nickname: "Charlie", ApplyMsg: "hello",
	})
	return nil
}

// ─── GroupService mock ───

func (m *mockLogicService) CreateGroup(ctx context.Context, req *pb_group.CreateGroupArgs, resp *pb_group.CreateGroupReply) error {
	resp.GroupId = 100
	return nil
}

func (m *mockLogicService) GetGroupList(ctx context.Context, req *pb_group.GetGroupListArgs, resp *pb_group.GetGroupListReply) error {
	resp.Groups = append(resp.Groups, &pb_group.GroupItem{
		GroupId: 100, GroupName: "TestGroup", OwnerId: 1,
	})
	return nil
}

func (m *mockLogicService) GetGroupInfo(ctx context.Context, req *pb_group.GetGroupInfoArgs, resp *pb_group.GetGroupInfoReply) error {
	resp.GroupId = req.GroupId
	resp.GroupName = "TestGroup"
	resp.OwnerId = 1
	return nil
}

func (m *mockLogicService) GetGroupMembers(ctx context.Context, req *pb_group.GetGroupMembersArgs, resp *pb_group.GetGroupMembersReply) error {
	resp.Members = append(resp.Members,
		&pb_group.GroupMemberItem{UserId: 1, Role: 0, Nickname: "Alice"},
		&pb_group.GroupMemberItem{UserId: 2, Role: 0, Nickname: "Bob"},
	)
	return nil
}

func (m *mockLogicService) JoinGroup(ctx context.Context, req *pb_group.JoinGroupArgs, resp *pb_group.JoinGroupReply) error {
	resp.Success = true
	return nil
}

func (m *mockLogicService) LeaveGroup(ctx context.Context, req *pb_group.LeaveGroupArgs, resp *pb_group.LeaveGroupReply) error {
	resp.Success = true
	return nil
}

func (m *mockLogicService) GetGroupHistory(ctx context.Context, req *pb_group.GetGroupHistoryArgs, resp *pb_group.GetGroupHistoryReply) error {
	resp.Messages = append(resp.Messages, &pb_group.GroupMsgItem{
		MsgId: 200, SenderId: 1, Content: "group msg", CreateTime: "2026-01-01 12:00:00",
	})
	if req.Limit > 1 {
		resp.HasMore = true
		resp.NextCursor = 199
	}
	return nil
}

func (m *mockLogicService) DissolveGroup(ctx context.Context, req *pb_group.DissolveGroupArgs, resp *pb_group.DissolveGroupReply) error {
	resp.Success = true
	return nil
}

func (m *mockLogicService) SearchGroup(ctx context.Context, req *pb_group.SearchGroupArgs, resp *pb_group.SearchGroupReply) error {
	resp.Groups = append(resp.Groups, &pb_group.GroupItem{
		GroupId: 200, GroupName: "SearchResult" + req.Keyword, OwnerId: 1,
	})
	return nil
}

func main() {
	allOk := true

	// 1. 创建服务端
	server := GrowRPC.NewServer()
	svc := new(mockLogicService)
	pb_user.RegisterUserServiceServer(server, svc)
	pb_msg.RegisterMsgServiceServer(server, svc)
	pb_friend.RegisterFriendServiceServer(server, svc)
	pb_group.RegisterGroupServiceServer(server, svc)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic("listen: " + err.Error())
	}
	go server.Accept(l)
	time.Sleep(100 * time.Millisecond)

	// 2. 创建客户端
	opt := &GrowRPC.Option{
		MagicNumber:    GrowRPC.MagicNumber,
		CodecType:      codec.ProtobufType,
		ConnectTimeout: 2 * time.Second,
	}
	client, err := GrowRPC.Dial("tcp", l.Addr().String(), opt)
	if err != nil {
		panic("dial: " + err.Error())
	}
	defer client.Close()

	// 3. 测试所有 RPC 方法
	tests := []struct {
		name  string
		check func() error
	}{
		{
			"UserService.UserLogin",
			func() error {
				args := &pb_user.UserLoginArgs{UserName: "alice", Password: "secret"}
				reply := &pb_user.UserLoginReply{}
				if err := client.Call(context.Background(), "UserService.UserLogin", args, reply); err != nil {
					return err
				}
				if reply.UserId != 1 || reply.Token != "mock-token-abc123" {
					return fmt.Errorf("unexpected reply: %+v", reply)
				}
				return nil
			},
		},
		{
			"UserService.UserRegister",
			func() error {
				args := &pb_user.UserRegisterArgs{UserName: "bob", Password: "pwd"}
				reply := &pb_user.UserRegisterReply{}
				if err := client.Call(context.Background(), "UserService.UserRegister", args, reply); err != nil {
					return err
				}
				if !reply.Success {
					return errors.New("expected success=true")
				}
				return nil
			},
		},
		{
			"UserService.GetUserInfo",
			func() error {
				args := &pb_user.GetUserInfoArgs{UserId: 1}
				reply := &pb_user.GetUserInfoReply{}
				if err := client.Call(context.Background(), "UserService.GetUserInfo", args, reply); err != nil {
					return err
				}
				if reply.UserId != 1 || reply.Username != "alice" {
					return fmt.Errorf("unexpected reply: %+v", reply)
				}
				return nil
			},
		},
		{
			"UserService.SearchUser",
			func() error {
				args := &pb_user.SearchUserArgs{Keyword: "test"}
				reply := &pb_user.SearchUserReply{}
				if err := client.Call(context.Background(), "UserService.SearchUser", args, reply); err != nil {
					return err
				}
				if len(reply.Users) == 0 {
					return errors.New("expected at least one user")
				}
				return nil
			},
		},
		{
			"MsgService.SyncUnread",
			func() error {
				args := &pb_msg.SyncUnreadArgs{ReceiverId: 1}
				reply := &pb_msg.SyncUnreadReply{}
				if err := client.Call(context.Background(), "MsgService.SyncUnread", args, reply); err != nil {
					return err
				}
				if len(reply.Messages) != 2 {
					return fmt.Errorf("expected 2 messages, got %d", len(reply.Messages))
				}
				return nil
			},
		},
		{
			"MsgService.NotifyDelivered",
			func() error {
				args := &pb_msg.NotifyDeliveredArgs{MsgId: 100}
				reply := &pb_msg.NotifyDeliveredReply{}
				if err := client.Call(context.Background(), "MsgService.NotifyDelivered", args, reply); err != nil {
					return err
				}
				if !reply.Success {
					return errors.New("expected success=true")
				}
				return nil
			},
		},
		{
			"MsgService.AckMessage",
			func() error {
				args := &pb_msg.AckMessageArgs{MsgId: 100}
				reply := &pb_msg.AckMessageReply{}
				if err := client.Call(context.Background(), "MsgService.AckMessage", args, reply); err != nil {
					return err
				}
				if !reply.Success {
					return errors.New("expected success=true")
				}
				return nil
			},
		},
		{
			"MsgService.ReadMessages",
			func() error {
				args := &pb_msg.ReadMessagesArgs{ReceiverId: 1}
				reply := &pb_msg.ReadMessagesReply{}
				if err := client.Call(context.Background(), "MsgService.ReadMessages", args, reply); err != nil {
					return err
				}
				if !reply.Success {
					return errors.New("expected success=true")
				}
				return nil
			},
		},
		{
			"MsgService.GetConversations",
			func() error {
				args := &pb_msg.GetConversationsArgs{UserId: 1}
				reply := &pb_msg.GetConversationsReply{}
				if err := client.Call(context.Background(), "MsgService.GetConversations", args, reply); err != nil {
					return err
				}
				if len(reply.Conversations) == 0 {
					return errors.New("expected at least one conversation")
				}
				return nil
			},
		},
		{
			"MsgService.GetChatHistory",
			func() error {
				args := &pb_msg.GetChatHistoryArgs{UserId: 1, TargetId: 2, Limit: 20}
				reply := &pb_msg.GetChatHistoryReply{}
				if err := client.Call(context.Background(), "MsgService.GetChatHistory", args, reply); err != nil {
					return err
				}
				if reply.TargetId != 2 {
					return fmt.Errorf("expected target_id=2, got %d", reply.TargetId)
				}
				return nil
			},
		},
		{
			"MsgService.MarkMessageRead",
			func() error {
				args := &pb_msg.MarkReadArgs{UserId: 1, TargetId: 2}
				reply := &pb_msg.MarkReadReply{}
				if err := client.Call(context.Background(), "MsgService.MarkMessageRead", args, reply); err != nil {
					return err
				}
				if !reply.Success {
					return errors.New("expected success=true")
				}
				return nil
			},
		},
		{
			"FriendService.ApplyFriend",
			func() error {
				args := &pb_friend.ApplyFriendArgs{UserId: 1, FriendId: 2, ApplyMsg: "hello"}
				reply := &pb_friend.ApplyFriendReply{}
				if err := client.Call(context.Background(), "FriendService.ApplyFriend", args, reply); err != nil {
					return err
				}
				if !reply.Success {
					return errors.New("expected success=true")
				}
				return nil
			},
		},
		{
			"FriendService.ResolveFriend",
			func() error {
				args := &pb_friend.ResolveFriendArgs{UserId: 1, FriendId: 2, Action: "accept"}
				reply := &pb_friend.ResolveFriendReply{}
				if err := client.Call(context.Background(), "FriendService.ResolveFriend", args, reply); err != nil {
					return err
				}
				if !reply.Success {
					return errors.New("expected success=true")
				}
				return nil
			},
		},
		{
			"FriendService.GetFriendList",
			func() error {
				args := &pb_friend.GetFriendListArgs{UserId: 1}
				reply := &pb_friend.GetFriendListReply{}
				if err := client.Call(context.Background(), "FriendService.GetFriendList", args, reply); err != nil {
					return err
				}
				if len(reply.Friends) == 0 {
					return errors.New("expected at least one friend")
				}
				return nil
			},
		},
		{
			"FriendService.DeleteFriend",
			func() error {
				args := &pb_friend.DeleteFriendArgs{UserId: 1, FriendId: 2}
				reply := &pb_friend.DeleteFriendReply{}
				if err := client.Call(context.Background(), "FriendService.DeleteFriend", args, reply); err != nil {
					return err
				}
				if !reply.Success {
					return errors.New("expected success=true")
				}
				return nil
			},
		},
		{
			"GroupService.CreateGroup",
			func() error {
				args := &pb_group.CreateGroupArgs{UserId: 1, GroupName: "test", MemberIds: []int64{2}}
				reply := &pb_group.CreateGroupReply{}
				if err := client.Call(context.Background(), "GroupService.CreateGroup", args, reply); err != nil {
					return err
				}
				if reply.GroupId == 0 {
					return errors.New("expected non-zero group_id")
				}
				return nil
			},
		},
		{
			"GroupService.GetGroupList",
			func() error {
				args := &pb_group.GetGroupListArgs{UserId: 1}
				reply := &pb_group.GetGroupListReply{}
				if err := client.Call(context.Background(), "GroupService.GetGroupList", args, reply); err != nil {
					return err
				}
				if len(reply.Groups) == 0 {
					return errors.New("expected at least one group")
				}
				return nil
			},
		},
		{
			"GroupService.GetGroupInfo",
			func() error {
				args := &pb_group.GetGroupInfoArgs{GroupId: 100}
				reply := &pb_group.GetGroupInfoReply{}
				if err := client.Call(context.Background(), "GroupService.GetGroupInfo", args, reply); err != nil {
					return err
				}
				if reply.GroupId != 100 {
					return fmt.Errorf("expected group_id=100, got %d", reply.GroupId)
				}
				return nil
			},
		},
		{
			"GroupService.GetGroupMembers",
			func() error {
				args := &pb_group.GetGroupMembersArgs{GroupId: 100}
				reply := &pb_group.GetGroupMembersReply{}
				if err := client.Call(context.Background(), "GroupService.GetGroupMembers", args, reply); err != nil {
					return err
				}
				if len(reply.Members) != 2 {
					return fmt.Errorf("expected 2 members, got %d", len(reply.Members))
				}
				return nil
			},
		},
		{
			"GroupService.JoinGroup",
			func() error {
				args := &pb_group.JoinGroupArgs{UserId: 3, GroupId: 100}
				reply := &pb_group.JoinGroupReply{}
				if err := client.Call(context.Background(), "GroupService.JoinGroup", args, reply); err != nil {
					return err
				}
				if !reply.Success {
					return errors.New("expected success=true")
				}
				return nil
			},
		},
		{
			"GroupService.LeaveGroup",
			func() error {
				args := &pb_group.LeaveGroupArgs{UserId: 3, GroupId: 100}
				reply := &pb_group.LeaveGroupReply{}
				if err := client.Call(context.Background(), "GroupService.LeaveGroup", args, reply); err != nil {
					return err
				}
				if !reply.Success {
					return errors.New("expected success=true")
				}
				return nil
			},
		},
		{
			"GroupService.GetGroupHistory",
			func() error {
				args := &pb_group.GetGroupHistoryArgs{GroupId: 100, Limit: 20}
				reply := &pb_group.GetGroupHistoryReply{}
				if err := client.Call(context.Background(), "GroupService.GetGroupHistory", args, reply); err != nil {
					return err
				}
				if len(reply.Messages) == 0 {
					return errors.New("expected at least one message")
				}
				return nil
			},
		},
	}

	for _, tt := range tests {
		fmt.Printf("[TEST] %s ... ", tt.name)
		if err := tt.check(); err != nil {
			fmt.Printf("FAIL: %v\n", err)
			allOk = false
		} else {
			fmt.Println("PASS")
		}
	}

	if allOk {
		fmt.Println("\n=== All 22 RPC tests PASSED ===")
	} else {
		fmt.Println("\n=== Some tests FAILED ===")
	}
}
