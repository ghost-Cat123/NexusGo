package service

import (
	"context"
	"errors"

	"NexusGo/apps/logic/dao"
	"NexusGo/apps/logic/models"
	"NexusGo/apps/pkg/proto/pb_user"
	"NexusGo/apps/pkg/utils"
)

func (s *LogicService) UserLogin(ctx context.Context, req *pb_user.UserLoginArgs, resp *pb_user.UserLoginReply) error {
	user, err := dao.FindUserByName(req.UserName)
	if err != nil {
		return errors.New("用户不存在")
	}

	if !utils.CheckPasswordHash(req.Password, user.Password) {
		return errors.New("密码错误")
	} else {
		resp.UserId = user.UserId
		resp.Token, err = utils.GenerateToken(user.UserId)
		if err != nil {
			return errors.New("token生成失败")
		}
	}
	return nil
}

func (s *LogicService) UserRegister(ctx context.Context, req *pb_user.UserRegisterArgs, resp *pb_user.UserRegisterReply) error {
	password, err := utils.HashPassword(req.Password)
	if err != nil {
		return err
	}

	// 缺省头像兜底
	finalAvatar := req.Avatar
	user := models.NewUser(req.UserId, req.UserName, password, req.Nickname, finalAvatar)
	err = dao.InsertUser(user)
	if err != nil {
		return err
	}
	resp.Success = true
	return nil
}

func (s *LogicService) GetUserInfo(ctx context.Context, req *pb_user.GetUserInfoArgs, resp *pb_user.GetUserInfoReply) error {
	user, err := dao.GetUserByID(req.UserId)
	if err != nil {
		return err
	}
	resp.UserId = user.UserId
	resp.Username = user.Username
	resp.Nickname = user.Nickname
	resp.Avatar = user.Avatar
	return nil
}

func (s *LogicService) SearchUser(ctx context.Context, req *pb_user.SearchUserArgs, resp *pb_user.SearchUserReply) error {
	users, err := dao.SearchUsers(req.Keyword)
	if err != nil {
		return err
	}
	for _, u := range users {
		resp.Users = append(resp.Users, &pb_user.SearchUserItem{
			UserId:   u.UserId,
			Username: u.Username,
			Nickname: u.Nickname,
			Avatar:   u.Avatar,
		})
	}
	return nil
}

func (s *LogicService) UpdateUserInfo(ctx context.Context, req *pb_user.UpdateUserInfoArgs, resp *pb_user.UpdateUserInfoReply) error {
	updates := make(map[string]interface{})
	if req.Nickname != "" {
		updates["nickname"] = req.Nickname
	}
	if req.Avatar != "" {
		updates["avatar"] = req.Avatar
	}
	if req.Password != "" {
		hashed, err := utils.HashPassword(req.Password)
		if err != nil {
			return err
		}
		updates["password"] = hashed
	}
	if len(updates) == 0 {
		resp.Success = true
		return nil
	}
	err := dao.UpdateUser(req.UserId, updates)
	if err != nil {
		return err
	}
	resp.Success = true
	return nil
}
