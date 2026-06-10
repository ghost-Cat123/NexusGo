package rpcclient

import (
	"GrowRPC"
	"GrowRPC/codec"
	"GrowRPC/xclient"
	"NexusGo/apps/pkg/config"
	"NexusGo/apps/pkg/proto/pb_friend"
	"NexusGo/apps/pkg/proto/pb_group"
	"NexusGo/apps/pkg/proto/pb_msg"
	"NexusGo/apps/pkg/proto/pb_user"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var LogicRpcClient *xclient.XClient
var UserServiceClient *pb_user.UserServiceClient
var MsgServiceClient *pb_msg.MsgServiceClient
var FriendServiceClient *pb_friend.FriendServiceClient
var GroupServiceClient *pb_group.GroupServiceClient

func InitRPCClient() {
	opt := &GrowRPC.Option{
		MagicNumber:    GrowRPC.MagicNumber,
		CodecType:      codec.ProtobufType,
		ConnectTimeout: 10 * time.Second,
	}

	var discovery xclient.Discovery

	etcdEndpoints := config.GlobalConfig.Server.EtcdEndpoints
	if len(etcdEndpoints) > 0 {
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   etcdEndpoints,
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			panic("etcd client init failed: " + err.Error())
		}
		discovery = xclient.NewEtcdDiscovery(cli, "LogicService")
	} else {
		discovery = xclient.NewMultiServerDiscovery([]string{"tcp@localhost:8001"})
	}

	LogicRpcClient = xclient.NewXClient(discovery, xclient.RandomSelect, opt)
	UserServiceClient = pb_user.NewUserServiceClient(LogicRpcClient)
	MsgServiceClient = pb_msg.NewMsgServiceClient(LogicRpcClient)
	FriendServiceClient = pb_friend.NewFriendServiceClient(LogicRpcClient)
	GroupServiceClient = pb_group.NewGroupServiceClient(LogicRpcClient)
}
