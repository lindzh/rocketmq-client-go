package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2/errors"
	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	producer "github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/magiconair/properties"
	"sync"
	"time"
)

type AdminExt struct {
	cli       internal.RMQClient
	opts      *adminOptions
	closeOnce sync.Once
}

func NewAdminExt(opts ...AdminOption) (*AdminExt, error) {
	defaultOpts := defaultAdminOptions()
	for _, opt := range opts {
		opt(defaultOpts)
	}
	namesrv, err := internal.NewNamesrv(defaultOpts.Resolver, defaultOpts.RemotingClientConfig)
	defaultOpts.Namesrv = namesrv
	if err != nil {
		return nil, err
	}

	cli := internal.GetOrNewRocketMQClient(defaultOpts.ClientOptions, nil)
	if cli == nil {
		return nil, fmt.Errorf("GetOrNewRocketMQClient faild")
	}
	defaultOpts.Namesrv = cli.GetNameSrv()
	return &AdminExt{
		cli:  cli,
		opts: defaultOpts,
	}, nil
}

func (a *AdminExt) Start() {
	a.cli.Start()
}

func (a *AdminExt) Close() error {
	a.closeOnce.Do(func() {
		a.cli.Shutdown()
	})
	return nil
}

func (a *AdminExt) FetchNameSrvList() []string {
	a.cli.GetNameSrv().UpdateNameServerAddress()
	return a.cli.GetNameSrv().AddrList()
}

func (a *AdminExt) UpdateKvConfig(nameSrvAddr *string, namespace string, key string, value string, timeoutMills time.Duration) error {
	request := &internal.PutKVConfigRequestHeader{}
	request.Key = key
	request.Namespace = namespace
	request.Value = value
	cmd := remote.NewRemotingCommand(internal.ReqPutKVConfig, request, nil)
	ctx, _ := context.WithTimeout(context.Background(), timeoutMills)
	res, err := a.cli.InvokeSync(ctx, *nameSrvAddr, cmd, timeoutMills)
	if err != nil {
		return err
	}
	if res.Code != internal.ResSuccess {
		return fmt.Errorf("update kv config name srv %s, response code: %d, remarks: %s", *nameSrvAddr, res.Code, res.Remark)
	}
	return nil
}

func (a *AdminExt) DeleteKvConfig(nameSrvAddr *string, namespace string, key string, timeoutMills time.Duration) error {
	request := &internal.DeleteKVConfigRequestHeader{}
	request.Key = key
	request.Namespace = namespace
	cmd := remote.NewRemotingCommand(internal.ReqDeleteKVConfig, request, nil)
	ctx, _ := context.WithTimeout(context.Background(), timeoutMills)
	res, err := a.cli.InvokeSync(ctx, *nameSrvAddr, cmd, timeoutMills)
	if err != nil {
		return err
	}
	if res.Code != internal.ResSuccess {
		return fmt.Errorf("delete kv config name srv %s, response code: %d, remarks: %s", *nameSrvAddr, res.Code, res.Remark)
	}
	return nil
}

func (a *AdminExt) GetKvConfig(nameSrvAddr *string, namespace string, key string, timeoutMills time.Duration) (string, error) {
	request := &internal.GetKVConfigRequestHeader{}
	request.Key = key
	request.Namespace = namespace
	cmd := remote.NewRemotingCommand(internal.ReqGetKVConfig, request, nil)
	ctx, _ := context.WithTimeout(context.Background(), timeoutMills)
	res, err := a.cli.InvokeSync(ctx, *nameSrvAddr, cmd, timeoutMills)
	if err != nil {
		return "", err
	}
	if res.Code == internal.ResQueryNotFound {
		return "", errors.ErrNotExisted
	}
	if res.Code != internal.ResSuccess {
		return "", fmt.Errorf("delete kv config name srv %s, response code: %d, remarks: %s", *nameSrvAddr, res.Code, res.Remark)
	}
	responseHeader := &internal.GetKVConfigResponseHeader{}
	responseHeader.Decode(res.ExtFields)
	return responseHeader.Value, nil
}

func (a *AdminExt) GetBrokerConfig(brokerAddr *string, timeoutMills time.Duration) (map[string]string, error) {
	request := &internal.NoParameterRequestHeader{}
	cmd := remote.NewRemotingCommand(internal.ReqGetBrokerConfig, request, nil)
	res, err := a.cli.InvokeSync(context.Background(), *brokerAddr, cmd, timeoutMills)
	if err != nil {
		return nil, err
	}
	if res.Code != internal.ResSuccess {
		return nil, fmt.Errorf("update config fail broker response code: %d, remarks: %s", res.Code, res.Remark)
	}
	p, err := properties.LoadString(string(res.Body))
	if err != nil {
		return nil, err
	}
	return p.Map(), nil
}

func (a *AdminExt) UpdateBrokerConfig(brokerAddr *string, config map[string]string, timeoutMills time.Duration) error {
	p := properties.LoadMap(config)
	value := p.String()
	body := []byte(value)
	request := &internal.NoParameterRequestHeader{}
	cmd := remote.NewRemotingCommand(internal.ReqUpdateBrokerConfig, request, body)
	ctx, _ := context.WithTimeout(context.Background(), timeoutMills)
	res, err := a.cli.InvokeSync(ctx, *brokerAddr, cmd, timeoutMills)
	if err != nil {
		return err
	}
	if res.Code != internal.ResSuccess {
		return fmt.Errorf("update config fail broker response code: %d, remarks: %s", res.Code, res.Remark)
	}
	return nil
}

func (a *AdminExt) UpdateBrokerRole(brokerAddr *string, role string, timeoutMills time.Duration) error {
	request := &internal.UpdateBrokerRoleRequestHeader{}
	request.BrokerRole = role
	cmd := remote.NewRemotingCommand(internal.ReqUpdateBrokerRole, request, nil)
	res, err := a.cli.InvokeSync(context.Background(), *brokerAddr, cmd, timeoutMills)
	if err != nil {
		return err
	}
	if res.Code != internal.ResSuccess {
		return fmt.Errorf("update broker role fail broker %s, response code: %d, remarks: %s", *brokerAddr, res.Code, res.Remark)
	}
	return nil
}

func (a *AdminExt) GetBrokerRuntimeStats(brokerAddr *string, needDiskCheck bool, needEarliestTime bool, timeoutMills time.Duration) (map[string]string, error) {
	request := &internal.GetBrokerRuntimeInfoRequestHeader{}
	request.NeedEarliestTime = needEarliestTime
	request.NeedDiskCheck = needDiskCheck
	cmd := remote.NewRemotingCommand(internal.ReqGetBrokerRuntimeInfo, request, nil)
	res, err := a.cli.InvokeSync(context.Background(), *brokerAddr, cmd, timeoutMills)
	if err != nil {
		return nil, err
	}
	if res.Code != internal.ResSuccess {
		return nil, fmt.Errorf("update config fail broker response code: %d, remarks: %s", res.Code, res.Remark)
	}
	kvTable := &internal.KVTable{}
	err1 := json.Unmarshal(res.Body, kvTable)
	if err1 != nil {
		return nil, err1
	}
	return kvTable.Table, nil
}

func (a *AdminExt) GetBrokerClusterInfo(nameSrvAddr *string, cluster string, timeoutMills time.Duration) (*internal.ClusterInfo, error) {
	request := &internal.GetClusterListRequestHeader{}
	request.Cluster = cluster
	cmd := remote.NewRemotingCommand(internal.ReqGetBrokerClusterInfo, request, nil)
	res, err := a.cli.InvokeSync(context.Background(), *nameSrvAddr, cmd, timeoutMills)
	if err != nil {
		return nil, err
	}
	if res.Code != internal.ResSuccess {
		return nil, fmt.Errorf("update config fail broker response code: %d, remarks: %s", res.Code, res.Remark)
	}
	clusterInfo := &internal.ClusterInfo{}
	err1 := clusterInfo.Decode(string(res.Body))
	if err1 != nil {
		return nil, err1
	}
	return clusterInfo, nil
}

func (a *AdminExt) GetTopicRoute(nameSrvAddr *string, topic string, timeoutMills time.Duration) (*internal.TopicRouteData, error) {
	request := &internal.GetRouteInfoRequestHeader{
		Topic: topic,
	}
	cmd := remote.NewRemotingCommand(internal.ReqGetRouteInfoByTopic, request, nil)
	res, err := a.cli.InvokeSync(context.Background(), *nameSrvAddr, cmd, timeoutMills)
	if err != nil {
		return nil, err
	}
	if res.Code == internal.ResTopicNotExist {
		return nil, errors.ErrTopicNotExist
	}
	if res.Code != internal.ResSuccess {
		return nil, fmt.Errorf("update config fail broker response code: %d, remarks: %s", res.Code, res.Remark)
	}
	routeData := &internal.TopicRouteData{}
	err = routeData.Decode(string(res.Body))
	if err != nil {
		return nil, err
	}
	return routeData, nil
}

func (a *AdminExt) SyncSendMessage(brokerAddr *string, mq *primitive.MessageQueue, msg *primitive.Message, timeoutMills time.Duration) (*primitive.SendResult, error) {
	p, err := producer.NewBlankProducer(producer.WithGroupName("PID_admin_ext"))
	if err != nil {
		return nil, err
	}
	req := p.BuildSendRequest(mq, msg)
	res, err1 := a.cli.InvokeSync(context.Background(), *brokerAddr, req, timeoutMills)
	if err1 != nil {
		return nil, err1
	}
	resp := primitive.NewSendResult()
	return resp, a.cli.ProcessSendResponse(mq.BrokerName, res, resp, msg)
}

func (a *AdminExt) CreateTopic(brokerAddr *string, topic string, queue int, perm int, timeoutMills time.Duration) error {
	request := &internal.CreateTopicRequestHeader{
		Topic:           topic,
		ReadQueueNums:   queue,
		WriteQueueNums:  queue,
		Perm:            perm,
		Order:           false,
		TopicFilterType: "SINGLE_TAG",
		TopicSysFlag:    0,
	}
	cmd := remote.NewRemotingCommand(internal.ReqCreateTopic, request, nil)
	res, err := a.cli.InvokeSync(context.Background(), *brokerAddr, cmd, timeoutMills)
	if err != nil {
		return err
	}
	if res.Code != internal.ResSuccess {
		return fmt.Errorf("create topic fail broker response code: %d, remarks: %s", res.Code, res.Remark)
	}
	return nil
}

func (a *AdminExt) SyncTopicConfigFromBroker(fromBrokerAddr string, brokerAddr *string, timeoutMills time.Duration) error {
	request := &internal.SyncTopicConfigRequestHeader{}
	request.FromBrokerAddr = fromBrokerAddr
	cmd := remote.NewRemotingCommand(internal.ReqSyncBrokerTopicConfig, request, nil)
	res, err := a.cli.InvokeSync(context.Background(), *brokerAddr, cmd, timeoutMills)
	if err != nil {
		return err
	}
	if res.Code != internal.ResSuccess {
		return fmt.Errorf("sync topic config from %s fail broker %s, response code: %d, remarks: %s", fromBrokerAddr, *brokerAddr, res.Code, res.Remark)
	}
	return nil
}

func (a *AdminExt) SyncSubscriptionGroupConfigFromBroker(fromBrokerAddr string, brokerAddr *string, timeoutMills time.Duration) error {
	request := &internal.SyncSubscriptionGroupConfigRequestHeader{}
	request.FromBrokerAddr = fromBrokerAddr
	cmd := remote.NewRemotingCommand(internal.ReqSyncBrokerSubscriptionGroupConfig, request, nil)
	res, err := a.cli.InvokeSync(context.Background(), *brokerAddr, cmd, timeoutMills)
	if err != nil {
		return err
	}
	if res.Code != internal.ResSuccess {
		return fmt.Errorf("sync sub group config from %s fail broker %s, response code: %d, remarks: %s", fromBrokerAddr, *brokerAddr, res.Code, res.Remark)
	}
	return nil
}
