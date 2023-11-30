package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"testing"
	"time"
)

const DOMAIN = "http://jmenv.igame.service.163.org/rocketmq/nsaddr_dev"
const TIMEOUT = time.Duration(3 * time.Second)

func GetAdminExt() (*AdminExt, error) {
	resolver := primitive.NewHttpResolver("", DOMAIN)
	admin, err := NewAdminExt(
		WithResolver(resolver),
	)
	if err != nil {
		return nil, err
	}
	//admin.Start()
	return admin, nil
}

func TestNameSrvList(t *testing.T) {
	admin, err := GetAdminExt()
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	list := admin.FetchNameSrvList()
	t.Logf("namesrv list: %v", list)
}

func TestUpdateKvConfig(t *testing.T) {
	admin, err := GetAdminExt()
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	list := admin.FetchNameSrvList()
	addr := list[0]
	err = admin.UpdateKvConfig(&addr, "test_123", "test", "test", TIMEOUT)
	if err != nil {
		fmt.Printf("update kv config error: %v\r\n", err)
		t.Fatal(err)
	}
	fmt.Println("update kv config success")
	v, e := admin.GetKvConfig(&addr, "test_123", "test", TIMEOUT)
	if e != nil {
		fmt.Printf("get kv config error: %v\r\n", e)
	}
	fmt.Printf("get kv config: %v\r\n", v)
	ed := admin.DeleteKvConfig(&addr, "test_123", "test", TIMEOUT)
	if ed != nil {
		fmt.Printf("delete kv config error: %v\r\n", ed)
	}
	fmt.Println("delete kv config success")
	v1, e1 := admin.GetKvConfig(&addr, "test_123", "test", TIMEOUT)
	if e1 != nil {
		fmt.Printf("after delete get kv config error: %v\r\n", e1)
	}
	fmt.Printf("after delete get kv config: %v\r\n", v1)
}

func TestBrokerCluster(t *testing.T) {
	admin, err := GetAdminExt()
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	list := admin.FetchNameSrvList()
	addr := list[0]
	brokerCluster, err := admin.GetBrokerClusterInfo(&addr, "devcluster1", TIMEOUT)
	if err != nil {
		t.Fatal(err)
		fmt.Printf("GetBrokerClusterInfo error: %v\r\n", err)
	} else {
		fmt.Println("GetBrokerClusterInfo success")
		bb, _ := json.Marshal(brokerCluster)
		fmt.Println(string(bb))
	}
}

func TestGetTopicRoute(t *testing.T) {
	admin, err := GetAdminExt()
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	list := admin.FetchNameSrvList()
	addr := list[0]
	route, err := admin.GetTopicRoute(&addr, "rocketmqx_client_normal_test", TIMEOUT)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("topic route: %v", route)
	fmt.Printf("topic route: %v\r\n", route)
}

func TestBrokerRuntimeStats(t *testing.T) {
	admin, err := GetAdminExt()
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	list := admin.FetchNameSrvList()
	fmt.Printf("namesrv list: %v\r\n", list)
	addr := "10.197.112.198:10911"
	stats, err := admin.GetBrokerRuntimeStats(&addr, true, true, TIMEOUT)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("broker runtime stats: %v", stats)
	fmt.Printf("broker runtime stats: %v\r\n", stats)
}

func TestUpdateBrokerRole(t *testing.T) {
	admin, err := GetAdminExt()
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	list := admin.FetchNameSrvList()
	fmt.Printf("namesrv list: %v\r\n", list)
	addr := "10.197.112.198:10911"
	err1 := admin.UpdateBrokerRole(&addr, "ASYNC_MASTER", TIMEOUT)
	if err1 != nil {
		t.Fatal(err1)
		fmt.Printf("update broker role error: %v\r\n", err1)
	}
	fmt.Printf("update broker role success\r\n")
}

func TestBrokerConfig(t *testing.T) {
	admin, err := GetAdminExt()
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	list := admin.FetchNameSrvList()
	fmt.Printf("namesrv list: %v\r\n", list)
	addr := "10.197.112.198:10911"
	conf, err1 := admin.GetBrokerConfig(&addr, TIMEOUT)
	if err1 != nil {
		t.Fatal(err1)
		fmt.Printf("get broker config error: %v\r\n", err1)
	} else {
		fmt.Printf("get broker config success: %v\r\n", conf)
		fmt.Printf("fileReservedTime: %v\r\n", conf["fileReservedTime"])
	}

	//fileReservedTime:72
	mp := make(map[string]string)
	mp["fileReservedTime"] = "71"
	err2 := admin.UpdateBrokerConfig(&addr, mp, TIMEOUT)
	if err2 != nil {
		t.Fatal(err2)
		fmt.Printf("update broker config error: %v\r\n", err2)
	} else {
		fmt.Printf("update broker config success\r\n")
	}

	conf3, err3 := admin.GetBrokerConfig(&addr, TIMEOUT)
	if err3 != nil {
		t.Fatal(err3)
		fmt.Printf("get broker config error: %v\r\n", err3)
	} else {
		fmt.Printf("get broker config success: %v\r\n", conf3)
		fmt.Printf("fileReservedTime: %v\r\n", conf3["fileReservedTime"])
	}
}

func TestDecodeRoute(t *testing.T) {
	data := "{\"brokerDatas\":[{\"brokerAddrs\":{1:\"10.197.112.198:10911\"},\"brokerName\":\"devbroker4\",\"cluster\":\"devcluster1\"},{\"brokerAddrs\":{0:\"10.197.112.200:10911\"},\"brokerName\":\"devbroker5\",\"cluster\":\"devcluster1\"}],\"filterServerTable\":{},\"queueDatas\":[{\"brokerName\":\"devbroker4\",\"perm\":6,\"readQueueNums\":8,\"topicSynFlag\":0,\"writeQueueNums\":8},{\"brokerName\":\"devbroker5\",\"perm\":6,\"readQueueNums\":8,\"topicSynFlag\":0,\"writeQueueNums\":8}]}\n"
	routeData := &internal.TopicRouteData{}
	err := routeData.Decode(data)
	if err != nil {
		fmt.Printf("decode error: %v\r\n", err)
	}
	fmt.Printf("decode result: %v\r\n", routeData)

	data1 := "{\"brokerDatas\":[{\"brokerAddrs\":{\"1\":\"10.197.112.198:10911\"},\"brokerName\":\"devbroker4\",\"cluster\":\"devcluster1\"},{\"brokerAddrs\":{\"0\":\"10.197.112.200:10911\"},\"brokerName\":\"devbroker5\",\"cluster\":\"devcluster1\"}],\"filterServerTable\":{},\"queueDatas\":[{\"brokerName\":\"devbroker4\",\"perm\":6,\"readQueueNums\":8,\"topicSynFlag\":0,\"writeQueueNums\":8},{\"brokerName\":\"devbroker5\",\"perm\":6,\"readQueueNums\":8,\"topicSynFlag\":0,\"writeQueueNums\":8}]}\n"
	routeData1 := &internal.TopicRouteData{}
	err1 := routeData1.Decode(data1)
	if err1 != nil {
		fmt.Printf("decode error: %v\r\n", err1)
	}
	fmt.Printf("decode result: %v\r\n", routeData1)
}

func TestAdminSendMessage(t *testing.T) {
	admin, err := GetAdminExt()
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	list := admin.FetchNameSrvList()
	fmt.Printf("namesrv list: %v\r\n", list)
	addr := "10.197.112.198:10911"
	msg := &primitive.Message{
		Topic: "rocketmqx_client_normal_test",
		Body:  []byte("hello rocketmqx"),
	}
	mq := &primitive.MessageQueue{
		Topic:      "rocketmqx_client_normal_test",
		BrokerName: "devbroker4",
		QueueId:    0,
	}

	ret, err1 := admin.SyncSendMessage(context.Background(), &addr, mq, msg, TIMEOUT)
	if err1 != nil {
		t.Fatal(err1)
		fmt.Printf("send message error: %v\r\n", err1)
	} else {
		fmt.Printf("send message success: %v\r\n", ret)
	}

}
