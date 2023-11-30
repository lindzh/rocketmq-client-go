package producer

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"testing"
)

const TEST_TOPIC = "rocketmqx_client_normal_test"

func TestSendMessage(t *testing.T) {
	p, _ := NewDefaultProducer(
		WithNsResolver(primitive.NewHttpResolver("", "http://jmenv.igame.service.163.org/rocketmq/nsaddr_dev")),
		WithRetry(2),
	)
	err := p.Start()
	if err != nil {
		fmt.Printf("start producer error: %s", err.Error())
	}
	msg := &primitive.Message{
		Topic: TEST_TOPIC,
		Body:  []byte("this is a message body"),
	}
	msg.WithProperty("key", "value")
	for i := 0; i < 10; i++ {
		ret, err := p.SendSync(context.TODO(), msg)
		if err != nil {
			fmt.Printf("send message error: %s\n", err.Error())
			continue
		}
		fmt.Printf("send message success. result=%s\n", ret.String())
	}
}
