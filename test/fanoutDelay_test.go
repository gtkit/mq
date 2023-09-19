// @Author xiaozhaofu 2023/7/6 20:57:00
package test

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"testing"
	"time"

	"mq/rabbit"
)

func TestDealySubMq(t *testing.T) {
	exampleDelay()
}

type DelayMsg1 struct {
}

func (m *DelayMsg1) Process([]byte, string) error {
	return nil
}
func (m *DelayMsg1) Failed(msg rabbit.FailedMsg) {

}

type DelayMsg2 struct {
}

func (m *DelayMsg2) Process([]byte, string) error {
	return nil
}
func (m *DelayMsg2) Failed(msg rabbit.FailedMsg) {

}

func exampleDelay() {
	rabbitmq1, err1 := rabbit.NewMQFanout(context.Background(), "exchange.delay", MQURL)
	defer rabbitmq1.Destroy()
	if err1 != nil {
		log.Println(err1)
	}
	rabbitmq2, err2 := rabbit.NewMQFanout(context.Background(), "exchange.delay", MQURL)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}
	rabbitmq3, err3 := rabbit.NewMQFanout(context.Background(), "exchange.delay", MQURL)
	defer rabbitmq3.Destroy()
	if err3 != nil {
		log.Println(err3)
	}

	go func() {
		for i := 0; i < 10000; i++ {
			time.Sleep(1 * time.Second)
			err := rabbitmq1.PublishDelay("消息："+strconv.Itoa(i), "2000")
			if err != nil {
				fmt.Println("----PublishDelay error:", err)
				return
			}
		}
	}()

	// 消费者1
	go func() {
		err := rabbitmq2.ConsumeDelay(&DelayMsg1{})
		if err != nil {
			fmt.Println("----ConsumeDelay 1 error: ", err)
			return
		}
	}()

	// 消费者2
	go func() {
		err := rabbitmq3.ConsumeDelay(&DelayMsg2{})
		if err != nil {
			fmt.Println("----ConsumeDelay 2 error: ", err)
			return
		}
	}()

	forever := make(chan bool)
	<-forever
}

func doExampleDelayMsg(msg []byte) error {
	fmt.Println("-----doExampleDelayMsg doConsumeMsg: ", string(msg))
	return nil
}

func doExampleDelayMsg2(msg []byte) error {
	fmt.Println("-----doExampleDelayMsg 22 doConsumeMsg: ", string(msg))
	return nil
}
