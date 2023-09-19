// @Author xiaozhaofu 2023/7/6 20:57:00
package test

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/pkg/errors"

	"mq/rabbit"
)

// const MQURL = "amqp://guest:guest@127.0.0.1:5672/"

const MQURL = "amqp://xiaozhaofu:123456@10.10.10.44:5672/"

func TestSubMq(t *testing.T) {
	example3()
}

type Example31 struct{}

func (m *Example31) Process(msg []byte, msgId string) error {
	fmt.Println("------------fanout Consume Msg Example31 ----------- : ", string(msg), " -----", msgId)
	// return nil
	return errors.New("test retry error")

}
func (m *Example31) Failed(msg rabbit.FailedMsg) {
	fmt.Printf("------------failed msg handler Example31----------- :  %s\n", string(msg.Message))
}

type Example32 struct {
}

func (m *Example32) Process(msg []byte, msgId string) error {
	fmt.Println("------------fanout Consume Msg Example32 ----------- : ", string(msg), " -----", msgId)
	// return nil
	return errors.New("test retry error")
}
func (m *Example32) Failed(msg rabbit.FailedMsg) {
	fmt.Printf("------------failed msg handler Example32----------- :  %s\n", string(msg.Message))
}

func example3() {
	rabbitmq1, err1 := rabbit.NewMQFanout(context.Background(), "exchange.example3", MQURL)
	defer rabbitmq1.Destroy()
	if err1 != nil {
		log.Println(err1)
	}
	rabbitmq2, err2 := rabbit.NewMQFanout(context.Background(), "exchange.example3", MQURL)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}
	rabbitmq3, err3 := rabbit.NewMQFanout(context.Background(), "exchange.example3", MQURL)
	defer rabbitmq3.Destroy()
	if err3 != nil {
		log.Println(err3)
	}

	go func() {
		for i := 0; i < 1000; i++ {
			time.Sleep(1 * time.Second)
			err := rabbitmq1.Publish("消息：" + strconv.Itoa(i))
			if err != nil {
				fmt.Println("----example3 Publish error:", err)
				return
			}
		}
	}()

	go func() {
		err := rabbitmq2.Consume(&Example31{})
		if err != nil {
			fmt.Println("----example3 Consume error: ", err)
			return
		}
	}()

	go func() {
		err := rabbitmq3.Consume(&Example32{})
		if err != nil {
			fmt.Println("----example3 Consume error: ", err)
			return
		}
	}()

	forever := make(chan bool)
	<-forever
}

func TestFanoutDlx(t *testing.T) {
	exampleFanoutDlx()
}

type FailToDlx struct {
}

func (m *FailToDlx) Process([]byte, string) error {
	return nil
}
func (m *FailToDlx) Failed(msg rabbit.FailedMsg) {

}

type ConsumeDlx struct {
}

func (m *ConsumeDlx) Process([]byte, string) error {
	return nil
}
func (m *ConsumeDlx) Failed(msg rabbit.FailedMsg) {

}

func exampleFanoutDlx() {
	rabbitmq1, err1 := rabbit.NewMQFanout(context.Background(), "exchange.example3", MQURL)
	defer rabbitmq1.Destroy()
	if err1 != nil {
		log.Println(err1)
	}
	rabbitmq2, err2 := rabbit.NewMQFanout(context.Background(), "exchange.example3", MQURL)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}
	rabbitmq3, err3 := rabbit.NewMQFanout(context.Background(), "exchange.example3", MQURL)
	defer rabbitmq3.Destroy()
	if err3 != nil {
		log.Println(err3)
	}

	go func() {
		for i := 0; i < 10000; i++ {
			time.Sleep(1 * time.Second)
			err := rabbitmq1.Publish("消息：" + strconv.Itoa(i))
			if err != nil {
				fmt.Println("----example3 Publish error:", err)
				return
			}
		}
	}()

	// 消费者1
	go func() {
		err := rabbitmq2.ConsumeFailToDlx(&FailToDlx{})
		if err != nil {
			fmt.Println("----ConsumeFailToDlx Consume error: ", err)
			return
		}
	}()

	// 消费者2 死信消费
	go func() {
		err := rabbitmq3.ConsumeDlx(&ConsumeDlx{})
		if err != nil {
			fmt.Println("----ConsumeDlx Consume error: ", err)
			return
		}
	}()

	forever := make(chan bool)
	<-forever
}

func doExample31Msg(msg []byte) error {
	fmt.Println("-----Example3-1 doConsumeMsg: ", string(msg))
	return nil
}
func doExample32Msg(msg []byte) error {
	fmt.Println("-----Example3-2 doConsumeMsg: ", string(msg))
	return nil
}

func doConsumeFailToDlx(msg []byte) error {
	fmt.Println("-----doConsumeFailToDlx --Msg: ", string(msg))
	return nil
	// return errors.New("...doConsumeFailToDlx error for test...")
}

func doConsumeDlx(msg []byte) error {
	fmt.Println(".....doConsumeDlx ....Msg: ", string(msg))
	return nil
}
