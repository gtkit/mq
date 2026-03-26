// @Author xiaozhaofu 2023/7/6 20:57:00
package test

import (
	"context"
	"errors"
	"log"
	"strconv"
	"testing"
	"time"

	"mq/rabbit"
)

func TestSubMq(t *testing.T) {
	requireRabbitMQ(t)
	example3()
}

func example3() {
	rabbitmq2, err2 := rabbit.NewConsumeFanout(
		"exchange.example3",
		mqURL,
		rabbit.WithContext(context.Background()),
	)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}
	rabbitmq3, err3 := rabbit.NewConsumeFanout(
		"exchange.example3",
		mqURL,
		rabbit.WithContext(context.Background()),
	)
	defer rabbitmq3.Destroy()
	if err3 != nil {
		log.Println(err3)
	}

	go func() {
		for i := 0; i < 1000; i++ {
			ctx, cancel := context.WithCancel(context.Background())
			rabbitmq1, _ := rabbit.NewPubFanout(
				"exchange.example3",
				mqURL,
				rabbit.WithContext(ctx),
			)
			time.Sleep(1 * time.Second)
			err := rabbitmq1.Publish("消息："+strconv.Itoa(i), &Example31{})
			if err != nil {
				log.Println("----example3 Publish error:", err)

			}
			cancel()
			rabbitmq1.Destroy()
		}
	}()

	go func() {
		err := rabbitmq2.Consume(&Example31{})
		if err != nil {
			log.Println("----example3 Consume error: ", err)
			return
		}
	}()

	go func() {
		err := rabbitmq3.Consume(&Example32{})
		if err != nil {
			log.Println("----example3 Consume error: ", err)
			return
		}
	}()

	forever := make(chan bool)
	<-forever
}

func TestFanoutDlx(t *testing.T) {
	requireRabbitMQ(t)
	exampleFanoutDlx()
}

func exampleFanoutDlx() {

	rabbitmq2, err2 := rabbit.NewConsumeFanout(
		"exchange.example3",
		mqURL,
		rabbit.WithContext(context.Background()),
	)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}
	rabbitmq3, err3 := rabbit.NewConsumeFanout(
		"exchange.example3",
		mqURL,
		rabbit.WithContext(context.Background()),
	)
	defer rabbitmq3.Destroy()
	if err3 != nil {
		log.Println(err3)
	}

	go func() {
		for i := 0; i < 10000; i++ {
			time.Sleep(1 * time.Second)
			ctx, cancel := context.WithCancel(context.Background())
			rabbitmq1, _ := rabbit.NewPubFanout(
				"exchange.example3",
				mqURL,
				rabbit.WithContext(ctx),
			)
			err := rabbitmq1.Publish("消息："+strconv.Itoa(i), &FailToDlx{})
			if err != nil {
				log.Println("----example3 Publish error:", err)
				return
			}
			cancel()
			rabbitmq1.Destroy()
		}
	}()

	// 消费者1
	go func() {
		log.Println("----ConsumeFailToDlx Consume ------")
		err := rabbitmq2.ConsumeFailToDlx(&FailToDlx{})
		if err != nil {
			log.Println("----ConsumeFailToDlx Consume error: ", err)
			return
		}
	}()
	time.Sleep(5 * time.Second)

	// 消费者2 死信消费
	go func() {
		log.Println("----ConsumeDlx Consume ------: ")
		err := rabbitmq3.ConsumeDlx(&ConsumeDlx{})
		if err != nil {
			log.Println("----ConsumeDlx Consume error: ", err)
			return
		}
	}()

	forever := make(chan bool)
	<-forever
}

type Example31 struct{}

func (m *Example31) Process(msg []byte, msgId string) error {
	log.Println("------------fanout Consume Msg Example31 ----------- : ", string(msg), " -----", msgId)
	// return nil
	return errors.New("test retry error")

}
func (m *Example31) Failed(msg rabbit.FailedMsg) {
	log.Printf("------------failed msg handler Example3:1----------- :  %s\n", string(msg.Message))
}

type Example32 struct {
}

func (m *Example32) Process(msg []byte, msgId string) error {
	log.Println("------------fanout Consume Msg Example3:2 ----------- : ", string(msg), " -----", msgId)
	// return nil
	return errors.New("test retry error")
}
func (m *Example32) Failed(msg rabbit.FailedMsg) {
	log.Printf("------------failed msg handler Example32----------- :  %s\n", string(msg.Message))
}

type FailToDlx struct {
}

func (m *FailToDlx) Process(msg []byte, msgId string) error {
	log.Println("------------fanout Consume Msg FailToDlx ----------- : ", string(msg), " -----", msgId)
	// return nil
	return errors.New("test retry error")
}
func (m *FailToDlx) Failed(msg rabbit.FailedMsg) {
	log.Printf("------------failed msg handler FailToDlx----------- :  %s\n", string(msg.Message))
}

type ConsumeDlx struct {
}

func (m *ConsumeDlx) Process(msg []byte, msgId string) error {
	log.Println("------------fanout Consume Msg ConsumeDlx ----------- : ", string(msg), " -----", msgId)
	return nil
}
func (m *ConsumeDlx) Failed(msg rabbit.FailedMsg) {
	log.Printf("------------failed msg handler ConsumeDlx----------- :  %s\n", string(msg.Message))
}
