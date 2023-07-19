// @Author xiaozhaofu 2023/7/6 20:57:00
package test

import (
	"fmt"
	"log"
	"strconv"
	"testing"
	"time"

	"mq/rabbit"
)

const MQURL = "amqp://guest:guest@127.0.0.1:5672/"

func TestSimpleMq(t *testing.T) {
	example12()
}
func TestSubMq(t *testing.T) {
	example3()
}
func example12() {
	rabbitmq1, err1 := rabbit.NewRabbitMQSimple("queue1", MQURL)

	defer rabbitmq1.Destroy()
	if err1 != nil {
		log.Println(err1)
	}
	rabbitmq2, err2 := rabbit.NewRabbitMQSimple("queue1", MQURL)

	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}

	go func() {
		for i := 0; i < 100; i++ {
			time.Sleep(1 * time.Second)
			err := rabbitmq1.Publish("消息：" + strconv.Itoa(i))
			if err != nil {
				fmt.Println("pulish err: ", err)
				return
			}
		}
	}()

	go func() {
		err := rabbitmq2.Consume(doConsumeMsg)
		if err != nil {
			fmt.Println("----Consume error: ", err)
			return
		}

	}()

	forever := make(chan bool)
	<-forever
}

func example3() {
	rabbitmq1, err1 := rabbit.NewRabbitMQSub("exchange.example3", MQURL)
	defer rabbitmq1.Destroy()
	if err1 != nil {
		log.Println(err1)
	}
	rabbitmq2, err2 := rabbit.NewRabbitMQSub("exchange.example3", MQURL)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}

	go func() {
		for i := 0; i < 10000; i++ {
			time.Sleep(500 * time.Millisecond)
			err := rabbitmq1.Publish("消息：" + strconv.Itoa(i))
			if err != nil {
				fmt.Println("----example3 Publish error:", err)
				return
			}
		}
	}()

	go func() {
		err := rabbitmq2.Consume(doExample3Msg)
		if err != nil {
			fmt.Println("----example3 Consume error: ", err)
			return
		}
	}()

	forever := make(chan bool)
	<-forever
}
func doConsumeMsg(msg []byte) error {
	fmt.Println("-----doConsumeMsg: ", string(msg))
	return nil
}
func doExample3Msg(msg []byte) error {
	fmt.Println("-----Example3 doConsumeMsg: ", string(msg))
	return nil
}
