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

//const MQURL = "amqp://xiaozhaofu:123456@10.10.10.44:5672/"

func TestSubMq(t *testing.T) {
	example3()
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
	rabbitmq3, err3 := rabbit.NewRabbitMQSub("exchange.example3", MQURL)
	defer rabbitmq3.Destroy()
	if err3 != nil {
		log.Println(err3)
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
		err := rabbitmq2.Consume(doExample31Msg)
		if err != nil {
			fmt.Println("----example3 Consume error: ", err)
			return
		}
	}()

	go func() {
		err := rabbitmq3.Consume(doExample32Msg)
		if err != nil {
			fmt.Println("----example3 Consume error: ", err)
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
