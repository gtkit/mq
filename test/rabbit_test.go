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
			}
		}
	}()

	go func() {
		err := rabbitmq2.Consume(doConsumeMsg)
		if err != nil {
			fmt.Println("----Consume error: ", err)
		}

	}()

	forever := make(chan bool)
	<-forever
}

func doConsumeMsg(msg []byte) error {
	fmt.Println("-----doConsumeMsg: ", string(msg))
	return nil
}
