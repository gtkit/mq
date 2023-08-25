package test

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"testing"
	"time"

	"mq/rabbit"
)

func TestSimpleMq(t *testing.T) {
	example12()
}
func example12() {
	var queueName = "queue3"
	rabbitmq1, err1 := rabbit.NewRabbitMQSimple(queueName, MQURL)
	defer rabbitmq1.Destroy()
	if err1 != nil {
		log.Println(err1)
	}
	rabbitmq2, err2 := rabbit.NewRabbitMQSimple(queueName, MQURL)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}

	go func() {
		for i := 0; i < 100; i++ {
			time.Sleep(2 * time.Second)
			err := rabbitmq1.PublishWithXdl("消息：" + strconv.Itoa(i))

			if err != nil {
				fmt.Println("pulish err: ", err)
				return
			}

		}
	}()

	// simple 消费
	go func() {
		err := rabbitmq2.ConsumeFailToDlx(doConsumeMsg)
		if err != nil {
			fmt.Println("----Consume error: ", err)
			return
		}

	}()

	// 死信消费
	go func() {
		err := rabbitmq2.ConsumeDlx(dlxDo)
		if err != nil {
			fmt.Println("----DlqConsume error: ", err)
			return
		}
		time.Sleep(2 * time.Second)

	}()

	forever := make(chan bool)
	<-forever
}

// 延迟队列
func TestSimpleDelay(t *testing.T) {
	example12Delay()
}
func example12Delay() {
	var queueName = "delay-queue"
	rabbitmq1, err1 := rabbit.NewRabbitMQSimple(queueName, MQURL)
	defer rabbitmq1.Destroy()
	if err1 != nil {
		log.Println(err1)
	}
	rabbitmq2, err2 := rabbit.NewRabbitMQSimple(queueName, MQURL)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}

	go func() {
		for i := 0; i < 100; i++ {
			time.Sleep(1 * time.Second)
			err := rabbitmq1.PublishDelay("消息："+strconv.Itoa(i), "2000")

			if err != nil {
				fmt.Println("pulish err: ", err)
				return
			}

		}
	}()

	// 死信
	go func() {
		err := rabbitmq2.ConsumeDelay(dlxDelay)
		if err != nil {
			fmt.Println("----DlqConsume error: ", err)
			return
		}
		time.Sleep(2 * time.Second)

	}()

	forever := make(chan bool)
	<-forever
}

func doConsumeMsg(msg []byte) error {
	fmt.Println("------------Simple ConsumeMsg dlx error: ", string(msg))
	return errors.New("test dlx error")
}
func dlxDo(msg []byte) error {
	fmt.Println("------------DLX message: ", string(msg))

	return nil
}
func dlxDelay(msg []byte) error {
	fmt.Println("-----delay message: ", string(msg))
	return nil
}
