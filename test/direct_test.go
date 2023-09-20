// @Author xiaozhaofu 2023/8/28 15:01:00
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

// 测试路由模式
func TestDirect(t *testing.T) {
	exampleDirect()
}

// 测试路由模式延迟队列
func TestDirectDelay(t *testing.T) {
	exampleDirectDelay()
}

// 测试路由模式死信队列
func TestDirectDlx(t *testing.T) {
	exampleDirectDlx()
}

type DirectFailToDlx struct {
}

func (m *DirectFailToDlx) Process(msg []byte, msgId string) error {
	return nil
}
func (m *DirectFailToDlx) Failed(msg rabbit.FailedMsg) {

}

type DirectDlx struct {
}

func (m *DirectDlx) Process(msg []byte, msgId string) error {
	return nil
}
func (m *DirectDlx) Failed(msg rabbit.FailedMsg) {

}

func exampleDirectDlx() {
	var (
		routingKey = "key.direct.dlx"
		exchange   = "exchange.direct.dlx"
		// queueName  = "queue.direct"
		queueName = ""
	)
	rabbitmq1, err1 := rabbit.NewMQDirect(context.Background(), exchange, queueName, routingKey, MQURL)
	defer rabbitmq1.Destroy()
	if err1 != nil {
		log.Println(err1)
	}

	// rabbitmq2, err2 := rabbit.NewRabbitMQDirect(exchange, routingKey, MQURL)
	// defer rabbitmq2.Destroy()
	// if err2 != nil {
	// 	log.Println(err2)
	// }
	go func() {
		for i := 0; i < 100; i++ {
			time.Sleep(1 * time.Second)
			msg := "消息：" + strconv.Itoa(i)
			err := rabbitmq1.Publish(msg)
			if err != nil {
				log.Println("----direct.dlx Publish error:", err)
				return
			}
			log.Println("----Publish Dlx success: ", msg, " ----", time.Now().Format(time.DateTime))
		}
	}()

	// 消费者1
	go func() {
		err := rabbitmq1.ConsumeFailToDlx(&DirectFailToDlx{})
		if err != nil {
			log.Println("----ConsumeFailToDlx Consume error: ", err)
			return
		}

	}()

	// 消费者2 死信消费
	go func() {
		err := rabbitmq1.ConsumeDlx(&DirectDlx{})
		if err != nil {
			log.Println("----ConsumeDlx Consume error: ", err)
			return
		}
	}()
	select {}
}

type DirectDelay struct {
}

func (m *DirectDelay) Process(msg []byte, msgId string) error {
	return nil
}
func (m *DirectDelay) Failed(msg rabbit.FailedMsg) {

}

func exampleDirectDelay() {
	var (
		routingKey = "key.direct.delay"
		exchange   = "exchange.direct.delay"
	)
	rabbitmq1, err1 := rabbit.NewMQDirect(context.Background(), exchange, "", routingKey, MQURL)
	defer rabbitmq1.Destroy()
	if err1 != nil {
		log.Println(err1)
	}
	rabbitmq2, err2 := rabbit.NewMQDirect(context.Background(), exchange, "", routingKey, MQURL)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}
	go func() {
		for i := 0; i < 100; i++ {
			time.Sleep(1 * time.Second)
			msg := "消息：" + strconv.Itoa(i)
			err := rabbitmq1.PublishDelay(msg, "2000")
			if err != nil {
				log.Println("----example3 Publish error:", err)
				return
			}
			log.Println("----PublishDelay success: ", msg, " ----", time.Now().Format(time.DateTime))
		}
	}()

	// 消费者1
	go func() {
		err := rabbitmq2.ConsumeDelay(&DirectDelay{})
		if err != nil {
			log.Println("----ConsumeFailToDlx Consume error: ", err)
			return
		}

	}()
	select {}

}

type Direct struct {
}

func (m *Direct) Process(msg []byte, msgId string) error {
	log.Println("------------Simple Consume Msg ----------- : ", string(msg), " -----", msgId)
	// return nil
	return errors.New("test retry error")
}
func (m *Direct) Failed(msg rabbit.FailedMsg) {
	log.Printf("------------failed msg handler ----------- :  %s\n", string(msg.Message))
}

func exampleDirect() {
	var (
		routingKey   = "my_direct_routingKey"
		queueName    = "my_direct_queue"
		exchangeName = "exchange_direct"
		// queueName = ""
	)
	rabbitmq1, err1 := rabbit.NewMQDirect(context.Background(), exchangeName, queueName, routingKey, MQURL)
	defer rabbitmq1.Destroy()
	if err1 != nil {
		log.Println("rabbitmq1-----", err1)
	}
	rabbitmq2, err2 := rabbit.NewMQDirect(context.Background(), exchangeName, queueName, routingKey, MQURL)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println("rabbitmq2----", err2)
	}
	go func() {
		for i := 0; i < 100; i++ {
			time.Sleep(1 * time.Second)
			err := rabbitmq1.Publish("消息：" + strconv.Itoa(i))
			if err != nil {
				log.Println("-----------------example Publish error:", err)
				time.Sleep(3 * time.Second)
				// return
				log.Println("******** do Publish Direct Msg failed: ", "消息："+strconv.Itoa(i))
			}

		}
	}()
	//
	// 消费者1
	go func() {
		err := rabbitmq2.Consume(&Direct{})
		if err != nil {
			log.Println("----ConsumeFailToDlx Consume error 1: ", err)
			// return
		}
	}()
	// go func() {
	// 	err := rabbitmq2.Consume(doConsumeDirect2)
	// 	if err != nil {
	// 		log.Println("----ConsumeFailToDlx Consume error 2: ", err)
	// 		// return
	// 		time.Sleep(5 * time.Second)
	// 		rabbitmq2.Consume(doConsumeDirect2)
	// 	}
	// }()

	select {}
}
