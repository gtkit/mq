// @Author xiaozhaofu 2023/8/28 15:01:00
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
	rabbitmq2, err1 := rabbit.NewConsumeDirect(rabbit.MQOption{
		ExchangeName: exchange,
		QueueName:    queueName,
		Routing:      routingKey,
		MqURL:        MQURL,
		ConnName:     "",
		Ctx:          context.Background(),
	})
	defer rabbitmq2.Destroy()
	if err1 != nil {
		log.Println(err1)
	}

	go func() {
		for i := 0; i < 100; i++ {
			time.Sleep(1 * time.Second)
			ctx, cancel := context.WithCancel(context.Background())
			rabbitmq1, _ := rabbit.NewPubDirect(rabbit.MQOption{
				ExchangeName: "exchange.delay",
				QueueName:    "",
				Routing:      "",
				MqURL:        MQURL,
				ConnName:     "",
				Ctx:          ctx,
			})

			msg := "消息：" + strconv.Itoa(i)
			err := rabbitmq1.Publish(msg, &DirectFailToDlx{})
			if err != nil {
				log.Println("----direct.dlx Publish error:", err)

			}
			log.Println("----Publish Dlx success: ", msg, " ----", time.Now().Format(time.DateTime))
			cancel()
			rabbitmq1.Destroy()
		}
	}()

	// 消费者1
	go func() {
		err := rabbitmq2.ConsumeFailToDlx(&DirectFailToDlx{})
		if err != nil {
			log.Println("----ConsumeFailToDlx Consume error: ", err)
			return
		}

	}()

	// 消费者2 死信消费
	go func() {
		err := rabbitmq2.ConsumeDlx(&DirectDlx{})
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

	rabbitmq2, err2 := rabbit.NewConsumeDirect(rabbit.MQOption{
		ExchangeName: exchange,
		QueueName:    "",
		Routing:      routingKey,
		MqURL:        MQURL,
		ConnName:     "",
		Ctx:          context.Background(),
	})
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}
	go func() {
		for i := 0; i < 100; i++ {
			time.Sleep(1 * time.Second)
			ctx, cancel := context.WithCancel(context.Background())
			rabbitmq1, _ := rabbit.NewPubDirect(rabbit.MQOption{
				ExchangeName: exchange,
				QueueName:    "",
				Routing:      routingKey,
				MqURL:        MQURL,
				ConnName:     "",
				Ctx:          ctx,
			})
			msg := "消息：" + strconv.Itoa(i)
			err := rabbitmq1.PublishDelay(msg, &DirectDelay{}, "2000")
			if err != nil {
				log.Println("----example3 Publish error:", err)
				return
			}
			log.Println("----PublishDelay success: ", msg, " ----", time.Now().Format(time.DateTime))
			cancel()
			rabbitmq1.Destroy()
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
	log.Println("------------Direct Consume Msg ----------- : ", string(msg), " -----", msgId)
	return nil
	// return errors.New("test retry error")
}
func (m *Direct) Failed(msg rabbit.FailedMsg) {
	log.Printf("------------Direct failed msg handler ----------- :  %s\n", string(msg.Message))
}

func exampleDirect() {
	var (
		routingKey   = "my_direct_routingKey"
		queueName    = "my_direct_queue"
		exchangeName = "exchange_direct"
		// queueName = ""
	)

	rabbitmq2, err2 := rabbit.NewConsumeDirect(rabbit.MQOption{
		ExchangeName: exchangeName,
		QueueName:    queueName,
		Routing:      routingKey,
		MqURL:        MQURL,
		ConnName:     "",
		Ctx:          context.Background(),
	})
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println("rabbitmq2----", err2)
	}
	go func() {
		for i := 0; i < 100; i++ {
			time.Sleep(1 * time.Second)
			ctx, cancel := context.WithCancel(context.Background())
			rabbitmq1, _ := rabbit.NewPubDirect(rabbit.MQOption{
				ExchangeName: "exchange.delay",
				QueueName:    "",
				Routing:      "",
				MqURL:        MQURL,
				ConnName:     "",
				Ctx:          ctx,
			})
			err := rabbitmq1.Publish("消息："+strconv.Itoa(i), &Direct{})
			if err != nil {
				log.Println("-----------------example Publish error:", err)
				time.Sleep(3 * time.Second)
				// return
				log.Println("******** do Publish Direct Msg failed: ", "消息："+strconv.Itoa(i))
			}
			cancel()
			rabbitmq1.Destroy()

		}
	}()
	//
	go func() {
		// for i := 0; i < 5; i++ {
		err := rabbitmq2.Consume(&Direct{})
		if err != nil {
			fmt.Println("----ConsumeFailToDlx Consume error 1: ", err)
			// return
		}
		// time.Sleep(2 * time.Second)
		// fmt.Println("consume----", i)
		// }
	}()

	select {}
}
