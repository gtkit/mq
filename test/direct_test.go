// @Author xiaozhaofu 2023/8/28 15:01:00
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

func exampleDirectDlx() {
	var (
		routingKey = "key.direct"
		exchange   = "exchange.direct"
		queueName  = "queue.direct"
	)
	rabbitmq1, err1 := rabbit.NewRabbitMQDirect(exchange, queueName, routingKey, MQURL)
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
				fmt.Println("----direct.dlx Publish error:", err)
				return
			}
			fmt.Println("----Publish Dlx success: ", msg, " ----", time.Now().Format(time.DateTime))
		}
	}()

	// 消费者1
	go func() {
		err := rabbitmq1.ConsumeFailToDlx(doConsumeDirectFailToDlx)
		if err != nil {
			fmt.Println("----ConsumeFailToDlx Consume error: ", err)
			return
		}

	}()

	// 消费者2 死信消费
	go func() {
		err := rabbitmq1.ConsumeDlx(doConsumeDirectDlx)
		if err != nil {
			fmt.Println("----ConsumeDlx Consume error: ", err)
			return
		}
	}()
	select {}
}

func exampleDirectDelay() {
	var (
		routingKey = "key.direct.delay"
		exchange   = "exchange.direct.delay"
	)
	rabbitmq1, err1 := rabbit.NewRabbitMQDirect(exchange, "", routingKey, MQURL)
	defer rabbitmq1.Destroy()
	if err1 != nil {
		log.Println(err1)
	}
	rabbitmq2, err2 := rabbit.NewRabbitMQDirect(exchange, "", routingKey, MQURL)
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
				fmt.Println("----example3 Publish error:", err)
				return
			}
			fmt.Println("----PublishDelay success: ", msg, " ----", time.Now().Format(time.DateTime))
		}
	}()

	// 消费者1
	go func() {
		err := rabbitmq2.ConsumeDelay(doConsumeDirectDelay)
		if err != nil {
			fmt.Println("----ConsumeFailToDlx Consume error: ", err)
			return
		}

	}()
	select {}

}

func exampleDirect() {
	var routingKey = "key.direct"
	rabbitmq1, err1 := rabbit.NewRabbitMQDirect("exchange.direct", "", routingKey, MQURL)
	defer rabbitmq1.Destroy()
	if err1 != nil {
		log.Println(err1)
	}
	rabbitmq2, err2 := rabbit.NewRabbitMQDirect("exchange.direct", "", routingKey, MQURL)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}
	go func() {
		for i := 0; i < 100; i++ {
			time.Sleep(1 * time.Second)
			err := rabbitmq1.Publish("消息：" + strconv.Itoa(i))
			if err != nil {
				fmt.Println("----example3 Publish error:", err)
				return
			}
		}
	}()
	//
	// 消费者1
	go func() {
		err := rabbitmq2.Consume(doConsumeDirect)
		if err != nil {
			fmt.Println("----ConsumeFailToDlx Consume error: ", err)
			return
		}
	}()

	select {}
}

func doConsumeDirect(msg []byte) error {
	fmt.Println(".....doConsume Direct ....Msg: ", string(msg))
	return nil
}

func doConsumeDirectDelay(msg []byte) error {
	fmt.Println("----ConsumeDelay success: ", string(msg), " ----", time.Now().Format(time.DateTime))
	return nil
}

func doConsumeDirectDlx(msg []byte) error {
	fmt.Println("----Consume Direct FailToDlx success: ", string(msg), " ----", time.Now().Format(time.DateTime))
	return nil
}

func doConsumeDirectFailToDlx(msg []byte) error {
	fmt.Println("-----doConsumeFailToDlx --Msg: ", string(msg))
	// return nil
	return errors.New("...doConsumeFailToDlx error for test...")
}
