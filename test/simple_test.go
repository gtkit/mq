package test

import (
	"context"
	"log"
	"strconv"
	"testing"
	"time"

	"mq/rabbit"
)

func TestSimpleMq(t *testing.T) {
	example12()
}

type Consumefail struct{}

func (m *Consumefail) Process(msg []byte, msgId string) error {
	log.Println("------------Simple Consume Msg ----------- : ", string(msg), " -----", msgId)
	return nil
	// return errors.New("test retry error")
}
func (m *Consumefail) Failed(msg rabbit.FailedMsg) {
	log.Printf("------------failed msg handler ----------- :  %s\n", string(msg.Message))
}

func example12() {
	var queueName = "queue-simple"
	rabbitmq2, err2 := rabbit.NewConsumeSimple(rabbit.MQOption{
		ExchangeName: "",
		QueueName:    queueName,
		Routing:      "",
		MqURL:        MQURL,
		ConnName:     "123",
		Ctx:          context.Background(),
	})
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}

	go func() {
		for i := 0; i < 100; i++ {
			ctx, cancel := context.WithCancel(context.Background())
			rabbitmq1, _ := rabbit.NewPubSimple(rabbit.MQOption{
				ExchangeName: "",
				QueueName:    queueName,
				Routing:      "",
				MqURL:        MQURL,
				ConnName:     "121",
				Ctx:          ctx,
			})

			err := rabbitmq1.Publish("消息："+strconv.Itoa(i), &Consumefail{})
			if err != nil {
				log.Println("pulish err: ", err)
				// return
			}
			time.Sleep(2 * time.Second)
			cancel()
			rabbitmq1.Destroy()
		}
	}()

	// simple 消费
	go func() {
		err := rabbitmq2.Consume(&Consumefail{})
		if err != nil {
			log.Println("----Consume error: ", err)
			return
		}

	}()

	forever := make(chan bool)
	<-forever
}

func TestSimpleMqDlx(t *testing.T) {
	example12Dlx()
}

type SimpleMqDlx struct {
}

func (m *SimpleMqDlx) Process(msg []byte, msgId string) error {
	return nil
}
func (m *SimpleMqDlx) Failed(msg rabbit.FailedMsg) {

}

type doDlx struct {
}

func (m *doDlx) Process(msg []byte, msgId string) error {
	return nil
}
func (m *doDlx) Failed(msg rabbit.FailedMsg) {

}
func example12Dlx() {
	var queueName = "queue3-dlx"

	rabbitmq2, err2 := rabbit.NewConsumeSimple(rabbit.MQOption{
		ExchangeName: "",
		QueueName:    queueName,
		Routing:      "",
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
			time.Sleep(2 * time.Second)
			ctx, cancel := context.WithCancel(context.Background())
			rabbitmq1, _ := rabbit.NewPubSimple(rabbit.MQOption{
				ExchangeName: "",
				QueueName:    queueName,
				Routing:      "",
				MqURL:        MQURL,
				ConnName:     "",
				Ctx:          ctx,
			})
			err := rabbitmq1.Publish("消息："+strconv.Itoa(i), &SimpleMqDlx{})

			if err != nil {
				log.Println("pulish err: ", err)
			}
			cancel()
			rabbitmq1.Destroy()

		}
	}()

	// simple 消费
	go func() {
		err := rabbitmq2.ConsumeFailToDlx(&SimpleMqDlx{})
		if err != nil {
			log.Println("----Consume error: ", err)
			return
		}

	}()

	// 死信消费
	go func() {
		err := rabbitmq2.ConsumeDlx(&doDlx{})
		if err != nil {
			log.Println("----DlqConsume error: ", err)
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

type SimpleDelay struct {
}

func (m *SimpleDelay) Process(msg []byte, msgId string) error {
	return nil
}
func (m *SimpleDelay) Failed(msg rabbit.FailedMsg) {

}
func example12Delay() {
	var queueName = "delay-queue"
	rabbitmq2, err2 := rabbit.NewConsumeSimple(rabbit.MQOption{
		ExchangeName: "",
		QueueName:    queueName,
		Routing:      "",
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
			rabbitmq1, _ := rabbit.NewPubSimple(rabbit.MQOption{
				ExchangeName: "",
				QueueName:    queueName,
				Routing:      "",
				MqURL:        MQURL,
				ConnName:     "",
				Ctx:          ctx,
			})

			err := rabbitmq1.PublishDelay("消息："+strconv.Itoa(i), &SimpleDelay{}, "2000")

			if err != nil {
				log.Println("pulish err: ", err)
			}
			cancel()
			rabbitmq1.Destroy()

		}
	}()

	// 死信
	go func() {
		err := rabbitmq2.ConsumeDelay(&SimpleDelay{})
		if err != nil {
			log.Println("----DlqConsume error: ", err)
			return
		}
		time.Sleep(2 * time.Second)

	}()

	forever := make(chan bool)
	<-forever
}
