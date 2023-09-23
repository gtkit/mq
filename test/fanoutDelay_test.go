// @Author xiaozhaofu 2023/7/6 20:57:00
package test

import (
	"context"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/pkg/errors"

	"mq/rabbit"
)

func TestDealySubMq(t *testing.T) {
	exampleDelay()
}

func exampleDelay() {

	rabbitmq2, err2 := rabbit.NewConsumeFanout(rabbit.MQOption{
		ExchangeName: "exchange.delay",
		QueueName:    "",
		Routing:      "",
		MqURL:        MQURL,
		ConnName:     "",
		Ctx:          context.Background(),
	})
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}
	rabbitmq3, err3 := rabbit.NewConsumeFanout(rabbit.MQOption{
		ExchangeName: "exchange.delay",
		QueueName:    "",
		Routing:      "",
		MqURL:        MQURL,
		ConnName:     "",
		Ctx:          context.Background(),
	})
	defer rabbitmq3.Destroy()
	if err3 != nil {
		log.Println(err3)
	}

	go func() {
		for i := 0; i < 10000; i++ {
			time.Sleep(1 * time.Second)
			ctx, cancel := context.WithCancel(context.Background())
			rabbitmq1, _ := rabbit.NewPubSimple(rabbit.MQOption{
				ExchangeName: "exchange.delay",
				QueueName:    "",
				Routing:      "",
				MqURL:        MQURL,
				ConnName:     "",
				Ctx:          ctx,
			})
			if err := rabbitmq1.PublishDelay("消息："+strconv.Itoa(i), &DelayMsg1{}, "2000"); err != nil {
				log.Println("----PublishDelay error:", err)

			}
			log.Println("--------------Delay Publish ----------msg----", "消息："+strconv.Itoa(i), " time: "+time.Now().Format(time.DateTime))
			cancel()
			rabbitmq1.Destroy()
		}
	}()

	// 消费者1
	go func() {
		if err := rabbitmq2.ConsumeDelay(&DelayMsg1{}); err != nil {
			log.Println("----ConsumeDelay 1 error: ", err)
			return
		}
	}()

	// 消费者2
	go func() {
		if err := rabbitmq3.ConsumeDelay(&DelayMsg2{}); err != nil {
			log.Println("----ConsumeDelay 2 error: ", err)
			return
		}
	}()

	forever := make(chan bool)
	<-forever
}

type DelayMsg1 struct {
}

func (m *DelayMsg1) Process(msg []byte, msgId string) error {
	log.Println("------------fanout Consume Msg delay 1----------- : ", string(msg), " -----time: "+time.Now().Format(time.DateTime))
	// return nil
	return errors.New("test failed error")
}
func (m *DelayMsg1) Failed(msg rabbit.FailedMsg) {
	log.Printf("------------failed msg handler delay 1----------- :  %s\n", string(msg.Message))
}

type DelayMsg2 struct {
}

func (m *DelayMsg2) Process(msg []byte, msgId string) error {
	log.Println("------------fanout Consume Msg delay 2----------- : ", string(msg), " -----time: "+time.Now().Format(time.DateTime))
	return nil
}
func (m *DelayMsg2) Failed(msg rabbit.FailedMsg) {
	log.Printf("------------failed msg handler delay 2----------- :  %s\n", string(msg.Message))
}
