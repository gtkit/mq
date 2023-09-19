package test

import (
	"context"
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

type Consumefail struct {
}

func (m *Consumefail) Process(msg []byte, msgId string) error {
	fmt.Println("------------Simple Consume Msg ----------- : ", string(msg), " -----", msgId)
	// return nil
	return errors.New("test retry error")
}
func (m *Consumefail) Failed(msg rabbit.FailedMsg) {
	fmt.Printf("------------failed msg handler ----------- :  %s\n", string(msg.Message))
}

func example12() {
	var queueName = "queue-simple"
	rabbitmq1, err1 := rabbit.NewMQSimple(context.Background(), queueName, MQURL)
	defer rabbitmq1.Destroy()
	if err1 != nil {
		log.Println(err1)
	}
	rabbitmq2, err2 := rabbit.NewMQSimple(context.Background(), queueName, MQURL)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}

	go func() {
		for i := 0; i < 100; i++ {
			time.Sleep(2 * time.Second)
			err := rabbitmq1.Publish("消息：" + strconv.Itoa(i))

			if err != nil {
				fmt.Println("pulish err: ", err)
				return
			}

		}
	}()

	// simple 消费
	go func() {
		var h rabbit.MsgHandler
		h = &Consumefail{}
		err := rabbitmq2.Consume(h)
		if err != nil {
			fmt.Println("----Consume error: ", err)
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

func (m *SimpleMqDlx) Process([]byte, string) error {
	return nil
}
func (m *SimpleMqDlx) Failed(msg rabbit.FailedMsg) {

}

type doDlx struct {
}

func (m *doDlx) Process([]byte, string) error {
	return nil
}
func (m *doDlx) Failed(msg rabbit.FailedMsg) {

}
func example12Dlx() {
	var queueName = "queue3-dlx"

	rabbitmq1, err1 := rabbit.NewMQSimple(context.Background(), queueName, MQURL)
	defer rabbitmq1.Destroy()
	if err1 != nil {
		log.Println(err1)
	}
	rabbitmq2, err2 := rabbit.NewMQSimple(context.Background(), queueName, MQURL)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}

	go func() {
		for i := 0; i < 100; i++ {
			time.Sleep(2 * time.Second)
			err := rabbitmq1.Publish("消息：" + strconv.Itoa(i))

			if err != nil {
				fmt.Println("pulish err: ", err)
				return
			}

		}
	}()

	// simple 消费
	go func() {
		err := rabbitmq2.ConsumeFailToDlx(&SimpleMqDlx{})
		if err != nil {
			fmt.Println("----Consume error: ", err)
			return
		}

	}()

	// 死信消费
	go func() {
		err := rabbitmq2.ConsumeDlx(&doDlx{})
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

type SimpleDelay struct {
}

func (m *SimpleDelay) Process([]byte, string) error {
	return nil
}
func (m *SimpleDelay) Failed(msg rabbit.FailedMsg) {

}
func example12Delay() {
	var queueName = "delay-queue"
	rabbitmq1, err1 := rabbit.NewMQSimple(context.Background(), queueName, MQURL)
	defer rabbitmq1.Destroy()
	if err1 != nil {
		log.Println(err1)
	}
	rabbitmq2, err2 := rabbit.NewMQSimple(context.Background(), queueName, MQURL)
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
		err := rabbitmq2.ConsumeDelay(&SimpleDelay{})
		if err != nil {
			fmt.Println("----DlqConsume error: ", err)
			return
		}
		time.Sleep(2 * time.Second)

	}()

	forever := make(chan bool)
	<-forever
}

func doConsumeMsg(msg []byte, msgId string) error {
	fmt.Println("------------Simple Consume Msg ----------- : ", string(msg))
	// return nil
	return errors.New("test retry error")
}
func doConsumeMsgDlx(msg []byte, msgId string) error {
	fmt.Println("------------Simple ConsumeMsg dlx error: ", string(msg))
	// return nil
	return errors.New("test dlx error")
}
func dlxDo(msg []byte, msgId string) error {
	fmt.Println("------------DLX message: ", string(msg))

	return nil
}
func dlxDelay(msg []byte, msgId string) error {
	fmt.Println("--------------Delay Consume ----------msg----", string(msg), " time: "+time.Now().Format(time.DateTime))
	return nil
}

func doFailedMsg(fm rabbit.FailedMsg) {
	fmt.Printf("---failed msg: %+v\n", fm)
}
