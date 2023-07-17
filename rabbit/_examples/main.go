// @Author xiaozhaofu 2023/7/4 20:35:00
package main

import (
	"log"
	"strconv"
	"time"

	"mq/rabbit/_examples/mq"
)

// conn, err := amqp.Dial(fmt.Sprintf(
// "amqp://%s:%s@%s:%s%s",
// config.Viper.GetString("rabbitmq.username"),
// config.Viper.GetString("rabbitmq.password"),
// config.Viper.GetString("rabbitmq.host"),
// config.Viper.GetString("rabbitmq.port"),
// "/"+strings.TrimPrefix(config.Viper.GetString("rabbitmq.vhost"), "/"),
// ))

const MQURL = "amqp://guest:guest@127.0.0.1:5672/"

func main() {
	// example12()
	example3()
	// example4()
	// example5()
}

// eg. Work Mode 1/2
func example12() {
	rabbitmq1, err1 := mq.NewRabbitMQSimple("queue1", MQURL)
	defer rabbitmq1.Destroy()
	if err1 != nil {
		log.Println(err1)
	}
	rabbitmq2, err2 := mq.NewRabbitMQSimple("queue1", MQURL)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}

	go func() {
		for i := 0; i < 100; i++ {
			time.Sleep(1 * time.Second)
			rabbitmq1.Publish("消息：" + strconv.Itoa(i))
		}
	}()

	go func() {
		msgs, err3 := rabbitmq2.Consume()
		if err3 != nil {
			log.Println(err3)
		}
		for d := range msgs {
			log.Printf("接受到了：%s", d.Body)
		}
	}()

	forever := make(chan bool)
	<-forever
}

// eg. Work Mode 3
func example3() {
	rabbitmq1, err1 := mq.NewRabbitMQSub("exchange.example3", MQURL)
	defer rabbitmq1.Destroy()
	if err1 != nil {
		log.Println(err1)
	}
	rabbitmq2, err2 := mq.NewRabbitMQSub("exchange.example3", MQURL)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}

	go func() {
		for i := 0; i < 10000; i++ {
			rabbitmq1.Publish("消息：" + strconv.Itoa(i))
		}
	}()

	go func() {
		msgs, err3 := rabbitmq2.Consume()
		if err3 != nil {
			log.Println(err3)
		}
		for d := range msgs {
			log.Printf("接受到了：%s", d.Body)
		}
	}()

	forever := make(chan bool)
	<-forever
}

// eg. Work Mode 4
func example4() {
	rabbitmq1, err1 := mq.NewRabbitMQRouting("exchange.example4", "key.one", MQURL)
	defer rabbitmq1.Destroy()
	if err1 != nil {
		log.Println(err1)
	}
	rabbitmq2, err2 := mq.NewRabbitMQRouting("exchange.example4", "key.one", MQURL)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}
	rabbitmq3, err3 := mq.NewRabbitMQRouting("exchange.example4", "key.two", MQURL)
	defer rabbitmq3.Destroy()
	if err3 != nil {
		log.Println(err3)
	}

	go func() {
		for i := 0; i < 10000; i++ {
			rabbitmq1.Publish("消息：" + strconv.Itoa(i))
		}
	}()

	go func() {
		msgs, err4 := rabbitmq2.Consume()
		if err4 != nil {
			log.Println(err4)
		}
		for d := range msgs {
			log.Printf("key.one接受到了：%s", d.Body)
		}
	}()
	go func() {
		msgs, err5 := rabbitmq3.Consume()
		if err5 != nil {
			log.Println(err5)
		}
		for d := range msgs {
			log.Printf("key.two接受到了：%s", d.Body)
		}
	}()

	forever := make(chan bool)
	<-forever
}

// eg. Work Mode 5
func example5() {
	rabbitmq1, err1 := mq.NewRabbitMQTopic("exchange.example5", "key.one", MQURL)
	defer rabbitmq1.Destroy()
	if err1 != nil {
		log.Println(err1)
	}
	rabbitmq2, err2 := mq.NewRabbitMQTopic("exchange.example5", "key.one", MQURL)
	defer rabbitmq2.Destroy()
	if err2 != nil {
		log.Println(err2)
	}
	rabbitmq3, err3 := mq.NewRabbitMQTopic("exchange.example5", "key.two", MQURL)
	defer rabbitmq3.Destroy()
	if err3 != nil {
		log.Println(err3)
	}
	rabbitmq4, err4 := mq.NewRabbitMQTopic("exchange.example5", "key.*", MQURL)
	defer rabbitmq4.Destroy()
	if err4 != nil {
		log.Println(err4)
	}

	go func() {
		for i := 0; i < 10000; i++ {
			rabbitmq1.Publish("消息：" + strconv.Itoa(i))
		}
	}()

	go func() {
		msgs, err5 := rabbitmq2.Consume()
		if err5 != nil {
			log.Println(err5)
		}
		for d := range msgs {
			log.Printf("key.one接受到了：%s", d.Body)
		}
	}()
	go func() {
		msgs, err6 := rabbitmq3.Consume()
		if err6 != nil {
			log.Println(err6)
		}
		for d := range msgs {
			log.Printf("key.two接受到了：%s", d.Body)
		}
	}()
	go func() {
		msgs, err7 := rabbitmq4.Consume()
		if err7 != nil {
			log.Println(err7)
		}
		for d := range msgs {
			log.Printf("key.*接受到了：%s", d.Body)
		}
	}()

	forever := make(chan bool)
	<-forever
}
