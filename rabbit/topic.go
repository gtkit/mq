// @Author xiaozhaofu 2023/7/18 19:56:00
package rabbit

import (
	"errors"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
5 Topic话题模式
*/
type RabbitMQTopic struct {
	*RabbitMQ
}

// 获取话题模式下的rabbitmq的实例
func NewRabbitMQTopic(exchangeName, routingKey, mqUrl string) (rabbitMQTopic *RabbitMQTopic, err error) {
	// 判断是否输入必要的信息
	if exchangeName == "" || routingKey == "" || mqUrl == "" {
		log.Fatalf("ExchangeName, routingKey and mqUrl is required,\nbut exchangeName, routingKey and mqUrl are %s and %s.", exchangeName, routingKey, mqUrl)
		return nil, errors.New("ExchangeName, routingKey and mqUrl is required")
	}
	rabbitmq, err := newRabbitMQ(exchangeName, "", routingKey, mqUrl)
	if err != nil {
		return nil, err
	}
	return &RabbitMQTopic{
		rabbitmq,
	}, nil
}

// topic模式。生产者。
func (r *RabbitMQTopic) Publish(message string) (err error) {
	// 1 尝试创建交换机,这里的kind的类型要改为topic
	err = r.channel.ExchangeDeclare(
		r.ExchangeName,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	// 2 发送消息。
	err = r.channel.Publish(
		r.ExchangeName,
		r.Key,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
	if err != nil {
		return err
	}
	return nil
}

// topic模式。消费者。"*"表示匹配一个单词。“#”表示匹配多个单词，亦可以是0个。
func (r *RabbitMQTopic) Consume() (consumeChan <-chan amqp.Delivery, err error) {
	// 1 创建交换机。这里的kind需要是“topic”类型。
	err = r.channel.ExchangeDeclare(
		r.ExchangeName,
		"topic",
		true, // 这里需要是true
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	// 2 创建队列。这里不用写队列名称。
	q, err := r.channel.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	// 3 将队列绑定到交换机里。
	err = r.channel.QueueBind(
		q.Name,
		r.Key,
		r.ExchangeName,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	// 4 消费消息
	consumeChan, err = r.channel.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return consumeChan, nil
}
