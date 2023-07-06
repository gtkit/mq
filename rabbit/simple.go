// @Author xiaozhaofu 2023/7/6 20:59:00
package rabbit

import (
	"errors"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
1 Simple模式，最简单最常用的模式
2 Work模式，一个消息只能被一个消费者消费
*/

// RabbitMQSimple 简单模式下的RabbitMQ实例
type RabbitMQSimple struct {
	*RabbitMQ
}

// NewRabbitMQSimple 创建简单模式下的实例，只需要queueName这个参数，其中exchange是默认的，key则不需要。
func NewRabbitMQSimple(queueName, mqUrl string) (rabbitMQSimple *RabbitMQSimple, err error) {
	// 判断是否输入必要的信息
	if queueName == "" || mqUrl == "" {
		log.Printf("QueueName and mqUrl is required,\nbut queueName and mqUrl are %s and %s.", queueName, mqUrl)
		return nil, errors.New("QueueName and mqUrl is required")
	}
	rabbitmq, err := NewRabbitMQ("", queueName, "", mqUrl)
	if err != nil {
		return nil, err
	}
	return &RabbitMQSimple{
		rabbitmq,
	}, nil
}

// Publish 直接模式,生产者.
func (r *RabbitMQSimple) Publish(message string) (err error) {
	// 1 申请队列，如不存在，则自动创建之，存在，则路过。
	_, err = r.channel.QueueDeclare(
		r.QueueName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	// 2 发送消息到队列中
	err = r.channel.Publish(
		r.ExchangeName,
		r.QueueName,
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

// Consume 直接模式，消费者
func (r *RabbitMQSimple) Consume() (consumeChan <-chan amqp.Delivery, err error) {
	// 1 申请队列,如果队列不存在则自动创建,存在则跳过
	q, err := r.channel.QueueDeclare(
		r.QueueName,
		false, // 是否持久化
		false, // 是否自动删除
		false, // 是否具有排他性
		false, // 是否阻塞处理
		nil,   // 额外的属性
	)
	if err != nil {
		return nil, err
	}

	// 2 接收消息
	consumeChan, err = r.channel.Consume(
		q.Name,
		"",   // 用来区分多个消费者
		true, // 是否自动应答,告诉我已经消费完了
		false,
		false, // 若设置为true,则表示为不能将同一个connection中发送的消息传递给这个connection中的消费者.
		false, // 消费队列是否设计阻塞
		nil,
	)
	if err != nil {
		return nil, err
	}

	return consumeChan, nil
}
