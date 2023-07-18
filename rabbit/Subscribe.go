// @Author xiaozhaofu 2023/7/18 19:53:00
package rabbit

import (
	"errors"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
3 Publish/Subscribe发布订阅模式
*/
type RabbitMqSubscription struct {
	*RabbitMQ
}

// NewRabbitMQSubscription 获取订阅模式下的rabbitmq的实例
func NewRabbitMQSub(exchangeName, mqUrl string) (rabbitMqSubscription *RabbitMqSubscription, err error) {
	// 判断是否输入必要的信息
	if exchangeName == "" || mqUrl == "" {
		log.Fatalf("ExchangeName and mqUrl is required,\nbut exchangeName and mqUrl are %s and %s.", exchangeName, mqUrl)
		return nil, errors.New("ExchangeName and mqUrl is required")
	}
	// 创建rabbitmq实例
	rabbitmq, err := newRabbitMQ(exchangeName, "", "", mqUrl)
	if err != nil {
		return nil, err
	}
	rabbitmq.SetConfirm()
	return &RabbitMqSubscription{
		rabbitmq,
	}, nil
}

// 订阅模式发布消息
func (mq *RabbitMqSubscription) Publish(message string) (err error) {
	select {
	case <-mq.ctx.Done():
		return fmt.Errorf("context cancel publish" + mq.ctx.Err().Error())
	default:
	}

	// 确认消息监听函数， 启动一个协程，监听消息发送情况
	mq.ListenConfirm()

	// 1 尝试连接交换机
	err = mq.channel.ExchangeDeclare(
		mq.ExchangeName,
		"fanout", // 这里一定要设计为"fanout"也就是广播类型。
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	// 2 发送消息
	err = mq.RabbitMQ.channel.PublishWithContext(
		mq.ctx,
		mq.RabbitMQ.ExchangeName, // 交换机名称
		"",
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

// 订阅模式消费者
func (r *RabbitMqSubscription) Consume() (consumeChan <-chan amqp.Delivery, err error) {
	// 1 试探性创建交换机exchange
	err = r.channel.ExchangeDeclare(
		r.ExchangeName,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	// 2 试探性创建队列queue
	q, err := r.channel.QueueDeclare(
		"", // 随机生产队列名称
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	// 3 绑定队列到交换机中
	err = r.channel.QueueBind(
		q.Name,
		"", // 在pub/sub模式下key要为空
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
