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

// NewRabbitMQSub 获取订阅模式下的rabbitmq的实例
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
		true,     // 持久化
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	// 2 发送消息
	return mq.RabbitMQ.channel.PublishWithContext(
		mq.ctx,
		mq.ExchangeName, // 交换机名称
		"",              // 路由参数，fanout类型交换机，自动忽略路由参数
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})

}

// 订阅模式消费者
// func (r *RabbitMqSubscription) Consume() (consumeChan <-chan amqp.Delivery, err error) {
func (mq *RabbitMqSubscription) Consume(handler func([]byte) error) (err error) {
	// 1 试探性创建交换机exchange
	err = mq.channel.ExchangeDeclare(
		mq.ExchangeName,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	// 2 试探性创建队列queue
	q, err := mq.channel.QueueDeclare(
		"", // 随机生产队列名称
		true,
		false,
		true, // true 表示这个queue只能被当前连接访问，当连接断开时queue会被删除
		false,
		nil,
	)
	if err != nil {
		return err
	}

	// 3 绑定队列到交换机中
	err = mq.channel.QueueBind(
		q.Name,
		"", // 在pub/sub模式下key要为空
		mq.ExchangeName,
		false, // 默认为非阻塞即可设置为false
		nil,
	)
	if err != nil {
		return err
	}

	// 4 消费消息
	deliveries, err := mq.channel.Consume(
		q.Name,
		"",    // 消费者名字，不填自动生成一个
		false, // 自动向队列确认消息已经处理
		false, // true 表示这个queue只能被这个consumer访问
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	for msg := range deliveries {
		select {
		case <-mq.Ctx().Done():
			fmt.Println("======ctx done==========")
			_ = msg.Reject(true)
			return fmt.Errorf("context cancel Consume")
		default:

		}
		// fmt.Println("-----messageId: ", msg.MessageId)
		err = handler(msg.Body)
		// 消费失败处理
		if err != nil {
			_ = msg.Reject(true)
			_ = msg.Nack(true, true)
			continue
		}
		err = msg.Ack(false) // true: 此交付和同一频道上所有先前未确认的交付将被确认。这对于批量处理交付很有用
		if err != nil {
			continue
		}

	}

	return nil
}
