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

// RabbitMQInterface 定义rabbitmq的接口
var _ RabbitMQInterface = (*RabbitMqSubscription)(nil)

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
func (mq *RabbitMqSubscription) Publish(message string) error {
	select {
	case <-mq.ctx.Done():
		return fmt.Errorf("context cancel publish" + mq.ctx.Err().Error())
	default:
	}

	// 确认消息监听函数， 启动一个协程，监听消息发送情况
	mq.ListenConfirm()

	// 1 尝试连接交换机
	if err := mq.exchangeDeclare(); err != nil {
		logger.Info("Publish exchangeDeclare error: ", err)
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
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent,
			Body:         []byte(message),
			//Expiration:   mq.MsgExpiration(), // 消息过期时间 单位毫秒
		})

}

// Consume 订阅模式消费者
func (mq *RabbitMqSubscription) Consume(handler func([]byte) error) error {
	// 1 创建交换机exchange
	logger.Info("----- begin consume----")
	if err := mq.exchangeDeclare(); err != nil {
		logger.Info("Consume exchangeDeclare error: ", err)
		return err
	}

	// 2 创建队列queue
	q, err := mq.queueDeclare()
	if err != nil {
		logger.Info("Consume queueDeclare error: ", err)
		return err
	}

	// 3 绑定队列到交换机中
	err = mq.channel.QueueBind(
		q.Name,          // 队列名称
		"",              // 在pub/sub模式下key要为空
		mq.ExchangeName, // 交换机名称
		false,           // 默认为非阻塞即可设置为false
		nil,
	)
	if err != nil {
		return err
	}

	// 4 消费消息
	deliveries, err := mq.channel.Consume(
		q.Name, // 队列名称
		"",     // 消费者名字，不填自动生成一个
		false,  // 自动向队列确认消息已经处理
		false,  // true 表示这个queue只能被这个consumer访问
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	for msg := range deliveries {
		select {
		case <-mq.Ctx().Done(): // 通过context控制消费者退出
			fmt.Println("======ctx done==========")
			// 拒绝一条消息，true表示将消息重新放回队列, 如果失败，记录日志 或 发送到其他队列(死信队列)等措施来处理错误
			if err := msg.Reject(true); err != nil {
				fmt.Println("ack error: ", err)
			}

			return fmt.Errorf("context cancel Consume")
		default:

		}

		// 处理消息
		err = handler(msg.Body)
		// 消费失败处理
		if err != nil {
			if err := msg.Reject(true); err != nil {
				// 拒绝一条消息，true表示将消息重新放回队列, 如果失败，记录日志 或 发送到其他队列等措施来处理错误
				fmt.Println("reject error: ", err)
			}
			continue
		}
		// 消费成功确认消息
		if err := msg.Ack(false); err != nil {
			// 确认一条消息，false表示确认当前消息，true表示确认当前消息和之前所有未确认的消息
			fmt.Println("ack error: ", err)
			continue
		}

	}

	return nil
}

func (mq *RabbitMqSubscription) exchangeDeclare() error {
	return mq.channel.ExchangeDeclare(
		mq.ExchangeName,
		"fanout", // 这里一定要设计为"fanout"也就是广播类型。
		true,     // 持久化
		false,
		false,
		false,
		nil,
	)
}
func (mq *RabbitMqSubscription) queueDeclare() (amqp.Queue, error) {
	return mq.channel.QueueDeclare(
		"", // 随机生产队列名称
		true,
		false,
		true, // true 表示这个queue只能被当前连接访问，当连接断开时queue会被删除
		false,
		nil,
	)
}
