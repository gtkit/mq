// @Author xiaozhaofu 2023/7/18 19:55:00
package rabbit

import (
	"errors"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
4 Routing路由模式
*/
type RabbitMqRouting struct {
	*RabbitMQ
}

// 获取路由模式下的rabbitmq的实例
func NewRabbitMQRouting(exchangeName, routingKey, mqUrl string) (rabbitMqRouting *RabbitMqRouting, err error) {
	// 判断是否输入必要的信息
	if exchangeName == "" || routingKey == "" || mqUrl == "" {
		log.Fatalf("ExchangeName, routingKey and mqUrl is required,\nbut exchangeName, routingKey and mqUrl are %s, %s and %s.", exchangeName, routingKey, mqUrl)
		return nil, errors.New("ExchangeName, routingKey and mqUrl is required")
	}
	rabbitmq, err := newRabbitMQ(exchangeName, "", routingKey, mqUrl)
	if err != nil {
		return nil, err
	}
	return &RabbitMqRouting{
		rabbitmq,
	}, nil
}

// 路由模式发送信息
func (r *RabbitMqRouting) Publish(message string) (err error) {
	// 1 尝试创建交换机，不存在创建
	err = r.channel.ExchangeDeclare(
		// 交换机名称
		r.ExchangeName,
		// 交换机类型 广播类型
		"direct",
		// 是否持久化
		true,
		// 是否字段删除
		false,
		// true表示这个exchange不可以被client用来推送消息，仅用来进行exchange和exchange之间的绑定
		false,
		// 是否阻塞 true表示要等待服务器的响应
		false,
		nil,
	)
	if err != nil {
		return err
	}

	// 2 发送信息
	err = r.channel.PublishWithContext(
		r.ctx,
		r.ExchangeName,
		// Binding Key
		r.Key,
		false,
		false,
		amqp.Publishing{
			// 类型
			ContentType: "text/plain",
			// 消息
			Body:       []byte(message),
			Expiration: r.MsgExpiration(),
		})
	if err != nil {
		return err
	}

	return nil
}

// 路由模式接收信息
func (r *RabbitMqRouting) Consume() (consumeChan <-chan amqp.Delivery, err error) {
	// 1 尝试创建交换机，不存在创建
	err = r.channel.ExchangeDeclare(
		// 交换机名称
		r.ExchangeName,
		// 交换机类型 广播类型
		"direct",
		// 是否持久化
		true,
		// 是否字段删除
		false,
		// true表示这个exchange不可以被client用来推送消息，仅用来进行exchange和exchange之间的绑定
		false,
		// 是否阻塞 true表示要等待服务器的响应
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	// 2 试探性创建队列
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

	// 3 绑定队列到exchange中
	err = r.channel.QueueBind(
		q.Name,
		// 在pub/sub模式下，这里的key要为空
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

	// 5 直接把amqp的channel返回给调用者
	return consumeChan, nil
}
