// @Author xiaozhaofu 2023/7/18 19:56:00
package rabbit

import (
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
5 Topic话题模式
*/
type MqTopic struct {
	*RabbitMQ
}

// NewMQTopic 获取话题模式下的rabbitmq的实例
func NewMQTopic(exchangeName, routingKey, mqUrl string) (rabbitMQTopic *MqTopic, err error) {
	// 判断是否输入必要的信息
	if exchangeName == "" || routingKey == "" || mqUrl == "" {
		return nil, errors.New("ExchangeName, routingKey and mqUrl is required")
	}
	rabbitmq, err := newRabbitMQ(exchangeName, "", routingKey, mqUrl)
	if err != nil {
		return nil, err
	}
	return &MqTopic{
		rabbitmq,
	}, nil
}

// Publish topic模式。生产者。
func (r *MqTopic) Publish(message string) (err error) {
	select {
	case <-r.ctx.Done():
		return fmt.Errorf("context cancel publish" + r.ctx.Err().Error())
	default:
	}

	// 确认消息监听函数， 启动一个协程，监听消息发送情况
	r.ListenConfirm()

	// 1 尝试创建交换机,这里的kind的类型要改为topic
	if err = r.exchangeDeclare(); err != nil {
		return err
	}

	// 2 发送消息。
	return r.channel.PublishWithContext(
		r.ctx,
		r.ExchangeName,
		r.Key,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})

}

// Consume topic模式。消费者。"*"表示匹配一个单词。“#”表示匹配多个单词，亦可以是0个。
func (r *MqTopic) Consume(handler func([]byte) error) error {
	// 1 创建交换机。这里的kind需要是“topic”类型。
	if err := r.exchangeDeclare(); err != nil {
		return err
	}

	// 2 创建队列。这里不用写队列名称。
	if err := r.queueDeclare(); err != nil {
		return err
	}

	// 3 将队列绑定到交换机里。
	if err := r.channel.QueueBind(
		r.QueueName,
		"*."+r.Key, // 路由参数，关键参数，使用了通配符 * 星号，匹配一个单词，如果使用 # 井号可以匹配多个单词.
		r.ExchangeName,
		false,
		nil,
	); err != nil {
		return err
	}

	// 4 消费消息
	deliveries, err := r.channel.Consume(
		r.QueueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	for msg := range deliveries {
		select {
		case <-r.Ctx().Done(): // 通过context控制消费者退出
			logger.Info("fanout Consume context cancel Consume")
			// 拒绝一条消息，true表示将消息重新放回队列, 如果失败，记录日志 或 发送到其他队列(死信队列)等措施来处理错误
			if err := msg.Reject(true); err != nil {
				logger.Info("ack error: ", err)
			}
			return errors.New("context cancel Consume")
		default:
		}

		// 处理消息
		err = handler(msg.Body)
		// 消费失败处理
		if err != nil {
			// 拒绝一条消息，true表示将消息重新放回队列, 如果失败，记录日志 或 发送到其他队列等措施来处理错误
			if err := msg.Reject(true); err != nil {
				logger.Info("reject error: ", err)
			}
			continue
		}
		// 消费成功确认消息
		if err := msg.Ack(false); err != nil {
			// 确认一条消息，false表示确认当前消息，true表示确认当前消息和之前所有未确认的消息
			logger.Info("ack error: ", err)
			return err
		}

	}

	return nil
}

func (r *MqTopic) exchangeDeclare() error {
	return r.channel.ExchangeDeclare(
		// 交换机名称
		r.ExchangeName,
		// 交换机类型 广播类型
		"topic",
		// 是否持久化
		true,
		// 是否自动删除
		false,
		// true表示这个exchange不可以被client用来推送消息，仅用来进行exchange和exchange之间的绑定
		false,
		// 是否阻塞 true表示要等待服务器的响应
		false,
		nil,
	)
}
