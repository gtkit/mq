// @Author xiaozhaofu 2023/8/5 19:49:00
package rabbit

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (r *RabbitMQ) PublishDelayQueue(queue, message, dlxExchange, routing, expiration string) error {
	queueName := queue + "_delay"
	_, err := r.channel.QueueDeclare(
		queueName,
		true,
		false,
		false, // 队列解锁
		false,
		amqp.Table{
			"x-dead-letter-exchange":    dlxExchange, // 声明当前队列绑定的 死信交换机
			"x-dead-letter-routing-key": routing,     // routing 模式路由名
		},
	)
	if err != nil {
		return err
	}

	// 注入消息 注册路由 routingKey
	err = r.channel.Publish(
		"",
		queueName,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
			Expiration:  expiration,
		})
	if err != nil {
		return err
	}

	fmt.Printf("push messag %s\n", message)
	return nil
}

// 消费死信队列
func (r *RabbitMQ) DlqConsume(queueName, dlxExchange, dlxRouting string, handler func([]byte) error) error {

	fmt.Println("--------------DlqConsume --------------")
	// 声明死信交换机
	if err := r.DlxDeclare(dlxExchange, "fanout"); err != nil {
		return err
	}

	// 声明 死信队列（用于与死信交换机绑定）
	q, err := r.channel.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		return err
	}

	// 绑定队列到 exchange 中
	if err := r.channel.QueueBind(q.Name, dlxRouting, dlxExchange, false, nil); err != nil {
		return err
	}

	// 消费消息
	deliveries, err := r.channel.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	for msg := range deliveries {
		select {
		case <-r.Ctx().Done():
			if err := msg.Reject(true); err != nil {
				// 拒绝一条消息，true表示将消息重新放回队列, 如果失败，记录日志 或 发送到其他队列等措施来处理错误
				fmt.Println("reject error: ", err)
			}
			return fmt.Errorf("context cancel Consume")
		default:

		}
		if err := handler(msg.Body); err != nil {
			if err = msg.Reject(true); err != nil {
				fmt.Println("reject error: ", err)
			}
			continue
		}
		if err := msg.Ack(false); err != nil {
			fmt.Println("---消息确认失败：", err)
			return err
		}

	}
	return nil

}

// DlxDeclare 声明死信交换机
// dlxExchange 死信交换机名称
// routingKind 死信交换机类型
func (mq *RabbitMQ) DlxDeclare(dlxExchange, routingKind string) error {
	fmt.Println("------------------DlxDeclare---------------")
	// 死信交换机
	return mq.channel.ExchangeDeclare(
		dlxExchange, // 死信交换机名字
		routingKind, // 死信交换机类型
		true,        // 是否持久化
		false,
		false,
		false,
		nil,
	)
}
