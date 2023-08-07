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
