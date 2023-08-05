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

func (r *RabbitMQ) ConsumeDelayQueue(queueName, dlxExchange, routing string, f func(interface{})) error {

	err := r.channel.ExchangeDeclare(
		dlxExchange,
		"direct", // 交换机类型 路由模式接收
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	// 声明 死信队列（用于与死信交换机绑定）
	q, err := r.channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	// 绑定队列到 exchange 中
	err = r.channel.QueueBind(
		q.Name,
		routing,
		dlxExchange,
		false,
		nil)
	if err != nil {
		return err
	}

	// 消费消息
	data, err := r.channel.Consume(
		q.Name,
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

	forever := make(chan bool)
	go func() {
		for d := range data {
			fmt.Printf("Received a message: %s\n", d.Body)
			f(d.Body)
		}
	}()
	<-forever
	return nil
}
