// @Author xiaozhaofu 2023/7/18 19:53:00
package rabbit

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

/*
3 Publish/Subscribe发布订阅模式
*/

// RabbitMQInterface 定义rabbitmq的接口
var _ RabbitMQInterface = (*RabbitMqFanout)(nil)

type RabbitMqFanout struct {
	*RabbitMQ
}

// NewRabbitMQFanout 获取订阅模式下的rabbitmq的实例
func NewRabbitMQFanout(exchangeName, mqUrl string) (rabbitMqFanout *RabbitMqFanout, err error) {
	// 判断是否输入必要的信息
	if exchangeName == "" || mqUrl == "" {
		return nil, errors.New("ExchangeName and mqUrl is required")
	}
	// 创建rabbitmq实例
	rabbitmq, err := newRabbitMQ(exchangeName, "", "", mqUrl)
	if err != nil {
		return nil, err
	}
	if err = rabbitmq.SetConfirm(); err != nil {
		return nil, err
	}

	return &RabbitMqFanout{
		rabbitmq,
	}, nil
}

// Publish 订阅模式发布消息
func (r *RabbitMqFanout) Publish(message string) error {
	select {
	case <-r.ctx.Done():
		return fmt.Errorf("context cancel publish" + r.ctx.Err().Error())
	default:
	}

	// 确认消息监听函数， 启动一个协程，监听消息发送情况
	r.ListenConfirm()

	// 1 尝试连接交换机
	if err := r.exchangeDeclare(); err != nil {
		logger.Info("Publish exchangeDeclare error: ", err)
		return err
	}

	// 2 发送消息
	return r.RabbitMQ.channel.PublishWithContext(
		r.ctx,
		r.ExchangeName, // 交换机名称
		"",             // 路由参数，fanout类型交换机，自动忽略路由参数
		false,
		false,
		amqp.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent, // 消息持久化
			Body:         []byte(message),
		})

}

// Consume 订阅模式消费者
func (r *RabbitMqFanout) Consume(handler func([]byte) error) error {
	// 1 创建交换机exchange
	logger.Info("----- begin consume----")
	if err := r.exchangeDeclare(); err != nil {
		logger.Info("Consume exchangeDeclare error: ", err)
		return err
	}

	// 2 创建队列queue
	q, err := r.queueDeclare()
	if err != nil {
		logger.Info("Consume queueDeclare error: ", err)
		return err
	}

	// 3 绑定队列到交换机中
	if err := r.queueBind(q.Name); err != nil {
		logger.Info("Consume queueBind error: ", err)
		return err
	}

	// 4 消费消息
	deliveries, err := r.channel.Consume(
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
		case <-r.Ctx().Done(): // 通过context控制消费者退出
			logger.Info("fanout Consume context cancel Consume")
			// 拒绝一条消息，true表示将消息重新放回队列, 如果失败，记录日志 或 发送到其他队列(死信队列)等措施来处理错误
			if err := msg.Reject(true); err != nil {
				logger.Info("ack error: ", err)
			}

			return fmt.Errorf("context cancel Consume")
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

// PublishDelay 发布延迟队列
func (r *RabbitMqFanout) PublishDelay(message string, ttl string) error {
	select {
	case <-r.ctx.Done():
		return fmt.Errorf("context cancel publish" + r.ctx.Err().Error())
	default:
	}

	// 确认消息监听函数， 启动一个协程，监听消息发送情况
	r.ListenConfirm()

	// 1 延迟交换机
	if err := r.channel.ExchangeDeclare(
		r.ExchangeName,
		"x-delayed-message", //
		true,                // 持久化
		false,
		false,
		false,
		amqp.Table{
			"x-delayed-type": "fanout",
		},
	); err != nil {
		return err
	}
	msgId := uuid.New().String()
	logger.Info("--------------Delay Publish ----------msgId----", msgId, " time: "+time.Now().Format(time.DateTime))
	// 2 发送消息
	return r.RabbitMQ.channel.PublishWithContext(
		r.ctx,
		r.ExchangeName, // 交换机名称
		"",             // 路由参数，fanout类型交换机，自动忽略路由参数
		false,
		false,
		amqp.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent, // 消息持久化
			Body:         []byte(message),
			MessageId:    msgId,
			// Expiration:   ttl,
			Headers: amqp.Table{
				"x-delay": ttl,
			},
		})

}

// ConsumeDelay 消费延迟队列
func (r *RabbitMqFanout) ConsumeDelay(handler func([]byte) error) error {
	// 1 声明延迟交换机.
	if err := r.channel.ExchangeDeclare(
		r.ExchangeName,
		"x-delayed-message", //
		true,                // 持久化
		false,
		false,
		false,
		amqp.Table{
			"x-delayed-type": "fanout",
		},
	); err != nil {
		return errors.WithMessage(err, "--DlqConsume DlxDeclare err")
	}

	// 2 声明延迟队列（用于与延迟交换机机绑定）.
	q, err := r.channel.QueueDeclare("", true, false, false, false, nil)
	if err != nil {
		return errors.WithMessage(err, "--DlqConsume QueueDeclare err")
	}

	// 3 绑定队列到 exchange 中.
	if err := r.channel.QueueBind(q.Name, "#", r.ExchangeName, false, nil); err != nil {
		logger.Info("--DlqConsume QueueBind err: ", err)
		return errors.WithMessage(err, "--DlqConsume QueueBind err")
	}

	// 消费消息.
	deliveries, err := r.channel.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		logger.Info("--DlqConsume channel.Consume err: ", err)
		return errors.WithMessage(err, "--DlqConsume channel.Consume err")
	}
	for msg := range deliveries {
		select {
		case <-r.Ctx().Done():
			if err := msg.Reject(true); err != nil {
				// 拒绝一条消息，true表示将消息重新放回队列, 如果失败，记录日志 或 发送到其他队列等措施来处理错误
				logger.Info("reject error: ", err)
			}
			return fmt.Errorf("context cancel Consume")
		default:
		}
		if err := handler(msg.Body); err != nil {
			logger.Info("--DlqConsume handler err: ", err)
			if err = msg.Reject(true); err != nil {
				logger.Info("reject error: ", err)
			}
			continue
		}
		if err := msg.Ack(false); err != nil {
			logger.Info("---消息确认失败：", err)
			return err
		}

	}
	return nil

}

// ConsumeFailToDlx 消费失败后投递到死信交换机
func (r *RabbitMqFanout) ConsumeFailToDlx(handler func([]byte) error) error {
	// 1 创建交换机exchange
	logger.Info("----- begin consume----")
	if err := r.exchangeDeclare(); err != nil {
		logger.Info("Consume exchangeDeclare error: ", err)
		return err
	}

	// 2 创建队列queue
	q, err := r.channel.QueueDeclare(
		"", // 随机生产队列名称
		true,
		false,
		true, // true 表示这个queue只能被当前连接访问，当连接断开时queue会被删除
		false,
		amqp.Table{
			"x-dead-letter-exchange": r.ExchangeName + "-dlx", // 声明当前队列绑定的 死信交换机
		},
	)
	if err != nil {
		logger.Info("Consume queueDeclare error: ", err)
		return err
	}

	// 3 绑定队列到交换机中
	if err := r.queueBind(q.Name); err != nil {
		logger.Info("Consume queueBind error: ", err)
		return err
	}

	// 4 消费消息
	deliveries, err := r.channel.Consume(
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
		case <-r.Ctx().Done(): // 通过context控制消费者退出
			logger.Info("fanout Consume context cancel Consume")
			// 拒绝一条消息，true表示将消息重新放回队列, 如果失败，记录日志 或 发送到其他队列(死信队列)等措施来处理错误
			if err := msg.Reject(false); err != nil { // false 表示不重新放回队列
				logger.Info("ack error: ", err)
			}
			return fmt.Errorf("context cancel Consume")
		default:

		}

		// 处理消息
		err = handler(msg.Body)
		// 消费失败处理
		if err != nil {
			// 拒绝一条消息，true表示将消息重新放回队列, 如果失败，记录日志 或 发送到其他队列等措施来处理错误, false 表示不重新放回队列
			if err := msg.Reject(false); err != nil {
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
		logger.Info("======消息确认成功: ", string(msg.Body))

	}

	return nil
}

// ConsumeDlx 死信消费
func (r *RabbitMqFanout) ConsumeDlx(handler func([]byte) error) error {
	// 1. 创建死信交换机.
	if err := r.dlxExchangeDeclare(); err != nil {
		logger.Info("Consume dlxExchangeDeclare error: ", err)
		return err
	}

	// 2. 创建死信队列.
	dlxq, err := r.channel.QueueDeclare(
		r.ExchangeName+"-dlx", // 死信队列名字
		true,
		false,
		false, // 队列解锁
		false,
		nil,
	)
	if err != nil {
		logger.Info("Consume dlxQueueDeclare error: ", err)
		return err
	}

	// 3. 绑定死信队列到死信交换机中.
	if err := r.channel.QueueBind(
		dlxq.Name,
		"#", // 死信队列路由, # 井号的意思就匹配所有路由参数，意思就是接收所有死信消息
		r.ExchangeName+"-dlx",
		false,
		nil,
	); err != nil {
		logger.Info("Consume dlxQueueBind error: ", err)
		return err
	}

	// 4 消费消息
	deliveries, err := r.channel.Consume(
		dlxq.Name, // 队列名称
		"",        // 消费者名字，不填自动生成一个
		false,     // 自动向队列确认消息已经处理
		false,     // true 表示这个queue只能被这个consumer访问
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
		logger.Info("======死信消息确认成功: ", string(msg.Body))

	}

	return nil
}

func (r *RabbitMqFanout) exchangeDeclare() error {
	return r.channel.ExchangeDeclare(
		r.ExchangeName,
		"fanout", // 这里一定要设计为"fanout"也就是广播类型。
		true,     // 持久化
		false,
		false,
		false,
		nil,
	)
}
func (r *RabbitMqFanout) queueDeclare() (amqp.Queue, error) {
	return r.channel.QueueDeclare(
		"", // 随机生产队列名称
		true,
		false,
		true, // true 表示这个queue只能被当前连接访问，当连接断开时queue会被删除
		false,
		nil,
	)
}

func (r *RabbitMqFanout) queueBind(qname string) error {
	return r.channel.QueueBind(
		qname, // 队列名称
		"",    // 在pub/sub模式下key要为空
		r.ExchangeName,
		false,
		nil,
	)
}

// DlxDeclare 声明死信交换机
func (r *RabbitMqFanout) dlxExchangeDeclare() error {
	// 死信交换机
	return r.channel.ExchangeDeclare(
		r.ExchangeName+"-dlx", // 死信交换机名字
		"fanout",              // 死信交换机类型
		true,                  // 是否持久化
		false,
		false,
		false,
		nil,
	)
}
