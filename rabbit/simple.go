// @Author 2023/7/6 20:59:00
package rabbit

import (
	"fmt"
	"log"
	"time"

	"github.com/pkg/errors"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

/*
1 Simple模式，最简单最常用的模式
2 Work模式，一个消息只能被一个消费者消费
*/
var _ RabbitMQInterface = (*RabbitMQSimple)(nil)

// RabbitMQSimple 简单模式下的RabbitMQ实例
type RabbitMQSimple struct {
	*RabbitMQ
}

// NewRabbitMQSimple 创建简单模式下的实例，只需要queueName这个参数，其中exchange是默认的，key则不需要。
func NewRabbitMQSimple(queueName, mqUrl string) (rabbitMQSimple *RabbitMQSimple, err error) {
	// 判断是否输入必要的信息
	if queueName == "" || mqUrl == "" {
		logger.Infof("QueueName and mqUrl is required,\nbut queueName and mqUrl are %s and %s.", queueName, mqUrl)
		return nil, errors.New("QueueName and mqUrl is required")
	}
	rabbitmq, err := newRabbitMQ("", queueName, "", mqUrl)
	if err != nil {
		return nil, err
	}

	if err = rabbitmq.SetConfirm(); err != nil {
		return nil, err
	}

	if err != nil {
		return nil, err
	}
	return &RabbitMQSimple{
		rabbitmq,
	}, nil
}

// Publish 直接模式,生产者.
func (r *RabbitMQSimple) Publish(message string) (err error) {

	select {
	case <-r.ctx.Done():
		return fmt.Errorf("context cancel publish")
	default:
	}
	// 确认消息监听函数， 启动一个协程，监听消息发送情况
	r.ListenConfirm()

	// 1.申请队列,如果队列不存在，则会自动创建，如果队列存在则跳过创建直接使用  这样的好处保障队列存在，消息能发送到队列当中
	if _, err = r.channel.QueueDeclare(
		r.QueueName, // 队列名字
		true,        // 进入的消息是否持久化 进入队列如果不消费那么消息就在队列里面 如果重启服务器那么这个消息就没啦 通常设置为false
		false,       // 是否为自动删除  意思是最后一个消费者断开链接以后是否将消息从队列当中删除  默认设置为false不自动删除
		false,       // 是否具有排他性
		false,       // 是否阻塞 发送消息以后是否要等待消费者的响应 消费了下一个才进来 就跟golang里面的无缓冲channle一个道理 默认为非阻塞即可设置为false
		nil,
	); err != nil {
		logger.Info("--QueueDeclare error:", err)
		return err
	}
	// confirmsCh := make(chan *amqp.DeferredConfirmation)

	// 2 发送消息到队列中
	msgId := uuid.New().String()
	return r.channel.PublishWithContext(
		r.ctx,
		r.ExchangeName, // 交换机名称，simple模式下默认为空 我们在上边已经赋值为空了  虽然为空 但其实也是在用的rabbitmq当中的default交换机运行
		r.QueueName,    // 路由参数， 这里使用队列的名字作为路由参数
		false,          // 如果为true 会根据exchange类型和routkey规则，如果无法找到符合条件的队列那么会把发送的消息返还给发送者
		false,          // 如果为true,当exchange发送消息到队列后发现队列上没有绑定消费者则会把消息返还给发送者
		amqp.Publishing{
			// 消息内容持久化，这个很关键
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			MessageId:    msgId,
			Body:         []byte(message),
			Headers: amqp.Table{
				"x-retry": 0,
			},
		})

}

// Consume 直接模式，消费者
func (r *RabbitMQSimple) Consume(handler func([]byte) error) (err error) {
	// 1 申请队列,如果队列不存在则自动创建,存在则跳过
	q, err := r.channel.QueueDeclare(
		r.QueueName,
		true,  // 是否持久化
		false, // 是否自动删除
		false, // 是否具有排他性
		false, // 是否阻塞处理
		nil,
	)
	if err != nil {
		logger.Info("--Consume QueueDeclare error: ", err)
		return err
	}

	// 2 接收消息
	deliveries, err := r.channel.Consume(
		q.Name, // 队列名
		"",     // 用来区分多个消费者， 消费者唯一id，不填，则自动生成一个唯一值
		false,  // 是否自动应答,告诉我已经消费完了
		false,  // true 表示这个queue只能被这个consumer访问
		false,  // 若设置为true,则表示为不能将同一个connection中发送的消息传递给这个connection中的消费者.
		false,  // 消费队列是否设计阻塞
		nil,
	)
	if err != nil {
		logger.Info("--channel.Consume error: ", err)
		return err
	}
	for msg := range deliveries {
		select {
		case <-r.Ctx().Done():
			logger.Info("======ctx done==========")
			if err := msg.Reject(false); err != nil { // 如果要放入死信交换机， Reject 要为false 才行
				// 拒绝一条消息，true表示将消息重新放回队列, 如果失败，记录日志 或 发送到其他队列等措施来处理错误
				logger.Info("reject error: ", err)
			}
			return fmt.Errorf("context cancel Consume")
		default:

		}

		if err := handler(msg.Body); err != nil {

			retry, ok := msg.Headers["x-retry"].(int32)
			if !ok {
				retry = int32(0)
			}
			if retry > 3 {
				if err = msg.Reject(false); err != nil {
					// 拒绝一条消息，true表示将消息重新放回队列, 如果失败，记录日志 或 发送到其他队列等措施来处理错误
					logger.Info("-----------reject error: ", err, "----------")
					// 多次消费失败后要对消息做处理 接插入db日志, db日志可以记录交换机 路由，queuename
				} else {
					logger.Info("----msg.Headers[x-retry]--", msg.Headers["x-retry"], "--Reject msg:", string(msg.Body))
				}
			} else {
				logger.Info("----msg.Headers[x-retry]--", msg.Headers["x-retry"], "-- msg:", string(msg.Body))
				msg.Headers["x-retry"] = retry + 1
				if err := r.RetryMsg(msg, "2000"); err != nil {
					logger.Info("---- publish retry msg error: ", err)
				} else {
					logger.Info("---- publish retry msg success -----", string(msg.Body))
				}
				_ = msg.Ack(false)

			}

			continue
		}

		// // 确认一条消息，false表示确认当前消息，true表示确认当前消息和之前所有未确认的消息
		if err := msg.Ack(false); err != nil {
			logger.Info("message ack error:", err, " message id: ", msg.MessageId)
			continue
		}

	}

	return nil
}

// PublishDelay 发送延迟队列
func (r *RabbitMQSimple) PublishDelay(message string, ttl string) error {

	select {
	case <-r.ctx.Done():
		return fmt.Errorf("context cancel publish")
	default:
	}
	// 确认消息监听函数， 启动一个协程，监听消息发送情况
	r.ListenConfirm()

	var dlxName = r.QueueName + "-delay-Ex"
	// 声明死信交换机
	if err := r.DlxDeclare(dlxName, "fanout"); err != nil {
		logger.Info("--DlqConsume DlxDeclare err 1: ", err)
		return err
	}

	// 1.申请队列,如果队列不存在，则会自动创建，如果队列存在则跳过创建直接使用  这样的好处保障队列存在，消息能发送到队列当中
	_, err := r.channel.QueueDeclare(
		r.QueueName, // 队列名字
		true,        // 进入的消息是否持久化 进入队列如果不消费那么消息就在队列里面 如果重启服务器那么这个消息就没啦 通常设置为false
		false,       // 是否为自动删除  意思是最后一个消费者断开链接以后是否将消息从队列当中删除  默认设置为false不自动删除
		false,       // 是否具有排他性
		false,       // 是否阻塞 发送消息以后是否要等待消费者的响应 消费了下一个才进来 就跟golang里面的无缓冲channle一个道理 默认为非阻塞即可设置为false
		amqp.Table{
			// "x-message-ttl":          "",
			"x-dead-letter-exchange": dlxName, // 死信交换机
			// "x-dead-letter-routing-key": dlxRouting,  // 死信路由
			// "x-dead-letter-queue": "dead-letter-queue" + mq.QueueName, // 死信队列

		}, // 其他的属性，没有则直接诶传入空即可 nil  nil,
	)

	if err != nil {
		logger.Info("--QueueDeclare error:", err)
		return err
	}
	// confirmsCh := make(chan *amqp.DeferredConfirmation)

	// 2 发送消息到队列中
	msgId := uuid.New().String()
	err = r.channel.PublishWithContext(
		r.ctx,
		r.ExchangeName, // 交换机名称，simple模式下默认为空 我们在上边已经赋值为空了  虽然为空 但其实也是在用的rabbitmq当中的default交换机运行
		r.QueueName,    // 路由参数， 这里使用队列的名字作为路由参数
		true,           // 如果为true 会根据exchange类型和routkey规则，如果无法找到符合条件的队列那么会把发送的消息返还给发送者
		false,          // 如果为true,当exchange发送消息到队列后发现队列上没有绑定消费者则会把消息返还给发送者
		amqp.Publishing{
			// Headers: amqp.Table{},
			// 消息内容持久化，这个很关键
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			MessageId:    msgId,
			Body:         []byte(message),
			Expiration:   ttl, // push 时 在消息本体上设置expiration超时时间，单位为毫秒级别 类型为 string
		})
	if err != nil {
		logger.Info("--PublishWithContext error: ", err)
	}
	logger.Info("--------------Delay Publish ----------msgId----", msgId, " time: "+time.Now().Format(time.DateTime))
	return err
}

// ConsumeDelay 消费延迟队列
func (r *RabbitMQSimple) ConsumeDelay(handler func([]byte) error) error {

	// 1 声明死信交换机
	dlxName := r.QueueName + "-delay-Ex"
	if err := r.DlxDeclare(dlxName, "fanout"); err != nil {
		return errors.WithMessage(err, "--DlqConsume DlxDeclare err")
	}

	// 声明 延迟死信队列（用于与死信交换机绑定）
	dlxQueue := r.QueueName + "-delay"
	q, err := r.channel.QueueDeclare(dlxQueue, true, false, false, false, nil)
	if err != nil {
		return errors.WithMessage(err, "--DlqConsume QueueDeclare err")
	}

	// 绑定队列到 exchange 中
	if err := r.channel.QueueBind(dlxQueue, "#", dlxName, false, nil); err != nil {
		logger.Info("--DlqConsume QueueBind err: ", err)
		return err
	}

	// 消费消息
	deliveries, err := r.channel.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		logger.Info("--DlqConsume channel.Consume err: ", err)
		return err
	}
	for msg := range deliveries {

		logger.Info("--------------Delay Consume ----------msgId----", msg.MessageId, " time: "+time.Now().Format(time.DateTime))
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

// PublishWithXdl 带有死信交换机的发送
func (r *RabbitMQSimple) PublishWithXdl(message string) (err error) {

	select {
	case <-r.ctx.Done():
		return fmt.Errorf("context cancel publish")
	default:
	}
	// 确认消息监听函数， 启动一个协程，监听消息发送情况
	r.ListenConfirm()

	// 声明死信交换机
	var dlxName = "dlx-" + r.QueueName
	if err := r.DlxDeclare(dlxName, "fanout"); err != nil {
		logger.Info("--DlqConsume DlxDeclare err: ", err)
		return err
	}
	// 1.申请队列,如果队列不存在，则会自动创建，如果队列存在则跳过创建直接使用  这样的好处保障队列存在，消息能发送到队列当中
	_, err = r.channel.QueueDeclare(
		r.QueueName, // 队列名字
		true,        // 进入的消息是否持久化 进入队列如果不消费那么消息就在队列里面 如果重启服务器那么这个消息就没啦 通常设置为false
		false,       // 是否为自动删除  意思是最后一个消费者断开链接以后是否将消息从队列当中删除  默认设置为false不自动删除
		false,       // 是否具有排他性
		false,       // 是否阻塞 发送消息以后是否要等待消费者的响应 消费了下一个才进来 就跟golang里面的无缓冲channle一个道理 默认为非阻塞即可设置为false
		amqp.Table{
			// "x-message-ttl":          "",
			"x-dead-letter-exchange": dlxName, // 死信交换机
			// "x-dead-letter-routing-key": dlxRouting,  // 死信路由
			// "x-dead-letter-queue": "dead-letter-queue" + mq.QueueName, // 死信队列

		}, // 其他的属性，没有则直接诶传入空即可 nil  nil,
	)

	if err != nil {
		logger.Info("--QueueDeclare error:", err)
		return err
	}
	// confirmsCh := make(chan *amqp.DeferredConfirmation)

	// 2 发送消息到队列中
	msgId := uuid.New().String()
	err = r.channel.PublishWithContext(
		r.ctx,
		r.ExchangeName, // 交换机名称，simple模式下默认为空 我们在上边已经赋值为空了  虽然为空 但其实也是在用的rabbitmq当中的default交换机运行
		r.QueueName,    // 路由参数， 这里使用队列的名字作为路由参数
		true,           // 如果为true 会根据exchange类型和routkey规则，如果无法找到符合条件的队列那么会把发送的消息返还给发送者
		false,          // 如果为true,当exchange发送消息到队列后发现队列上没有绑定消费者则会把消息返还给发送者
		amqp.Publishing{
			// Headers: amqp.Table{},
			// 消息内容持久化，这个很关键
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			MessageId:    msgId,
			Body:         []byte(message),
		})
	if err != nil {
		logger.Info("--PublishWithContext error: ", err)
	}
	logger.Info("------------Simple Publish --------msgId----", msgId, " time: "+time.Now().Format(time.DateTime))
	return err
}

// ConsumeFailToDlx 带有死信交换机的消费
func (r *RabbitMQSimple) ConsumeFailToDlx(handler func([]byte) error) error {
	// 1 声明死信交换机
	var dlxName = "dlx-" + r.QueueName
	if err := r.DlxDeclare(dlxName, "fanout"); err != nil {
		log.Fatal("--DlqConsume DlxDeclare err: ", err)
		return err
	}
	q, err := r.channel.QueueDeclare(
		r.QueueName,
		true,  // 是否持久化
		false, // 是否自动删除
		false, // 是否具有排他性
		false, // 是否阻塞处理
		amqp.Table{
			// "x-message-ttl":          "",
			"x-dead-letter-exchange": dlxName, // 死信交换机
			// "x-dead-letter-routing-key": dlxRouting,  // 死信路由
			// "x-dead-letter-queue": "dead-letter-queue" + mq.QueueName, // 死信队列

		}, // 额外的属性
		// nil,
	)
	if err != nil {
		log.Fatal("--Consume QueueDeclare error: ", err)
		return err
	}

	// 2 接收消息
	deliveries, err := r.channel.Consume(
		q.Name, // 队列名
		"",     // 用来区分多个消费者， 消费者唯一id，不填，则自动生成一个唯一值
		false,  // 是否自动应答,告诉我已经消费完了
		false,  // true 表示这个queue只能被这个consumer访问
		false,  // 若设置为true,则表示为不能将同一个connection中发送的消息传递给这个connection中的消费者.
		false,  // 消费队列是否设计阻塞
		nil,
	)
	if err != nil {
		logger.Info("--channel.Consume error: ", err)
		return err
	}
	for msg := range deliveries {
		logger.Info("------------Simple Consume --------msgId----", msg.MessageId, " time: "+time.Now().Format(time.DateTime))
		select {
		case <-r.Ctx().Done():
			logger.Info("======ctx done==========")
			if err := msg.Reject(false); err != nil { // 如果要放入死信交换机， Reject 要为false 才行
				// 拒绝一条消息，true表示将消息重新放回队列, 如果失败，记录日志 或 发送到其他队列等措施来处理错误
				logger.Info("reject error: ", err)
			}
			return fmt.Errorf("context cancel Consume")
		default:

		}
		// logger.Info("-----messageId: ", msg.MessageId)

		if err := handler(msg.Body); err != nil {
			if err = msg.Reject(false); err != nil {
				// 拒绝一条消息，true表示将消息重新放回队列, 如果失败，记录日志 或 发送到其他队列等措施来处理错误
				logger.Info("-----------reject error: ", err, "----------")
			}
			// mq.DlqConsume(mq.QueueName, "dead-letter-queue-"+mq.QueueName, "", dlxDo)

			continue
		}

		// // 确认一条消息，false表示确认当前消息，true表示确认当前消息和之前所有未确认的消息
		if err := msg.Ack(false); err != nil {
			logger.Info("message ack error:", err, " message id: ", msg.MessageId)
			continue
		}

	}

	return nil
}

// ConsumeDlx 消费死信队列
func (r *RabbitMQSimple) ConsumeDlx(handler func([]byte) error) error {

	// 死信交换机
	dlxName := "dlx-" + r.QueueName
	if err := r.DlxDeclare(dlxName, "fanout"); err != nil {
		log.Fatal("--DlqConsume DlxDeclare err: ", err)
		return err
	}

	// 声明 死信队列（用于与死信交换机绑定）
	dlqName := "dlq-" + r.QueueName
	q, err := r.channel.QueueDeclare(dlqName, true, false, false, false, nil)
	if err != nil {
		logger.Info("--DlqConsume QueueDeclare err: ", err)
		return err
	}

	// 绑定队列到 exchange 中
	if err := r.channel.QueueBind(dlqName, "#", dlxName, false, nil); err != nil {
		logger.Info("--DlqConsume QueueBind err: ", err)
		return err
	}

	// 消费消息
	deliveries, err := r.channel.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		logger.Info("--DlqConsume channel.Consume err: ", err)
		return err
	}
	for msg := range deliveries {
		logger.Info("------------DLX Consume --------msgId----", msg.MessageId, " time: "+time.Now().Format(time.DateTime))
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
		if err := msg.Ack(true); err != nil {
			logger.Info("---消息确认失败：", err)
			return err
		}

	}
	return nil

}
