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
var _ RabbitMQInterface = (*MqSimple)(nil)

// MqSimple 简单模式下的RabbitMQ实例
type MqSimple struct {
	*RabbitMQ
}

// NewPubSimple 创建简单模式下的实例，只需要queueName这个参数，其中exchange是默认的，key则不需要。
func NewPubSimple(option MQOption) (rabbitMQSimple *MqSimple, err error) {
	// 判断是否输入必要的信息
	if option.QueueName == "" || option.MqURL == "" {
		logger.Infof("QueueName and mqUrl is required,\nbut queueName and mqUrl are %s and %s.", "", "")
		return nil, errors.New("QueueName and mqUrl is required")
	}
	rabbitmq, err := newRabbitMQ(option)
	if err != nil {
		return nil, err
	}

	if err = rabbitmq.SetConfirm(); err != nil {
		return nil, err
	}

	return &MqSimple{
		rabbitmq,
	}, nil
}

func NewConsumeSimple(option MQOption) (rabbitMQSimple *MqSimple, err error) {
	// 判断是否输入必要的信息
	if option.QueueName == "" || option.MqURL == "" {
		logger.Infof("QueueName and mqUrl is required,\nbut queueName and mqUrl are %s and %s.", "", "")
		return nil, errors.New("QueueName and mqUrl is required")
	}

	rabbitmq, err := newRabbitMQ(option)
	if err != nil {
		return nil, err
	}

	rabbitmq.NotifyConnectionClose()
	rabbitmq.NotifyChannelClose()

	return &MqSimple{
		rabbitmq,
	}, nil
}

// Publish 直接模式,生产者.
func (r *MqSimple) Publish(message string, handler MsgHandler) (err error) {
	select {
	case <-r.Ctx.Done():
		handler.Failed(FailedMsg{
			ExchangeName: r.ExchangeName,
			QueueName:    r.QueueName,
			Routing:      r.Routing,
			MsgId:        "",
			Message:      []byte(message),
		})
		return fmt.Errorf("context cancel publish")
	default:
	}
	// 确认消息监听函数， 启动一个协程，监听消息发送情况
	var (
		ack  = make(chan uint64)
		nack = make(chan uint64)
	)
	r.NotifyConfirm(ack, nack)

	// 1.申请队列,如果队列不存在，则会自动创建，如果队列存在则跳过创建直接使用  这样的好处保障队列存在，消息能发送到队列当中
	if _, err = r.channel.QueueDeclare(
		r.QueueName, // 队列名字
		true,        // 进入的消息是否持久化 进入队列如果不消费那么消息就在队列里面 如果重启服务器那么这个消息就没啦 通常设置为false
		false,       // 是否为自动删除  意思是最后一个消费者断开链接以后是否将消息从队列当中删除  默认设置为false不自动删除
		false,       // 是否具有排他性
		false,       // 是否阻塞 发送消息以后是否要等待消费者的响应 消费了下一个才进来 就跟golang里面的无缓冲channle一个道理 默认为非阻塞即可设置为false
		amqp.Table{
			"x-max-priority": 10, // 设置队列最大优先级, 建议最好在1到10之间
		},
	); err != nil {
		logger.Info("--QueueDeclare error:", err)
		return err
	}

	// 2 发送消息到队列中
	msgId := uuid.New().String()
	logger.Info("--- begin pubish---", message)
	if err := r.channel.PublishWithContext(
		r.Ctx,
		r.ExchangeName, // 交换机名称，simple模式下默认为空 我们在上边已经赋值为空了  虽然为空 但其实也是在用的rabbitmq当中的default交换机运行
		r.QueueName,    // 路由参数， 这里使用队列的名字作为路由参数
		true,           // 如果为true 会根据exchange类型和routkey规则，如果无法找到符合条件的队列那么会把发送的消息返还给发送者
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
			Priority: 1, // 设置消息优先级
		}); err != nil {
		return err
	}

	select {
	case a := <-ack:
		logger.Info("------------ publish success ---------", a, " ===", message)
		return nil
	case n := <-nack:
		logger.Info("------------ publish failed----------", n, " ===", message)
		handler.Failed(FailedMsg{
			ExchangeName: r.ExchangeName,
			QueueName:    r.QueueName,
			Routing:      r.Routing,
			MsgId:        msgId,
			Message:      []byte(message),
		})
		return errors.New("**** publish failed *****")
	case notify := <-r.channel.NotifyReturn(make(chan amqp.Return)):
		if notify.ReplyCode == amqp.NoRoute {
			return errors.New("**** no amqp route *****")
		}
		logger.Info("----- notify return ----", string(notify.Body))

	}

	return nil

}

// Consume 直接模式，消费者
func (r *MqSimple) Consume(handler MsgHandler) error {
	// 1 申请队列,如果队列不存在则自动创建,存在则跳过
	if _, err := r.channel.QueueDeclare(
		r.QueueName,
		true,  // 是否持久化
		false, // 是否自动删除
		false, // 是否具有排他性
		false, // 是否阻塞处理
		amqp.Table{
			"x-max-priority": 10, // 设置队列最大优先级, 建议最好在1到10之间
		},
	); err != nil {
		logger.Info("--Consume QueueDeclare error: ", err)
		return err
	}

	// 2 接收消息
	deliveries, err := r.channel.Consume(
		r.QueueName, // 队列名
		"",          // 用来区分多个消费者， 消费者唯一id，不填，则自动生成一个唯一值
		false,       // 是否自动应答,告诉我已经消费完了
		false,       // true 表示这个queue只能被这个consumer访问
		false,       // 若设置为true,则表示为不能将同一个connection中发送的消息传递给这个connection中的消费者.
		false,       // 消费队列是否设计阻塞
		nil,
	)
	if err != nil {
		logger.Info("--channel.Consume error: ", err)
		return err
	}

	// 监听连接断开时自动重连
	go func() {
		for {
			select {
			case <-r.conn.NotifyClose(make(chan *amqp.Error)):
				time.Sleep(1 * time.Second)
				_ = r.Consume(handler)
			case <-r.Ctx.Done():
				return
			}
		}
	}()

	for msg := range deliveries {
		select {
		case <-r.Ctx.Done():
			logger.Info("======ctx done==========")
			handler.Failed(FailedMsg{
				ExchangeName: r.ExchangeName,
				QueueName:    r.QueueName,
				Routing:      r.Routing,
				MsgId:        msg.MessageId,
				Message:      msg.Body,
			})
			if err := msg.Reject(false); err != nil { // 如果要放入死信交换机， Reject 要为false 才行
				// 拒绝一条消息，true表示将消息重新放回队列, 如果失败，记录日志 或 发送到其他队列等措施来处理错误
				logger.Info("reject error: ", err)
			}
			return fmt.Errorf("context cancel Consume")
		default:
		}

		if err := handler.Process(msg.Body, msg.MessageId); err != nil {
			go func() {
				retry, ok := msg.Headers["x-retry"].(int32)
				if !ok {
					retry = int32(0)
				}
				if retry > 3 {
					// 多次消费失败后要对消息做处理 接插入db日志, db日志可以记录交换机 路由，queuename
					handler.Failed(FailedMsg{
						ExchangeName: r.ExchangeName,
						QueueName:    r.QueueName,
						Routing:      r.Routing,
						MsgId:        msg.MessageId,
						Message:      msg.Body,
					})
					// db create
					if err = msg.Reject(false); err != nil {
						// 拒绝一条消息，true表示将消息重新放回队列, 如果失败，记录日志 或 发送到其他队列等措施来处理错误
						logger.Info("-----------reject error: ", err, "----------")
					}
				} else {
					msg.Headers["x-retry"] = retry + 1
					if err := r.RetryMsg(msg, "1000"); err != nil {
						logger.Info("---- publish retry msg error: ", err)
					}
					if err = msg.Ack(false); err != nil {
						logger.Info("retry msg ack err: ", err)
					}

				}
			}()
			continue
		}

		// // 确认一条消息，false表示确认当前消息，true表示确认当前消息和之前所有未确认的消息
		if err := msg.Ack(false); err != nil {
			logger.Info("message ack error:", err, " message id: ", msg.MessageId)
		}

	}

	return nil
}

func (r *MqSimple) RetryMsg(msg amqp.Delivery, ttl string) error {
	select {
	case <-r.Ctx.Done():
		return fmt.Errorf("context cancel publish")
	default:
	}
	// 确认消息监听函数， 启动一个协程，监听消息发送情况
	r.ListenConfirm()

	// 声明死信交换机
	dlxName := r.QueueName + "-retry-Ex"
	if err := r.DlxDeclare(dlxName, "fanout"); err != nil {
		logger.Info("--DlqConsume DlxDeclare err 1: ", err)
		return err
	}

	// 绑定主队列到 exchange 中
	if err := r.channel.QueueBind(r.QueueName, "#", dlxName, false, nil); err != nil {
		logger.Info("--DlqConsume QueueBind err: ", err)
		return err
	}

	// 声明重试队列
	retryQueue := r.QueueName + "-retry"
	if _, err := r.channel.QueueDeclare(
		retryQueue,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange": dlxName, // 死信交换机
			"x-max-priority":         10,
		},
	); err != nil {
		logger.Info("---retry queue err: ", err)
	}
	priority, ok := msg.Headers["x-retry"].(uint8)
	if !ok {
		priority = 1
	}
	return r.channel.PublishWithContext(
		r.Ctx,
		r.ExchangeName, // 交换机名称，simple模式下默认为空 我们在上边已经赋值为空了  虽然为空 但其实也是在用的rabbitmq当中的default交换机运行
		retryQueue,     // 路由参数， 这里使用队列的名字作为路由参数
		false,          // 如果为true 会根据exchange类型和routkey规则，如果无法找到符合条件的队列那么会把发送的消息返还给发送者
		false,          // 如果为true,当exchange发送消息到队列后发现队列上没有绑定消费者则会把消息返还给发送者
		amqp.Publishing{
			// 消息内容持久化，这个很关键
			DeliveryMode: amqp.Persistent,
			ContentType:  msg.ContentType,
			Body:         msg.Body,
			Headers:      msg.Headers,
			MessageId:    msg.MessageId,
			Timestamp:    time.Now(),
			Expiration:   ttl,
			Priority:     priority, // 设置消息优先级
		})

}

// PublishDelay 发送延迟队列
func (r *MqSimple) PublishDelay(message string, handler MsgHandler, ttl string) error {

	select {
	case <-r.Ctx.Done():
		return fmt.Errorf("context cancel publish")
	default:
	}
	// 确认消息监听函数， 启动一个协程，监听消息发送情况
	// 确认消息监听函数， 启动一个协程，监听消息发送情况
	var (
		ack  = make(chan uint64)
		nack = make(chan uint64)
	)
	r.NotifyConfirm(ack, nack)

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
	if err := r.channel.PublishWithContext(
		r.Ctx,
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
			Headers: amqp.Table{
				"x-retry": 0,
			},
		}); err != nil {
		return err
	}

	select {
	case a := <-ack:
		logger.Info("------------ publish success ---------", a, " ===", message)
		return nil
	case n := <-nack:
		logger.Info("------------ publish failed----------", n, " ===", message)
		handler.Failed(FailedMsg{
			ExchangeName: r.ExchangeName,
			QueueName:    r.QueueName,
			Routing:      r.Routing,
			MsgId:        msgId,
			Message:      []byte(message),
		})
		return errors.New("**** publish delay failed *****")
	case notify := <-r.channel.NotifyReturn(make(chan amqp.Return)):
		if notify.ReplyCode == amqp.NoRoute {
			return errors.New("**** no amqp route *****")
		}
		logger.Info("----- notify return ----", string(notify.Body))
		return nil
	}

}

// ConsumeDelay 消费延迟队列
func (r *MqSimple) ConsumeDelay(handler MsgHandler) error {

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
		select {
		case <-r.Ctx.Done():
			handler.Failed(FailedMsg{
				ExchangeName: r.ExchangeName,
				QueueName:    r.QueueName,
				Routing:      r.Routing,
				MsgId:        msg.MessageId,
				Message:      msg.Body,
			})
			if err := msg.Reject(false); err != nil {
				// 拒绝一条消息，true表示将消息重新放回队列, 如果失败，记录日志 或 发送到其他队列等措施来处理错误
				logger.Info("reject error: ", err)
			}
			return fmt.Errorf("context cancel Consume")
		default:
		}
		if err := handler.Process(msg.Body, msg.MessageId); err != nil {
			logger.Info("--DlqConsume handler err: ", err)
			// 失败处理
			handler.Failed(FailedMsg{
				ExchangeName: r.ExchangeName,
				QueueName:    r.QueueName,
				Routing:      r.Routing,
				MsgId:        msg.MessageId,
				Message:      msg.Body,
			})
			if err = msg.Reject(false); err != nil {
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

// PublishWithDlx 带有死信交换机的发送
func (r *MqSimple) PublishWithDlx(message string) error {
	select {
	case <-r.Ctx.Done():
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
	return r.channel.PublishWithContext(
		r.Ctx,
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

}

// ConsumeFailToDlx 带有死信交换机的消费
func (r *MqSimple) ConsumeFailToDlx(handler MsgHandler) error {
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
		select {
		case <-r.Ctx.Done():
			if err := msg.Reject(false); err != nil { // 如果要放入死信交换机， Reject 要为false 才行
				// 拒绝一条消息，true表示将消息重新放回队列, 如果失败，记录日志 或 发送到其他队列等措施来处理错误
				logger.Info("reject error: ", err)
			}
			return fmt.Errorf("context cancel Consume")
		default:

		}

		if err := handler.Process(msg.Body, msg.MessageId); err != nil {
			// 如果要放入死信交换机， Reject 要为false 才行, true表示将消息重新放回队列, 如果失败，记录日志 或 发送到其他队列等措施来处理错误
			if err = msg.Reject(false); err != nil {
				logger.Info("-----------reject error: ", err, "----------")
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

// ConsumeDlx 消费死信队列
func (r *MqSimple) ConsumeDlx(handler MsgHandler) error {
	// 死信交换机
	dlxName := "dlx-" + r.QueueName
	if err := r.DlxDeclare(dlxName, "fanout"); err != nil {
		logger.Info("--DlqConsume DlxDeclare err: ", err)
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
		failedmsg := FailedMsg{
			ExchangeName: r.ExchangeName,
			QueueName:    r.QueueName,
			Routing:      r.Routing,
			MsgId:        msg.MessageId,
			Message:      msg.Body,
		}
		select {
		case <-r.Ctx.Done():
			handler.Failed(failedmsg)
			if err := msg.Reject(false); err != nil {
				// 拒绝一条消息，true表示将消息重新放回队列, 如果失败，记录日志 或 发送到其他队列等措施来处理错误
				logger.Info("reject error: ", err)
			}
			return fmt.Errorf("context cancel Consume")
		default:
		}
		// 处理消息
		if err := handler.Process(msg.Body, msg.MessageId); err != nil {
			logger.Info("--DlqConsume handler err: ", err)
			handler.Failed(failedmsg)
			if err = msg.Reject(false); err != nil {
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
