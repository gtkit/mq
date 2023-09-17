// Package rabbit @Author 2023/7/6 20:27:00
package rabbit

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
https://blog.csdn.net/qq_28710983/article/details/105129432#:~:text=%E7%A1%AE%E8%AE%A4%E6%B6%88%E6%81%AF%E6%88%90%E5%8A%9F%E5%8F%91%E5%B8%83%E5%88%B0%20rabbitmq%20SetConfirm%E5%87%BD%E6%95%B0%20err%20%3A%3D%20channel.Confirm%20%28false%29%20if,err%29%20%7D%20this.notifyConfirm%20%3D%20this.Channel.NotifyPublish%20%28make%20%28chan%20amqp.Confirmation%29%29
   二次封装了RabbitMQ五种模式：
   1 Simple模式，最简单最常用的模式，一个消息只能被一个消费者消费
   	应用场景: 短信，聊天
   2 Work模式，一个消息只能被一个消费者消费
   	应用场景: 抢红包，和资源任务调度

   3 Publish/Subscribe发布订阅模式，消息被路由投递给多个队列，一个消息被多个消费者获取,生产端不允许指定消费
   	应用场景：邮件群发，广告
   4 Routing路由模式,一个消息被多个消费者获取，并且消息的目标队列可以被生产者指定
   	应用场景: 根据生产者的要求发送给特定的一个或者一批队列发送信息
   5 Topic话题模式,一个消息被多个消息获取，消息的目标queue可用BindKey以通配符
   	（#:一个或多个词，*：一个词）的方式指定。
*/

// todo：设置连接重试的次数
const Delay = 1 // reconnect after delay seconds
// RabbitMQ RabbitMQ实例
type RabbitMQ struct {
	conn         *amqp.Connection // 连接
	channel      *amqp.Channel    // 管道
	ExchangeName string           // 交换机名称
	QueueName    string           // 队列名称
	Key          string           // Binding Key/Routing Key, Simple模式 几乎用不到
	MqURL        string           // 连接信息-amqp://账号:密码@地址:端口号/-amqp://guest:guest@127.0.0.1:5672/
	ctx          context.Context

	notifyConfirm chan amqp.Confirmation // 确认发送到mq的channel

	notifyClose chan *amqp.Error // 如果异常关闭，会接受数据

	msgExpiration string // 消息过期时间
}

// RabbitMQInterface 定义RabbitMQ实例的接口
// 每种RabbitMQ实例都有发布和消费两种功能
type RabbitMQInterface interface {
	Publish(message string) (err error)
	Consume(handler func([]byte) error) (err error)

	// PublishDelay 延迟队列
	PublishDelay(message string, ttl string) error
	ConsumeDelay(handler func([]byte) error) error

	// ConsumeFailToDlx 消息消费失败进入死信队列
	ConsumeFailToDlx(handler func([]byte) error) error
	ConsumeDlx(handler func([]byte) error) error
}

// NewRabbitMQ 创建一个RabbitMQ实例
func newRabbitMQ(exchangeName, queueName, key, mqUrl string) (mq *RabbitMQ, err error) {

	mq = &RabbitMQ{
		QueueName:    queueName,
		ExchangeName: exchangeName,
		Key:          key,
		MqURL:        mqUrl,
		ctx:          context.Background(),
	}

	// 创建rabbitmq连接
	config := amqp.Config{
		Vhost:      "/",
		Properties: amqp.NewConnectionProperties(),
		Heartbeat:  10 * time.Second,
		Locale:     "en_US",
	}
	config.Properties.SetClientConnectionName("rabbit-with-" + exchangeName + "-" + queueName)

	mq.conn, err = amqp.DialConfig(mq.MqURL, config)
	if err != nil {
		return nil, err
	}

	// get reconnect connection
	mq.NotifyConnectionClose(config)

	mq.channel, err = mq.conn.Channel()
	if err != nil {
		return nil, err
	}

	// 设置公平调度分发
	if err = mq.channel.Qos(1, 0, false); err != nil {
		return nil, err
	}

	// auto reconnect channel
	mq.NotifyChannelClose()

	return
}

func (r *RabbitMQ) NotifyConnectionClose(config amqp.Config) {
	go func() {
		for {
			reason, ok := <-r.conn.NotifyClose(make(chan *amqp.Error))
			if !ok {
				logger.Info("connection closed")
				break
			}
			logger.Infof("connection closed, reason: %v", reason)
			for {
				time.Sleep(Delay * time.Second)
				reconnect, err := amqp.DialConfig(r.MqURL, config)
				if err == nil {
					r.conn = reconnect
					r.channel, _ = r.conn.Channel()
					logger.Info("connection reconnect success")
					break
				}

				logger.Infof("connection reconnect failed, err: %v", err)
			}

		}
	}()
}

// NotifyChannelClose auto reconnect channel
func (r *RabbitMQ) NotifyChannelClose() {
	go func() {
		for {
			logger.Info("---------r.channel.NotifyClose---------")
			reason, ok := <-r.channel.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok || r.channel.IsClosed() {
				logger.Info("--channel has closed")
				_ = r.channel.Close() // close again, ensure closed flag set when connection closed
				// break
			}
			logger.Infof("--channel closed, reason: %v", reason)
			for {
				time.Sleep(Delay * time.Second)
				ch, err := r.conn.Channel()
				if err == nil {
					logger.Info("--channel recreate success")
					r.channel = ch
					break
				} else {
					logger.Infof("--channel recreate failed, err: %v", err)
				}

			}
		}
	}()

}

// Destroy 断开channel和connection
func (r *RabbitMQ) Destroy() {
	logger.Infof("%s,%s is closed!!!", r.ExchangeName, r.QueueName)
	r.channel.Close()
	r.conn.Close()

}

func (r *RabbitMQ) Ctx() context.Context {
	return r.ctx
}

// SetConfirm 设置监听消息发送
func (r *RabbitMQ) SetConfirm() error {
	err := r.channel.Confirm(false)
	if err != nil {
		logger.Info("this.Channel.Confirm  ", err)
		return err
	}
	r.notifyConfirm = r.channel.NotifyPublish(make(chan amqp.Confirmation))
	return nil
}

// ListenConfirm 确认消息成功发布到rabbitmq channel,即消息从生产者到 Broker
func (r *RabbitMQ) ListenConfirm() {
	go func() {
		for c := range r.notifyConfirm {
			if c.Ack {
				fmt.Println("confirm:消息发送成功")
			} else {
				// 这里表示消息发送到mq失败,可以处理失败流程
				fmt.Println("confirm:消息发送失败")
			}
		}
	}()
}

// NotifyReturn  确保消息从交换机到队列入列成功
func (r *RabbitMQ) NotifyReturn() {
	// 前提需要设定Publish的mandatory为true
	go func() {
		// 消息是否正确入列
		for p := range r.channel.NotifyReturn(make(chan amqp.Return)) {
			// 这里是OK使用延迟交换机， 如果没有使用延迟交换机去掉_, ok :=ret.Headers["x-delay"] 和 if中的ok
			// _, ok := p.Headers["x-delay"]
			// if string(p.Body) != "" && !ok {
			if string(p.Body) != "" {
				logger.Info("消息没有正确入列:", string(p.Body), "; MessageId:", p.MessageId)
			}

		}
	}()

}

func (r *RabbitMQ) queueDeclare() error {
	q, err := r.channel.QueueDeclare(
		r.QueueName, // 如果为空,则随机生产队列名称
		true,
		false,
		false, // true 表示这个queue只能被当前连接访问，当连接断开时queue会被删除
		false,
		nil,
	)
	if err != nil {
		return err
	}
	if r.QueueName == "" {
		r.QueueName = q.Name
	}

	return nil
}

// queueBind 绑定队列和交换机
func (r *RabbitMQ) queueBind() error {
	return r.channel.QueueBind(
		r.QueueName, // 队列名称
		r.Key,       // 在pub/sub模式下key要为空
		r.ExchangeName,
		false,
		nil,
	)
}

func (r *RabbitMQ) RetryMsg(msg amqp.Delivery, ttl string) error {
	select {
	case <-r.ctx.Done():
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
		},
	); err != nil {
		logger.Info("---retry queue err: ", err)
	}
	return r.channel.PublishWithContext(
		r.ctx,
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
			Timestamp:    time.Now(),
			Expiration:   ttl,
		})

}

// DlxDeclare 声明死信交换机
// dlxExchange 死信交换机名称
// routingKind 死信交换机类型
func (r *RabbitMQ) DlxDeclare(dlxExchange, routingKind string) error {
	// 死信交换机
	return r.channel.ExchangeDeclare(
		dlxExchange, // 死信交换机名字
		routingKind, // 死信交换机类型
		true,        // 是否持久化
		false,
		false,
		false,
		nil,
	)
}

func setupCloseHandler(exitCh chan struct{}) {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	fmt.Println("---- signal.Notify begin----")
	go func() {
		<-c
		logger.Infof("close handler: Ctrl+C pressed in Terminal")
		close(exitCh)
	}()
}

func ParseUri(uri string) (amqp.URI, error) {
	return amqp.ParseURI(uri)
}
