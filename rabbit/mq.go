// @Author xiaozhaofu 2023/7/6 20:27:00
package rabbit

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pkg/errors"

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

// todo：设置重试的次数
const delay = 3 // reconnect after delay seconds
// RabbitMQ RabbitMQ实例
type RabbitMQ struct {
	conn         *amqp.Connection // 连接
	channel      *amqp.Channel    // 管道
	ExchangeName string           // 交换机名称
	QueueName    string           // 队列名称
	Key          string           // Binding Key/Routing Key, Simple模式 几乎用不到
	MqURL        string           // 连接信息-amqp://账号:密码@地址:端口号/-amqp://guest:guest@127.0.0.1:5672/
	ctx          context.Context
	cancel       context.CancelFunc

	notifyConfirm chan amqp.Confirmation // 确认发送到mq的channel

	notifyClose chan *amqp.Error // 如果异常关闭，会接受数据

	msgExpiration string // 消息过期时间
}

// RabbitMQInterface 定义RabbitMQ实例的接口
// 每种RabbitMQ实例都有发布和消费两种功能
type RabbitMQInterface interface {
	Publish(message string) (err error)
	Consume(handler func([]byte) error) (err error)
}

// NewRabbitMQ 创建一个RabbitMQ实例
func newRabbitMQ(exchangeName, queueName, key, mqUrl string) (mq *RabbitMQ, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	mq = &RabbitMQ{
		QueueName:    queueName,
		ExchangeName: exchangeName,
		Key:          key,
		MqURL:        mqUrl,
		ctx:          ctx,
		cancel:       cancel,
	}

	// 创建rabbitmq连接
	config := amqp.Config{
		//Vhost:      "/",
		Properties: amqp.NewConnectionProperties(),
		Heartbeat:  10 * time.Second,
		Locale:     "en_US",
	}
	config.Properties.SetClientConnectionName("producer-with-confirms")

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
	// auto reconnect channel
	mq.NotifyChannelClose()

	return
}

func (mq *RabbitMQ) NotifyConnectionClose(config amqp.Config) {
	go func() {
		for {
			fmt.Println("---------mq.conn.NotifyClose---------")
			reason, ok := <-mq.conn.NotifyClose(make(chan *amqp.Error))
			if !ok {
				log.Println("connection closed")
				break
			}
			log.Printf("connection closed, reason: %v", reason)
			for {
				time.Sleep(delay * time.Second)
				reconnect, err := amqp.DialConfig(mq.MqURL, config)
				if err == nil {
					mq.conn = reconnect
					log.Println("reconnect success")
					break
				}
				log.Printf("reconnect failed, err: %v", err)
			}

		}
	}()
}

// NotifyChannelClose auto reconnect channel
func (mq *RabbitMQ) NotifyChannelClose() {
	go func() {
		for {
			fmt.Println("---------mq.channel.NotifyClose---------")
			reason, ok := <-mq.channel.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok || mq.channel.IsClosed() {
				log.Println("channel closed")
				_ = mq.channel.Close() // close again, ensure closed flag set when connection closed
				break
			}
			log.Printf("channel closed, reason: %v", reason)
			for {
				time.Sleep(delay * time.Second)
				ch, err := mq.conn.Channel()
				if err == nil {
					log.Println("channel recreate success")
					mq.channel = ch
					break
				}
				log.Printf("channel recreate failed, err: %v", err)
			}
		}
	}()

}

// Destroy 断开channel和connection
func (mq *RabbitMQ) Destroy() {
	log.Printf("%s,%s is closed!!!", mq.ExchangeName, mq.QueueName)
	mq.channel.Close()
	mq.conn.Close()
	mq.cancel()

}

func (mq *RabbitMQ) Ctx() context.Context {
	return mq.ctx
}

// SetConfirm 设置监听消息发送
func (mq *RabbitMQ) SetConfirm() error {
	err := mq.channel.Confirm(false)
	if err != nil {
		log.Println("this.Channel.Confirm  ", err)
		return errors.WithMessage(err, "Channel.Confirm")
	}
	mq.notifyConfirm = mq.channel.NotifyPublish(make(chan amqp.Confirmation))
	return nil
}

// ListenConfirm 确认消息成功发布到rabbitmq channel,即消息从生产者到 Broker
func (mq *RabbitMQ) ListenConfirm() {
	go func() {
		for c := range mq.notifyConfirm {
			if c.Ack {
				log.Println("confirm:消息发送成功")
			} else {
				// 这里表示消息发送到mq失败,可以处理失败流程
				log.Println("confirm:消息发送失败")
			}
		}
	}()
}

// NotifyReturn  确保消息从交换机到队列入列成功
func (mq *RabbitMQ) NotifyReturn() {
	// 前提需要设定Publish的mandatory为true
	go func() {
		// 消息是否正确入列
		for p := range mq.channel.NotifyReturn(make(chan amqp.Return)) {
			// 这里是OK使用延迟交换机， 如果没有使用延迟交换机去掉_, ok :=ret.Headers["x-delay"] 和 if中的ok
			// _, ok := p.Headers["x-delay"]
			// if string(p.Body) != "" && !ok {
			if string(p.Body) != "" {
				log.Println("消息没有正确入列:", string(p.Body), "; MessageId:", p.MessageId)
			}

		}
	}()

}

func setupCloseHandler(exitCh chan struct{}) {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	fmt.Println("---- signal.Notify begin----")
	go func() {
		<-c
		log.Printf("close handler: Ctrl+C pressed in Terminal")
		close(exitCh)
	}()
}

func ParseUri(uri string) (amqp.URI, error) {
	return amqp.ParseURI(uri)
}
