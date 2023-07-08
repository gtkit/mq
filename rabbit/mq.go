// @Author xiaozhaofu 2023/7/6 20:27:00
package rabbit

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
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
}

// RabbitMQInterface 定义RabbitMQ实例的接口
// 每种RabbitMQ实例都有发布和消费两种功能
type RabbitMQInterface interface {
	Publish(message string) (err error)
	Consume() (consumeChan <-chan amqp.Delivery, err error)
}

// NewRabbitMQ 创建一个RabbitMQ实例
func NewRabbitMQ(exchangeName, queueName, key, mqUrl string) (mq *RabbitMQ, err error) {
	fmt.Println("--------- NewRabbitMQ -------------")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
		Vhost:      "/",
		Properties: amqp.NewConnectionProperties(),
	}
	config.Properties.SetClientConnectionName("producer-with-confirms")

	// rabbitmq.conn, err = amqp.Dial(rabbitmq.MqURL)
	mq.conn, err = amqp.DialConfig(mq.MqURL, config)
	if err != nil {
		return nil, err
	}
	mq.conn.ConnectionState()
	//
	mq.channel, err = mq.conn.Channel()
	if err != nil {
		return nil, err
	}
	return
}

// Destroy 断开channel和connection
func (r *RabbitMQ) Destroy() {

	r.channel.Close()
	r.conn.Close()
	r.cancel()
	log.Printf("%s,%s is closed!!!", r.ExchangeName, r.QueueName)

}
func ParseUri(uri string) (amqp.URI, error) {
	return amqp.ParseURI(uri)
}
