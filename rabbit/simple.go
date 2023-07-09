// @Author xiaozhaofu 2023/7/6 20:59:00
package rabbit

import (
	"errors"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
1 Simple模式，最简单最常用的模式
2 Work模式，一个消息只能被一个消费者消费
*/

// RabbitMQSimple 简单模式下的RabbitMQ实例
type RabbitMQSimple struct {
	*RabbitMQ
}

// NewRabbitMQSimple 创建简单模式下的实例，只需要queueName这个参数，其中exchange是默认的，key则不需要。
func NewRabbitMQSimple(queueName, mqUrl string) (rabbitMQSimple *RabbitMQSimple, err error) {
	// 判断是否输入必要的信息
	if queueName == "" || mqUrl == "" {
		log.Printf("QueueName and mqUrl is required,\nbut queueName and mqUrl are %s and %s.", queueName, mqUrl)
		return nil, errors.New("QueueName and mqUrl is required")
	}
	rabbitmq, err := NewRabbitMQ("", queueName, "", mqUrl)
	rabbitmq.SetConfirm()

	if err != nil {
		return nil, err
	}
	return &RabbitMQSimple{
		rabbitmq,
	}, nil
}

// Publish 直接模式,生产者.
func (r *RabbitMQSimple) Publish(message string) (err error) {

	// 确认消息监听函数， 启动一个协程，监听消息发送情况
	go r.ListenConfirm()
	go r.NotifyReturn()

	//1.申请队列,如果队列不存在，则会自动创建，如果队列存在则跳过创建直接使用  这样的好处保障队列存在，消息能发送到队列当中
	_, err = r.channel.QueueDeclare(
		r.QueueName, // 队列名字
		false,       //进入的消息是否持久化 进入队列如果不消费那么消息就在队列里面 如果重启服务器那么这个消息就没啦 通常设置为false
		false,       //是否为自动删除  意思是最后一个消费者断开链接以后是否将消息从队列当中删除  默认设置为false不自动删除
		false,       //是否具有排他性
		false,       //是否阻塞 发送消息以后是否要等待消费者的响应 消费了下一个才进来 就跟golang里面的无缓冲channle一个道理 默认为非阻塞即可设置为false
		nil,         //其他的属性，没有则直接诶传入空即可 nil  nil,
	)

	if err != nil {
		return err
	}
	//confirmsCh := make(chan *amqp.DeferredConfirmation)
	// 2 发送消息到队列中
	err = r.channel.PublishWithContext(
		r.ctx,
		r.ExchangeName, // 交换机名称，simple模式下默认为空 我们在上边已经赋值为空了  虽然为空 但其实也是在用的rabbitmq当中的default交换机运行
		r.QueueName,    // 路由参数， 这里使用队列的名字作为路由参数
		true,           //如果为true 会根据exchange类型和routkey规则，如果无法找到符合条件的队列那么会把发送的消息返还给发送者
		false,          //如果为true,当exchange发送消息到队列后发现队列上没有绑定消费者则会把消息返还给发送者
		amqp.Publishing{
			Headers: amqp.Table{},
			// 消息内容持久化，这个很关键
			//DeliveryMode: amqp.Persistent,
			ContentType: "text/plain",
			Body:        []byte(message),
		})
	return err
}

// Consume 直接模式，消费者
func (r *RabbitMQSimple) Consume() (consumeChan <-chan amqp.Delivery, err error) {
	// 1 申请队列,如果队列不存在则自动创建,存在则跳过
	q, err := r.channel.QueueDeclare(
		r.QueueName,
		false, // 是否持久化
		false, // 是否自动删除
		false, // 是否具有排他性
		false, // 是否阻塞处理
		nil,   // 额外的属性
	)
	if err != nil {
		return nil, err
	}

	// 2 接收消息
	consumeChan, err = r.channel.Consume(
		q.Name, // 队列名
		"",     // 用来区分多个消费者， 消费者唯一id，不填，则自动生成一个唯一值
		true,   // 是否自动应答,告诉我已经消费完了
		false,
		false, // 若设置为true,则表示为不能将同一个connection中发送的消息传递给这个connection中的消费者.
		false, // 消费队列是否设计阻塞
		nil,
	)
	if err != nil {
		return nil, err
	}

	return consumeChan, nil
}
