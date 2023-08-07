// @Author xiaozhaofu 2023/7/6 20:59:00
package rabbit

import (
	"errors"
	"fmt"
	"log"

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
		log.Printf("QueueName and mqUrl is required,\nbut queueName and mqUrl are %s and %s.", queueName, mqUrl)
		return nil, errors.New("QueueName and mqUrl is required")
	}
	rabbitmq, err := newRabbitMQ("", queueName, "", mqUrl)
	if err != nil {
		return nil, err
	}

	rabbitmq.SetConfirm()

	if err != nil {
		return nil, err
	}
	return &RabbitMQSimple{
		rabbitmq,
	}, nil
}

// Publish 直接模式,生产者.
func (mq *RabbitMQSimple) Publish(message string) (err error) {

	select {
	case <-mq.ctx.Done():
		return fmt.Errorf("context cancel publish")
	default:
	}
	// 确认消息监听函数， 启动一个协程，监听消息发送情况
	mq.ListenConfirm()

	// ------- 死信交换机
	if err = mq.channel.ExchangeDeclare(
		"dead-letter-exchange-"+mq.QueueName, // 死信交换机
		"fanout",                             // 死信交换机类型
		true,                                 // 是否持久化
		false,                                // 是否自动删除
		false,                                // 是否内置
		false,                                // 是否等待服务器响应
		nil,                                  // 其他参数
	); err != nil {
		log.Println("dead-letter-exchange-"+mq.QueueName+" exchange declare error", err)
		return err
	}

	// 1.申请队列,如果队列不存在，则会自动创建，如果队列存在则跳过创建直接使用  这样的好处保障队列存在，消息能发送到队列当中
	_, err = mq.channel.QueueDeclare(
		mq.QueueName, // 队列名字
		true,         // 进入的消息是否持久化 进入队列如果不消费那么消息就在队列里面 如果重启服务器那么这个消息就没啦 通常设置为false
		false,        // 是否为自动删除  意思是最后一个消费者断开链接以后是否将消息从队列当中删除  默认设置为false不自动删除
		false,        // 是否具有排他性
		false,        // 是否阻塞 发送消息以后是否要等待消费者的响应 消费了下一个才进来 就跟golang里面的无缓冲channle一个道理 默认为非阻塞即可设置为false
		amqp.Table{
			// "x-message-ttl":          "",
			"x-dead-letter-exchange": "dead-letter-exchange-" + mq.QueueName, // 死信交换机
			// "x-dead-letter-routing-key": dlxRouting,  // 死信路由
			// "x-dead-letter-queue": "dead-letter-queue" + mq.QueueName, // 死信队列

		}, // 其他的属性，没有则直接诶传入空即可 nil  nil,
	)

	if err != nil {
		return err
	}
	// confirmsCh := make(chan *amqp.DeferredConfirmation)

	// 2 发送消息到队列中
	msgId := uuid.New().String()
	return mq.channel.PublishWithContext(
		mq.ctx,
		mq.ExchangeName, // 交换机名称，simple模式下默认为空 我们在上边已经赋值为空了  虽然为空 但其实也是在用的rabbitmq当中的default交换机运行
		mq.QueueName,    // 路由参数， 这里使用队列的名字作为路由参数
		true,            // 如果为true 会根据exchange类型和routkey规则，如果无法找到符合条件的队列那么会把发送的消息返还给发送者
		false,           // 如果为true,当exchange发送消息到队列后发现队列上没有绑定消费者则会把消息返还给发送者
		amqp.Publishing{
			// Headers: amqp.Table{},
			// 消息内容持久化，这个很关键
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			MessageId:    msgId,
			Body:         []byte(message),
			Expiration:   mq.MsgExpiration(), // push 时 在消息本体上设置expiration超时时间，单位为毫秒级别 类型为 string
		})

}

// Consume 直接模式，消费者
func (mq *RabbitMQSimple) Consume(handler func([]byte) error) (err error) {
	// 1 申请队列,如果队列不存在则自动创建,存在则跳过
	q, err := mq.channel.QueueDeclare(
		mq.QueueName,
		true,  // 是否持久化
		false, // 是否自动删除
		false, // 是否具有排他性
		false, // 是否阻塞处理
		nil,   // 额外的属性
	)
	if err != nil {
		return err
	}

	// 2 接收消息
	deliveries, err := mq.channel.Consume(
		q.Name, // 队列名
		"",     // 用来区分多个消费者， 消费者唯一id，不填，则自动生成一个唯一值
		false,  // 是否自动应答,告诉我已经消费完了
		false,  // true 表示这个queue只能被这个consumer访问
		false,  // 若设置为true,则表示为不能将同一个connection中发送的消息传递给这个connection中的消费者.
		false,  // 消费队列是否设计阻塞
		nil,
	)
	if err != nil {
		return err
	}
	for msg := range deliveries {
		select {
		case <-mq.Ctx().Done():
			fmt.Println("======ctx done==========")
			if err := msg.Reject(true); err != nil {
				// 拒绝一条消息，true表示将消息重新放回队列, 如果失败，记录日志 或 发送到其他队列等措施来处理错误
				fmt.Println("reject error: ", err)
			}
			return fmt.Errorf("context cancel Consume")
		default:

		}
		// fmt.Println("-----messageId: ", msg.MessageId)

		if err := handler(msg.Body); err != nil {
			fmt.Println("---test handler error----")
			if err = msg.Reject(true); err != nil {
				// 拒绝一条消息，true表示将消息重新放回队列, 如果失败，记录日志 或 发送到其他队列等措施来处理错误
				fmt.Println("reject error: ", err)
			}
			// mq.DlqConsume(mq.QueueName, "dead-letter-queue-"+mq.QueueName, "", dlxDo)

			continue
		}

		// // 确认一条消息，false表示确认当前消息，true表示确认当前消息和之前所有未确认的消息
		if err := msg.Ack(false); err != nil {
			fmt.Println("message ack error:", err, " message id: ", msg.MessageId)
			continue
		}

	}

	return nil
}

func dlxDo(msg []byte) error {
	fmt.Println("-----dlx message: ", string(msg))
	return nil
}
