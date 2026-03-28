package rabbit

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type MqDirect struct {
	*RabbitMQ
}

var _ RabbitMQInterface = (*MqDirect)(nil)

func newMqDirect(exchangeName, routingKey, mqURL string, opts ...Option) (*MqDirect, error) {
	exchangeName = strings.TrimSpace(exchangeName)
	routingKey = strings.TrimSpace(routingKey)
	if exchangeName == "" || routingKey == "" {
		return nil, fmt.Errorf("exchange name and routing key are required")
	}

	option, err := newOption(mqURL, opts...)
	if err != nil {
		return nil, err
	}
	option.ExchangeName = exchangeName
	option.Routing = routingKey
	option, err = normalizeOption(option)
	if err != nil {
		return nil, err
	}

	rabbitmq, err := newRabbitMQ(option)
	if err != nil {
		return nil, err
	}

	return &MqDirect{RabbitMQ: rabbitmq}, nil
}

// NewPubDirect 创建 direct 模式发布端实例。
func NewPubDirect(exchangeName, routingKey, mqURL string, opts ...Option) (*MqDirect, error) {
	return newMqDirect(exchangeName, routingKey, mqURL, opts...)
}

// NewConsumeDirect 创建 direct 模式消费端实例。
func NewConsumeDirect(exchangeName, routingKey, mqURL string, opts ...Option) (*MqDirect, error) {
	return newMqDirect(exchangeName, routingKey, mqURL, opts...)
}

// Publish 向 direct exchange 发布一条持久化消息。
func (r *MqDirect) Publish(message string, handler MsgHandler) error {
	ctx := r.contextOrBackground()
	body := []byte(message)

	select {
	case <-ctx.Done():
		notifyFailed(handler, r.failedMessage(body, ""))
		return r.canceledError("publish")
	default:
	}

	msgID := uuid.NewString()
	err := r.publishWithChannel(func(ch *amqp.Channel) error {
		if err := r.ensurePublishDeclared("exchange", ch, r.declareExchange); err != nil {
			return err
		}

		confirmation, err := ch.PublishWithDeferredConfirmWithContext(
			ctx,
			r.ExchangeName,
			r.Routing,
			false,
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				MessageId:    msgID,
				Body:         body,
				Headers: amqp.Table{
					"x-retry": int32(0),
				},
			},
		)
		if err != nil {
			return err
		}

		return waitForDeferredConfirm(ctx, confirmation)
	})
	if err != nil {
		notifyFailed(handler, r.failedMessage(body, msgID))
		return err
	}

	return nil
}

// Consume 持续消费 direct 模式消息。
func (r *MqDirect) Consume(handler MsgHandler) error {
	if handler == nil {
		return fmt.Errorf("handler is required")
	}

	ctx := r.contextOrBackground()
	retryAttempt := 0

	for {
		select {
		case <-ctx.Done():
			return r.canceledError("consume")
		default:
		}

		ch, err := r.openConsumerChannel()
		if err != nil {
			if waitErr := r.waitRetry("consume", &retryAttempt, "direct consumer open channel failed: %v, reconnecting...", err); waitErr != nil {
				return waitErr
			}
			continue
		}

		queue, err := r.declareBoundQueue(ch, nil)
		if err != nil {
			closeAMQPChannel(ch)
			if waitErr := r.waitRetry("consume", &retryAttempt, "direct consumer declare queue failed: %v, reconnecting...", err); waitErr != nil {
				return waitErr
			}
			continue
		}

		deliveries, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
		if err != nil {
			closeAMQPChannel(ch)
			if waitErr := r.waitRetry("consume", &retryAttempt, "direct consumer start consume failed: %v, reconnecting...", err); waitErr != nil {
				return waitErr
			}
			continue
		}
		retryAttempt = 0

		notifyClose := ch.NotifyClose(make(chan *amqp.Error, 1))
		reconnect := false

		for !reconnect {
			select {
			case <-ctx.Done():
				closeAMQPChannel(ch)
				return r.canceledError("consume")
			case err, ok := <-notifyClose:
				closeAMQPChannel(ch)
				if ok && err != nil {
					currentLogger().Infof("direct consumer channel closed: %v", err)
				}
				reconnect = true
			case msg, ok := <-deliveries:
				if !ok {
					closeAMQPChannel(ch)
					reconnect = true
					continue
				}

				if err := r.handleDelivery(msg, handler); err != nil {
					closeAMQPChannel(ch)
					return err
				}
			}
		}

		select {
		case <-ctx.Done():
			return r.canceledError("consume")
		case <-time.After(time.Second):
		}
	}
}

// RetryMsg 将当前 delivery 手动发送到 retry queue。
func (r *MqDirect) RetryMsg(msg amqp.Delivery, ttl string) error {
	headers := copyHeaders(msg.Headers)
	return r.publishRetryMessage(msg, headers, ttl)
}

// PublishDelay 发布一条延迟消息。
// ttl 单位为毫秒字符串，例如 "5000" 表示 5 秒。
func (r *MqDirect) PublishDelay(message string, handler MsgHandler, ttl string) error {
	ctx := r.contextOrBackground()
	body := []byte(message)

	select {
	case <-ctx.Done():
		notifyFailed(handler, r.failedMessage(body, ""))
		return r.canceledError("publish delay")
	default:
	}

	delayQueue := r.baseName() + ".delay"
	msgID := uuid.NewString()
	err := r.publishWithChannel(func(ch *amqp.Channel) error {
		if err := r.ensurePublishDeclared("exchange", ch, r.declareExchange); err != nil {
			return err
		}

		if err := r.ensurePublishDeclared("delay:"+delayQueue, ch, func(ch *amqp.Channel) error {
			_, err := ch.QueueDeclare(
				delayQueue,
				true,
				false,
				false,
				false,
				amqp.Table{
					"x-dead-letter-exchange":    r.ExchangeName,
					"x-dead-letter-routing-key": r.Routing,
					"x-max-priority":            simpleQueueMaxPriority,
				},
			)
			return err
		}); err != nil {
			return err
		}

		confirmation, err := ch.PublishWithDeferredConfirmWithContext(
			ctx,
			"",
			delayQueue,
			true,
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				MessageId:    msgID,
				Body:         body,
				Expiration:   ttl,
				Headers: amqp.Table{
					"x-retry": int32(0),
				},
			},
		)
		if err != nil {
			return err
		}

		return waitForDeferredConfirm(ctx, confirmation)
	})
	if err != nil {
		notifyFailed(handler, r.failedMessage(body, msgID))
		return err
	}

	return nil
}

// ConsumeDelay 在 direct 模式下等价于 Consume。
func (r *MqDirect) ConsumeDelay(handler MsgHandler) error {
	return r.Consume(handler)
}

// ConsumeFailToDlx 消费主队列，并在业务处理失败时直接转入死信队列。
func (r *MqDirect) ConsumeFailToDlx(handler MsgHandler) error {
	if handler == nil {
		return fmt.Errorf("handler is required")
	}

	ctx := r.contextOrBackground()
	retryAttempt := 0

	for {
		select {
		case <-ctx.Done():
			return r.canceledError("consume fail-to-dlx")
		default:
		}

		ch, err := r.openConsumerChannel()
		if err != nil {
			if waitErr := r.waitRetry("consume fail-to-dlx", &retryAttempt, "direct fail-to-dlx open channel failed: %v, reconnecting...", err); waitErr != nil {
				return waitErr
			}
			continue
		}

		queue, _, err := r.declareDLXTopology(ch)
		if err != nil {
			closeAMQPChannel(ch)
			if waitErr := r.waitRetry("consume fail-to-dlx", &retryAttempt, "direct fail-to-dlx declare topology failed: %v, reconnecting...", err); waitErr != nil {
				return waitErr
			}
			continue
		}

		deliveries, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
		if err != nil {
			closeAMQPChannel(ch)
			if waitErr := r.waitRetry("consume fail-to-dlx", &retryAttempt, "direct fail-to-dlx start consume failed: %v, reconnecting...", err); waitErr != nil {
				return waitErr
			}
			continue
		}
		retryAttempt = 0

		notifyClose := ch.NotifyClose(make(chan *amqp.Error, 1))
		reconnect := false

		for !reconnect {
			select {
			case <-ctx.Done():
				closeAMQPChannel(ch)
				return r.canceledError("consume fail-to-dlx")
			case err, ok := <-notifyClose:
				closeAMQPChannel(ch)
				if ok && err != nil {
					currentLogger().Infof("direct fail-to-dlx channel closed: %v", err)
				}
				reconnect = true
			case msg, ok := <-deliveries:
				if !ok {
					closeAMQPChannel(ch)
					reconnect = true
					continue
				}

				if err := r.handleDLXDelivery(msg, handler); err != nil {
					closeAMQPChannel(ch)
					return err
				}
			}
		}

		select {
		case <-ctx.Done():
			return r.canceledError("consume fail-to-dlx")
		case <-time.After(time.Second):
		}
	}
}

// ConsumeDlx 持续消费 direct 模式的死信队列。
func (r *MqDirect) ConsumeDlx(handler MsgHandler) error {
	if handler == nil {
		return fmt.Errorf("handler is required")
	}

	ctx := r.contextOrBackground()
	retryAttempt := 0

	for {
		select {
		case <-ctx.Done():
			return r.canceledError("consume dlx")
		default:
		}

		ch, err := r.openConsumerChannel()
		if err != nil {
			if waitErr := r.waitRetry("consume dlx", &retryAttempt, "direct dlx open channel failed: %v, reconnecting...", err); waitErr != nil {
				return waitErr
			}
			continue
		}

		_, dlqName, err := r.declareDLXTopology(ch)
		if err != nil {
			closeAMQPChannel(ch)
			if waitErr := r.waitRetry("consume dlx", &retryAttempt, "direct dlx declare topology failed: %v, reconnecting...", err); waitErr != nil {
				return waitErr
			}
			continue
		}

		deliveries, err := ch.Consume(dlqName, "", false, false, false, false, nil)
		if err != nil {
			closeAMQPChannel(ch)
			if waitErr := r.waitRetry("consume dlx", &retryAttempt, "direct dlx start consume failed: %v, reconnecting...", err); waitErr != nil {
				return waitErr
			}
			continue
		}
		retryAttempt = 0

		notifyClose := ch.NotifyClose(make(chan *amqp.Error, 1))
		reconnect := false

		for !reconnect {
			select {
			case <-ctx.Done():
				closeAMQPChannel(ch)
				return r.canceledError("consume dlx")
			case err, ok := <-notifyClose:
				closeAMQPChannel(ch)
				if ok && err != nil {
					currentLogger().Infof("direct dlx consumer channel closed: %v", err)
				}
				reconnect = true
			case msg, ok := <-deliveries:
				if !ok {
					closeAMQPChannel(ch)
					reconnect = true
					continue
				}

				if err := r.handleDLQDelivery(msg, handler); err != nil {
					closeAMQPChannel(ch)
					return err
				}
			}
		}

		select {
		case <-ctx.Done():
			return r.canceledError("consume dlx")
		case <-time.After(time.Second):
		}
	}
}

func (r *MqDirect) declareExchange(ch *amqp.Channel) error {
	return ch.ExchangeDeclare(r.ExchangeName, amqp.ExchangeDirect, true, false, false, false, nil)
}

func (r *MqDirect) declareBoundQueue(ch *amqp.Channel, args amqp.Table) (amqp.Queue, error) {
	if err := r.declareExchange(ch); err != nil {
		return amqp.Queue{}, err
	}

	queueArgs := amqp.Table{
		"x-max-priority": simpleQueueMaxPriority,
	}
	for key, value := range args {
		queueArgs[key] = value
	}

	queue, err := ch.QueueDeclare(r.QueueName, true, false, false, false, queueArgs)
	if err != nil {
		return amqp.Queue{}, err
	}

	if err := ch.QueueBind(queue.Name, r.Routing, r.ExchangeName, false, nil); err != nil {
		return amqp.Queue{}, err
	}

	return queue, nil
}

func (r *MqDirect) declareDLXTopology(ch *amqp.Channel) (amqp.Queue, string, error) {
	dlxExchange := r.ExchangeName + ".dlx"
	dlxRouting := r.Routing + ".dlx"
	dlqName := r.baseName() + ".dlq"

	if err := ch.ExchangeDeclare(dlxExchange, amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
		return amqp.Queue{}, "", err
	}

	queue, err := r.declareBoundQueue(ch, amqp.Table{
		"x-dead-letter-exchange":    dlxExchange,
		"x-dead-letter-routing-key": dlxRouting,
	})
	if err != nil {
		return amqp.Queue{}, "", err
	}

	if _, err := ch.QueueDeclare(dlqName, true, false, false, false, nil); err != nil {
		return amqp.Queue{}, "", err
	}

	if err := ch.QueueBind(dlqName, dlxRouting, dlxExchange, false, nil); err != nil {
		return amqp.Queue{}, "", err
	}

	return queue, dlqName, nil
}

func (r *MqDirect) handleDelivery(msg amqp.Delivery, handler MsgHandler) error {
	select {
	case <-r.contextOrBackground().Done():
		notifyFailed(handler, r.failedMessage(msg.Body, msg.MessageId))
		if err := msg.Nack(false, true); err != nil {
			return err
		}
		return r.canceledError("consume")
	default:
	}

	if err := handler.Process(msg.Body, msg.MessageId); err != nil {
		retry := retryCount(msg.Headers)
		if retry >= r.maxRetry() {
			notifyFailed(handler, r.failedMessage(msg.Body, msg.MessageId))
			return msg.Reject(false)
		}

		headers := copyHeaders(msg.Headers)
		headers["x-retry"] = retry + 1

		if err := r.publishRetryMessage(msg, headers, r.retryTTL()); err != nil {
			if nackErr := msg.Nack(false, true); nackErr != nil {
				return fmt.Errorf("publish retry message: %w; nack original: %v", err, nackErr)
			}
			return err
		}

		return r.ackAfterRetryPublish(msg)
	}

	return msg.Ack(false)
}

func (r *MqDirect) handleDLXDelivery(msg amqp.Delivery, handler MsgHandler) error {
	return r.RabbitMQ.handleDLXDelivery(msg, handler, "consume fail-to-dlx")
}

func (r *MqDirect) handleDLQDelivery(msg amqp.Delivery, handler MsgHandler) error {
	return r.RabbitMQ.handleDLQDelivery(msg, handler, "consume dlx")
}

func (r *MqDirect) publishRetryMessage(msg amqp.Delivery, headers amqp.Table, ttl string) error {
	ctx := r.contextOrBackground()

	select {
	case <-ctx.Done():
		return r.canceledError("retry publish")
	default:
	}

	retryQueue := r.baseName() + ".retry"
	messageID := msg.MessageId
	if messageID == "" {
		messageID = uuid.NewString()
	}

	return r.publishWithChannel(func(ch *amqp.Channel) error {
		if err := r.ensurePublishDeclared("exchange", ch, r.declareExchange); err != nil {
			return err
		}

		if err := r.ensurePublishDeclared("retry:"+retryQueue, ch, func(ch *amqp.Channel) error {
			_, err := ch.QueueDeclare(
				retryQueue,
				true,
				false,
				false,
				false,
				amqp.Table{
					"x-dead-letter-exchange":    r.ExchangeName,
					"x-dead-letter-routing-key": r.Routing,
					"x-max-priority":            simpleQueueMaxPriority,
				},
			)
			return err
		}); err != nil {
			return err
		}

		confirmation, err := ch.PublishWithDeferredConfirmWithContext(
			ctx,
			"",
			retryQueue,
			true,
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  msg.ContentType,
				Body:         msg.Body,
				Headers:      headers,
				MessageId:    messageID,
				Timestamp:    time.Now(),
				Expiration:   ttl,
				Priority:     msg.Priority,
			},
		)
		if err != nil {
			return err
		}

		return waitForDeferredConfirm(ctx, confirmation)
	})
}

func (r *MqDirect) baseName() string {
	if strings.TrimSpace(r.QueueName) != "" {
		return safeNamePart(r.QueueName)
	}

	return safeNamePart(r.ExchangeName) + "." + safeNamePart(r.Routing)
}
