package rabbit

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

const simpleQueueMaxPriority = 10

var _ RabbitMQInterface = (*MqSimple)(nil)

type MqSimple struct {
	*RabbitMQ
}

func NewPubSimple(queueName, mqURL string, opts ...Option) (*MqSimple, error) {
	queueName = strings.TrimSpace(queueName)
	if queueName == "" {
		return nil, fmt.Errorf("queue name is required")
	}

	option, err := newOption(mqURL, opts...)
	if err != nil {
		return nil, err
	}
	option.QueueName = queueName

	rabbitmq, err := newRabbitMQ(option)
	if err != nil {
		return nil, err
	}

	return &MqSimple{RabbitMQ: rabbitmq}, nil
}

func NewConsumeSimple(queueName, mqURL string, opts ...Option) (*MqSimple, error) {
	queueName = strings.TrimSpace(queueName)
	if queueName == "" {
		return nil, fmt.Errorf("queue name is required")
	}

	option, err := newOption(mqURL, opts...)
	if err != nil {
		return nil, err
	}
	option.QueueName = queueName

	rabbitmq, err := newRabbitMQ(option)
	if err != nil {
		return nil, err
	}

	return &MqSimple{RabbitMQ: rabbitmq}, nil
}

func (r *MqSimple) Publish(message string, handler MsgHandler) error {
	ctx := r.contextOrBackground()

	select {
	case <-ctx.Done():
		notifyFailed(handler, r.failedMessage([]byte(message), ""))
		return r.canceledError("publish")
	default:
	}

	ch, err := r.openPublishChannel()
	if err != nil {
		return err
	}
	defer closeAMQPChannel(ch)

	if _, err := r.declareQueue(ch, nil); err != nil {
		return err
	}

	msgID := uuid.NewString()
	confirmation, err := ch.PublishWithDeferredConfirmWithContext(
		ctx,
		"",
		r.QueueName,
		true,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			MessageId:    msgID,
			Body:         []byte(message),
			Headers: amqp.Table{
				"x-retry": int32(0),
			},
			Priority: 1,
		},
	)
	if err != nil {
		return err
	}

	if err := waitForDeferredConfirm(ctx, confirmation); err != nil {
		notifyFailed(handler, r.failedMessage([]byte(message), msgID))
		return err
	}

	return nil
}

func (r *MqSimple) Consume(handler MsgHandler) error {
	if handler == nil {
		return fmt.Errorf("handler is required")
	}

	ctx := r.contextOrBackground()

	for {
		select {
		case <-ctx.Done():
			return r.canceledError("consume")
		default:
		}

		ch, err := r.openConsumerChannel()
		if err != nil {
			return err
		}

		queue, err := r.declareQueue(ch, nil)
		if err != nil {
			closeAMQPChannel(ch)
			return err
		}

		deliveries, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
		if err != nil {
			closeAMQPChannel(ch)
			return err
		}

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
					logger.Infof("simple consumer channel closed: %v", err)
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

func (r *MqSimple) RetryMsg(msg amqp.Delivery, ttl string) error {
	headers := copyHeaders(msg.Headers)
	return r.publishRetryMessage(msg, headers, ttl)
}

func (r *MqSimple) PublishDelay(message string, handler MsgHandler, ttl string) error {
	ctx := r.contextOrBackground()

	select {
	case <-ctx.Done():
		notifyFailed(handler, r.failedMessage([]byte(message), ""))
		return r.canceledError("publish delay")
	default:
	}

	ch, err := r.openPublishChannel()
	if err != nil {
		return err
	}
	defer closeAMQPChannel(ch)

	if _, err := r.declareQueue(ch, nil); err != nil {
		return err
	}

	delayQueue := r.QueueName + "-delay"
	if _, err := ch.QueueDeclare(
		delayQueue,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": r.QueueName,
			"x-max-priority":            simpleQueueMaxPriority,
		},
	); err != nil {
		return err
	}

	msgID := uuid.NewString()
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
			Body:         []byte(message),
			Expiration:   ttl,
			Headers: amqp.Table{
				"x-retry": int32(0),
			},
		},
	)
	if err != nil {
		return err
	}

	if err := waitForDeferredConfirm(ctx, confirmation); err != nil {
		notifyFailed(handler, r.failedMessage([]byte(message), msgID))
		return err
	}

	return nil
}

func (r *MqSimple) ConsumeDelay(handler MsgHandler) error {
	return r.Consume(handler)
}

func (r *MqSimple) PublishWithDlx(message string) error {
	ctx := r.contextOrBackground()

	select {
	case <-ctx.Done():
		return r.canceledError("publish with dlx")
	default:
	}

	ch, err := r.openPublishChannel()
	if err != nil {
		return err
	}
	defer closeAMQPChannel(ch)

	if _, _, err := r.declareDLXTopology(ch); err != nil {
		return err
	}

	msgID := uuid.NewString()
	confirmation, err := ch.PublishWithDeferredConfirmWithContext(
		ctx,
		"",
		r.QueueName,
		true,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			MessageId:    msgID,
			Body:         []byte(message),
		},
	)
	if err != nil {
		return err
	}

	return waitForDeferredConfirm(ctx, confirmation)
}

func (r *MqSimple) ConsumeFailToDlx(handler MsgHandler) error {
	if handler == nil {
		return fmt.Errorf("handler is required")
	}

	ctx := r.contextOrBackground()

	for {
		select {
		case <-ctx.Done():
			return r.canceledError("consume fail-to-dlx")
		default:
		}

		ch, err := r.openConsumerChannel()
		if err != nil {
			return err
		}

		queue, _, err := r.declareDLXTopology(ch)
		if err != nil {
			closeAMQPChannel(ch)
			return err
		}

		deliveries, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
		if err != nil {
			closeAMQPChannel(ch)
			return err
		}

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
					logger.Infof("simple fail-to-dlx channel closed: %v", err)
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

func (r *MqSimple) ConsumeDlx(handler MsgHandler) error {
	if handler == nil {
		return fmt.Errorf("handler is required")
	}

	ctx := r.contextOrBackground()

	for {
		select {
		case <-ctx.Done():
			return r.canceledError("consume dlx")
		default:
		}

		ch, err := r.openConsumerChannel()
		if err != nil {
			return err
		}

		_, dlqName, err := r.declareDLXTopology(ch)
		if err != nil {
			closeAMQPChannel(ch)
			return err
		}

		deliveries, err := ch.Consume(dlqName, "", false, false, false, false, nil)
		if err != nil {
			closeAMQPChannel(ch)
			return err
		}

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
					logger.Infof("simple dlx consumer channel closed: %v", err)
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

func (r *MqSimple) declareQueue(ch *amqp.Channel, args amqp.Table) (amqp.Queue, error) {
	queueArgs := amqp.Table{
		"x-max-priority": simpleQueueMaxPriority,
	}
	for key, value := range args {
		queueArgs[key] = value
	}

	return ch.QueueDeclare(r.QueueName, true, false, false, false, queueArgs)
}

func (r *MqSimple) declareDLXTopology(ch *amqp.Channel) (amqp.Queue, string, error) {
	dlxName := "dlx-" + r.QueueName
	dlqName := "dlq-" + r.QueueName

	if err := ch.ExchangeDeclare(dlxName, amqp.ExchangeFanout, true, false, false, false, nil); err != nil {
		return amqp.Queue{}, "", err
	}

	queue, err := r.declareQueue(ch, amqp.Table{
		"x-dead-letter-exchange": dlxName,
	})
	if err != nil {
		return amqp.Queue{}, "", err
	}

	if _, err := ch.QueueDeclare(dlqName, true, false, false, false, nil); err != nil {
		return amqp.Queue{}, "", err
	}

	if err := ch.QueueBind(dlqName, "#", dlxName, false, nil); err != nil {
		return amqp.Queue{}, "", err
	}

	return queue, dlqName, nil
}

func (r *MqSimple) handleDelivery(msg amqp.Delivery, handler MsgHandler) error {
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
		if retry >= Retry {
			notifyFailed(handler, r.failedMessage(msg.Body, msg.MessageId))
			return msg.Reject(false)
		}

		headers := copyHeaders(msg.Headers)
		headers["x-retry"] = retry + 1

		if err := r.publishRetryMessage(msg, headers, "1000"); err != nil {
			if nackErr := msg.Nack(false, true); nackErr != nil {
				return fmt.Errorf("publish retry message: %w; nack original: %v", err, nackErr)
			}
			return err
		}

		return msg.Ack(false)
	}

	return msg.Ack(false)
}

func (r *MqSimple) handleDLXDelivery(msg amqp.Delivery, handler MsgHandler) error {
	select {
	case <-r.contextOrBackground().Done():
		if err := msg.Nack(false, true); err != nil {
			return err
		}
		return r.canceledError("consume fail-to-dlx")
	default:
	}

	if err := handler.Process(msg.Body, msg.MessageId); err != nil {
		notifyFailed(handler, r.failedMessage(msg.Body, msg.MessageId))
		return msg.Reject(false)
	}

	return msg.Ack(false)
}

func (r *MqSimple) handleDLQDelivery(msg amqp.Delivery, handler MsgHandler) error {
	select {
	case <-r.contextOrBackground().Done():
		if err := msg.Nack(false, true); err != nil {
			return err
		}
		return r.canceledError("consume dlx")
	default:
	}

	if err := handler.Process(msg.Body, msg.MessageId); err != nil {
		notifyFailed(handler, r.failedMessage(msg.Body, msg.MessageId))
		if err := msg.Nack(false, true); err != nil {
			return err
		}
		return nil
	}

	return msg.Ack(false)
}

func (r *MqSimple) publishRetryMessage(msg amqp.Delivery, headers amqp.Table, ttl string) error {
	ctx := r.contextOrBackground()

	select {
	case <-ctx.Done():
		return r.canceledError("retry publish")
	default:
	}

	ch, err := r.openPublishChannel()
	if err != nil {
		return err
	}
	defer closeAMQPChannel(ch)

	retryQueue := r.QueueName + "-retry"
	if _, err := ch.QueueDeclare(
		retryQueue,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": r.QueueName,
			"x-max-priority":            simpleQueueMaxPriority,
		},
	); err != nil {
		return err
	}

	messageID := msg.MessageId
	if messageID == "" {
		messageID = uuid.NewString()
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
}
