package rabbit

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

var _ RabbitMQInterface = (*MqFanout)(nil)

type MqFanout struct {
	*RabbitMQ
}

func NewPubFanout(exchangeName, mqURL string, opts ...Option) (*MqFanout, error) {
	exchangeName = strings.TrimSpace(exchangeName)
	if exchangeName == "" {
		return nil, fmt.Errorf("exchange name is required")
	}

	option, err := newOption(mqURL, opts...)
	if err != nil {
		return nil, err
	}
	option.ExchangeName = exchangeName

	rabbitmq, err := newRabbitMQ(option)
	if err != nil {
		return nil, err
	}

	return &MqFanout{RabbitMQ: rabbitmq}, nil
}

func NewConsumeFanout(exchangeName, mqURL string, opts ...Option) (*MqFanout, error) {
	exchangeName = strings.TrimSpace(exchangeName)
	if exchangeName == "" {
		return nil, fmt.Errorf("exchange name is required")
	}

	option, err := newOption(mqURL, opts...)
	if err != nil {
		return nil, err
	}
	option.ExchangeName = exchangeName

	rabbitmq, err := newRabbitMQ(option)
	if err != nil {
		return nil, err
	}

	return &MqFanout{RabbitMQ: rabbitmq}, nil
}

func (r *MqFanout) Publish(message string, handler MsgHandler) error {
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

	if err := r.declareExchange(ch); err != nil {
		return err
	}

	msgID := uuid.NewString()
	confirmation, err := ch.PublishWithDeferredConfirmWithContext(
		ctx,
		r.ExchangeName,
		"",
		false,
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

	if err := waitForDeferredConfirm(ctx, confirmation); err != nil {
		notifyFailed(handler, r.failedMessage([]byte(message), msgID))
		return err
	}

	return nil
}

func (r *MqFanout) Consume(handler MsgHandler) error {
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

		queue, err := r.declareBoundQueue(ch, nil)
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
					logger.Infof("fanout consumer channel closed: %v", err)
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

func (r *MqFanout) PublishDelay(message string, handler MsgHandler, ttl string) error {
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

	if err := r.declareExchange(ch); err != nil {
		return err
	}

	delayQueue := r.baseName() + ".delay"
	if _, err := ch.QueueDeclare(
		delayQueue,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange": r.ExchangeName,
			"x-max-priority":         simpleQueueMaxPriority,
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

func (r *MqFanout) ConsumeDelay(handler MsgHandler) error {
	return r.Consume(handler)
}

func (r *MqFanout) ConsumeFailToDlx(handler MsgHandler) error {
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
					logger.Infof("fanout fail-to-dlx channel closed: %v", err)
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

func (r *MqFanout) ConsumeDlx(handler MsgHandler) error {
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
					logger.Infof("fanout dlx consumer channel closed: %v", err)
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

func (r *MqFanout) declareExchange(ch *amqp.Channel) error {
	return ch.ExchangeDeclare(r.ExchangeName, amqp.ExchangeFanout, true, false, false, false, nil)
}

func (r *MqFanout) declareBoundQueue(ch *amqp.Channel, args amqp.Table) (amqp.Queue, error) {
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

	if err := ch.QueueBind(queue.Name, "", r.ExchangeName, false, nil); err != nil {
		return amqp.Queue{}, err
	}

	return queue, nil
}

func (r *MqFanout) declareDLXTopology(ch *amqp.Channel) (amqp.Queue, string, error) {
	dlxExchange := r.ExchangeName + ".dlx"
	dlqName := r.baseName() + ".dlq"

	if err := ch.ExchangeDeclare(dlxExchange, amqp.ExchangeFanout, true, false, false, false, nil); err != nil {
		return amqp.Queue{}, "", err
	}

	queue, err := r.declareBoundQueue(ch, amqp.Table{
		"x-dead-letter-exchange": dlxExchange,
	})
	if err != nil {
		return amqp.Queue{}, "", err
	}

	if _, err := ch.QueueDeclare(dlqName, true, false, false, false, nil); err != nil {
		return amqp.Queue{}, "", err
	}

	if err := ch.QueueBind(dlqName, "", dlxExchange, false, nil); err != nil {
		return amqp.Queue{}, "", err
	}

	return queue, dlqName, nil
}

func (r *MqFanout) handleDelivery(msg amqp.Delivery, handler MsgHandler) error {
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
		notifyFailed(handler, r.failedMessage(msg.Body, msg.MessageId))
		return msg.Reject(false)
	}

	return msg.Ack(false)
}

func (r *MqFanout) handleDLXDelivery(msg amqp.Delivery, handler MsgHandler) error {
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

func (r *MqFanout) handleDLQDelivery(msg amqp.Delivery, handler MsgHandler) error {
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

func (r *MqFanout) baseName() string {
	if strings.TrimSpace(r.QueueName) != "" {
		return safeNamePart(r.QueueName)
	}

	return safeNamePart(r.ExchangeName)
}
