package rabbit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	Delay = 1
	Retry = 3
)

type MQOption struct {
	ExchangeName string
	QueueName    string
	Routing      string
	MqURL        string
	ConnName     string
	Ctx          context.Context
}

type RabbitMQ struct {
	MQOption
	conn      *amqp.Connection
	closeOnce sync.Once
	cancel    context.CancelFunc
}

type MsgHandler interface {
	Process([]byte, string) error
	Failed(FailedMsg)
}

type FailedMsg struct {
	ExchangeName string
	QueueName    string
	Routing      string
	MsgId        string
	Message      []byte
}

type RabbitMQInterface interface {
	Publish(message string, handler MsgHandler) error
	Consume(handler MsgHandler) error
	PublishDelay(message string, handler MsgHandler, ttl string) error
	ConsumeDelay(handler MsgHandler) error
	ConsumeFailToDlx(handler MsgHandler) error
	ConsumeDlx(handler MsgHandler) error
}

func newRabbitMQ(option MQOption) (*RabbitMQ, error) {
	normalized, err := normalizeOption(option)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(normalized.Ctx)
	normalized.Ctx = ctx

	config := amqp.Config{
		Vhost:      "/",
		Properties: amqp.NewConnectionProperties(),
		Heartbeat:  10 * time.Second,
		Locale:     "en_US",
	}
	config.Properties.SetClientConnectionName(normalized.ConnName)

	conn, err := amqp.DialConfig(normalized.MqURL, config)
	if err != nil {
		cancel()
		return nil, err
	}

	return &RabbitMQ{
		MQOption: normalized,
		conn:     conn,
		cancel:   cancel,
	}, nil
}

func (r *RabbitMQ) Destroy() {
	if r == nil {
		return
	}

	r.closeOnce.Do(func() {
		if r.cancel != nil {
			r.cancel()
		}

		if r.conn != nil {
			if err := r.conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
				logger.Infof("close connection error: %v", err)
			}
		}
	})
}

func (r *RabbitMQ) contextOrBackground() context.Context {
	if r == nil || r.Ctx == nil {
		return context.Background()
	}

	return r.Ctx
}

func (r *RabbitMQ) canceledError(operation string) error {
	if err := r.contextOrBackground().Err(); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}

	return nil
}

func (r *RabbitMQ) failedMessage(message []byte, msgID string) FailedMsg {
	if r == nil {
		return FailedMsg{
			MsgId:   msgID,
			Message: message,
		}
	}

	return FailedMsg{
		ExchangeName: r.ExchangeName,
		QueueName:    r.QueueName,
		Routing:      r.Routing,
		MsgId:        msgID,
		Message:      message,
	}
}

func notifyFailed(handler MsgHandler, msg FailedMsg) {
	if handler == nil {
		return
	}

	handler.Failed(msg)
}

func ParseUri(uri string) (amqp.URI, error) {
	return amqp.ParseURI(uri)
}
