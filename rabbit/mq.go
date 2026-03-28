package rabbit

import (
	"context"
	"errors"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	defaultMaxRetry = 3
	defaultRetryTTL = "2000"
)

type MQOption struct {
	ExchangeName string
	QueueName    string
	Routing      string
	MqURL        string
	ConnName     string
	Ctx          context.Context
	MaxRetry     int32
	RetryTTL     string
}

type RabbitMQ struct {
	MQOption
	connMu    sync.Mutex
	pubMu     sync.Mutex
	conn      *amqp.Connection
	pubCh     *amqp.Channel
	pubDecls  map[string]struct{}
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
	ctx, cancel := context.WithCancel(option.Ctx)
	option.Ctx = ctx

	rabbitmq := &RabbitMQ{
		MQOption: option,
		cancel:   cancel,
	}

	conn, err := rabbitmq.dial()
	if err != nil {
		cancel()
		return nil, err
	}

	rabbitmq.conn = conn

	return rabbitmq, nil
}

func (r *RabbitMQ) Destroy() {
	if r == nil {
		return
	}

	r.closeOnce.Do(func() {
		if r.cancel != nil {
			r.cancel()
		}

		r.closePublishChannel()

		r.connMu.Lock()
		conn := r.conn
		r.conn = nil
		r.connMu.Unlock()

		if conn != nil {
			if err := conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
				currentLogger().Infof("close connection error: %v", err)
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
