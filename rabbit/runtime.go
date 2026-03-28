package rabbit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var nameSanitizer = strings.NewReplacer(
	" ", "_",
	"/", "_",
	"\\", "_",
	":", "_",
	"*", "star",
	"#", "hash",
)

func retryCount(headers amqp.Table) int32 {
	if headers == nil {
		return 0
	}

	value, ok := headers["x-retry"]
	if !ok {
		return 0
	}

	switch typed := value.(type) {
	case int:
		return int32(typed)
	case int8:
		return int32(typed)
	case int16:
		return int32(typed)
	case int32:
		return typed
	case int64:
		if typed > math.MaxInt32 {
			return math.MaxInt32
		}
		if typed < math.MinInt32 {
			return math.MinInt32
		}
		return int32(typed)
	case uint8:
		return int32(typed)
	case uint16:
		return int32(typed)
	case uint32:
		if typed > math.MaxInt32 {
			return math.MaxInt32
		}
		return int32(typed)
	case uint64:
		if typed > math.MaxInt32 {
			return math.MaxInt32
		}
		return int32(typed)
	default:
		return 0
	}
}

func (r *RabbitMQ) dial() (*amqp.Connection, error) {
	if r == nil {
		return nil, errors.New("rabbitmq is not initialized")
	}

	config := amqp.Config{
		Vhost:      "/",
		Properties: amqp.NewConnectionProperties(),
		Heartbeat:  10 * time.Second,
		Locale:     "en_US",
	}
	config.Properties.SetClientConnectionName(r.ConnName)

	return amqp.DialConfig(r.MqURL, config)
}

func (r *RabbitMQ) reconnect() error {
	if r == nil {
		return errors.New("rabbitmq is not initialized")
	}

	r.connMu.Lock()
	defer r.connMu.Unlock()

	if r.conn != nil && !r.conn.IsClosed() {
		return nil
	}

	conn, err := r.dial()
	if err != nil {
		return err
	}

	staleConn := r.conn
	r.conn = conn
	r.pubMu.Lock()
	if r.pubCh != nil {
		_ = r.pubCh.Close()
		r.pubCh = nil
	}
	r.pubDecls = nil
	r.pubMu.Unlock()

	if staleConn != nil && !staleConn.IsClosed() {
		_ = staleConn.Close()
	}

	return nil
}

func (r *RabbitMQ) getConn() *amqp.Connection {
	r.connMu.Lock()
	defer r.connMu.Unlock()

	return r.conn
}

func (r *RabbitMQ) openConsumerChannel() (*amqp.Channel, error) {
	if err := r.reconnect(); err != nil {
		return nil, err
	}

	conn := r.getConn()
	if conn == nil {
		return nil, errors.New("rabbitmq connection is not initialized")
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	if err := ch.Qos(1, 0, false); err != nil {
		_ = ch.Close()
		return nil, err
	}

	return ch, nil
}

func (r *RabbitMQ) closePublishChannel() {
	r.pubMu.Lock()
	defer r.pubMu.Unlock()

	if r.pubCh == nil {
		r.pubDecls = nil
		return
	}

	_ = r.pubCh.Close()
	r.pubCh = nil
	r.pubDecls = nil
}

func closeAMQPChannel(ch *amqp.Channel) {
	if ch == nil {
		return
	}

	_ = ch.Close()
}

func waitForDeferredConfirm(ctx context.Context, confirmation *amqp.DeferredConfirmation) error {
	if confirmation == nil {
		return nil
	}

	acked, err := confirmation.WaitContext(ctx)
	if err != nil {
		return err
	}

	if !acked {
		return fmt.Errorf("publish not acknowledged")
	}

	return nil
}

func copyHeaders(headers amqp.Table) amqp.Table {
	if len(headers) == 0 {
		return amqp.Table{}
	}

	cloned := make(amqp.Table, len(headers))
	for key, value := range headers {
		cloned[key] = value
	}

	return cloned
}

func safeNamePart(value string) string {
	if strings.TrimSpace(value) == "" {
		return "default"
	}

	return nameSanitizer.Replace(value)
}

func (r *RabbitMQ) publishWithChannel(fn func(*amqp.Channel) error) error {
	if err := r.reconnect(); err != nil {
		return err
	}

	conn := r.getConn()
	if conn == nil {
		return errors.New("rabbitmq connection is not initialized")
	}

	r.pubMu.Lock()
	defer r.pubMu.Unlock()

	if r.pubCh == nil || r.pubCh.IsClosed() {
		ch, err := conn.Channel()
		if err != nil {
			return err
		}

		if err := ch.Confirm(false); err != nil {
			_ = ch.Close()
			return err
		}

		r.pubCh = ch
		r.pubDecls = make(map[string]struct{})
	}

	if err := fn(r.pubCh); err != nil {
		if r.pubCh != nil && r.pubCh.IsClosed() {
			r.pubCh = nil
			r.pubDecls = nil
		}
		return err
	}

	return nil
}

func retryBackoffDelay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}

	delay := time.Second * time.Duration(1<<attempt)
	if delay > 30*time.Second {
		return 30 * time.Second
	}

	return delay
}

func (r *RabbitMQ) waitRetry(operation string, attempt *int, format string, args ...any) error {
	delay := retryBackoffDelay(0)
	if attempt != nil {
		delay = retryBackoffDelay(*attempt)
		*attempt = *attempt + 1
	}

	currentLogger().Infof(format, args...)

	select {
	case <-r.contextOrBackground().Done():
		return r.canceledError(operation)
	case <-time.After(delay):
		return nil
	}
}

func (r *RabbitMQ) maxRetry() int32 {
	if r == nil || r.MaxRetry <= 0 {
		return defaultMaxRetry
	}

	return r.MaxRetry
}

func (r *RabbitMQ) ensurePublishDeclared(key string, ch *amqp.Channel, declare func(*amqp.Channel) error) error {
	if key == "" {
		return declare(ch)
	}

	if _, ok := r.pubDecls[key]; ok {
		return nil
	}

	if err := declare(ch); err != nil {
		return err
	}

	if r.pubDecls == nil {
		r.pubDecls = make(map[string]struct{})
	}
	r.pubDecls[key] = struct{}{}

	return nil
}

func (r *RabbitMQ) retryTTL() string {
	if r == nil || strings.TrimSpace(r.RetryTTL) == "" {
		return defaultRetryTTL
	}

	return r.RetryTTL
}

func (r *RabbitMQ) handleDLXDelivery(msg amqp.Delivery, handler MsgHandler, operation string) error {
	select {
	case <-r.contextOrBackground().Done():
		if err := msg.Nack(false, true); err != nil {
			return err
		}
		return r.canceledError(operation)
	default:
	}

	if err := handler.Process(msg.Body, msg.MessageId); err != nil {
		notifyFailed(handler, r.failedMessage(msg.Body, msg.MessageId))
		return msg.Reject(false)
	}

	return msg.Ack(false)
}

func (r *RabbitMQ) handleDLQDelivery(msg amqp.Delivery, handler MsgHandler, operation string) error {
	select {
	case <-r.contextOrBackground().Done():
		if err := msg.Nack(false, true); err != nil {
			return err
		}
		return r.canceledError(operation)
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

func (r *RabbitMQ) ackAfterRetryPublish(msg amqp.Delivery) error {
	if err := msg.Ack(false); err != nil {
		currentLogger().Errorf("ack after retry publish failed: %v (message may be redelivered)", err)
	}

	return nil
}
