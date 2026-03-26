package rabbit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
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

func (r *RabbitMQ) openPublishChannel() (*amqp.Channel, error) {
	if r == nil || r.conn == nil {
		return nil, errors.New("rabbitmq connection is not initialized")
	}

	ch, err := r.conn.Channel()
	if err != nil {
		return nil, err
	}

	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, err
	}

	return ch, nil
}

func (r *RabbitMQ) openConsumerChannel() (*amqp.Channel, error) {
	if r == nil || r.conn == nil {
		return nil, errors.New("rabbitmq connection is not initialized")
	}

	ch, err := r.conn.Channel()
	if err != nil {
		return nil, err
	}

	if err := ch.Qos(1, 0, false); err != nil {
		_ = ch.Close()
		return nil, err
	}

	return ch, nil
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

	replacer := strings.NewReplacer(
		" ", "_",
		"/", "_",
		"\\", "_",
		":", "_",
		"*", "star",
		"#", "hash",
	)

	return replacer.Replace(value)
}
