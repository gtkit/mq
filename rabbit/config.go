package rabbit

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

type Option func(*MQOption)

func WithContext(ctx context.Context) Option {
	return func(option *MQOption) {
		if ctx != nil {
			option.Ctx = ctx
		}
	}
}

func WithConnectionName(name string) Option {
	return func(option *MQOption) {
		option.ConnName = name
	}
}

func WithQueueName(name string) Option {
	return func(option *MQOption) {
		option.QueueName = name
	}
}

func newOption(mqURL string, opts ...Option) (MQOption, error) {
	option := MQOption{
		MqURL: mqURL,
	}

	for _, apply := range opts {
		if apply == nil {
			continue
		}

		apply(&option)
	}

	return normalizeOption(option)
}

func normalizeOption(option MQOption) (MQOption, error) {
	option.ExchangeName = strings.TrimSpace(option.ExchangeName)
	option.QueueName = strings.TrimSpace(option.QueueName)
	option.Routing = strings.TrimSpace(option.Routing)
	option.MqURL = strings.TrimSpace(option.MqURL)
	option.ConnName = strings.TrimSpace(option.ConnName)

	if option.MqURL == "" {
		return MQOption{}, fmt.Errorf("mq url is required")
	}

	if option.Ctx == nil {
		option.Ctx = context.Background()
	}

	if option.ConnName == "" {
		option.ConnName = "mq-" + uuid.NewString()
	}

	return option, nil
}
