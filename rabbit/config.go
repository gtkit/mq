package rabbit

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

type Option func(*MQOption)

// WithContext 为当前 mq 实例设置生命周期 context。
// 当 context 被取消后，发布操作会尽快返回，消费循环也会退出。
func WithContext(ctx context.Context) Option {
	return func(option *MQOption) {
		if ctx != nil {
			option.Ctx = ctx
		}
	}
}

// WithConnectionName 设置 RabbitMQ connection name。
// 该名称会显示在 RabbitMQ 管理界面中，便于定位连接来源。
func WithConnectionName(name string) Option {
	return func(option *MQOption) {
		option.ConnName = name
	}
}

// WithQueueName 显式设置队列名称。
// 对 direct、fanout、topic 模式，建议通过该选项固定消费队列名。
func WithQueueName(name string) Option {
	return func(option *MQOption) {
		option.QueueName = name
	}
}

// WithMaxRetry 设置消费失败时的最大重试次数。
// 当值小于等于 0 时，会回退到默认值。
func WithMaxRetry(maxRetry int32) Option {
	return func(option *MQOption) {
		option.MaxRetry = maxRetry
	}
}

// WithRetryTTL 设置失败重试消息在 retry queue 中停留的毫秒数。
// 参数使用 RabbitMQ 期望的字符串格式，例如 "2000" 表示 2 秒。
func WithRetryTTL(ttl string) Option {
	return func(option *MQOption) {
		option.RetryTTL = ttl
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

	if option.MaxRetry <= 0 {
		option.MaxRetry = defaultMaxRetry
	}

	option.RetryTTL = strings.TrimSpace(option.RetryTTL)
	if option.RetryTTL == "" {
		option.RetryTTL = defaultRetryTTL
	}

	return option, nil
}
