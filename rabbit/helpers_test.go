package rabbit

import (
	"context"
	"errors"
	"testing"
)

type recordingHandler struct {
	failed []FailedMsg
}

func (h *recordingHandler) Process([]byte, string) error {
	return nil
}

func (h *recordingHandler) Failed(msg FailedMsg) {
	h.failed = append(h.failed, msg)
}

func TestDestroyIsIdempotent(t *testing.T) {
	t.Parallel()

	var mq RabbitMQ

	mq.Destroy()
	mq.Destroy()
}

func TestDestroyCancelsOwnedContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	mq := &RabbitMQ{
		MQOption: MQOption{
			Ctx: ctx,
		},
		cancel: cancel,
	}

	mq.Destroy()

	select {
	case <-ctx.Done():
	default:
		t.Fatal("Destroy() did not cancel the owned context")
	}
}

func TestPublishOnCanceledContextWrapsContextError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	handler := &recordingHandler{}
	mq := &MqSimple{
		RabbitMQ: &RabbitMQ{
			MQOption: MQOption{
				QueueName: "jobs",
				Ctx:       ctx,
			},
		},
	}

	err := mq.Publish("payload", handler)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish() error = %v, want wrapped context cancellation", err)
	}

	if len(handler.failed) != 1 {
		t.Fatalf("Failed() calls = %d, want 1", len(handler.failed))
	}

	if string(handler.failed[0].Message) != "payload" {
		t.Fatalf("Failed() message = %q, want %q", string(handler.failed[0].Message), "payload")
	}
}

func TestPublishOnCanceledContextAllowsNilHandler(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	mq := &MqSimple{
		RabbitMQ: &RabbitMQ{
			MQOption: MQOption{
				QueueName: "jobs",
				Ctx:       ctx,
			},
		},
	}

	err := mq.Publish("payload", nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish() error = %v, want wrapped context cancellation", err)
	}
}
