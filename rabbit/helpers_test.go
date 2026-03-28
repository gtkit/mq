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

type stubLogger struct{}

func (stubLogger) Info(args ...any) {}

func (stubLogger) Infof(string, ...any) {}

func (stubLogger) Errorf(string, ...any) {}

type formattedOnlyLogger struct {
	infofCalls  int
	errorfCalls int
}

func (l *formattedOnlyLogger) Infof(string, ...any) {
	l.infofCalls++
}

func (l *formattedOnlyLogger) Errorf(string, ...any) {
	l.errorfCalls++
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

func TestSetLoggerReplacesGlobalLogger(t *testing.T) {
	t.Parallel()

	original := currentLogger()
	SetLogger(stubLogger{})
	t.Cleanup(func() {
		SetLogger(original)
	})

	if _, ok := currentLogger().(stubLogger); !ok {
		t.Fatal("SetLogger() did not replace the global logger")
	}
}

func TestSetExternalLoggerAdaptsFormattedLogger(t *testing.T) {
	t.Parallel()

	original := currentLogger()
	external := &formattedOnlyLogger{}

	if !SetExternalLogger(external) {
		t.Fatal("SetExternalLogger() = false, want true")
	}

	t.Cleanup(func() {
		SetLogger(original)
	})

	currentLogger().Info("payload")
	currentLogger().Errorf("err: %s", "boom")

	if external.infofCalls != 1 {
		t.Fatalf("Infof() calls = %d, want 1", external.infofCalls)
	}

	if external.errorfCalls != 1 {
		t.Fatalf("Errorf() calls = %d, want 1", external.errorfCalls)
	}
}

func TestSetExternalLoggerRejectsUnsupportedType(t *testing.T) {
	t.Parallel()

	if SetExternalLogger(struct{}{}) {
		t.Fatal("SetExternalLogger() = true, want false")
	}
}
