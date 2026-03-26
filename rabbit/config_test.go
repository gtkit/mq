package rabbit

import (
	"context"
	"testing"
)

func TestNormalizeOptionDefaultsContextAndConnectionName(t *testing.T) {
	t.Parallel()

	opt, err := normalizeOption(MQOption{
		QueueName: "jobs",
		MqURL:     "amqp://guest:guest@localhost:5672/",
	})
	if err != nil {
		t.Fatalf("normalizeOption() error = %v", err)
	}

	if opt.Ctx == nil {
		t.Fatal("normalizeOption() returned nil context")
	}

	if opt.ConnName == "" {
		t.Fatal("normalizeOption() returned empty connection name")
	}
}

func TestNormalizeOptionPreservesProvidedContextAndConnectionName(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), struct{}{}, "marker")

	opt, err := normalizeOption(MQOption{
		QueueName: "jobs",
		MqURL:     "amqp://guest:guest@localhost:5672/",
		ConnName:  "producer-a",
		Ctx:       ctx,
	})
	if err != nil {
		t.Fatalf("normalizeOption() error = %v", err)
	}

	if opt.Ctx != ctx {
		t.Fatal("normalizeOption() replaced the provided context")
	}

	if opt.ConnName != "producer-a" {
		t.Fatalf("normalizeOption() connName = %q, want %q", opt.ConnName, "producer-a")
	}
}

func TestNewOptionAppliesFunctionalOptions(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), struct{}{}, "marker")

	opt, err := newOption(
		"amqp://guest:guest@localhost:5672/",
		WithQueueName("jobs"),
		WithConnectionName("producer-a"),
		WithContext(ctx),
	)
	if err != nil {
		t.Fatalf("newOption() error = %v", err)
	}

	if opt.QueueName != "jobs" {
		t.Fatalf("newOption() queueName = %q, want %q", opt.QueueName, "jobs")
	}

	if opt.ConnName != "producer-a" {
		t.Fatalf("newOption() connName = %q, want %q", opt.ConnName, "producer-a")
	}

	if opt.Ctx != ctx {
		t.Fatal("newOption() replaced the provided context")
	}
}
