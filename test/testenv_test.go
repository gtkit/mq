package test

import (
	"os"
	"testing"
)

const defaultMQURL = "amqp://guest:guest@127.0.0.1:5672/"

var mqURL = defaultMQURL

func requireRabbitMQ(t *testing.T) {
	t.Helper()

	if os.Getenv("MQ_INTEGRATION") != "1" {
		t.Skip("set MQ_INTEGRATION=1 to run RabbitMQ integration tests")
	}

	if value := os.Getenv("MQ_URL"); value != "" {
		mqURL = value
		return
	}

	mqURL = defaultMQURL
}
