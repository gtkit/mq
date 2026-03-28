package main

import (
	"context"
	"errors"
	"log"

	"github.com/gtkit/mq/rabbit"
)

const mqURL = "amqp://guest:guest@127.0.0.1:5672/"

type handler struct{}

func (h *handler) Process(body []byte, msgID string) error {
	log.Printf("simple consumer recv: msgID=%s body=%s", msgID, string(body))

	if string(body) == "fail" {
		return errors.New("mock simple consume error")
	}

	return nil
}

func (h *handler) Failed(msg rabbit.FailedMsg) {
	log.Printf("simple consumer failed: msgID=%s body=%s", msg.MsgId, string(msg.Message))
}

func main() {
	h := &handler{}

	mq, err := rabbit.NewConsumeSimple(
		"demo.simple.queue",
		mqURL,
		rabbit.WithContext(context.Background()),
		rabbit.WithConnectionName("demo-simple-consumer"),
		rabbit.WithMaxRetry(3),
		rabbit.WithRetryTTL("2000"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Destroy()

	if err := mq.Consume(h); err != nil {
		log.Fatal(err)
	}
}
