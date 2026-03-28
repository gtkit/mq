package main

import (
	"context"
	"log"

	"github.com/gtkit/mq/rabbit"
)

const mqURL = "amqp://guest:guest@127.0.0.1:5672/"

type handler struct{}

func (h *handler) Process([]byte, string) error {
	return nil
}

func (h *handler) Failed(msg rabbit.FailedMsg) {
	log.Printf("topic producer failed: msgID=%s body=%s", msg.MsgId, string(msg.Message))
}

func main() {
	h := &handler{}

	mq, err := rabbit.NewPubTopic(
		"demo.topic.exchange",
		"demo.topic.created",
		mqURL,
		rabbit.WithContext(context.Background()),
		rabbit.WithConnectionName("demo-topic-producer"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Destroy()

	if err := mq.Publish("hello topic", h); err != nil {
		log.Fatal(err)
	}

	if err := mq.PublishDelay("hello topic delay", h, "5000"); err != nil {
		log.Fatal(err)
	}
}
