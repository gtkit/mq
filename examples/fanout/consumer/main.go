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
	log.Printf("fanout consumer recv: msgID=%s body=%s", msgID, string(body))

	if string(body) == "fail" {
		return errors.New("mock fanout consume error")
	}

	return nil
}

func (h *handler) Failed(msg rabbit.FailedMsg) {
	log.Printf("fanout consumer failed: msgID=%s body=%s", msg.MsgId, string(msg.Message))
}

func main() {
	h := &handler{}

	mq, err := rabbit.NewConsumeFanout(
		"demo.fanout.exchange",
		mqURL,
		rabbit.WithContext(context.Background()),
		rabbit.WithConnectionName("demo-fanout-consumer"),
		rabbit.WithQueueName("demo.fanout.queue"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Destroy()

	if err := mq.Consume(h); err != nil {
		log.Fatal(err)
	}
}
