# mq

RabbitMQ wrapper for Go 1.26.1.

## Features

- Supports `simple`, `direct`, `fanout`, and `topic` modes.
- Publisher confirms use per-operation channels to avoid shared-channel races.
- Consumers use explicit reconnect loops and context-driven shutdown.
- Retry headers are normalized through `x-retry`.
- Delay queues use TTL + dead-letter routing and do not require the `x-delayed-message` plugin.
- Dead-letter consumption is supported for `simple`, `direct`, `fanout`, and `topic`.

## Dependencies

- `github.com/rabbitmq/amqp091-go v1.10.0`
- `github.com/google/uuid v1.6.0`

## Notes

- `Destroy()` is idempotent and cancels the instance-owned context.
- Publish paths are concurrency-safe because each publish call opens its own AMQP channel.
- Fanout mode does not auto-retry failed deliveries, because retrying through a fanout exchange would duplicate messages for subscribers that already succeeded.
- Optional configuration now uses function options instead of passing a config struct to constructors.

## Usage

```go
ctx := context.Background()

producer, err := rabbit.NewPubDirect(
    "orders.exchange",
    "orders.created",
    "amqp://guest:guest@127.0.0.1:5672/",
    rabbit.WithContext(ctx),
    rabbit.WithConnectionName("orders-producer"),
)
if err != nil {
    return err
}
defer producer.Destroy()
```

## Tests

Unit tests run with:

```bash
go test ./...
```

Integration tests are opt-in:

```bash
MQ_INTEGRATION=1 go test ./test -count=1
```

Custom RabbitMQ address:

```bash
MQ_INTEGRATION=1 MQ_URL=amqp://guest:guest@127.0.0.1:5672/ go test ./test -count=1
```
