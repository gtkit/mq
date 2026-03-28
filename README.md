# mq

`mq` 是一个基于 `github.com/rabbitmq/amqp091-go` 的 RabbitMQ Go 封装，支持以下四种常见消息模型：

- `simple`：普通队列
- `direct`：直连交换机
- `fanout`：广播交换机
- `topic`：主题交换机

项目当前基于 `Go 1.26.1`。

本文档会尽量把库里已经提供的能力、每个方法的用途、典型使用方式、重试与死信行为、日志注入方式都说明清楚。

## 一、特性概览

- 支持 `simple`、`direct`、`fanout`、`topic` 四种 RabbitMQ 模式
- 消费端支持自动重连
- 连接断开后会自动重建 connection，再继续建立 consumer channel
- 发布端复用 confirm channel，避免每条消息都重新打开 channel
- 发布时对 exchange / queue / retry queue / delay queue 做 channel 级声明缓存，减少重复声明开销
- 支持延迟消息，基于 `TTL + Dead Letter` 实现，不依赖 `x-delayed-message` 插件
- 支持失败重试，使用消息头 `x-retry` 记录当前重试次数
- 支持死信消费
- 支持外部 logger 注入
- `Destroy()` 可重复调用，并会安全关闭内部资源

## 二、安装

```bash
go get github.com/rabbitmq/amqp091-go
go get github.com/google/uuid
```

如果你的项目直接引用当前仓库：

```bash
go get <your-module-path>/mq
```

## 三、目录说明

核心代码在 [rabbit](/Users/xiaozhaofu/go/src/mq/rabbit) 目录下。

主要文件：

- [rabbit/simple.go](/Users/xiaozhaofu/go/src/mq/rabbit/simple.go)：普通队列模式
- [rabbit/direct.go](/Users/xiaozhaofu/go/src/mq/rabbit/direct.go)：direct 模式
- [rabbit/fanout.go](/Users/xiaozhaofu/go/src/mq/rabbit/fanout.go)：fanout 模式
- [rabbit/topic.go](/Users/xiaozhaofu/go/src/mq/rabbit/topic.go)：topic 模式
- [rabbit/config.go](/Users/xiaozhaofu/go/src/mq/rabbit/config.go)：Option 配置
- [rabbit/log.go](/Users/xiaozhaofu/go/src/mq/rabbit/log.go)：日志接口与注入
- [rabbit/mq.go](/Users/xiaozhaofu/go/src/mq/rabbit/mq.go)：公共结构与生命周期
- [rabbit/runtime.go](/Users/xiaozhaofu/go/src/mq/rabbit/runtime.go)：重连、重试、运行时辅助逻辑

## 四、基础概念

### 1. `MsgHandler`

业务消费逻辑通过 `MsgHandler` 注入：

```go
type MsgHandler interface {
    Process([]byte, string) error
    Failed(FailedMsg)
}
```

方法说明：

- `Process(body []byte, msgID string) error`
  作用：真正处理消息
  规则：返回 `nil` 表示处理成功；返回 `error` 表示处理失败

- `Failed(msg FailedMsg)`
  作用：在消息最终失败、发布失败、上下文取消等场景下接收失败通知
  说明：如果你不需要失败回调，也可以传入一个空实现

### 2. `FailedMsg`

失败回调会收到以下结构：

```go
type FailedMsg struct {
    ExchangeName string
    QueueName    string
    Routing      string
    MsgId        string
    Message      []byte
}
```

字段含义：

- `ExchangeName`：交换机名称
- `QueueName`：队列名称
- `Routing`：routing key
- `MsgId`：消息 ID
- `Message`：消息体

### 3. 生命周期

每个 `MqSimple` / `MqDirect` / `MqFanout` / `MqTopic` 实例内部都维护：

- 一个 RabbitMQ connection
- 消费时临时创建的 consumer channel
- 一个复用的 publish channel
- 一个受内部管理的 context

实例使用结束后，必须调用：

```go
defer mq.Destroy()
```

`Destroy()` 的作用：

- 取消内部 context
- 关闭 publish channel
- 关闭 connection
- 可重复调用，不会 panic

## 五、通用配置项

所有构造函数都支持可选参数 `opts ...Option`。

### 1. `WithContext`

```go
rabbit.WithContext(ctx)
```

作用：

- 控制当前 mq 实例的生命周期
- 当 `ctx.Done()` 后，正在运行的消费循环会退出
- 发布方法会在发送前检查 context 是否已取消

### 2. `WithConnectionName`

```go
rabbit.WithConnectionName("order-service-producer")
```

作用：

- 设置 RabbitMQ connection name
- 便于在 RabbitMQ 管理后台中识别来源连接

未设置时会自动生成，例如：

```text
mq-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```

### 3. `WithQueueName`

```go
rabbit.WithQueueName("order.queue")
```

作用：

- 显式指定消费使用的队列名
- 对 `direct` / `fanout` / `topic` 模式尤其有用

如果不指定，队列名行为由各模式自己的逻辑决定。

### 4. `WithMaxRetry`

```go
rabbit.WithMaxRetry(5)
```

作用：

- 设置消息处理失败时的最大重试次数

默认值：

```go
3
```

说明：

- 当前仅对 `simple` / `direct` / `topic` 的失败重试生效
- `fanout` 不做自动 retry fanout，因为 fanout 重投会让已经成功的订阅者再次收到消息

### 5. `WithRetryTTL`

```go
rabbit.WithRetryTTL("5000")
```

作用：

- 设置失败重试消息在 retry queue 中停留的毫秒数

默认值：

```go
"2000"
```

注意：

- 单位是毫秒字符串，不是 `time.Duration`
- 例如 `"1000"` 表示 1 秒，`"5000"` 表示 5 秒

## 六、日志注入

库内部定义的最小日志接口如下：

```go
type Logger interface {
    Info(args ...any)
    Infof(template string, args ...any)
    Errorf(template string, args ...any)
}
```

### 1. 注入自定义 Logger

如果你的 logger 已经实现了上面的接口：

```go
rabbit.SetLogger(myLogger)
```

### 2. 注入外部 logger 实例

如果你的 logger 没有显式实现本库 `Logger`，但提供了常见的格式化方法，例如：

- `Infof(format string, args ...any)`
- `Errorf(format string, args ...any)`

那么可以直接这样注入：

```go
ok := rabbit.SetExternalLogger(externalLogger)
if !ok {
    panic("logger does not match mq logger adapter")
}
```

### 3. 注入 `github.com/gtkit/logger`

如果你项目里使用的是 `github.com/gtkit/logger` 实例，并且该实例提供了 `Infof` / `Errorf`，可以直接注入：

```go
import gtlogger "github.com/gtkit/logger"

func init() {
    l := gtlogger.Default()
    if !rabbit.SetExternalLogger(l) {
        panic("inject gtkit logger failed")
    }
}
```

如果你的 `gtkit/logger` 实例本身已经实现了 `Info(args ...any)`、`Infof(...)`、`Errorf(...)`，也可以直接：

```go
rabbit.SetLogger(l)
```

### 4. 不注入 logger 时的默认行为

默认使用 Go 标准库 `log.Printf` / `log.Println`。

## 七、公开方法总览

下面按模式分别列出公开方法。

---

## 八、Simple 模式

### 1. 构造函数

```go
func NewPubSimple(queueName, mqURL string, opts ...Option) (*MqSimple, error)
func NewConsumeSimple(queueName, mqURL string, opts ...Option) (*MqSimple, error)
```

参数说明：

- `queueName`：队列名，必填
- `mqURL`：RabbitMQ 连接串，必填
- `opts`：可选配置

推荐约定：

- 发布端使用 `NewPubSimple`
- 消费端使用 `NewConsumeSimple`

虽然它们底层能力一致，但这样语义更清晰。

### 2. 发布普通消息

```go
func (r *MqSimple) Publish(message string, handler MsgHandler) error
```

作用：

- 向普通队列发送一条持久化消息

行为说明：

- 自动声明主队列
- 消息头会带上 `x-retry: 0`
- 使用 publisher confirm，确保 broker 已确认
- 如果 publish 失败，会通过 `handler.Failed(...)` 通知

使用示例：

```go
err := mq.Publish("hello simple", handler)
```

### 3. 消费普通消息

```go
func (r *MqSimple) Consume(handler MsgHandler) error
```

作用：

- 持续消费普通队列中的消息

行为说明：

- 自动设置 `Qos(1)`，一次只拉一条未确认消息
- 处理成功后 `Ack`
- 处理失败时进入 retry 逻辑
- connection 断开时自动重连
- channel 关闭时自动重建
- 重连采用指数退避，最大 30 秒

### 4. 发布延迟消息

```go
func (r *MqSimple) PublishDelay(message string, handler MsgHandler, ttl string) error
```

作用：

- 发送延迟消息

实现方式：

- 创建 `queueName-delay`
- 在 delay queue 上设置 TTL
- TTL 到期后 dead-letter 回主队列

示例：

```go
err := mq.PublishDelay("delay message", handler, "5000")
```

表示 5 秒后投递到主队列。

### 5. 消费延迟消息

```go
func (r *MqSimple) ConsumeDelay(handler MsgHandler) error
```

说明：

- `simple` 模式下延迟消息最终仍会进入主队列
- 因此 `ConsumeDelay` 内部就是 `Consume`

### 6. 发送带 DLX 拓扑的消息

```go
func (r *MqSimple) PublishWithDlx(message string) error
```

作用：

- 预先声明带死信交换机的 simple 队列拓扑后，再发送消息

适用场景：

- 你打算使用 `ConsumeFailToDlx` / `ConsumeDlx`

### 7. 消费失败转死信

```go
func (r *MqSimple) ConsumeFailToDlx(handler MsgHandler) error
```

作用：

- 消费主队列
- 当 `Process` 返回错误时，直接 `Reject(false)`，让消息进入 DLX / DLQ

和 `Consume` 的区别：

- `Consume`：失败时先重试
- `ConsumeFailToDlx`：失败时直接进入死信

### 8. 消费死信队列

```go
func (r *MqSimple) ConsumeDlx(handler MsgHandler) error
```

作用：

- 消费死信队列中的消息

### 9. 手动重试当前消息

```go
func (r *MqSimple) RetryMsg(msg amqp.Delivery, ttl string) error
```

作用：

- 把当前 delivery 手动发送到 retry queue

适合场景：

- 你自己扩展消费逻辑时，希望显式控制 retry TTL

---

## 九、Direct 模式

### 1. 构造函数

```go
func NewPubDirect(exchangeName, routingKey, mqURL string, opts ...Option) (*MqDirect, error)
func NewConsumeDirect(exchangeName, routingKey, mqURL string, opts ...Option) (*MqDirect, error)
```

参数说明：

- `exchangeName`：direct exchange 名称
- `routingKey`：绑定和发送使用的 routing key
- `mqURL`：RabbitMQ 地址

### 2. 普通发布

```go
func (r *MqDirect) Publish(message string, handler MsgHandler) error
```

作用：

- 向指定 direct exchange + routing key 发布消息

### 3. 普通消费

```go
func (r *MqDirect) Consume(handler MsgHandler) error
```

作用：

- 声明 exchange
- 声明并绑定 queue
- 持续消费消息

失败处理：

- `Process` 返回错误时走 retry queue
- 超过 `MaxRetry` 后拒绝消息

### 4. 发布延迟消息

```go
func (r *MqDirect) PublishDelay(message string, handler MsgHandler, ttl string) error
```

作用：

- 先投递到 `.delay` 队列
- TTL 到期后路由回原 exchange + routing key

### 5. 消费延迟消息

```go
func (r *MqDirect) ConsumeDelay(handler MsgHandler) error
```

说明：

- 延迟消息最终还是落回正常队列
- 所以内部等价于 `Consume`

### 6. 消费失败直接进入死信

```go
func (r *MqDirect) ConsumeFailToDlx(handler MsgHandler) error
```

### 7. 消费死信队列

```go
func (r *MqDirect) ConsumeDlx(handler MsgHandler) error
```

### 8. 手动重试当前消息

```go
func (r *MqDirect) RetryMsg(msg amqp.Delivery, ttl string) error
```

---

## 十、Fanout 模式

### 1. 构造函数

```go
func NewPubFanout(exchangeName, mqURL string, opts ...Option) (*MqFanout, error)
func NewConsumeFanout(exchangeName, mqURL string, opts ...Option) (*MqFanout, error)
```

参数说明：

- `exchangeName`：fanout exchange 名称
- `mqURL`：RabbitMQ 地址

### 2. 发布广播消息

```go
func (r *MqFanout) Publish(message string, handler MsgHandler) error
```

作用：

- 向 fanout exchange 广播消息

### 3. 消费广播消息

```go
func (r *MqFanout) Consume(handler MsgHandler) error
```

说明：

- 失败时不做自动 retry
- 因为 fanout 模式下重投 exchange 可能导致已经成功的订阅者再次收到消息

### 4. 发布延迟广播消息

```go
func (r *MqFanout) PublishDelay(message string, handler MsgHandler, ttl string) error
```

### 5. 消费延迟广播消息

```go
func (r *MqFanout) ConsumeDelay(handler MsgHandler) error
```

### 6. 消费失败转死信

```go
func (r *MqFanout) ConsumeFailToDlx(handler MsgHandler) error
```

### 7. 消费死信

```go
func (r *MqFanout) ConsumeDlx(handler MsgHandler) error
```

---

## 十一、Topic 模式

### 1. 构造函数

```go
func NewPubTopic(exchangeName, routingKey, mqURL string, opts ...Option) (*MqTopic, error)
func NewConsumeTopic(exchangeName, routingKey, mqURL string, opts ...Option) (*MqTopic, error)
```

兼容旧方法：

```go
func NewMQTopic(exchangeName, routingKey, mqURL string, opts ...Option) (*MqTopic, error)
```

说明：

- `NewMQTopic` 仍然可用
- 但建议新代码使用 `NewPubTopic` / `NewConsumeTopic`

### 2. 发布主题消息

```go
func (r *MqTopic) Publish(message string, handler MsgHandler) error
```

### 3. 消费主题消息

```go
func (r *MqTopic) Consume(handler MsgHandler) error
```

### 4. 发布延迟主题消息

```go
func (r *MqTopic) PublishDelay(message string, handler MsgHandler, ttl string) error
```

### 5. 消费延迟主题消息

```go
func (r *MqTopic) ConsumeDelay(handler MsgHandler) error
```

### 6. 消费失败转死信

```go
func (r *MqTopic) ConsumeFailToDlx(handler MsgHandler) error
```

### 7. 消费死信队列

```go
func (r *MqTopic) ConsumeDlx(handler MsgHandler) error
```

---

## 十二、完整示例

仓库内已经提供可直接运行的示例目录：

- [examples/simple/producer/main.go](/Users/xiaozhaofu/go/src/mq/examples/simple/producer/main.go)
- [examples/simple/consumer/main.go](/Users/xiaozhaofu/go/src/mq/examples/simple/consumer/main.go)
- [examples/direct/producer/main.go](/Users/xiaozhaofu/go/src/mq/examples/direct/producer/main.go)
- [examples/direct/consumer/main.go](/Users/xiaozhaofu/go/src/mq/examples/direct/consumer/main.go)
- [examples/fanout/producer/main.go](/Users/xiaozhaofu/go/src/mq/examples/fanout/producer/main.go)
- [examples/fanout/consumer/main.go](/Users/xiaozhaofu/go/src/mq/examples/fanout/consumer/main.go)
- [examples/topic/producer/main.go](/Users/xiaozhaofu/go/src/mq/examples/topic/producer/main.go)
- [examples/topic/consumer/main.go](/Users/xiaozhaofu/go/src/mq/examples/topic/consumer/main.go)

本地运行时，可以在两个终端中分别执行 producer 和 consumer，例如：

```bash
go run ./examples/direct/consumer
go run ./examples/direct/producer
```

### 1. Direct 模式发布示例

```go
package main

import (
    "context"
    "log"

    "github.com/gtkit/mq/rabbit"
)

type handler struct{}

func (h *handler) Process(body []byte, msgID string) error {
    return nil
}

func (h *handler) Failed(msg rabbit.FailedMsg) {
    log.Printf("publish failed, msgID=%s body=%s", msg.MsgId, string(msg.Message))
}

func main() {
    h := &handler{}

    mq, err := rabbit.NewPubDirect(
        "order.exchange",
        "order.created",
        "amqp://guest:guest@127.0.0.1:5672/",
        rabbit.WithContext(context.Background()),
        rabbit.WithConnectionName("order-producer"),
        rabbit.WithRetryTTL("3000"),
        rabbit.WithMaxRetry(5),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer mq.Destroy()

    if err := mq.Publish("create order #1001", h); err != nil {
        log.Fatal(err)
    }
}
```

### 2. Direct 模式消费示例

```go
package main

import (
    "context"
    "errors"
    "log"

    "github.com/gtkit/mq/rabbit"
)

type handler struct{}

func (h *handler) Process(body []byte, msgID string) error {
    log.Printf("consume msgID=%s body=%s", msgID, string(body))

    if string(body) == "bad" {
        return errors.New("mock business error")
    }
    return nil
}

func (h *handler) Failed(msg rabbit.FailedMsg) {
    log.Printf("finally failed, msgID=%s body=%s", msg.MsgId, string(msg.Message))
}

func main() {
    h := &handler{}

    mq, err := rabbit.NewConsumeDirect(
        "order.exchange",
        "order.created",
        "amqp://guest:guest@127.0.0.1:5672/",
        rabbit.WithContext(context.Background()),
        rabbit.WithConnectionName("order-consumer"),
        rabbit.WithQueueName("order.created.queue"),
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
```

### 3. Simple 延迟消息示例

```go
mq, err := rabbit.NewPubSimple(
    "sms.queue",
    "amqp://guest:guest@127.0.0.1:5672/",
)
if err != nil {
    return err
}
defer mq.Destroy()

err = mq.PublishDelay("send after 10 seconds", handler, "10000")
if err != nil {
    return err
}
```

### 4. 注入 `gtkit/logger`

```go
import (
    gtlogger "github.com/gtkit/logger"
    "github.com/gtkit/mq/rabbit"
)

func init() {
    l := gtlogger.Default()
    if !rabbit.SetExternalLogger(l) {
        panic("inject gtkit logger failed")
    }
}
```

---

## 十三、失败重试机制

当前失败重试主要适用于：

- `simple`
- `direct`
- `topic`

重试流程如下：

1. 消费到消息
2. 调用 `handler.Process(body, msgID)`
3. 如果返回 `nil`，则 `Ack`
4. 如果返回 `error`，读取 `x-retry`
5. 若当前重试次数未超过 `MaxRetry`，则消息进入 retry queue
6. retry queue TTL 到期后，再回到原始目标队列
7. 超过最大重试次数后，消息被 `Reject(false)`

补充说明：

- retry 次数记录在 `Headers["x-retry"]`
- 默认最大重试次数为 `3`
- 默认 retry TTL 为 `"2000"`

### 关于 `Ack` 失败

当消息已经成功发布到 retry queue，但原消息 `Ack` 失败时，库会：

- 记录错误日志
- 不让消费循环直接退出

这仍然是 `at-least-once` 语义，不保证绝对去重。业务侧如果要求严格幂等，需要自己基于 `msgID` 做去重。

## 十四、死信机制

库支持两类消费方式：

- 普通消费：`Consume`
- 失败直接转死信：`ConsumeFailToDlx`

同时支持死信队列消费：

- `ConsumeDlx`

适合理解为：

- `Consume`：失败先重试
- `ConsumeFailToDlx`：失败直接死信
- `ConsumeDlx`：专门处理已经死信的消息

## 十五、自动重连机制

### 1. Consumer 重连

消费端会处理两类异常：

- channel 关闭
- connection 关闭

当 RabbitMQ 重启、网络闪断、TCP 断开时：

- 内部会先重建 connection
- 再重新建立 consumer channel
- 再重新声明消费拓扑
- 再继续消费

### 2. 重连退避策略

消费端重试采用指数退避：

```text
1s -> 2s -> 4s -> 8s -> 16s -> 30s -> 30s ...
```

当重新建立消费成功后，退避计数会清零。

### 3. Publish 重连

发布端使用复用的 confirm channel：

- channel 不可用时自动重建
- connection 已关闭时先重建 connection
- 新 channel 建立后重新声明当前 channel 上需要的 exchange / queue

## 十六、注意事项

### 1. `Destroy()` 必须调用

如果你创建了 mq 实例，不管是发布还是消费，结束时都应该调用：

```go
defer mq.Destroy()
```

### 2. `handler` 不建议传 `nil`

虽然部分发布方法允许 `handler == nil`，但建议始终提供实现，便于接收失败通知。

### 3. `fanout` 不做自动 retry

这是有意设计，不是遗漏。

原因是：

- fanout 重投会再次广播
- 已成功消费的订阅者也可能再次收到消息

### 4. `RetryTTL` 是字符串

示例：

- `"1000"`：1 秒
- `"5000"`：5 秒
- `"60000"`：60 秒

### 5. 消费方法会阻塞

例如：

```go
err := mq.Consume(handler)
```

这是一个阻塞调用，通常应该放在 goroutine 中运行，或者作为进程主循环。

### 6. 推荐业务自己做幂等

RabbitMQ 常见语义是至少一次投递，因此建议业务基于：

- `msgID`
- 订单号
- 业务唯一键

自行做幂等保护。

## 十七、测试

### 1. 单元测试

```bash
go test ./rabbit/...
```

### 2. 全量测试

```bash
go test ./...
```

### 3. 集成测试

```bash
MQ_INTEGRATION=1 go test ./test -count=1
```

自定义 RabbitMQ 地址：

```bash
MQ_INTEGRATION=1 MQ_URL=amqp://guest:guest@127.0.0.1:5672/ go test ./test -count=1
```

## 十八、建议使用方式

如果你的业务场景是：

- 一个服务发布消息，另一个服务消费消息
- 希望失败自动重试
- 希望 RabbitMQ 重启后自动恢复
- 希望日志可接入自己项目现有日志体系

那么推荐你至少这样初始化：

```go
mq, err := rabbit.NewConsumeDirect(
    "biz.exchange",
    "biz.route",
    "amqp://guest:guest@127.0.0.1:5672/",
    rabbit.WithContext(context.Background()),
    rabbit.WithConnectionName("biz-consumer"),
    rabbit.WithQueueName("biz.queue"),
    rabbit.WithMaxRetry(5),
    rabbit.WithRetryTTL("3000"),
)
```

日志初始化：

```go
if !rabbit.SetExternalLogger(projectLogger) {
    panic("inject logger failed")
}
```

## 十九、当前 API 速查表

### Simple

- `NewPubSimple`
- `NewConsumeSimple`
- `Publish`
- `Consume`
- `PublishDelay`
- `ConsumeDelay`
- `PublishWithDlx`
- `ConsumeFailToDlx`
- `ConsumeDlx`
- `RetryMsg`

### Direct

- `NewPubDirect`
- `NewConsumeDirect`
- `Publish`
- `Consume`
- `PublishDelay`
- `ConsumeDelay`
- `ConsumeFailToDlx`
- `ConsumeDlx`
- `RetryMsg`

### Fanout

- `NewPubFanout`
- `NewConsumeFanout`
- `Publish`
- `Consume`
- `PublishDelay`
- `ConsumeDelay`
- `ConsumeFailToDlx`
- `ConsumeDlx`

### Topic

- `NewPubTopic`
- `NewConsumeTopic`
- `NewMQTopic`
- `Publish`
- `Consume`
- `PublishDelay`
- `ConsumeDelay`
- `ConsumeFailToDlx`
- `ConsumeDlx`

---

如果后续你需要，我可以继续把 README 再补两类内容：

- 每个方法对应的时序图
- 一份可直接复制运行的 producer / consumer 示例目录
