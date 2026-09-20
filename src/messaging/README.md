# messaging

负责秒杀订单异步化和业务事件可靠投递。Redis List 是秒杀订单主队列，Kafka/Redpanda 用于订单创建后的可选事件发布；内存 Broker 只供单元测试。

## 事件与可靠性

- 事件：`OrderCreated`、`StockReserved`、`OrderPaid`、`OrderCancelled`、`PromotionFinished`。
- 订单写库与事件发送之间采用 Transactional Outbox 思路。
- `AsyncOutboxDispatcher` 在后台线程 drain `FileOutbox`，避免 Kafka/下游发布延迟阻塞 HTTP 请求线程；失败记录保留并在重试或重启后继续处理。
- 投递语义按至少一次设计，消费者必须以 `event_id`/业务幂等键去重。
- `InMemoryBroker` 支持按订阅配置最大投递次数；超过次数的事件转入订阅级死信主题（默认 `<topic>.DLQ`），并保留原始事件标识与失败上下文，便于测试故障恢复和后续人工补偿。
- Kafka/Redpanda Consumer 提供 Consumer Group、失败重试、DLQ 和库存补偿；它不是默认秒杀订单队列。生产部署仍需要配置 DLQ 告警、重投工具和跨 Redis/MySQL 对账。本项目仍采用至少一次语义，不宣称恰好一次。
