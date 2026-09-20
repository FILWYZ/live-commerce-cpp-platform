# MySQL 持久化契约

本目录只定义秒杀消费后台需要落库的交易账本和 Outbox 边界。LocalKV/WAL 用于零依赖单机运行，MySQL 适配用于多实例和真实依赖联调。

必须落库的关键约束：

- `orders.idempotency_key` 唯一，防止重复消费创建订单。
- `inventory(sku_id)` 使用条件更新，数据库路径不能绕过库存账本。
- `outbox_events.event_id` 唯一，消费者按事件 ID 幂等。
