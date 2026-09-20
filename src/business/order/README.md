# order

负责秒杀消费者落单后的订单持久化模型和查询。消费者使用 `order_id` 与 `idempotency_key` 保证重复消息不会创建多个订单。
