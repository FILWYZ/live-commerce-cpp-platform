# 秒杀消费后台业务域

项目只保留一条主线，跨模块调用通过明确的 Service/Repository 接口完成：

- `user`：注册、登录、Session 和请求鉴权。
- `product`：秒杀 SKU 元数据和商品查询。
- `promotion`：秒杀活动生命周期、资格判断和用户去重。
- `inventory`：可用、预留、已售库存，以及 Redis/MySQL 对账。
- `flash_sale`：秒杀提交、消息消费、重试、补偿和 DLQ。
- `order`：消费者落单后的订单模型、幂等和查询。

业务层不直接依赖具体 Redis、Kafka 或 MySQL 实现，由 `kv`、`messaging` 和 `storage` 提供适配。项目的核心是“Redis 快速准入与库存预留 + 异步消费 + MySQL 事务落单”。
