# tools

放置可重复使用的工程工具：

- `http_load_generator`：HTTP 压测客户端，支持自定义并发数并输出吞吐、p50/p95/p99 和最大延迟。
- `http_business_load_generator`：商品、库存、活动和监控只读接口压测。
- `flash_sale_soak_test`：秒杀流水线长稳样本。
- `mysql_capacity_test`：真实 MySQL 索引查询与并发容量样本。
- `redis_concurrency_test`、`kafka_flash_sale_test`、`external_smoke_test`：外部依赖集成验证。
