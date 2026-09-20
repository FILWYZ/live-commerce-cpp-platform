# deploy

- `docker`：构建秒杀消费后台镜像。
- `compose`：启动本地 API、Redis 和 Redpanda；API 只暴露 HTTP 端口。
- `scripts`：构建、测试和本地联调入口。

Compose 启动后，可通过 `ECOMMERCE_REDIS_HOST=redis ECOMMERCE_KAFKA_BROKERS=redpanda:9092` 让 API 使用外部依赖。
