# observability

负责指标、结构化日志、链路追踪、健康检查和运行状态输出。

## 第一批指标

`qps`、`p50/p99/p999 latency`、`active_connections`、`event_loop_lag`、`queue_depth`、`cache_hit_ratio`、`singleflight_coalesced`、`rate_limited_total`、`stock_reserve_success/fail`、`order_idempotent_hit`、`consumer_lag`、`wal_fsync_latency`。

提供 `/metrics`、`/health` 和 `/debug/stats`；HTTP 网关会生成或透传 `X-Request-Id`，错误请求输出结构化 JSON 日志，包含 `request_id`、方法、目标、状态码和耗时字段。
