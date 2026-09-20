# configs

存放本地开发和压测配置，不把环境参数硬编码进 C++。

- `dev.yaml`：单实例开发配置。
- `benchmark.yaml`：本地接口和业务压测参数。

多实例演练通过 `deploy/compose/docker-compose.multi.yml` 配置，避免再维护一套与主链路无关的集群协议配置。
