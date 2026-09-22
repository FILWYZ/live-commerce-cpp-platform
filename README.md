# C++ 直播电商秒杀消费后台

一个面向大厂后端实习面试的 C++17 工程实践项目。项目只围绕“秒杀入口 + 异步消费 + 订单落库”展开，用于展示高并发接入、库存一致性、消息可靠性、MySQL 事务、故障恢复和测试工程能力。

## 项目目标

在突发秒杀流量下：

1. 入口快速完成鉴权、限流、活动准入和库存预留；
2. 通过异步队列把突发流量与 MySQL 写入解耦；
3. 通过幂等、事务、Outbox、重试、DLQ 和补偿保证订单最终可恢复；
4. 通过单元测试、集成测试、真实 Redis/MySQL/Kafka 测试和压测验证设计。

## 核心链路

```text
HTTP/Reactor
  -> Session 鉴权与 Token Bucket 限流
  -> 活动状态、用户去重和 quota
  -> Redis Lua 原子库存预留
  -> Redis List 异步队列
  -> 秒杀订单消费者
  -> MySQL 事务：库存镜像 + 订单 + Outbox
  -> Kafka/Redpanda 事件投递
  -> 重试、DLQ、补偿和对账
```

入口返回 `202 QUEUED` 只代表请求已经准入并入队，不代表订单已经完成。订单最终结果以消费端的持久化状态为准。

## 技术栈

```text
C++17 · POSIX Socket · Reactor/EventLoop · HTTP/1.1
Redis · Lua · Redis List · MySQL · Kafka/Redpanda
WAL/Snapshot · Transactional Outbox · CMake · Docker Compose
ASAN/UBSAN/TSAN · CTest · P50/P95/P99 压测
```

## 目录

```text
apps/        服务启动、依赖组装和路由注册
src/         network/gateway/business/cache/kv/storage/messaging 等后端实现
tests/       单元、集成、恢复和外部依赖测试
tools/       HTTP、Redis、MySQL、Kafka 和长稳测试工具
benchmarks/  并发库存基准
configs/     开发和压测配置
deploy/      Docker、Compose、构建测试和故障切换脚本
```

## 快速运行

### 零依赖本地模式

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build --parallel
ctest --test-dir build --output-on-failure
ECOMMERCE_INVENTORY_MODE=local ./build/ecommerce_api_demo 8080
```

### Redis/MySQL/Redpanda 集成模式

```bash
docker compose -f deploy/compose/docker-compose.yml up --build
```

## 项目边界

本项目只覆盖秒杀消费后台，不包含购物车、优惠券、支付、物流等业务域。当前 Compose 是单机实验环境，多实例只验证 API 层的基本共享和故障切换，不宣称具备 Redis Cluster、MySQL 主从、跨机房容灾或生产级最大容量。
