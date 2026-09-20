# kv

统一 KV 访问抽象，隔离业务与具体 KV 后端。

## 当前接口

已实现 `IKVStore`、`KVClient` 及以下操作：`Get`、`Set`、`Del`、`CAS`、`Expire`。业务幂等语义由业务层显式传入。

## 后端演进

1. Redis 适配器：已使用 hiredis 实现 Get/Set/Del/CAS/Expire、超时连接和 Lua 原子 CAS；`evalInteger` 使用 `SCRIPT LOAD + EVALSHA`，并处理 `NOSCRIPT` 重载。
2. 秒杀闸门：`RedisFlashSaleGate` 通过 Lua 原子维护活动状态、配额、跨进程用户幂等和回滚；Key 使用 promotion hash tag，便于 Redis Cluster 同槽执行。
3. 自研 KV 适配器：接入 `storage` 的 Memory KV、WAL、Snapshot。
4. 本地恢复路径：通过 `storage` 的 Memory KV、WAL 和 Snapshot 提供零依赖运行与重启恢复。
