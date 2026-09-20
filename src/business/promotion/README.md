# promotion

统一秒杀活动域，支持创建、预热、开始、结束、资格判断和查询。活动状态为 `CREATED -> PREHEATING -> RUNNING -> FINISHED`，库存操作统一交给 `inventory`。

`FlashSaleAdmissionGate` 将活动状态、配额和用户幂等从业务流程中独立出来。默认构建使用进程内实现；配置 Redis 时，Gateway 使用 `RedisFlashSaleGate` 进行跨进程 admission。闸门只是流量过滤层，不等同于最终库存扣减。
