# benchmarks

当前基准围绕后端主链路组织：

- `flash_sale/inventory_benchmark`：并发预占和库存不变量。
- `tools/http_load_generator`：单接口吞吐与尾延迟。
- `tools/http_business_load_generator`：带鉴权的混合业务读写。
- `tools/mysql_capacity_test`：索引查询和数据规模样本。

每次正式压测应记录硬件、编译参数、数据规模、并发数、持续时间、吞吐、p99/p999 和失败数；运行逻辑和结果解释统一见 [`PROJECT_TECHNICAL_GUIDE.md`](../PROJECT_TECHNICAL_GUIDE.md)。
