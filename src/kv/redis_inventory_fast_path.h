#pragma once

#include "business/inventory/inventory_service.h"
#include "common/error/status.h"

#include <cstdint>
#include <string>

namespace live::kv { class RedisKVStore; }

namespace live::business::inventory {

enum class RedisInventoryDecision { kSuccess, kDuplicate, kInsufficient, kConflict, kNotFound };

struct RedisInventoryResult {
    RedisInventoryDecision decision{RedisInventoryDecision::kNotFound};
};

// Redis Lua fast path for a single SKU's reserve/confirm/release state
// machine. All keys for one SKU share a hash tag so the scripts remain
// single-slot operations when Redis Cluster is introduced.
class RedisInventoryFastPath {
public:
    explicit RedisInventoryFastPath(::live::kv::RedisKVStore* redis) : redis_(redis) {}

    common::Status initializeSku(const std::string& sku_id, std::int64_t quantity);
    common::Status reserve(const std::string& sku_id, std::int64_t quantity,
                           const std::string& operation_id, RedisInventoryResult* result);
    common::Status confirm(const std::string& sku_id, const std::string& operation_id,
                           RedisInventoryResult* result);
    common::Status release(const std::string& sku_id, const std::string& operation_id,
                           RedisInventoryResult* result);
    common::Status rollbackConfirmation(const std::string& sku_id, const std::string& operation_id,
                                        RedisInventoryResult* result);
    common::Status rollbackRelease(const std::string& sku_id, const std::string& operation_id,
                                   RedisInventoryResult* result);
    common::Status queryStock(const std::string& sku_id, StockSnapshot* snapshot);
    common::Status repairStock(const std::string& sku_id, const StockSnapshot& snapshot);

private:
    static std::string stockKey(const std::string& sku_id);
    static std::string operationKey(const std::string& sku_id);
    common::Status runTransition(const std::string& script, const std::string& sku_id,
                                 const std::string& operation_id, RedisInventoryResult* result);

    ::live::kv::RedisKVStore* redis_;
};

}  // namespace live::business::inventory
