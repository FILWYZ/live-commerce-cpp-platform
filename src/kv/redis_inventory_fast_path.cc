#include "kv/redis_inventory_fast_path.h"

#include "kv/redis_kv_store.h"

#include <sstream>
#include <vector>

namespace live::business::inventory {
namespace {

constexpr const char* kInitializeScript =
    "if redis.call('EXISTS', KEYS[1])==1 then "
    "if redis.call('HGET', KEYS[1], 'total')~=ARGV[1] then return 0 end; return 2 end; "
    "redis.call('HSET', KEYS[1], 'total', ARGV[1], 'available', ARGV[1], 'reserved', '0', 'sold', '0'); return 1";
constexpr const char* kReserveScript =
    "local old=redis.call('HGET', KEYS[2], ARGV[2]); "
    "if old then if old==ARGV[1]..'|reserved' then return 2 else return -2 end end; "
    "local available=tonumber(redis.call('HGET', KEYS[1], 'available')) or -1; "
    "local quantity=tonumber(ARGV[1]); if available<quantity then return 0 end; "
    "redis.call('HINCRBY', KEYS[1], 'available', -quantity); "
    "redis.call('HINCRBY', KEYS[1], 'reserved', quantity); "
    "redis.call('HSET', KEYS[2], ARGV[2], ARGV[1]..'|reserved'); return 1";
constexpr const char* kConfirmScript =
    "local record=redis.call('HGET', KEYS[2], ARGV[1]); if not record then return -1 end; "
    "local separator=string.find(record, '|'); local quantity=tonumber(string.sub(record, 1, separator-1)); "
    "local state=string.sub(record, separator+1); if state=='confirmed' then return 1 end; "
    "if state~='reserved' then return -2 end; "
    "redis.call('HINCRBY', KEYS[1], 'reserved', -quantity); redis.call('HINCRBY', KEYS[1], 'sold', quantity); "
    "redis.call('HSET', KEYS[2], ARGV[1], quantity..'|confirmed'); return 1";
constexpr const char* kReleaseScript =
    "local record=redis.call('HGET', KEYS[2], ARGV[1]); if not record then return -1 end; "
    "local separator=string.find(record, '|'); local quantity=tonumber(string.sub(record, 1, separator-1)); "
    "local state=string.sub(record, separator+1); if state=='released' then return 1 end; "
    "if state~='reserved' then return -2 end; "
    "redis.call('HINCRBY', KEYS[1], 'reserved', -quantity); redis.call('HINCRBY', KEYS[1], 'available', quantity); "
    "redis.call('HSET', KEYS[2], ARGV[1], quantity..'|released'); return 1";
constexpr const char* kRollbackConfirmationScript =
    "local record=redis.call('HGET', KEYS[2], ARGV[1]); if not record then return -1 end; "
    "local separator=string.find(record, '|'); local quantity=tonumber(string.sub(record, 1, separator-1)); "
    "if string.sub(record, separator+1)~='confirmed' then return -2 end; "
    "redis.call('HINCRBY', KEYS[1], 'sold', -quantity); redis.call('HINCRBY', KEYS[1], 'reserved', quantity); "
    "redis.call('HSET', KEYS[2], ARGV[1], quantity..'|reserved'); return 1";
constexpr const char* kRollbackReleaseScript =
    "local record=redis.call('HGET', KEYS[2], ARGV[1]); if not record then return -1 end; "
    "local separator=string.find(record, '|'); local quantity=tonumber(string.sub(record, 1, separator-1)); "
    "if string.sub(record, separator+1)~='released' then return -2 end; "
    "redis.call('HINCRBY', KEYS[1], 'available', -quantity); redis.call('HINCRBY', KEYS[1], 'reserved', quantity); "
    "redis.call('HSET', KEYS[2], ARGV[1], quantity..'|reserved'); return 1";
constexpr const char* kRepairScript =
    "local reserved=tonumber(redis.call('HGET', KEYS[1], 'reserved')) or -1; "
    "if reserved~=tonumber(ARGV[2]) then return 0 end; "
    "redis.call('HSET', KEYS[1], 'available', ARGV[1], 'reserved', ARGV[2], 'sold', ARGV[3], 'total', ARGV[1]+ARGV[2]+ARGV[3]); return 1";

common::Status run(::live::kv::RedisKVStore* redis, const std::string& script,
                   const std::vector<std::string>& keys, const std::vector<std::string>& arguments,
                   std::int64_t* result) {
    if (redis == nullptr) return common::Status::FailedPrecondition("Redis inventory fast path is not configured");
    return redis->evalInteger(script, keys, arguments, result);
}

RedisInventoryDecision decisionFor(std::int64_t value) {
    if (value == 1) return RedisInventoryDecision::kSuccess;
    if (value == 2) return RedisInventoryDecision::kDuplicate;
    if (value == 0) return RedisInventoryDecision::kInsufficient;
    if (value == -2) return RedisInventoryDecision::kConflict;
    return RedisInventoryDecision::kNotFound;
}

}  // namespace

std::string RedisInventoryFastPath::stockKey(const std::string& sku_id) {
    return "flash_inventory:{" + sku_id + "}:stock";
}

std::string RedisInventoryFastPath::operationKey(const std::string& sku_id) {
    return "flash_inventory:{" + sku_id + "}:operations";
}

common::Status RedisInventoryFastPath::initializeSku(const std::string& sku_id, std::int64_t quantity) {
    if (sku_id.empty() || quantity <= 0) return common::Status::InvalidArgument("invalid Redis inventory SKU");
    std::int64_t result = 0;
    const auto status = run(redis_, kInitializeScript, {stockKey(sku_id)}, {std::to_string(quantity)}, &result);
    if (!status.ok()) return status;
    return result == 0 ? common::Status::FailedPrecondition("Redis inventory total does not match initialization") : common::Status::Ok();
}

common::Status RedisInventoryFastPath::reserve(const std::string& sku_id, std::int64_t quantity,
                                              const std::string& operation_id, RedisInventoryResult* result) {
    if (sku_id.empty() || quantity <= 0 || operation_id.empty() || result == nullptr) return common::Status::InvalidArgument("invalid Redis reserve");
    std::int64_t code = 0;
    const auto status = run(redis_, kReserveScript, {stockKey(sku_id), operationKey(sku_id)},
                            {std::to_string(quantity), operation_id}, &code);
    if (!status.ok()) return status;
    result->decision = decisionFor(code);
    return common::Status::Ok();
}

common::Status RedisInventoryFastPath::runTransition(const std::string& script, const std::string& sku_id,
                                                     const std::string& operation_id, RedisInventoryResult* result) {
    if (sku_id.empty() || operation_id.empty() || result == nullptr) return common::Status::InvalidArgument("invalid Redis inventory transition");
    std::int64_t code = 0;
    const auto status = run(redis_, script, {stockKey(sku_id), operationKey(sku_id)}, {operation_id}, &code);
    if (!status.ok()) return status;
    result->decision = decisionFor(code);
    return common::Status::Ok();
}

common::Status RedisInventoryFastPath::confirm(const std::string& sku_id, const std::string& operation_id,
                                              RedisInventoryResult* result) {
    return runTransition(kConfirmScript, sku_id, operation_id, result);
}

common::Status RedisInventoryFastPath::release(const std::string& sku_id, const std::string& operation_id,
                                              RedisInventoryResult* result) {
    return runTransition(kReleaseScript, sku_id, operation_id, result);
}

common::Status RedisInventoryFastPath::rollbackConfirmation(const std::string& sku_id, const std::string& operation_id,
                                                            RedisInventoryResult* result) {
    return runTransition(kRollbackConfirmationScript, sku_id, operation_id, result);
}

common::Status RedisInventoryFastPath::rollbackRelease(const std::string& sku_id, const std::string& operation_id,
                                                       RedisInventoryResult* result) {
    return runTransition(kRollbackReleaseScript, sku_id, operation_id, result);
}

common::Status RedisInventoryFastPath::queryStock(const std::string& sku_id, StockSnapshot* snapshot) {
    if (sku_id.empty() || snapshot == nullptr) return common::Status::InvalidArgument("invalid Redis stock query");
    std::string available;
    std::string reserved;
    std::string sold;
    if (const auto status = redis_->hashGet(stockKey(sku_id), "available", &available); !status.ok()) return status;
    if (const auto status = redis_->hashGet(stockKey(sku_id), "reserved", &reserved); !status.ok()) return status;
    if (const auto status = redis_->hashGet(stockKey(sku_id), "sold", &sold); !status.ok()) return status;
    try {
        snapshot->available = std::stoll(available);
        snapshot->reserved = std::stoll(reserved);
        snapshot->sold = std::stoll(sold);
    } catch (...) {
        return common::Status::Internal("invalid Redis stock value");
    }
    return common::Status::Ok();
}

common::Status RedisInventoryFastPath::repairStock(const std::string& sku_id, const StockSnapshot& snapshot) {
    if (sku_id.empty() || snapshot.available < 0 || snapshot.reserved < 0 || snapshot.sold < 0) {
        return common::Status::InvalidArgument("invalid Redis replacement stock");
    }
    std::int64_t result = 0;
    const auto status = run(redis_, kRepairScript, {stockKey(sku_id)},
                            {std::to_string(snapshot.available), std::to_string(snapshot.reserved), std::to_string(snapshot.sold)}, &result);
    if (!status.ok()) return status;
    if (result == 0) return common::Status::FailedPrecondition("Redis stock has active reservations");
    return common::Status::Ok();
}

}  // namespace live::business::inventory
