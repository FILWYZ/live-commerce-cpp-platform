#include "kv/redis_inventory_gateway.h"

namespace live::business::inventory {

void RedisInventoryGateway::copyDecision(RedisInventoryDecision decision, InventoryReservationResult* result) {
    if (result == nullptr) return;
    result->decision = decision == RedisInventoryDecision::kSuccess ? InventoryDecision::kSuccess :
        (decision == RedisInventoryDecision::kDuplicate ? InventoryDecision::kDuplicate :
         (decision == RedisInventoryDecision::kInsufficient ? InventoryDecision::kInsufficient :
          (decision == RedisInventoryDecision::kConflict ? InventoryDecision::kConflict : InventoryDecision::kNotFound)));
}

common::Status RedisInventoryGateway::initializeSku(const std::string& sku_id, std::int64_t quantity) {
    return fast_path_.initializeSku(sku_id, quantity);
}

common::Status RedisInventoryGateway::reserve(const std::string& sku_id, std::int64_t quantity,
                                              const std::string& operation_id,
                                              InventoryReservationResult* result) {
    if (result == nullptr) return common::Status::InvalidArgument("inventory result must not be null");
    RedisInventoryResult redis_result;
    const auto status = fast_path_.reserve(sku_id, quantity, operation_id, &redis_result);
    copyDecision(redis_result.decision, result);
    return status;
}

common::Status RedisInventoryGateway::confirm(const std::string& sku_id, const std::string& operation_id,
                                              InventoryReservationResult* result) {
    if (result == nullptr) return common::Status::InvalidArgument("inventory result must not be null");
    RedisInventoryResult redis_result;
    const auto status = fast_path_.confirm(sku_id, operation_id, &redis_result);
    copyDecision(redis_result.decision, result);
    return status;
}

common::Status RedisInventoryGateway::release(const std::string& sku_id, const std::string& operation_id,
                                              InventoryReservationResult* result) {
    if (result == nullptr) return common::Status::InvalidArgument("inventory result must not be null");
    RedisInventoryResult redis_result;
    const auto status = fast_path_.release(sku_id, operation_id, &redis_result);
    copyDecision(redis_result.decision, result);
    return status;
}

common::Status RedisInventoryGateway::rollbackConfirmation(const std::string& sku_id, const std::string& operation_id,
                                                           InventoryReservationResult* result) {
    if (result == nullptr) return common::Status::InvalidArgument("inventory result must not be null");
    RedisInventoryResult redis_result;
    const auto status = fast_path_.rollbackConfirmation(sku_id, operation_id, &redis_result);
    copyDecision(redis_result.decision, result);
    return status;
}

common::Status RedisInventoryGateway::rollbackRelease(const std::string& sku_id, const std::string& operation_id,
                                                      InventoryReservationResult* result) {
    if (result == nullptr) return common::Status::InvalidArgument("inventory result must not be null");
    RedisInventoryResult redis_result;
    const auto status = fast_path_.rollbackRelease(sku_id, operation_id, &redis_result);
    copyDecision(redis_result.decision, result);
    return status;
}

common::Status RedisInventoryGateway::queryStock(const std::string& sku_id, StockSnapshot* snapshot) {
    return fast_path_.queryStock(sku_id, snapshot);
}

common::Status RedisInventoryGateway::repairStock(const std::string& sku_id, const StockSnapshot& snapshot) {
    return fast_path_.repairStock(sku_id, snapshot);
}

}  // namespace live::business::inventory
