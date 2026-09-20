#pragma once

#include "business/inventory/inventory_gateway.h"
#include "kv/redis_inventory_fast_path.h"

namespace live::business::inventory {

class RedisInventoryGateway final : public IInventoryGateway {
public:
    explicit RedisInventoryGateway(::live::kv::RedisKVStore* redis) : fast_path_(redis) {}

    common::Status initializeSku(const std::string& sku_id, std::int64_t quantity) override;
    common::Status reserve(const std::string& sku_id, std::int64_t quantity,
                           const std::string& operation_id,
                           InventoryReservationResult* result) override;
    common::Status confirm(const std::string& sku_id, const std::string& operation_id,
                           InventoryReservationResult* result) override;
    common::Status release(const std::string& sku_id, const std::string& operation_id,
                           InventoryReservationResult* result) override;
    common::Status rollbackConfirmation(const std::string& sku_id, const std::string& operation_id,
                                        InventoryReservationResult* result) override;
    common::Status rollbackRelease(const std::string& sku_id, const std::string& operation_id,
                                   InventoryReservationResult* result) override;
    common::Status queryStock(const std::string& sku_id, StockSnapshot* snapshot) override;
    common::Status repairStock(const std::string& sku_id, const StockSnapshot& snapshot) override;

private:
    static void copyDecision(RedisInventoryDecision decision, InventoryReservationResult* result);
    RedisInventoryFastPath fast_path_;
};

}  // namespace live::business::inventory
