#pragma once

#include "business/inventory/inventory_service.h"

#include <cstdint>
#include <string>

namespace live::business::inventory {

enum class InventoryDecision { kSuccess, kDuplicate, kInsufficient, kConflict, kNotFound };

struct InventoryReservationResult {
    InventoryDecision decision{InventoryDecision::kNotFound};
};

// The flash-sale path depends on this small contract instead of knowing
// whether inventory is local or Redis-backed.  Implementations must make a
// transition idempotent for the same operation_id.
class IInventoryGateway {
public:
    virtual ~IInventoryGateway() = default;
    virtual common::Status initializeSku(const std::string& sku_id, std::int64_t quantity) = 0;
    virtual common::Status reserve(const std::string& sku_id, std::int64_t quantity,
                                   const std::string& operation_id,
                                   InventoryReservationResult* result) = 0;
    virtual common::Status confirm(const std::string& sku_id, const std::string& operation_id,
                                   InventoryReservationResult* result) = 0;
    virtual common::Status release(const std::string& sku_id, const std::string& operation_id,
                                   InventoryReservationResult* result) = 0;
    virtual common::Status rollbackConfirmation(const std::string& sku_id, const std::string& operation_id,
                                                InventoryReservationResult* result) = 0;
    virtual common::Status rollbackRelease(const std::string& sku_id, const std::string& operation_id,
                                           InventoryReservationResult* result) = 0;
    virtual common::Status queryStock(const std::string& sku_id, StockSnapshot* snapshot) = 0;
    virtual common::Status repairStock(const std::string& sku_id, const StockSnapshot& snapshot) = 0;
};

class LocalInventoryGateway final : public IInventoryGateway {
public:
    explicit LocalInventoryGateway(InventoryService* inventory) : inventory_(inventory) {}

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
    InventoryService* inventory_{nullptr};
};

}  // namespace live::business::inventory
