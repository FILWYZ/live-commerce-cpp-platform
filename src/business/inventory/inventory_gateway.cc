#include "business/inventory/inventory_gateway.h"

namespace live::business::inventory {

common::Status LocalInventoryGateway::initializeSku(const std::string& sku_id, std::int64_t quantity) {
    return inventory_ == nullptr ? common::Status::FailedPrecondition("local inventory is not configured")
                                 : inventory_->addSku(sku_id, quantity);
}

common::Status LocalInventoryGateway::reserve(const std::string& sku_id, std::int64_t quantity,
                                              const std::string& operation_id,
                                              InventoryReservationResult* result) {
    if (result == nullptr) return common::Status::InvalidArgument("inventory result must not be null");
    if (inventory_ == nullptr) return common::Status::FailedPrecondition("local inventory is not configured");
    const auto status = inventory_->reserveStock(sku_id, quantity, operation_id);
    result->decision = status.ok() ? InventoryDecision::kSuccess :
        (status.code() == common::ErrorCode::kResourceExhausted ? InventoryDecision::kInsufficient :
         (status.code() == common::ErrorCode::kNotFound ? InventoryDecision::kNotFound : InventoryDecision::kConflict));
    return status;
}

common::Status LocalInventoryGateway::confirm(const std::string&, const std::string& operation_id,
                                              InventoryReservationResult* result) {
    if (result == nullptr) return common::Status::InvalidArgument("inventory result must not be null");
    if (inventory_ == nullptr) return common::Status::FailedPrecondition("local inventory is not configured");
    const auto status = inventory_->confirmStock(operation_id);
    result->decision = status.ok() ? InventoryDecision::kSuccess : InventoryDecision::kNotFound;
    return status;
}

common::Status LocalInventoryGateway::release(const std::string&, const std::string& operation_id,
                                              InventoryReservationResult* result) {
    if (result == nullptr) return common::Status::InvalidArgument("inventory result must not be null");
    if (inventory_ == nullptr) return common::Status::FailedPrecondition("local inventory is not configured");
    const auto status = inventory_->releaseStock(operation_id);
    result->decision = status.ok() ? InventoryDecision::kSuccess : InventoryDecision::kNotFound;
    return status;
}

common::Status LocalInventoryGateway::rollbackConfirmation(const std::string&, const std::string& operation_id,
                                                           InventoryReservationResult* result) {
    if (result == nullptr) return common::Status::InvalidArgument("inventory result must not be null");
    if (inventory_ == nullptr) return common::Status::FailedPrecondition("local inventory is not configured");
    const auto status = inventory_->rollbackConfirmation(operation_id);
    result->decision = status.ok() ? InventoryDecision::kSuccess : InventoryDecision::kNotFound;
    return status;
}

common::Status LocalInventoryGateway::rollbackRelease(const std::string&, const std::string& operation_id,
                                                      InventoryReservationResult* result) {
    if (result == nullptr) return common::Status::InvalidArgument("inventory result must not be null");
    if (inventory_ == nullptr) return common::Status::FailedPrecondition("local inventory is not configured");
    const auto status = inventory_->rollbackRelease(operation_id);
    result->decision = status.ok() ? InventoryDecision::kSuccess : InventoryDecision::kNotFound;
    return status;
}

common::Status LocalInventoryGateway::queryStock(const std::string& sku_id, StockSnapshot* snapshot) {
    return inventory_ == nullptr ? common::Status::FailedPrecondition("local inventory is not configured")
                                 : inventory_->queryStock(sku_id, snapshot);
}

common::Status LocalInventoryGateway::repairStock(const std::string& sku_id, const StockSnapshot& snapshot) {
    return inventory_ == nullptr ? common::Status::FailedPrecondition("local inventory is not configured")
                                 : inventory_->replaceStock(sku_id, snapshot);
}

}  // namespace live::business::inventory
