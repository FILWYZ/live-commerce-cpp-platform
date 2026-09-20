#include "business/flash_sale/flash_sale_order_consumer.h"

#include "business/order/order_service.h"

#include <thread>

namespace live::business::flash_sale {

common::Status FlashSaleOrderConsumer::process(const ::live::messaging::FlashSaleOrderMessage& message) {
    if (repository_ == nullptr || inventory_ == nullptr) return common::Status::FailedPrecondition("flash-sale consumer is not configured");
    ::live::business::order::Order order;
    if (const auto status = repository_->persistQueuedOrder(message, &order); !status.ok()) return status;
    ::live::business::inventory::InventoryReservationResult result;
    const auto status = inventory_->confirm(message.sku_id, message.order_id, &result);
    if (!status.ok() || (result.decision != ::live::business::inventory::InventoryDecision::kSuccess &&
                         result.decision != ::live::business::inventory::InventoryDecision::kDuplicate)) {
        return status.ok() ? common::Status::Internal("inventory confirmation failed") : status;
    }
    ++processed_;
    return common::Status::Ok();
}

common::Status FlashSaleOrderConsumer::compensate(const ::live::messaging::FlashSaleOrderMessage& message) {
    if (inventory_ != nullptr) {
        ::live::business::inventory::InventoryReservationResult result;
        (void)inventory_->release(message.sku_id, message.order_id, &result);
    }
    if (admission_gate_ != nullptr) (void)admission_gate_->rollback(message.promotion_id, message.user_id);
    return common::Status::Ok();
}

common::Status FlashSaleOrderConsumer::processOnce(std::chrono::milliseconds timeout) {
    if (queue_ == nullptr || max_attempts_ == 0) return common::Status::FailedPrecondition("flash-sale queue is not configured");
    ::live::messaging::FlashSaleOrderMessage message;
    const auto pop_status = queue_->pop(&message, timeout);
    if (!pop_status.ok()) return pop_status;
    common::Status last_status = common::Status::Internal("order consumer failed");
    for (std::size_t attempt = 1; attempt <= max_attempts_; ++attempt) {
        last_status = process(message);
        if (last_status.ok()) return last_status;
        if (attempt < max_attempts_) std::this_thread::yield();
    }
    if (dead_letter_queue_ != nullptr && dead_letter_queue_->push(message).ok()) {
        ++dead_lettered_;
    }
    // A poison message must not keep stock reserved forever. The durable
    // order repository is idempotent, so compensation is safe to retry during
    // reconciliation if a database write happened before a later failure.
    (void)compensate(message);
    return last_status;
}

common::Status FlashSaleOrderConsumer::start() {
    if (queue_ == nullptr || repository_ == nullptr || inventory_ == nullptr) return common::Status::FailedPrecondition("flash-sale consumer is not configured");
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) return common::Status::AlreadyExists("flash-sale consumer is already running");
    worker_ = std::thread([this] { run(); });
    return common::Status::Ok();
}

void FlashSaleOrderConsumer::run() {
    while (running_.load()) {
        const auto status = processOnce(std::chrono::milliseconds(250));
        if (!status.ok() && !status.isIncomplete()) std::this_thread::yield();
    }
}

void FlashSaleOrderConsumer::stop() {
    if (!running_.exchange(false)) return;
    if (worker_.joinable()) worker_.join();
}

}  // namespace live::business::flash_sale
