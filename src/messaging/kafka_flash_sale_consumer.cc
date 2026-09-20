#include "messaging/kafka_flash_sale_consumer.h"

#include "business/order/order_service.h"

#include <chrono>

namespace live::messaging {

common::Status KafkaFlashSaleOrderConsumer::handle(const Event& event) {
    FlashSaleOrderMessage message;
    if (const auto status = FlashSaleOrderMessage::deserialize(event.payload, &message); !status.ok()) {
        std::lock_guard<std::mutex> lock(attempts_mutex_);
        auto& count = attempts_[event.event_id];
        if (++count < max_attempts_) return status;
        const auto dlq_status = sendDeadLetter(event);
        if (!dlq_status.ok()) return status;
        attempts_.erase(event.event_id);
        ++dead_lettered_;
        return common::Status::Ok();
    }
    ::live::business::order::Order order;
    if (const auto status = repository_->persistQueuedOrder(message, &order); !status.ok()) {
        std::lock_guard<std::mutex> lock(attempts_mutex_);
        auto& count = attempts_[event.event_id];
        if (++count < max_attempts_) return status;
        if (dead_letter_producer_ == nullptr || dead_letter_topic_.empty()) return status;
        const auto dlq_status = dead_letter_producer_->publish({
            "dlq-" + event.event_id, dead_letter_topic_, event.key, event.payload, event.sequence});
        if (!dlq_status.ok()) return status;
        (void)compensate(message);
        attempts_.erase(event.event_id);
        ++dead_lettered_;
        return common::Status::Ok();
    }

    ::live::business::inventory::InventoryReservationResult result;
    const auto confirm_status = inventory_->confirm(message.sku_id, message.order_id, &result);
    if (!confirm_status.ok() || (result.decision != ::live::business::inventory::InventoryDecision::kSuccess &&
                                 result.decision != ::live::business::inventory::InventoryDecision::kDuplicate)) {
        std::lock_guard<std::mutex> lock(attempts_mutex_);
        auto& count = attempts_[event.event_id];
        if (++count < max_attempts_) return confirm_status.ok()
            ? common::Status::Internal("Kafka flash-sale inventory confirmation failed") : confirm_status;
        if (dead_letter_producer_ == nullptr || dead_letter_topic_.empty()) {
            return confirm_status.ok() ? common::Status::Internal("Kafka flash-sale inventory confirmation failed") : confirm_status;
        }
        const auto dlq_status = dead_letter_producer_->publish({
            "dlq-" + event.event_id, dead_letter_topic_, event.key, event.payload, event.sequence});
        if (!dlq_status.ok()) return confirm_status;
        (void)compensate(message);
        attempts_.erase(event.event_id);
        ++dead_lettered_;
        return common::Status::Ok();
    }
    {
        std::lock_guard<std::mutex> lock(attempts_mutex_);
        attempts_.erase(event.event_id);
    }
    ++processed_;
    return common::Status::Ok();
}

common::Status KafkaFlashSaleOrderConsumer::sendDeadLetter(const Event& event) {
    if (dead_letter_producer_ == nullptr || dead_letter_topic_.empty()) {
        return common::Status::FailedPrecondition("Kafka dead-letter producer is not configured");
    }
    return dead_letter_producer_->publish({"dlq-" + event.event_id, dead_letter_topic_, event.key,
                                           event.payload, event.sequence});
}

common::Status KafkaFlashSaleOrderConsumer::compensate(const FlashSaleOrderMessage& message) {
    ::live::business::inventory::InventoryReservationResult result;
    if (inventory_ != nullptr) (void)inventory_->release(message.sku_id, message.order_id, &result);
    if (admission_gate_ != nullptr) (void)admission_gate_->rollback(message.promotion_id, message.user_id);
    return common::Status::Ok();
}

common::Status KafkaFlashSaleOrderConsumer::start(const std::string& brokers, const std::string& group_id) {
    if (consumer_ == nullptr || repository_ == nullptr || inventory_ == nullptr || topic_.empty() ||
        dead_letter_topic_.empty() || max_attempts_ == 0) {
        return common::Status::FailedPrecondition("Kafka flash-sale consumer is not configured");
    }
    if (const auto status = consumer_->start(brokers, group_id, {topic_}); !status.ok()) return status;
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) return common::Status::AlreadyExists("Kafka flash-sale consumer is already running");
    worker_ = std::thread([this] { run(); });
    return common::Status::Ok();
}

void KafkaFlashSaleOrderConsumer::run() {
    while (running_.load(std::memory_order_relaxed)) {
        const auto status = consumer_->poll(250, [this](const Event& event) {
            // librdkafka advances its local fetch position even when the
            // callback does not commit. Retry the same event before polling
            // another record; otherwise a transient DB error would leave the
            // message uncommitted but not immediately redelivered.
            auto result = handle(event);
            for (std::size_t attempt = 1; !result.ok() && attempt < max_attempts_; ++attempt) {
                result = handle(event);
            }
            return result;
        });
        if (!status.ok()) std::this_thread::yield();
    }
}

void KafkaFlashSaleOrderConsumer::stop() {
    if (!running_.exchange(false)) return;
    if (worker_.joinable()) worker_.join();
    if (consumer_ != nullptr) consumer_->stop();
}

}  // namespace live::messaging
