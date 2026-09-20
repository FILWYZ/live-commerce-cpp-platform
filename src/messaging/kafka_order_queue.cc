#include "messaging/kafka_order_queue.h"

#include "messaging/kafka_client.h"

namespace live::messaging {

common::Status KafkaOrderQueue::push(const FlashSaleOrderMessage& message) {
    if (producer_ == nullptr || topic_.empty()) return common::Status::FailedPrecondition("Kafka order queue is not configured");
    if (const auto status = message.validate(); !status.ok()) return status;
    const auto payload = message.serialize();
    if (payload.empty()) return common::Status::InvalidArgument("invalid Kafka order message");
    return producer_->publish({"flash-order-" + message.order_id, topic_, message.order_id, payload, 0});
}

common::Status KafkaOrderQueue::pop(FlashSaleOrderMessage*, std::chrono::milliseconds) {
    return common::Status::FailedPrecondition("Kafka order queue is consumed by KafkaFlashSaleOrderConsumer");
}

}  // namespace live::messaging
