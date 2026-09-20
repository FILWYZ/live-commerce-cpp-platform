#pragma once

#include "messaging/order_queue.h"

#include <string>

namespace live::messaging {

class KafkaProducer;

// Producer side of the flash-sale queue. Consumption is handled by
// KafkaFlashSaleOrderConsumer because Kafka offset commits must happen only
// after the business handler has completed.
class KafkaOrderQueue final : public IOrderQueue {
public:
    KafkaOrderQueue(KafkaProducer* producer, std::string topic)
        : producer_(producer), topic_(std::move(topic)) {}

    common::Status push(const FlashSaleOrderMessage& message) override;
    common::Status pop(FlashSaleOrderMessage* message, std::chrono::milliseconds timeout) override;
    std::size_t pending() const override { return 0; }

private:
    KafkaProducer* producer_{nullptr};
    std::string topic_;
};

}  // namespace live::messaging
