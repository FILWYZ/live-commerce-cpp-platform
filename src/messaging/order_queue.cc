#include "messaging/order_queue.h"

namespace live::messaging {

common::Status InMemoryOrderQueue::push(const FlashSaleOrderMessage& message) {
    if (const auto status = message.validate(); !status.ok()) return status;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (messages_.size() >= 100000) return common::Status::ResourceExhausted("order queue is full");
        messages_.push_back(message);
    }
    condition_.notify_one();
    return common::Status::Ok();
}

common::Status InMemoryOrderQueue::pop(FlashSaleOrderMessage* message, std::chrono::milliseconds timeout) {
    if (message == nullptr) return common::Status::InvalidArgument("queue message must not be null");
    std::unique_lock<std::mutex> lock(mutex_);
    if (!condition_.wait_for(lock, timeout, [this] { return !messages_.empty(); })) {
        return common::Status::Incomplete("order queue poll timed out");
    }
    *message = std::move(messages_.front());
    messages_.pop_front();
    return common::Status::Ok();
}

std::size_t InMemoryOrderQueue::pending() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return messages_.size();
}

}  // namespace live::messaging
