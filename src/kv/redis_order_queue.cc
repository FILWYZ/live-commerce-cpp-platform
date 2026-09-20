#include "kv/redis_order_queue.h"

#include "kv/redis_kv_store.h"

namespace live::kv {

common::Status RedisOrderQueue::connectBlockingConsumer(const std::string& host, std::uint16_t port,
                                                        std::chrono::milliseconds timeout) {
    auto connection = std::make_unique<RedisKVStore>();
    if (const auto status = connection->connect(host, port, timeout); !status.ok()) return status;
    blocking_redis_ = std::move(connection);
    return common::Status::Ok();
}

common::Status RedisOrderQueue::push(const ::live::messaging::FlashSaleOrderMessage& message) {
    if (redis_ == nullptr || queue_key_.empty()) return common::Status::FailedPrecondition("Redis order queue is not configured");
    const std::string payload = message.serialize();
    if (payload.empty()) return common::Status::InvalidArgument("invalid order queue message");
    return redis_->listPush(queue_key_, payload);
}

common::Status RedisOrderQueue::pop(::live::messaging::FlashSaleOrderMessage* message, std::chrono::milliseconds timeout) {
    RedisKVStore* consumer_redis = blocking_redis_ != nullptr ? blocking_redis_.get() : redis_;
    if (consumer_redis == nullptr || queue_key_.empty()) return common::Status::FailedPrecondition("Redis order queue is not configured");
    std::string payload;
    if (const auto status = consumer_redis->listBlockingPop(queue_key_, timeout, &payload); !status.ok()) return status;
    return ::live::messaging::FlashSaleOrderMessage::deserialize(payload, message);
}

std::size_t RedisOrderQueue::pending() const {
    if (redis_ == nullptr || queue_key_.empty()) return 0;
    std::size_t size = 0;
    return redis_->listLength(queue_key_, &size).ok() ? size : 0;
}

}  // namespace live::kv
