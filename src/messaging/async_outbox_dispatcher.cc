#include "messaging/async_outbox_dispatcher.h"

#include <algorithm>

namespace live::messaging {

AsyncOutboxDispatcher::AsyncOutboxDispatcher(FileOutbox* outbox, Publish publish,
                                             std::chrono::milliseconds retry_interval)
    : outbox_(outbox), publish_(std::move(publish)), retry_interval_(retry_interval), current_delay_(retry_interval) {
    if (retry_interval_ <= std::chrono::milliseconds::zero()) retry_interval_ = std::chrono::milliseconds(20);
    current_delay_ = retry_interval_;
}

AsyncOutboxDispatcher::~AsyncOutboxDispatcher() { stop(); }

common::Status AsyncOutboxDispatcher::start() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (outbox_ == nullptr || !publish_) return common::Status::InvalidArgument("invalid outbox dispatcher");
    if (started_) return common::Status::AlreadyExists("outbox dispatcher already started");
    started_ = true;
    stop_requested_ = false;
    wake_requested_ = false;
    current_delay_ = retry_interval_;
    worker_ = std::thread(&AsyncOutboxDispatcher::run, this);
    return common::Status::Ok();
}

void AsyncOutboxDispatcher::stop() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!started_) return;
        stop_requested_ = true;
    }
    condition_.notify_all();
    if (worker_.joinable()) worker_.join();
    std::lock_guard<std::mutex> lock(mutex_);
    started_ = false;
}

void AsyncOutboxDispatcher::wake() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        wake_requested_ = true;
    }
    condition_.notify_one();
}

std::size_t AsyncOutboxDispatcher::pending() const {
    return outbox_ == nullptr ? 0 : outbox_->size();
}

void AsyncOutboxDispatcher::run() {
    while (true) {
        std::unique_lock<std::mutex> lock(mutex_);
        condition_.wait_for(lock, current_delay_, [this] { return stop_requested_ || wake_requested_; });
        const bool stopping = stop_requested_;
        wake_requested_ = false;
        lock.unlock();

        // One final best-effort drain is attempted during shutdown. If the
        // publisher is unavailable, FileOutbox retains the records for the
        // next process start.
        bool delivered = true;
        if (outbox_ != nullptr && outbox_->size() > 0) {
            delivered = outbox_->drain(publish_).ok();
        }
        if (stopping) return;
        if (delivered) {
            current_delay_ = retry_interval_;
        } else {
            const auto doubled = current_delay_ * 2;
            current_delay_ = std::min(doubled, std::chrono::milliseconds(2000));
        }
    }
}

}  // namespace live::messaging
