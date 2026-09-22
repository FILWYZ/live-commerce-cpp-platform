#pragma once

#include "messaging/outbox.h"

#include <chrono>
#include <condition_variable>
#include <functional>
#include <cstddef>
#include <mutex>
#include <thread>

namespace live::messaging {

// Moves event delivery out of the request thread while keeping FileOutbox as
// the durable source of pending events. Failed records remain on disk and are
// retried on the next wakeup or after process restart.
class AsyncOutboxDispatcher {
public:
    using Publish = std::function<common::Status(const Event&)>;

    AsyncOutboxDispatcher(FileOutbox* outbox, Publish publish,
                          std::chrono::milliseconds retry_interval = std::chrono::milliseconds(20),
                          std::size_t batch_size = 128);
    ~AsyncOutboxDispatcher();

    AsyncOutboxDispatcher(const AsyncOutboxDispatcher&) = delete;
    AsyncOutboxDispatcher& operator=(const AsyncOutboxDispatcher&) = delete;

    common::Status start();
    void stop();
    void wake();
    std::size_t pending() const;

private:
    void run();

    FileOutbox* outbox_;
    Publish publish_;
    std::chrono::milliseconds retry_interval_;
    std::chrono::milliseconds current_delay_;
    const std::size_t batch_size_;
    mutable std::mutex mutex_;
    std::condition_variable condition_;
    bool started_{false};
    bool stop_requested_{false};
    bool wake_requested_{false};
    std::thread worker_;
};

}  // namespace live::messaging
