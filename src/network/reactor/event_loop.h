#pragma once

#include <atomic>
#include <functional>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

namespace live::network {

class Channel;

class EventLoop {
public:
    using Functor = std::function<void()>;

    EventLoop();
    ~EventLoop();

    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    void loop();
    void quit();
    void runInLoop(Functor callback);

    bool isInLoopThread() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return thread_id_ == std::this_thread::get_id();
    }
    void updateChannel(Channel* channel);
    void removeChannel(Channel* channel);

private:
    void wakeup();
    void handleWakeup();
    void doPendingFunctors();

    std::thread::id thread_id_;
    std::atomic<bool> quit_{false};
    int poller_fd_{-1};
    int wakeup_read_fd_{-1};
    int wakeup_write_fd_{-1};
    std::unordered_map<int, Channel*> channels_;
    mutable std::mutex mutex_;
    std::vector<Functor> pending_functors_;
};

}  // namespace live::network
