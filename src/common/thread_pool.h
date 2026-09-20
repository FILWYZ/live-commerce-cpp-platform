#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace live::common {

// A bounded worker pool for blocking business work. The queue limit is
// intentional: overload is rejected at the gateway instead of allowing an
// unbounded request backlog to exhaust memory.
class ThreadPool {
public:
    ThreadPool(std::size_t worker_count, std::size_t queue_capacity)
        : queue_capacity_(queue_capacity == 0 ? 1 : queue_capacity) {
        worker_count = worker_count == 0 ? 1 : worker_count;
        workers_.reserve(worker_count);
        for (std::size_t i = 0; i < worker_count; ++i) workers_.emplace_back([this] { run(); });
    }

    ~ThreadPool() { stop(); }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    bool submit(std::function<void()> task) {
        if (!task) return false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopping_ || tasks_.size() >= queue_capacity_) return false;
            tasks_.push_back(std::move(task));
        }
        condition_.notify_one();
        return true;
    }

    void stop() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopping_) return;
            stopping_ = true;
        }
        condition_.notify_all();
        for (auto& worker : workers_) {
            if (worker.joinable()) worker.join();
        }
        workers_.clear();
    }

    std::size_t pending() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return tasks_.size();
    }

private:
    void run() {
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                condition_.wait(lock, [this] { return stopping_ || !tasks_.empty(); });
                if (tasks_.empty() && stopping_) return;
                task = std::move(tasks_.front());
                tasks_.pop_front();
            }
            task();
        }
    }

    const std::size_t queue_capacity_;
    mutable std::mutex mutex_;
    std::condition_variable condition_;
    std::deque<std::function<void()>> tasks_;
    bool stopping_{false};
    std::vector<std::thread> workers_;
};

}  // namespace live::common
