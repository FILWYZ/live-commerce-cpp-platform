#pragma once

#include <chrono>
#include <algorithm>
#include <mutex>

namespace live::traffic {

class TokenBucket {
public:
    TokenBucket(double rate_per_second, double burst)
        : rate_per_second_(rate_per_second), capacity_(burst), tokens_(burst), last_refill_(Clock::now()) {}

    bool tryAcquire(double tokens = 1.0) {
        std::lock_guard<std::mutex> lock(mutex_);
        refill();
        if (tokens <= 0 || tokens > tokens_) return false;
        tokens_ -= tokens;
        return true;
    }

private:
    using Clock = std::chrono::steady_clock;
    void refill() {
        const auto now = Clock::now();
        const double seconds = std::chrono::duration<double>(now - last_refill_).count();
        tokens_ = std::min(capacity_, tokens_ + seconds * rate_per_second_);
        last_refill_ = now;
    }

    double rate_per_second_;
    double capacity_;
    double tokens_;
    Clock::time_point last_refill_;
    std::mutex mutex_;
};

}  // namespace live::traffic
