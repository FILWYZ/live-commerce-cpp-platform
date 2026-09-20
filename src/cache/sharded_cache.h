#pragma once

#include "common/error/status.h"

#include <chrono>
#include <atomic>
#include <functional>
#include <future>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace live::cache {

class ShardedCache {
public:
    using Loader = std::function<common::Status(std::string*)>;

    explicit ShardedCache(std::size_t shard_count = 16, std::size_t capacity_per_shard = 1024);

    common::Status get(const std::string& key, std::string* value);
    common::Status set(const std::string& key, const std::string& value,
                       std::chrono::milliseconds ttl = std::chrono::milliseconds::zero());
    common::Status erase(const std::string& key);
    common::Status getOrLoad(const std::string& key, const Loader& loader, std::string* value,
                             std::chrono::milliseconds ttl = std::chrono::milliseconds::zero());

    std::size_t hitCount() const;
    std::size_t missCount() const;
    std::size_t singleflightCount() const;

private:
    struct Entry { std::string value; std::chrono::steady_clock::time_point expires_at{}; };
    struct Shard {
        mutable std::mutex mutex;
        std::size_t capacity{0};
        std::list<std::string> lru;
        std::unordered_map<std::string, std::pair<Entry, std::list<std::string>::iterator>> entries;
        std::unordered_map<std::string, std::shared_future<std::pair<common::Status, std::string>>> inflight;
    };

    Shard& shardFor(const std::string& key) const;
    static bool expired(const Entry& entry);
    std::vector<Shard> shards_;
    mutable std::hash<std::string> hasher_;
    std::atomic<std::size_t> hits_{0};
    std::atomic<std::size_t> misses_{0};
    std::atomic<std::size_t> singleflight_{0};
};

}  // namespace live::cache
