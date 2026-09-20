#include "cache/sharded_cache.h"

#include <atomic>

namespace live::cache {

ShardedCache::ShardedCache(std::size_t shard_count, std::size_t capacity_per_shard)
    : shards_(shard_count == 0 ? 1 : shard_count) {
    for (auto& shard : shards_) shard.capacity = capacity_per_shard == 0 ? 1 : capacity_per_shard;
}

bool ShardedCache::expired(const Entry& entry) {
    return entry.expires_at != std::chrono::steady_clock::time_point{} &&
           std::chrono::steady_clock::now() >= entry.expires_at;
}

ShardedCache::Shard& ShardedCache::shardFor(const std::string& key) const {
    return const_cast<Shard&>(shards_[hasher_(key) % shards_.size()]);
}

common::Status ShardedCache::get(const std::string& key, std::string* value) {
    if (value == nullptr || key.empty()) return common::Status::InvalidArgument("invalid cache get arguments");
    auto& shard = shardFor(key);
    std::lock_guard<std::mutex> lock(shard.mutex);
    const auto it = shard.entries.find(key);
    if (it == shard.entries.end() || expired(it->second.first)) {
        if (it != shard.entries.end()) {
            shard.lru.erase(it->second.second);
            shard.entries.erase(it);
        }
        misses_.fetch_add(1, std::memory_order_relaxed);
        return common::Status::NotFound("cache miss");
    }
    shard.lru.splice(shard.lru.begin(), shard.lru, it->second.second);
    *value = it->second.first.value;
    hits_.fetch_add(1, std::memory_order_relaxed);
    return common::Status::Ok();
}

common::Status ShardedCache::set(const std::string& key, const std::string& value,
                                 std::chrono::milliseconds ttl) {
    if (key.empty()) return common::Status::InvalidArgument("cache key must not be empty");
    auto& shard = shardFor(key);
    std::lock_guard<std::mutex> lock(shard.mutex);
    auto it = shard.entries.find(key);
    Entry entry{value};
    if (ttl.count() > 0) entry.expires_at = std::chrono::steady_clock::now() + ttl;
    if (it != shard.entries.end()) {
        it->second.first = std::move(entry);
        shard.lru.splice(shard.lru.begin(), shard.lru, it->second.second);
        return common::Status::Ok();
    }
    shard.lru.push_front(key);
    shard.entries.emplace(key, std::make_pair(std::move(entry), shard.lru.begin()));
    if (shard.entries.size() > shard.capacity) {
        const auto& old_key = shard.lru.back();
        shard.entries.erase(old_key);
        shard.lru.pop_back();
    }
    return common::Status::Ok();
}

common::Status ShardedCache::erase(const std::string& key) {
    auto& shard = shardFor(key);
    std::lock_guard<std::mutex> lock(shard.mutex);
    const auto it = shard.entries.find(key);
    if (it != shard.entries.end()) {
        shard.lru.erase(it->second.second);
        shard.entries.erase(it);
    }
    return common::Status::Ok();
}

common::Status ShardedCache::getOrLoad(const std::string& key, const Loader& loader, std::string* value,
                                       std::chrono::milliseconds ttl) {
    auto status = get(key, value);
    if (status.ok()) return status;
    if (!loader || value == nullptr) return common::Status::InvalidArgument("invalid cache loader");

    auto& shard = shardFor(key);
    std::shared_future<std::pair<common::Status, std::string>> future;
    std::shared_ptr<std::promise<std::pair<common::Status, std::string>>> promise;
    {
        std::lock_guard<std::mutex> lock(shard.mutex);
        const auto it = shard.inflight.find(key);
        if (it != shard.inflight.end()) {
            future = it->second;
            singleflight_.fetch_add(1, std::memory_order_relaxed);
        } else {
            promise = std::make_shared<std::promise<std::pair<common::Status, std::string>>>();
            future = promise->get_future().share();
            shard.inflight.emplace(key, future);
        }
    }

    if (!promise) {
        const auto result = future.get();
        if (result.first.ok()) *value = result.second;
        return result.first;
    }

    std::string loaded;
    const auto load_status = loader(&loaded);
    if (load_status.ok()) set(key, loaded, ttl);
    promise->set_value({load_status, loaded});
    {
        std::lock_guard<std::mutex> lock(shard.mutex);
        shard.inflight.erase(key);
    }
    if (load_status.ok()) *value = std::move(loaded);
    return load_status;
}

std::size_t ShardedCache::hitCount() const { return hits_.load(std::memory_order_relaxed); }
std::size_t ShardedCache::missCount() const { return misses_.load(std::memory_order_relaxed); }
std::size_t ShardedCache::singleflightCount() const { return singleflight_.load(std::memory_order_relaxed); }

}  // namespace live::cache
