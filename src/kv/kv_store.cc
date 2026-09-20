#include "kv/kv_store.h"

namespace live::kv {

bool InMemoryKVStore::expired(const Entry& entry) {
    return entry.expires_at != std::chrono::steady_clock::time_point{} &&
           std::chrono::steady_clock::now() >= entry.expires_at;
}

common::Status InMemoryKVStore::get(const std::string& key, std::string* value) {
    if (value == nullptr || key.empty()) {
        return common::Status::InvalidArgument("key and value must be valid");
    }
    std::unique_lock<std::shared_mutex> lock(mutex_);
    const auto it = entries_.find(key);
    if (it == entries_.end() || expired(it->second)) {
        if (it != entries_.end()) entries_.erase(it);
        return common::Status::NotFound("key not found");
    }
    *value = it->second.value;
    return common::Status::Ok();
}

common::Status InMemoryKVStore::set(const std::string& key, const std::string& value,
                                    std::chrono::milliseconds ttl) {
    if (key.empty()) return common::Status::InvalidArgument("key must not be empty");
    std::unique_lock<std::shared_mutex> lock(mutex_);
    Entry entry{value};
    if (ttl.count() > 0) entry.expires_at = std::chrono::steady_clock::now() + ttl;
    entries_[key] = std::move(entry);
    return common::Status::Ok();
}

common::Status InMemoryKVStore::del(const std::string& key) {
    if (key.empty()) return common::Status::InvalidArgument("key must not be empty");
    std::unique_lock<std::shared_mutex> lock(mutex_);
    entries_.erase(key);
    return common::Status::Ok();
}

common::Status InMemoryKVStore::cas(const std::string& key, const std::optional<std::string>& expected,
                                    const std::string& value, bool* updated,
                                    std::chrono::milliseconds ttl) {
    if (updated == nullptr || key.empty()) return common::Status::InvalidArgument("invalid CAS arguments");
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = entries_.find(key);
    if (it != entries_.end() && expired(it->second)) it = entries_.erase(it);
    const bool matches = expected.has_value() ? (it != entries_.end() && it->second.value == *expected)
                                              : (it == entries_.end());
    *updated = matches;
    if (!matches) return common::Status::Ok();
    Entry entry{value};
    if (ttl.count() > 0) entry.expires_at = std::chrono::steady_clock::now() + ttl;
    entries_[key] = std::move(entry);
    return common::Status::Ok();
}

common::Status InMemoryKVStore::expire(const std::string& key, std::chrono::milliseconds ttl) {
    if (key.empty() || ttl.count() <= 0) return common::Status::InvalidArgument("invalid expire arguments");
    std::unique_lock<std::shared_mutex> lock(mutex_);
    const auto it = entries_.find(key);
    if (it == entries_.end() || expired(it->second)) return common::Status::NotFound("key not found");
    it->second.expires_at = std::chrono::steady_clock::now() + ttl;
    return common::Status::Ok();
}

}  // namespace live::kv
