#pragma once

#include "common/error/status.h"

#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace live::kv {

class IKVStore {
public:
    virtual ~IKVStore() = default;
    virtual common::Status get(const std::string& key, std::string* value) = 0;
    virtual common::Status set(const std::string& key, const std::string& value,
                               std::chrono::milliseconds ttl = std::chrono::milliseconds::zero()) = 0;
    virtual common::Status del(const std::string& key) = 0;
    virtual common::Status cas(const std::string& key, const std::optional<std::string>& expected,
                               const std::string& value, bool* updated,
                               std::chrono::milliseconds ttl = std::chrono::milliseconds::zero()) = 0;
    virtual common::Status expire(const std::string& key, std::chrono::milliseconds ttl) = 0;
};

class InMemoryKVStore final : public IKVStore {
public:
    common::Status get(const std::string& key, std::string* value) override;
    common::Status set(const std::string& key, const std::string& value,
                       std::chrono::milliseconds ttl = std::chrono::milliseconds::zero()) override;
    common::Status del(const std::string& key) override;
    common::Status cas(const std::string& key, const std::optional<std::string>& expected,
                       const std::string& value, bool* updated,
                       std::chrono::milliseconds ttl = std::chrono::milliseconds::zero()) override;
    common::Status expire(const std::string& key, std::chrono::milliseconds ttl) override;

private:
    struct Entry {
        std::string value;
        std::chrono::steady_clock::time_point expires_at{};
    };

    static bool expired(const Entry& entry);
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, Entry> entries_;
};

class KVClient {
public:
    explicit KVClient(std::shared_ptr<IKVStore> store) : store_(std::move(store)) {}

    common::Status get(const std::string& key, std::string* value) { return store_->get(key, value); }
    common::Status set(const std::string& key, const std::string& value,
                       std::chrono::milliseconds ttl = std::chrono::milliseconds::zero()) {
        return store_->set(key, value, ttl);
    }
    common::Status del(const std::string& key) { return store_->del(key); }
    common::Status cas(const std::string& key, const std::optional<std::string>& expected,
                       const std::string& value, bool* updated,
                       std::chrono::milliseconds ttl = std::chrono::milliseconds::zero()) {
        return store_->cas(key, expected, value, updated, ttl);
    }
    common::Status expire(const std::string& key, std::chrono::milliseconds ttl) {
        return store_->expire(key, ttl);
    }

private:
    std::shared_ptr<IKVStore> store_;
};

}  // namespace live::kv
