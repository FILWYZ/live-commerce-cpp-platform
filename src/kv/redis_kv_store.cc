#include "kv/redis_kv_store.h"

#include <hiredis/hiredis.h>

#include <algorithm>
#include <cstring>
#include <cmath>
#include <sstream>
#include <string_view>

namespace live::kv {
namespace {

common::Status redisError(redisContext* context) {
    return common::Status::Internal(context == nullptr ? "Redis is not connected" : context->errstr);
}

}  // namespace

RedisKVStore::~RedisKVStore() { close(); }

common::Status RedisKVStore::connect(const std::string& host, std::uint16_t port,
                                     std::chrono::milliseconds timeout) {
    std::lock_guard<std::mutex> lock(mutex_);
    close();
    timeval tv{};
    tv.tv_sec = static_cast<long>(timeout.count() / 1000);
    tv.tv_usec = static_cast<long>((timeout.count() % 1000) * 1000);
    context_ = redisConnectWithTimeout(host.c_str(), port, tv);
    if (context_ == nullptr || context_->err != 0) {
        const auto status = redisError(context_);
        close();
        return status;
    }
    return common::Status::Ok();
}

void RedisKVStore::close() {
    if (context_ != nullptr) {
        redisFree(context_);
        context_ = nullptr;
    }
    script_shas_.clear();
}

common::Status RedisKVStore::ensureConnected() const {
    return context_ == nullptr ? common::Status::FailedPrecondition("Redis is not connected") : common::Status::Ok();
}

common::Status RedisKVStore::get(const std::string& key, std::string* value) {
    if (key.empty() || value == nullptr) return common::Status::InvalidArgument("invalid Redis GET");
    std::lock_guard<std::mutex> lock(mutex_);
    if (const auto status = ensureConnected(); !status.ok()) return status;
    auto* reply = static_cast<redisReply*>(redisCommand(context_, "GET %b", key.data(), key.size()));
    if (reply == nullptr) return redisError(context_);
    common::Status status = common::Status::Ok();
    if (reply->type == REDIS_REPLY_NIL) status = common::Status::NotFound("key not found");
    else if (reply->type != REDIS_REPLY_STRING) status = common::Status::Internal("unexpected Redis GET reply");
    else *value = std::string(reply->str, reply->len);
    freeReplyObject(reply);
    return status;
}

common::Status RedisKVStore::set(const std::string& key, const std::string& value,
                                 std::chrono::milliseconds ttl) {
    if (key.empty()) return common::Status::InvalidArgument("invalid Redis SET");
    std::lock_guard<std::mutex> lock(mutex_);
    if (const auto status = ensureConnected(); !status.ok()) return status;
    redisReply* reply = nullptr;
    if (ttl.count() > 0) reply = static_cast<redisReply*>(redisCommand(context_, "PSETEX %b %lld %b", key.data(), key.size(), static_cast<long long>(ttl.count()), value.data(), value.size()));
    else reply = static_cast<redisReply*>(redisCommand(context_, "SET %b %b", key.data(), key.size(), value.data(), value.size()));
    if (reply == nullptr) return redisError(context_);
    const bool ok = reply->type == REDIS_REPLY_STATUS && std::string(reply->str, reply->len) == "OK";
    freeReplyObject(reply);
    return ok ? common::Status::Ok() : common::Status::Internal("Redis SET failed");
}

common::Status RedisKVStore::del(const std::string& key) {
    if (key.empty()) return common::Status::InvalidArgument("invalid Redis DEL");
    std::lock_guard<std::mutex> lock(mutex_);
    if (const auto status = ensureConnected(); !status.ok()) return status;
    auto* reply = static_cast<redisReply*>(redisCommand(context_, "DEL %b", key.data(), key.size()));
    if (reply == nullptr) return redisError(context_);
    const bool ok = reply->type == REDIS_REPLY_INTEGER;
    freeReplyObject(reply);
    return ok ? common::Status::Ok() : common::Status::Internal("Redis DEL failed");
}

common::Status RedisKVStore::cas(const std::string& key, const std::optional<std::string>& expected,
                                 const std::string& value, bool* updated,
                                 std::chrono::milliseconds ttl) {
    if (key.empty() || updated == nullptr) return common::Status::InvalidArgument("invalid Redis CAS");
    std::lock_guard<std::mutex> lock(mutex_);
    if (const auto status = ensureConnected(); !status.ok()) return status;
    static const char* script =
        "local c=redis.call('get',KEYS[1]); "
        "if ARGV[1]=='__MISSING__' then if c then return 0 end "
        "elseif not c or c~=ARGV[1] then return 0 end; "
        "if tonumber(ARGV[2])>0 then redis.call('psetex',KEYS[1],ARGV[2],ARGV[3]) "
        "else redis.call('set',KEYS[1],ARGV[3]) end; return 1";
    const std::string expected_value = expected.has_value() ? *expected : "__MISSING__";
    const std::string ttl_value = std::to_string(std::max<std::int64_t>(0, ttl.count()));
    const char* argv[] = {"EVAL", script, "1", key.data(), expected_value.data(), ttl_value.data(), value.data()};
    std::size_t lengths[] = {4, std::strlen(script), 1, key.size(), expected_value.size(), ttl_value.size(), value.size()};
    auto* reply = static_cast<redisReply*>(redisCommandArgv(context_, 7, argv, lengths));
    if (reply == nullptr) return redisError(context_);
    *updated = reply->type == REDIS_REPLY_INTEGER && reply->integer == 1;
    freeReplyObject(reply);
    return common::Status::Ok();
}

common::Status RedisKVStore::expire(const std::string& key, std::chrono::milliseconds ttl) {
    if (key.empty() || ttl.count() <= 0) return common::Status::InvalidArgument("invalid Redis EXPIRE");
    std::lock_guard<std::mutex> lock(mutex_);
    if (const auto status = ensureConnected(); !status.ok()) return status;
    auto* reply = static_cast<redisReply*>(redisCommand(context_, "PEXPIRE %b %lld", key.data(), key.size(), static_cast<long long>(ttl.count())));
    if (reply == nullptr) return redisError(context_);
    const bool ok = reply->type == REDIS_REPLY_INTEGER && reply->integer == 1;
    freeReplyObject(reply);
    return ok ? common::Status::Ok() : common::Status::NotFound("key not found");
}

common::Status RedisKVStore::evalInteger(const std::string& script, const std::vector<std::string>& keys,
                                          const std::vector<std::string>& arguments, std::int64_t* result) {
    if (script.empty() || result == nullptr) return common::Status::InvalidArgument("invalid Redis script arguments");
    std::lock_guard<std::mutex> lock(mutex_);
    if (const auto status = ensureConnected(); !status.ok()) return status;

    for (int attempt = 0; attempt < 2; ++attempt) {
        auto script_it = script_shas_.find(script);
        if (script_it == script_shas_.end()) {
            auto* load_reply = static_cast<redisReply*>(redisCommand(context_, "SCRIPT LOAD %b", script.data(), script.size()));
            if (load_reply == nullptr) return redisError(context_);
            if (load_reply->type != REDIS_REPLY_STRING || load_reply->str == nullptr) {
                const std::string message = load_reply->str == nullptr ? "Redis SCRIPT LOAD failed" : load_reply->str;
                freeReplyObject(load_reply);
                return common::Status::Internal(message);
            }
            script_it = script_shas_.emplace(script, std::string(load_reply->str, load_reply->len)).first;
            freeReplyObject(load_reply);
        }

        std::vector<std::string> command_storage;
        command_storage.reserve(3 + keys.size() + arguments.size());
        command_storage.emplace_back("EVALSHA");
        command_storage.push_back(script_it->second);
        command_storage.push_back(std::to_string(keys.size()));
        command_storage.insert(command_storage.end(), keys.begin(), keys.end());
        command_storage.insert(command_storage.end(), arguments.begin(), arguments.end());

        std::vector<const char*> argv;
        std::vector<std::size_t> lengths;
        argv.reserve(command_storage.size());
        lengths.reserve(command_storage.size());
        for (const auto& item : command_storage) {
            argv.push_back(item.data());
            lengths.push_back(item.size());
        }
        auto* reply = static_cast<redisReply*>(redisCommandArgv(context_, static_cast<int>(argv.size()), argv.data(), lengths.data()));
        if (reply == nullptr) return redisError(context_);
        if (reply->type == REDIS_REPLY_ERROR && reply->str != nullptr &&
            std::string_view(reply->str, reply->len).find("NOSCRIPT") != std::string_view::npos) {
            script_shas_.erase(script);
            freeReplyObject(reply);
            continue;
        }
        if (reply->type != REDIS_REPLY_INTEGER) {
            const std::string message = reply->str == nullptr ? "Redis script did not return integer" : reply->str;
            freeReplyObject(reply);
            return common::Status::Internal(message);
        }
        *result = reply->integer;
        freeReplyObject(reply);
        return common::Status::Ok();
    }
    return common::Status::Internal("Redis script disappeared while executing");
}

common::Status RedisKVStore::hashGet(const std::string& key, const std::string& field, std::string* value) {
    if (key.empty() || field.empty() || value == nullptr) return common::Status::InvalidArgument("invalid Redis HGET");
    std::lock_guard<std::mutex> lock(mutex_);
    if (const auto status = ensureConnected(); !status.ok()) return status;
    auto* reply = static_cast<redisReply*>(redisCommand(context_, "HGET %b %b", key.data(), key.size(), field.data(), field.size()));
    if (reply == nullptr) return redisError(context_);
    common::Status status = common::Status::Ok();
    if (reply->type == REDIS_REPLY_NIL) status = common::Status::NotFound("hash field not found");
    else if (reply->type != REDIS_REPLY_STRING) status = common::Status::Internal("unexpected Redis HGET reply");
    else *value = std::string(reply->str, reply->len);
    freeReplyObject(reply);
    return status;
}

common::Status RedisKVStore::hashSet(const std::string& key, const std::string& field, const std::string& value) {
    if (key.empty() || field.empty()) return common::Status::InvalidArgument("invalid Redis HSET");
    std::lock_guard<std::mutex> lock(mutex_);
    if (const auto status = ensureConnected(); !status.ok()) return status;
    auto* reply = static_cast<redisReply*>(redisCommand(context_, "HSET %b %b %b",
                                                        key.data(), key.size(), field.data(), field.size(), value.data(), value.size()));
    if (reply == nullptr) return redisError(context_);
    const bool ok = reply->type == REDIS_REPLY_INTEGER;
    freeReplyObject(reply);
    return ok ? common::Status::Ok() : common::Status::Internal("Redis HSET failed");
}

common::Status RedisKVStore::listPush(const std::string& key, const std::string& value) {
    if (key.empty() || value.empty()) return common::Status::InvalidArgument("invalid Redis LPUSH");
    std::lock_guard<std::mutex> lock(mutex_);
    if (const auto status = ensureConnected(); !status.ok()) return status;
    auto* reply = static_cast<redisReply*>(redisCommand(context_, "LPUSH %b %b",
                                                        key.data(), key.size(), value.data(), value.size()));
    if (reply == nullptr) return redisError(context_);
    const bool ok = reply->type == REDIS_REPLY_INTEGER;
    freeReplyObject(reply);
    return ok ? common::Status::Ok() : common::Status::Internal("Redis LPUSH failed");
}

common::Status RedisKVStore::listBlockingPop(const std::string& key, std::chrono::milliseconds timeout,
                                             std::string* value) {
    if (key.empty() || value == nullptr || timeout.count() < 0) return common::Status::InvalidArgument("invalid Redis BRPOP");
    std::lock_guard<std::mutex> lock(mutex_);
    if (const auto status = ensureConnected(); !status.ok()) return status;
    const long long seconds = timeout.count() == 0 ? 0 : std::max<long long>(1, (timeout.count() + 999) / 1000);
    auto* reply = static_cast<redisReply*>(redisCommand(context_, "BRPOP %b %lld", key.data(), key.size(), seconds));
    if (reply == nullptr) return redisError(context_);
    common::Status status = common::Status::Ok();
    if (reply->type == REDIS_REPLY_NIL) status = common::Status::Incomplete("Redis BRPOP timed out");
    else if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 2 || reply->element[1]->type != REDIS_REPLY_STRING) {
        status = common::Status::Internal("unexpected Redis BRPOP reply");
    } else {
        *value = std::string(reply->element[1]->str, reply->element[1]->len);
    }
    freeReplyObject(reply);
    return status;
}

common::Status RedisKVStore::listLength(const std::string& key, std::size_t* length) {
    if (key.empty() || length == nullptr) return common::Status::InvalidArgument("invalid Redis LLEN");
    std::lock_guard<std::mutex> lock(mutex_);
    if (const auto status = ensureConnected(); !status.ok()) return status;
    auto* reply = static_cast<redisReply*>(redisCommand(context_, "LLEN %b", key.data(), key.size()));
    if (reply == nullptr) return redisError(context_);
    if (reply->type != REDIS_REPLY_INTEGER || reply->integer < 0) {
        freeReplyObject(reply);
        return common::Status::Internal("unexpected Redis LLEN reply");
    }
    *length = static_cast<std::size_t>(reply->integer);
    freeReplyObject(reply);
    return common::Status::Ok();
}

}  // namespace live::kv
