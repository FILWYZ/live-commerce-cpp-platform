#pragma once

#include "common/error/status.h"

#include <cstdint>
#include <chrono>
#include <mutex>
#include <string>
#include <unordered_map>

namespace live::storage { class LocalKVEngine; }
namespace live::kv { class IKVStore; }

namespace live::business::user {

enum class UserRole { kCustomer, kMerchant, kAdmin };

struct User {
    std::string id;
    std::string username;
    std::string password_hash;
    UserRole role{UserRole::kCustomer};
    bool enabled{true};
};

struct Session {
    std::string token;
    User user;
    std::uint64_t expires_at_unix_seconds{0};
};

class UserService {
public:
    explicit UserService(::live::storage::LocalKVEngine* storage = nullptr,
                         std::chrono::seconds session_ttl = std::chrono::hours(24))
        : storage_(storage), session_ttl_(session_ttl <= std::chrono::seconds::zero()
                                             ? std::chrono::hours(24) : session_ttl) {}
    UserService(::live::storage::LocalKVEngine* storage, ::live::kv::IKVStore* session_store,
                std::chrono::seconds session_ttl = std::chrono::hours(24))
        : storage_(storage), session_store_(session_store), session_ttl_(session_ttl <= std::chrono::seconds::zero()
                                             ? std::chrono::hours(24) : session_ttl) {}

    common::Status restore();
    common::Status registerUser(std::string username, std::string password, UserRole role, User* user);
    common::Status login(const std::string& username, const std::string& password, Session* session);
    common::Status authenticate(const std::string& token, User* user) const;
    common::Status logout(const std::string& token);
    common::Status disable(const std::string& user_id);

private:
    static std::string serialize(const User& user);
    static bool deserialize(const std::string& value, User* user);
    common::Status persist(const User& user);

    mutable std::mutex mutex_;
    mutable std::unordered_map<std::string, User> users_by_id_;
    mutable std::unordered_map<std::string, std::string> id_by_username_;
    mutable std::unordered_map<std::string, std::string> user_id_by_token_;
    mutable std::unordered_map<std::string, std::uint64_t> token_expiry_by_token_;
    std::uint64_t next_user_id_{1};
    ::live::storage::LocalKVEngine* storage_{nullptr};
    ::live::kv::IKVStore* session_store_{nullptr};
    const std::chrono::seconds session_ttl_;
};

}  // namespace live::business::user
