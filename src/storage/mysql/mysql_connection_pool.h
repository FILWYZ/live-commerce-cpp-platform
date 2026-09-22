#pragma once

#include "common/error/status.h"

#include <mysql/mysql.h>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

namespace live::storage::mysql {

class MySqlConnectionPool {
public:
    class Lease {
    public:
        Lease() = default;
        Lease(MySqlConnectionPool* pool, MYSQL* connection) : pool_(pool), connection_(connection) {}
        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;
        Lease(Lease&& other) noexcept;
        Lease& operator=(Lease&& other) noexcept;
        ~Lease();

        MYSQL* get() const { return connection_; }
        explicit operator bool() const { return connection_ != nullptr; }

    private:
        MySqlConnectionPool* pool_{nullptr};
        MYSQL* connection_{nullptr};
    };

    MySqlConnectionPool() = default;
    ~MySqlConnectionPool();

    common::Status connect(const std::vector<std::string>& hosts, std::uint16_t port,
                           const std::string& user, const std::string& password,
                           const std::string& database, std::size_t pool_size);
    void close();
    Lease acquire(std::chrono::milliseconds timeout = std::chrono::seconds(3));
    bool connected() const;
    std::string activeEndpoint() const;
    std::size_t size() const;

private:
    void release(MYSQL* connection);

    mutable std::mutex mutex_;
    std::condition_variable condition_;
    std::vector<MYSQL*> idle_;
    std::vector<MYSQL*> all_;
    std::string active_endpoint_;
    std::size_t in_use_{0};
    bool closing_{false};
};

}  // namespace live::storage::mysql
