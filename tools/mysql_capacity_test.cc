#include "storage/mysql/mysql_transaction_store.h"
#include "messaging/order_message.h"
#include "business/order/order_service.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace {

std::size_t envSize(const char* name, std::size_t fallback, std::size_t maximum) {
    const char* value = std::getenv(name);
    if (value == nullptr || *value == '\0') return fallback;
    try { const auto parsed = std::stoull(value); return parsed == 0 || parsed > maximum ? fallback : static_cast<std::size_t>(parsed); }
    catch (...) { return fallback; }
}

}  // namespace

int main() {
    const std::size_t workers = envSize("MYSQL_CAPACITY_WORKERS", 8, 128);
    const std::size_t requests = envSize("MYSQL_CAPACITY_REQUESTS", 1000, 1000000);
    const std::string host = std::getenv("ECOMMERCE_MYSQL_HOST") == nullptr ? "127.0.0.1" : std::getenv("ECOMMERCE_MYSQL_HOST");
    const std::uint16_t port = static_cast<std::uint16_t>(envSize("ECOMMERCE_MYSQL_PORT", 3306, 65535));
    const std::string user = std::getenv("ECOMMERCE_MYSQL_USER") == nullptr ? "ecommerce" : std::getenv("ECOMMERCE_MYSQL_USER");
    const std::string password = std::getenv("ECOMMERCE_MYSQL_PASSWORD") == nullptr ? "ecommerce" : std::getenv("ECOMMERCE_MYSQL_PASSWORD");
    const std::string database = std::getenv("ECOMMERCE_MYSQL_DATABASE") == nullptr ? "ecommerce" : std::getenv("ECOMMERCE_MYSQL_DATABASE");

    live::storage::mysql::MySqlTransactionStore store;
    if (!store.connect(host, port, user, password, database).ok()) {
        std::cerr << "mysql capacity test skipped: unable to connect\n";
        return 77;
    }
    const std::string sku = "capacity-sku-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const std::string run_id = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    if (!store.initializeSku(sku, static_cast<std::int64_t>(requests)).ok()) return 1;
    std::atomic<std::size_t> success{0};
    std::atomic<std::size_t> next{0};
    std::vector<std::int64_t> latency_us;
    std::map<std::string, std::size_t> failures;
    std::mutex latency_mutex;
    const auto begin = std::chrono::steady_clock::now();
    std::vector<std::thread> threads;
    for (std::size_t worker = 0; worker < workers; ++worker) {
        threads.emplace_back([&] {
            while (true) {
                const auto index = next.fetch_add(1);
                if (index >= requests) break;
                live::business::order::Order order;
                const auto started = std::chrono::steady_clock::now();
                live::messaging::FlashSaleOrderMessage message;
                message.promotion_id = "capacity-promotion";
                message.order_id = "capacity-order-" + run_id + "-" + std::to_string(index);
                message.idempotency_key = "capacity-idem-" + run_id + "-" + std::to_string(index);
                message.user_id = "capacity-user";
                message.sku_id = sku;
                message.quantity = 1;
                const auto status = store.persistQueuedOrder(message, &order);
                const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - started).count();
                { std::lock_guard<std::mutex> lock(latency_mutex); latency_us.push_back(elapsed); }
                if (status.ok()) ++success;
                else { std::lock_guard<std::mutex> lock(latency_mutex); ++failures[status.message()]; }
            }
        });
    }
    for (auto& thread : threads) thread.join();
    const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - begin).count();
    std::sort(latency_us.begin(), latency_us.end());
    auto percentile = [&](double ratio) { return latency_us.empty() ? 0LL : latency_us[static_cast<std::size_t>(ratio * (latency_us.size() - 1))]; };
    std::cout << "workers=" << workers << " requests=" << requests << " success=" << success.load()
              << " throughput_rps=" << (elapsed == 0.0 ? 0.0 : requests / elapsed)
              << " p50_us=" << percentile(0.50) << " p95_us=" << percentile(0.95)
              << " p99_us=" << percentile(0.99);
    for (const auto& [message, count] : failures) std::cout << " failure[" << message << "]=" << count;
    std::cout << '\n';
    return success == requests ? 0 : 1;
}
