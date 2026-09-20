#include "kv/redis_inventory_fast_path.h"
#include "kv/redis_kv_store.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

int main() {
    const std::string redis_host = std::getenv("ECOMMERCE_REDIS_TEST_HOST") == nullptr ? "127.0.0.1" : std::getenv("ECOMMERCE_REDIS_TEST_HOST");
    const std::uint16_t redis_port = static_cast<std::uint16_t>(std::getenv("ECOMMERCE_REDIS_TEST_PORT") == nullptr ? 6379 : std::strtoul(std::getenv("ECOMMERCE_REDIS_TEST_PORT"), nullptr, 10));
    const std::string sku = "redis-concurrency-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    live::kv::RedisKVStore setup;
    if (!setup.connect(redis_host, redis_port).ok()) return 2;
    live::business::inventory::RedisInventoryFastPath setup_inventory(&setup);
    if (!setup_inventory.initializeSku(sku, 100).ok()) return 1;

    std::atomic<int> successful{0};
    std::atomic<bool> failed{false};
    constexpr int kRequests = 1000;
    constexpr int kWorkers = 64;
    std::vector<std::thread> workers;
    for (int worker = 0; worker < kWorkers; ++worker) {
        workers.emplace_back([&, worker] {
            live::kv::RedisKVStore redis;
            if (!redis.connect(redis_host, redis_port).ok()) { failed = true; return; }
            live::business::inventory::RedisInventoryFastPath inventory(&redis);
            for (int request = worker; request < kRequests; request += kWorkers) {
                live::business::inventory::RedisInventoryResult result;
                const auto status = inventory.reserve(sku, 1, "operation-" + std::to_string(request), &result);
                if (!status.ok()) { failed = true; return; }
                if (result.decision == live::business::inventory::RedisInventoryDecision::kSuccess) ++successful;
            }
        });
    }
    for (auto& worker : workers) worker.join();
    live::business::inventory::StockSnapshot stock;
    if (failed.load() || successful.load() != 100 || !setup_inventory.queryStock(sku, &stock).ok() ||
        stock.available != 0 || stock.reserved != 100 || stock.sold != 0) {
        std::cerr << "Redis concurrency invariant failed: success=" << successful.load()
                  << " available=" << stock.available << " reserved=" << stock.reserved << '\n';
        return 1;
    }
    std::cout << "redis_concurrency_test passed: 1000 requests, 100 reservations\n";
    return 0;
}
