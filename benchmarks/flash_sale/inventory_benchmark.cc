#include "business/inventory/inventory_service.h"

#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

int main() {
    live::business::inventory::InventoryService inventory;
    inventory.addSku("sku-1", 100);
    std::atomic<int> success{0};
    const auto begin = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
    for (int worker = 0; worker < 32; ++worker) {
        workers.emplace_back([&, worker] {
            for (int i = worker; i < 10000; i += 32) {
                if (inventory.reserveStock("sku-1", 1, "benchmark-" + std::to_string(i)).ok()) {
                    success.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto& worker : workers) worker.join();
    const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - begin).count();
    live::business::inventory::StockSnapshot snapshot;
    inventory.queryStock("sku-1", &snapshot);
    std::cout << "requests=10000 success=" << success.load() << " available=" << snapshot.available
              << " reserved=" << snapshot.reserved << " elapsed_us=" << elapsed << '\n';
    return success == 100 && snapshot.available == 0 && snapshot.reserved == 100 ? 0 : 1;
}
