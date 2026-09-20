#include "business/inventory/inventory_service.h"

#include <atomic>
#include <cstdio>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

void check(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

void testInventoryDoesNotOversell() {
    live::business::inventory::InventoryService inventory;
    check(inventory.addSku("hot-sku", 200).ok(), "inventory setup");
    std::atomic<int> success{0};
    std::vector<std::thread> workers;
    for (int worker = 0; worker < 32; ++worker) {
        workers.emplace_back([&inventory, &success, worker] {
            for (int i = worker; i < 4000; i += 32) {
                if (inventory.reserveStock("hot-sku", 1, "reserve-" + std::to_string(i)).ok()) {
                    success.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto& worker : workers) worker.join();
    live::business::inventory::StockSnapshot stock;
    check(inventory.queryStock("hot-sku", &stock).ok(), "inventory query");
    check(success.load() == 200, "inventory accepted more than total stock");
    check(stock.available == 0 && stock.reserved == 200 && stock.sold == 0, "inventory state mismatch");
    check(inventory.validateInvariants().ok(), "inventory invariant failure");
}

}  // namespace

int main() {
    try {
        testInventoryDoesNotOversell();
        return 0;
    } catch (const std::exception& error) {
        std::fprintf(stderr, "commerce_concurrency_test failed: %s\n", error.what());
        return 1;
    }
}
