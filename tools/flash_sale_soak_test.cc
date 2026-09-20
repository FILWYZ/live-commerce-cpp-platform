#include "business/flash_sale/flash_sale_order_consumer.h"
#include "business/flash_sale/flash_sale_order_service.h"
#include "business/inventory/inventory_gateway.h"
#include "business/inventory/inventory_service.h"
#include "business/order/order_service.h"
#include "business/promotion/promotion_service.h"
#include "messaging/order_queue.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <vector>

namespace {

class SoakRepository final : public live::business::flash_sale::IFlashSaleOrderRepository {
public:
    live::common::Status persistQueuedOrder(const live::messaging::FlashSaleOrderMessage& message,
                                            live::business::order::Order* order) override {
        if (order == nullptr) return live::common::Status::InvalidArgument("order output missing");
        *order = {message.order_id, message.idempotency_key, message.user_id, message.sku_id,
                  message.quantity, live::business::order::OrderState::kCreated};
        return live::common::Status::Ok();
    }
};

int configuredSeconds() {
    const char* value = std::getenv("FLASH_SOAK_SECONDS");
    if (value == nullptr) return 30;
    const int seconds = std::atoi(value);
    return seconds > 0 && seconds <= 300 ? seconds : 30;
}

}  // namespace

int main() {
    constexpr std::int64_t kInitialStock = 1000000;
    live::business::inventory::InventoryService inventory;
    if (!inventory.addSku("soak-sku", kInitialStock).ok()) return 1;
    live::business::inventory::LocalInventoryGateway inventory_gateway(&inventory);
    live::business::promotion::PromotionService promotions;
    if (!promotions.create({"soak-promo", "soak-sku", static_cast<std::size_t>(kInitialStock)}).ok()) return 1;
    if (!promotions.preheat("soak-promo").ok() || !promotions.start("soak-promo").ok()) return 1;
    live::messaging::InMemoryOrderQueue queue;
    live::business::flash_sale::FlashSaleOrderService service(promotions.admissionGate(), &inventory_gateway, &queue);
    SoakRepository repository;
    live::business::flash_sale::FlashSaleOrderConsumer consumer(
        &queue, nullptr, &repository, &inventory_gateway, promotions.admissionGate());
    if (!consumer.start().ok()) return 1;

    std::atomic<bool> running{true};
    std::atomic<std::uint64_t> submitted{0};
    std::atomic<std::uint64_t> queued{0};
    std::vector<std::thread> producers;
    for (int worker = 0; worker < 16; ++worker) {
        producers.emplace_back([&] {
            std::uint64_t sequence = 0;
            while (running.load(std::memory_order_relaxed)) {
                const auto id = submitted.fetch_add(1, std::memory_order_relaxed);
                live::business::flash_sale::FlashSaleSubmitResult result;
                const auto status = service.submit({"soak-promo", "soak-sku", "soak-user-" + std::to_string(id),
                                                    "soak-idem-" + std::to_string(id), 1}, &result);
                if (!status.ok()) {
                    if (status.code() != live::common::ErrorCode::kResourceExhausted) {
                        std::cerr << "producer error: " << status.message() << '\n';
                        std::abort();
                    }
                    continue;
                }
                if (result.code == live::business::flash_sale::SubmitCode::kQueued) ++queued;
                if (++sequence % 1024 == 0) std::this_thread::yield();
            }
        });
    }
    std::this_thread::sleep_for(std::chrono::seconds(configuredSeconds()));
    running.store(false, std::memory_order_relaxed);
    for (auto& producer : producers) producer.join();
    while (queue.pending() != 0) {
        const auto status = consumer.processOnce(std::chrono::milliseconds(100));
        if (!status.ok() && !status.isIncomplete()) {
            std::cerr << "consumer error: " << status.message() << '\n';
            std::abort();
        }
    }
    consumer.stop();

    live::business::inventory::StockSnapshot stock;
    if (!inventory.queryStock("soak-sku", &stock).ok() || stock.available < 0 || stock.reserved < 0 || stock.sold < 0 ||
        stock.available + stock.reserved + stock.sold != kInitialStock || stock.reserved != 0 || queue.pending() != 0 ||
        queued.load() != consumer.processed()) {
        std::cerr << "soak invariant failed: available=" << stock.available << " reserved=" << stock.reserved
                  << " sold=" << stock.sold << " queue=" << queue.pending() << " queued=" << queued.load()
                  << " processed=" << consumer.processed() << '\n';
        return 1;
    }
    std::cout << "flash_sale_soak_test passed: seconds=" << configuredSeconds()
              << " submitted=" << submitted.load() << " processed=" << consumer.processed() << '\n';
    return 0;
}
