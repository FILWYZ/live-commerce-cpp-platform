#include "business/flash_sale/flash_sale_order_consumer.h"
#include "business/flash_sale/flash_sale_order_service.h"
#include "business/inventory/inventory_gateway.h"
#include "business/inventory/inventory_service.h"
#include "business/order/order_service.h"
#include "business/promotion/promotion_service.h"
#include "messaging/order_queue.h"
#include "storage/local_engine/local_kv_engine.h"

#include <atomic>
#include <cassert>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {

void check(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

class FakeRepository final : public live::business::flash_sale::IFlashSaleOrderRepository {
public:
    live::common::Status persistQueuedOrder(const live::messaging::FlashSaleOrderMessage& message,
                                            live::business::order::Order* order) override {
        if (order == nullptr) return live::common::Status::InvalidArgument("order output missing");
        ++persisted;
        *order = {message.order_id, message.idempotency_key, message.user_id, message.sku_id,
                  message.quantity, live::business::order::OrderState::kCreated};
        return live::common::Status::Ok();
    }
    std::atomic<int> persisted{0};
};

class FailingQueue final : public live::messaging::IOrderQueue {
public:
    live::common::Status push(const live::messaging::FlashSaleOrderMessage&) override {
        return live::common::Status::Internal("injected queue failure");
    }
    live::common::Status pop(live::messaging::FlashSaleOrderMessage*, std::chrono::milliseconds) override {
        return live::common::Status::Incomplete("empty");
    }
    std::size_t pending() const override { return 0; }
};

}  // namespace

int main() {
    const auto root = std::filesystem::temp_directory_path() / "flash_sale_pipeline_test";
    std::filesystem::remove_all(root);
    std::filesystem::create_directories(root);
    live::storage::LocalKVEngine storage;
    check(storage.open((root / "snapshot").string(), (root / "wal").string()).ok(), "storage open");
    live::business::inventory::InventoryService inventory(&storage);
    check(inventory.addSku("flash-sku", 100).ok(), "inventory setup");
    live::business::inventory::LocalInventoryGateway inventory_gateway(&inventory);

    live::business::promotion::PromotionService promotion;
    check(promotion.create({"flash-1", "flash-sku", 100}).ok(), "promotion setup");
    check(promotion.preheat("flash-1").ok(), "promotion preheat");
    check(promotion.start("flash-1").ok(), "promotion start");

    live::messaging::InMemoryOrderQueue queue;
    live::business::flash_sale::FlashSaleOrderService service(promotion.admissionGate(), &inventory_gateway, &queue);
    std::atomic<int> queued{0};
    std::vector<std::thread> workers;
    for (int i = 0; i < 1000; ++i) {
        workers.emplace_back([&, i] {
            live::business::flash_sale::FlashSaleSubmitResult result;
            const auto status = service.submit({"flash-1", "flash-sku", "user-" + std::to_string(i),
                                                "idem-" + std::to_string(i), 1}, &result);
            check(status.ok(), "flash-sale submit");
            if (result.code == live::business::flash_sale::SubmitCode::kQueued) ++queued;
        });
    }
    for (auto& worker : workers) worker.join();
    check(queued.load() == 100, "flash-sale quota");
    check(queue.pending() == 100, "flash-sale queue size");

    FakeRepository repository;
    live::business::flash_sale::FlashSaleOrderConsumer consumer(
        &queue, nullptr, &repository, &inventory_gateway, promotion.admissionGate());
    while (queue.pending() != 0) check(consumer.processOnce(std::chrono::milliseconds(10)).ok(), "consumer process");
    live::business::inventory::StockSnapshot stock;
    check(inventory.queryStock("flash-sku", &stock).ok(), "stock query");
    check(stock.available == 0 && stock.reserved == 0 && stock.sold == 100, "stock invariant");
    check(repository.persisted.load() == 100, "consumer persistence");

    live::business::promotion::PromotionService compensation_promotion;
    check(compensation_promotion.create({"flash-2", "flash-sku", 1}).ok(), "compensation promotion setup");
    check(compensation_promotion.preheat("flash-2").ok(), "compensation promotion preheat");
    check(compensation_promotion.start("flash-2").ok(), "compensation promotion start");
    FailingQueue failing_queue;
    live::business::flash_sale::FlashSaleOrderService failing_service(
        compensation_promotion.admissionGate(), &inventory_gateway, &failing_queue);
    live::business::flash_sale::FlashSaleSubmitResult failed;
    const auto status = failing_service.submit({"flash-2", "flash-sku", "rollback-user", "rollback-idem", 1}, &failed);
    check(!status.ok(), "queue failure should be visible");
    check(inventory.queryStock("flash-sku", &stock).ok(), "compensated stock query");
    check(stock.available == 0 && stock.reserved == 0 && stock.sold == 100, "compensated stock invariant");
    check(inventory.validateInvariants().ok(), "compensated inventory invariant");

    std::filesystem::remove_all(root);
    std::cout << "flash_sale_pipeline_test passed\n";
    return 0;
}
