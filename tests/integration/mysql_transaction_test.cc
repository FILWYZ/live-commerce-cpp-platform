#include "business/inventory/inventory_service.h"
#include "business/order/order_service.h"
#include "messaging/order_message.h"
#include "storage/mysql/mysql_transaction_store.h"

#include <cstdio>
#include <cstdlib>
#include <chrono>
#include <stdexcept>

namespace {

void check(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

const char* envOr(const char* name, const char* fallback) {
    const char* value = std::getenv(name);
    return value == nullptr || *value == '\0' ? fallback : value;
}

}  // namespace

int main() {
    const char* host = std::getenv("ECOMMERCE_MYSQL_TEST_HOST");
    if (host == nullptr || *host == '\0') {
        std::puts("mysql_transaction_test skipped: ECOMMERCE_MYSQL_TEST_HOST is not configured");
        return 0;
    }
    try {
        const auto suffix = std::to_string(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
        const std::string sku_id = "integration-sku-" + suffix;
        live::storage::mysql::MySqlTransactionStore store;
        check(store.connect(host, static_cast<std::uint16_t>(std::strtoul(envOr("ECOMMERCE_MYSQL_TEST_PORT", "3306"), nullptr, 10)),
                            envOr("ECOMMERCE_MYSQL_TEST_USER", "ecommerce"),
                            envOr("ECOMMERCE_MYSQL_TEST_PASSWORD", "ecommerce"),
                            envOr("ECOMMERCE_MYSQL_TEST_DATABASE", "ecommerce")).ok(), "mysql connect");
        check(store.initializeSku(sku_id, 2).ok(), "mysql inventory setup");

        live::messaging::FlashSaleOrderMessage message;
        message.promotion_id = "mysql-promo";
        message.order_id = "mysql-flash-order-" + suffix;
        message.idempotency_key = "mysql-flash-idem-" + suffix;
        message.user_id = "mysql-user";
        message.sku_id = sku_id;
        message.quantity = 1;

        live::business::order::Order first;
        check(store.persistQueuedOrder(message, &first).ok(), "mysql flash-sale persistence");
        live::business::order::Order duplicate;
        check(store.persistQueuedOrder(message, &duplicate).ok() && duplicate.id == first.id,
              "mysql flash-sale idempotency");
        live::business::order::Order loaded;
        check(store.getOrder(first.id, &loaded).ok() && loaded.sku_id == sku_id && loaded.quantity == 1,
              "mysql order query");
        live::business::inventory::StockSnapshot stock;
        check(store.queryStock(sku_id, &stock).ok() && stock.available == 1 && stock.reserved == 1 && stock.sold == 0,
              "mysql inventory reservation mirror");
        int published = 0;
        check(store.drainOutbox([&published](const live::messaging::Event&) {
            ++published;
            return live::common::Status::Ok();
        }).ok() && published == 1, "mysql outbox drain");
        return 0;
    } catch (const std::exception& error) {
        std::fprintf(stderr, "mysql_transaction_test failed: %s\n", error.what());
        return 1;
    }
}
