#pragma once

#include "business/order/order_transaction_store.h"
#include "business/flash_sale/flash_sale_order_repository.h"
#include "storage/mysql/mysql_connection_pool.h"
#include "observability/metrics/metrics_registry.h"

#include <mysql/mysql.h>

#include <cstdint>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace live::storage::mysql {

class MySqlTransactionStore final : public ::live::business::order::IOrderTransactionStore,
                                    public ::live::business::flash_sale::IFlashSaleOrderRepository {
public:
    MySqlTransactionStore();
    ~MySqlTransactionStore() override;

    common::Status connect(const std::string& host, std::uint16_t port,
                           const std::string& user, const std::string& password,
                           const std::string& database);
    common::Status connect(const std::vector<std::string>& hosts, std::uint16_t port,
                           const std::string& user, const std::string& password,
                           const std::string& database, std::size_t pool_size);
    void close();
    bool connected() const;
    void setMetrics(::live::observability::MetricsRegistry* metrics) { metrics_ = metrics; }

    common::Status getOrder(const std::string& order_id, ::live::business::order::Order* order) const override;
    common::Status initializeSku(const std::string& sku_id, std::int64_t quantity) override;
    common::Status queryStock(const std::string& sku_id, ::live::business::inventory::StockSnapshot* snapshot) const override;
    common::Status drainOutbox(const std::function<common::Status(const ::live::messaging::Event&)>& publish,
                               std::size_t limit = 100) override;
    common::Status persistQueuedOrder(const ::live::messaging::FlashSaleOrderMessage& message,
                                      ::live::business::order::Order* order) override;

private:
    common::Status execute(MYSQL* connection, const std::string& sql) const;
    common::Status begin(MYSQL* connection) const;
    common::Status rollback(MYSQL* connection) const;
    common::Status commit(MYSQL* connection) const;
    std::string quote(MYSQL* connection, const std::string& value) const;
    common::Status loadOrder(MYSQL* connection, const std::string& order_id, ::live::business::order::Order* order) const;
    common::Status appendOutbox(MYSQL* connection, const ::live::messaging::Event& event) const;
    void recordSql(const char* operation, const common::Status& status,
                   std::chrono::steady_clock::time_point started) const;
    std::unique_ptr<MySqlConnectionPool> pool_;
    ::live::observability::MetricsRegistry* metrics_{nullptr};
};

}  // namespace live::storage::mysql
