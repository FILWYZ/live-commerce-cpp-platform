#pragma once

#include "common/error/status.h"
#include "messaging/in_memory_broker.h"

#include <cstdint>
#include <functional>
#include <string>

namespace live::business::order { struct Order; }
namespace live::business::inventory { struct StockSnapshot; }

namespace live::business::order {

// A transaction store owns the database transaction that spans inventory,
// order and outbox rows. The LocalKV implementation keeps the existing
// zero-dependency path; the MySQL implementation provides the production-like
// path used by integration tests and multi-instance deployments.
class IOrderTransactionStore {
public:
    virtual ~IOrderTransactionStore() = default;

    virtual common::Status getOrder(const std::string& order_id, Order* order) const = 0;
    virtual common::Status initializeSku(const std::string& sku_id, std::int64_t quantity) = 0;
    virtual common::Status queryStock(const std::string& sku_id,
                                      inventory::StockSnapshot* snapshot) const = 0;
    virtual common::Status drainOutbox(const std::function<common::Status(const messaging::Event&)>& publish,
                                       std::size_t limit = 100) = 0;
};

}  // namespace live::business::order
