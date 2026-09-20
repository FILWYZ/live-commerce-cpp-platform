#pragma once

#include "business/inventory/inventory_gateway.h"
#include "business/order/order_transaction_store.h"

#include <cstddef>
#include <string>
#include <vector>

namespace live::business::inventory {

struct ReconcileReport {
    std::size_t checked{0};
    std::size_t mismatches{0};
    std::size_t repaired{0};
    std::size_t blocked{0};
};

class InventoryReconciler {
public:
    InventoryReconciler(::live::business::order::IOrderTransactionStore* durable,
                        IInventoryGateway* fast_path)
        : durable_(durable), fast_path_(fast_path) {}

    common::Status reconcile(const std::vector<std::string>& sku_ids, ReconcileReport* report);

private:
    ::live::business::order::IOrderTransactionStore* durable_{nullptr};
    IInventoryGateway* fast_path_{nullptr};
};

}  // namespace live::business::inventory
