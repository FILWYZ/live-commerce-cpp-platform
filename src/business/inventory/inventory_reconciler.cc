#include "business/inventory/inventory_reconciler.h"

namespace live::business::inventory {

common::Status InventoryReconciler::reconcile(const std::vector<std::string>& sku_ids, ReconcileReport* report) {
    if (durable_ == nullptr || fast_path_ == nullptr || report == nullptr || sku_ids.empty()) {
        return common::Status::InvalidArgument("reconcile dependencies and sku list are required");
    }
    *report = {};
    for (const auto& sku_id : sku_ids) {
        ++report->checked;
        StockSnapshot durable;
        StockSnapshot cached;
        if (const auto status = durable_->queryStock(sku_id, &durable); !status.ok()) return status;
        if (const auto status = fast_path_->queryStock(sku_id, &cached); !status.ok()) return status;
        if (durable.available == cached.available && durable.reserved == cached.reserved && durable.sold == cached.sold) continue;
        ++report->mismatches;
        // The durable transaction store is the source of truth. The repair is
        // guarded by the current Redis reserved count so that an active flash
        // sale reservation is never silently overwritten.
        if (const auto status = fast_path_->repairStock(sku_id, durable); !status.ok()) {
            if (status.code() == common::ErrorCode::kFailedPrecondition) {
                ++report->blocked;
                continue;
            }
            return status;
        }
        ++report->repaired;
    }
    return common::Status::Ok();
}

}  // namespace live::business::inventory
