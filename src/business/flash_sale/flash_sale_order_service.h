#pragma once

#include "business/inventory/inventory_gateway.h"
#include "business/promotion/flash_sale_gate.h"
#include "common/error/status.h"
#include "messaging/order_queue.h"

#include <cstdint>
#include <string>

namespace live::business::flash_sale {

enum class SubmitCode { kQueued, kDuplicate, kNotStarted, kFinished, kExhausted, kRejected };

struct FlashSaleSubmitRequest {
    std::string promotion_id;
    std::string sku_id;
    std::string user_id;
    std::string idempotency_key;
    std::int64_t quantity{1};
};

struct FlashSaleSubmitResult {
    SubmitCode code{SubmitCode::kRejected};
    std::string order_id;
    std::string message;
};

class FlashSaleOrderService final {
public:
    FlashSaleOrderService(::live::business::promotion::IAdmissionGate* admission_gate,
                          ::live::business::inventory::IInventoryGateway* inventory,
                          ::live::messaging::IOrderQueue* queue)
        : admission_gate_(admission_gate), inventory_(inventory), queue_(queue) {}

    common::Status submit(const FlashSaleSubmitRequest& request, FlashSaleSubmitResult* result);

private:
    static std::string orderId(const FlashSaleSubmitRequest& request);
    static SubmitCode mapGateCode(::live::business::promotion::AdmissionCode code);

    ::live::business::promotion::IAdmissionGate* admission_gate_{nullptr};
    ::live::business::inventory::IInventoryGateway* inventory_{nullptr};
    ::live::messaging::IOrderQueue* queue_{nullptr};
};

}  // namespace live::business::flash_sale
