#include "business/flash_sale/flash_sale_order_service.h"

namespace live::business::flash_sale {

std::string FlashSaleOrderService::orderId(const FlashSaleSubmitRequest& request) {
    return "flash-order-" + request.promotion_id + "-" + request.user_id;
}

SubmitCode FlashSaleOrderService::mapGateCode(::live::business::promotion::AdmissionCode code) {
    using ::live::business::promotion::AdmissionCode;
    switch (code) {
        case AdmissionCode::kAccepted: return SubmitCode::kQueued;
        case AdmissionCode::kDuplicate: return SubmitCode::kDuplicate;
        case AdmissionCode::kFinished: return SubmitCode::kFinished;
        case AdmissionCode::kExhausted: return SubmitCode::kExhausted;
        case AdmissionCode::kNotStarted: return SubmitCode::kNotStarted;
    }
    return SubmitCode::kRejected;
}

common::Status FlashSaleOrderService::submit(const FlashSaleSubmitRequest& request, FlashSaleSubmitResult* result) {
    if (result == nullptr) return common::Status::InvalidArgument("flash-sale result must not be null");
    *result = {};
    if (request.promotion_id.empty() || request.sku_id.empty() || request.user_id.empty() ||
        request.idempotency_key.empty() || request.quantity <= 0) {
        return common::Status::InvalidArgument("invalid flash-sale order request");
    }
    if (admission_gate_ == nullptr || inventory_ == nullptr || queue_ == nullptr) {
        return common::Status::FailedPrecondition("flash-sale order path is not configured");
    }

    ::live::business::promotion::AdmissionDecision admission;
    if (const auto status = admission_gate_->tryAcquire(request.promotion_id, request.user_id, &admission); !status.ok()) {
        result->message = status.message();
        return status;
    }
    result->code = mapGateCode(admission.code);
    result->order_id = orderId(request);
    if (admission.code == ::live::business::promotion::AdmissionCode::kDuplicate) {
        result->message = "user already admitted";
        return common::Status::Ok();
    }
    if (admission.code != ::live::business::promotion::AdmissionCode::kAccepted) {
        result->message = "promotion admission rejected";
        return common::Status::Ok();
    }

    ::live::business::inventory::InventoryReservationResult reservation;
    const auto reserve_status = inventory_->reserve(request.sku_id, request.quantity, result->order_id, &reservation);
    if (!reserve_status.ok() || reservation.decision != ::live::business::inventory::InventoryDecision::kSuccess) {
        (void)admission_gate_->rollback(request.promotion_id, request.user_id);
        result->code = reservation.decision == ::live::business::inventory::InventoryDecision::kInsufficient
            ? SubmitCode::kExhausted : SubmitCode::kRejected;
        result->message = reserve_status.ok() ? "inventory rejected" : reserve_status.message();
        return reserve_status.ok() ? common::Status::Ok() : reserve_status;
    }

    ::live::messaging::FlashSaleOrderMessage message;
    message.promotion_id = request.promotion_id;
    message.order_id = result->order_id;
    message.idempotency_key = request.idempotency_key;
    message.user_id = request.user_id;
    message.sku_id = request.sku_id;
    message.quantity = request.quantity;
    if (const auto status = queue_->push(message); !status.ok()) {
        ::live::business::inventory::InventoryReservationResult compensation;
        (void)inventory_->release(request.sku_id, result->order_id, &compensation);
        (void)admission_gate_->rollback(request.promotion_id, request.user_id);
        result->code = SubmitCode::kRejected;
        result->message = status.message();
        return status;
    }
    result->code = SubmitCode::kQueued;
    result->message = "order accepted and queued";
    return common::Status::Ok();
}

}  // namespace live::business::flash_sale
