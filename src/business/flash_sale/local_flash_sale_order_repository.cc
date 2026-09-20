#include "business/flash_sale/local_flash_sale_order_repository.h"

#include "business/order/order_service.h"

namespace live::business::flash_sale {

common::Status LocalFlashSaleOrderRepository::persistQueuedOrder(
    const ::live::messaging::FlashSaleOrderMessage& message, ::live::business::order::Order* order) {
    if (orders_ == nullptr) return common::Status::FailedPrecondition("local flash-sale order repository is not configured");
    return orders_->createOrderAfterReservation(message.order_id, message.user_id, message.sku_id,
                                                message.quantity, message.idempotency_key, order);
}

}  // namespace live::business::flash_sale
