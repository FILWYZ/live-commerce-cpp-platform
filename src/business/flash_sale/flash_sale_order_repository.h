#pragma once

#include "common/error/status.h"
#include "messaging/order_message.h"

namespace live::business::order { struct Order; }

namespace live::business::flash_sale {

class IFlashSaleOrderRepository {
public:
    virtual ~IFlashSaleOrderRepository() = default;
    // Persists the order and its CREATED event. The operation is idempotent
    // on message.idempotency_key and must not reserve inventory again.
    virtual common::Status persistQueuedOrder(const ::live::messaging::FlashSaleOrderMessage& message,
                                              ::live::business::order::Order* order) = 0;
};

}  // namespace live::business::flash_sale
