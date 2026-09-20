#pragma once

#include "business/flash_sale/flash_sale_order_repository.h"

namespace live::business::order { class OrderService; }

namespace live::business::flash_sale {

class LocalFlashSaleOrderRepository final : public IFlashSaleOrderRepository {
public:
    explicit LocalFlashSaleOrderRepository(::live::business::order::OrderService* orders) : orders_(orders) {}

    common::Status persistQueuedOrder(const ::live::messaging::FlashSaleOrderMessage& message,
                                      ::live::business::order::Order* order) override;

private:
    ::live::business::order::OrderService* orders_{nullptr};
};

}  // namespace live::business::flash_sale
