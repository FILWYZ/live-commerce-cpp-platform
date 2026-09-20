#pragma once

#include "common/error/status.h"

#include <cstdint>
#include <string>

namespace live::messaging {

// Versioned, length-delimited queue payload. It avoids delimiter injection
// and lets a future consumer reject incompatible messages explicitly.
struct FlashSaleOrderMessage {
    std::uint16_t version{1};
    std::string promotion_id;
    std::string order_id;
    std::string idempotency_key;
    std::string user_id;
    std::string sku_id;
    std::int64_t quantity{0};

    common::Status validate() const;
    std::string serialize() const;
    static common::Status deserialize(const std::string& value, FlashSaleOrderMessage* message);
};

}  // namespace live::messaging
