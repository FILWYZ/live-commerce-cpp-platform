#pragma once

#include "common/error/status.h"
#include "business/promotion/flash_sale_gate.h"

#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace live::business::promotion {

enum class PromotionState { kCreated, kPreheating, kRunning, kFinished };

struct Promotion {
    std::string id;
    std::string sku_id;
    std::size_t participant_limit{0};
    PromotionState state{PromotionState::kCreated};
};

class PromotionService {
public:
    explicit PromotionService(std::unique_ptr<IAdmissionGate> gate = std::make_unique<FlashSaleAdmissionGate>())
        : gate_(std::move(gate)) {}

    common::Status create(Promotion promotion);
    common::Status preheat(const std::string& id);
    common::Status start(const std::string& id);
    common::Status finish(const std::string& id);
    common::Status get(const std::string& id, Promotion* promotion) const;
    IAdmissionGate* admissionGate() const { return gate_.get(); }

private:
    static bool transitionAllowed(PromotionState from, PromotionState to);
    mutable std::mutex mutex_;
    std::unordered_map<std::string, Promotion> promotions_;
    std::unique_ptr<IAdmissionGate> gate_;
};

}  // namespace live::business::promotion
