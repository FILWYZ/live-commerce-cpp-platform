#include "business/promotion/promotion_service.h"

namespace live::business::promotion {

common::Status PromotionService::create(Promotion promotion) {
    if (promotion.id.empty() || promotion.sku_id.empty() || promotion.participant_limit == 0) {
        return common::Status::InvalidArgument("invalid promotion");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (promotions_.find(promotion.id) != promotions_.end()) return common::Status::AlreadyExists("promotion exists");
    if (gate_ == nullptr) return common::Status::FailedPrecondition("admission gate is not configured");
    if (const auto status = gate_->configure(promotion.id, promotion.participant_limit); !status.ok()) return status;
    promotions_.emplace(promotion.id, std::move(promotion));
    return common::Status::Ok();
}

bool PromotionService::transitionAllowed(PromotionState from, PromotionState to) {
    return (from == PromotionState::kCreated && to == PromotionState::kPreheating) ||
           (from == PromotionState::kPreheating && to == PromotionState::kRunning) ||
           (from == PromotionState::kRunning && to == PromotionState::kFinished);
}

common::Status PromotionService::preheat(const std::string& id) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = promotions_.find(id);
    if (it == promotions_.end()) return common::Status::NotFound("promotion not found");
    if (!transitionAllowed(it->second.state, PromotionState::kPreheating)) return common::Status::FailedPrecondition("invalid promotion transition");
    if (const auto status = gate_->preheat(id); !status.ok()) return status;
    it->second.state = PromotionState::kPreheating;
    return common::Status::Ok();
}

common::Status PromotionService::start(const std::string& id) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = promotions_.find(id);
    if (it == promotions_.end()) return common::Status::NotFound("promotion not found");
    if (!transitionAllowed(it->second.state, PromotionState::kRunning)) return common::Status::FailedPrecondition("invalid promotion transition");
    if (const auto status = gate_->start(id); !status.ok()) return status;
    it->second.state = PromotionState::kRunning;
    return common::Status::Ok();
}

common::Status PromotionService::finish(const std::string& id) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = promotions_.find(id);
    if (it == promotions_.end()) return common::Status::NotFound("promotion not found");
    if (!transitionAllowed(it->second.state, PromotionState::kFinished)) return common::Status::FailedPrecondition("invalid promotion transition");
    if (const auto status = gate_->finish(id); !status.ok()) return status;
    it->second.state = PromotionState::kFinished;
    return common::Status::Ok();
}

common::Status PromotionService::get(const std::string& id, Promotion* promotion) const {
    if (promotion == nullptr) return common::Status::InvalidArgument("promotion output must not be null");
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = promotions_.find(id);
    if (it == promotions_.end()) return common::Status::NotFound("promotion not found");
    *promotion = it->second;
    return common::Status::Ok();
}

}  // namespace live::business::promotion
