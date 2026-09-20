#include "business/inventory/inventory_service.h"

#include "storage/local_engine/local_kv_engine.h"

#include <sstream>
#include <utility>

namespace live::business::inventory {

std::string InventoryService::encodeStock(const StockSnapshot& stock) {
    return std::to_string(stock.available) + "|" + std::to_string(stock.reserved) + "|" + std::to_string(stock.sold);
}

bool InventoryService::decodeStock(const std::string& value, StockSnapshot* stock) {
    if (stock == nullptr) return false;
    std::istringstream input(value);
    char separator = 0;
    return static_cast<bool>(input >> stock->available >> separator) && separator == '|' &&
           static_cast<bool>(input >> stock->reserved >> separator) && separator == '|' &&
           static_cast<bool>(input >> stock->sold);
}

std::string InventoryService::encodeReservation(const Reservation& reservation) {
    return reservation.sku_id + "|" + std::to_string(reservation.quantity) + "|" +
           std::to_string(static_cast<int>(reservation.state));
}

bool InventoryService::decodeReservation(const std::string& value, Reservation* reservation) {
    if (reservation == nullptr) return false;
    std::istringstream input(value);
    char separator = 0;
    int state = 0;
    return static_cast<bool>(std::getline(input, reservation->sku_id, '|')) &&
           static_cast<bool>(input >> reservation->quantity >> separator) && separator == '|' &&
           static_cast<bool>(input >> state) && state >= 0 && state <= 2 &&
           (reservation->state = static_cast<ReservationState>(state), true);
}

common::Status InventoryService::persistStockAndReservation(const std::string& sku_id,
                                                            const StockSnapshot& stock,
                                                            const std::string& operation_id,
                                                            const Reservation& reservation) {
    if (storage_ == nullptr) return common::Status::Ok();
    return storage_->writeBatch({
        {::live::storage::WalOperation::kSet, "inventory/stock/" + sku_id, encodeStock(stock)},
        {::live::storage::WalOperation::kSet, "inventory/reservation/" + operation_id, encodeReservation(reservation)},
    });
}

common::Status InventoryService::restore() {
    if (storage_ == nullptr) return common::Status::Ok();
    std::lock_guard<std::mutex> lock(mutex_);
    stocks_.clear();
    reservations_.clear();
    static const std::string stock_prefix = "inventory/stock/";
    static const std::string reservation_prefix = "inventory/reservation/";
    for (const auto& [key, value] : storage_->scanPrefix(stock_prefix)) {
        StockSnapshot stock;
        if (!decodeStock(value, &stock)) return common::Status::Internal("invalid persisted stock");
        stocks_[key.substr(stock_prefix.size())] = stock;
    }
    for (const auto& [key, value] : storage_->scanPrefix(reservation_prefix)) {
        Reservation reservation;
        if (!decodeReservation(value, &reservation)) return common::Status::Internal("invalid persisted reservation");
        reservations_[key.substr(reservation_prefix.size())] = reservation;
    }
    return common::Status::Ok();
}

common::Status InventoryService::addSku(std::string sku_id, std::int64_t quantity) {
    if (sku_id.empty() || quantity <= 0) {
        return common::Status::InvalidArgument("sku_id and quantity must be valid");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (stocks_.find(sku_id) != stocks_.end()) {
        return common::Status::AlreadyExists("sku already exists");
    }
    const std::string key = sku_id;
    stocks_.emplace(key, StockSnapshot{quantity, 0, 0});
    if (storage_ != nullptr) {
        if (const auto status = storage_->set("inventory/stock/" + key, encodeStock(stocks_.at(key))); !status.ok()) {
            stocks_.erase(key);
            return status;
        }
    }
    return common::Status::Ok();
}

common::Status InventoryService::reserveStock(const std::string& sku_id,
                                              std::int64_t quantity,
                                              const std::string& business_operation_id) {
    if (sku_id.empty() || quantity <= 0 || business_operation_id.empty()) {
        return common::Status::InvalidArgument("invalid reserve arguments");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    const auto reservation_it = reservations_.find(business_operation_id);
    if (reservation_it != reservations_.end()) {
        const auto& reservation = reservation_it->second;
        if (reservation.sku_id == sku_id && reservation.quantity == quantity) {
            return common::Status::Ok();
        }
        return common::Status::FailedPrecondition("operation id was reused with different data");
    }

    const auto stock_it = stocks_.find(sku_id);
    if (stock_it == stocks_.end()) {
        return common::Status::NotFound("sku not found");
    }
    if (stock_it->second.available < quantity) {
        return common::Status::ResourceExhausted("insufficient stock");
    }
    stock_it->second.available -= quantity;
    stock_it->second.reserved += quantity;
    reservations_.emplace(business_operation_id, Reservation{sku_id, quantity});
    if (const auto status = persistStockAndReservation(sku_id, stock_it->second, business_operation_id,
                                                       reservations_.at(business_operation_id)); !status.ok()) {
        stock_it->second.available += quantity;
        stock_it->second.reserved -= quantity;
        reservations_.erase(business_operation_id);
        return status;
    }
    return common::Status::Ok();
}

common::Status InventoryService::confirmStock(const std::string& business_operation_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto reservation_it = reservations_.find(business_operation_id);
    if (reservation_it == reservations_.end()) {
        return common::Status::NotFound("reservation not found");
    }
    auto& reservation = reservation_it->second;
    if (reservation.state == ReservationState::kConfirmed) {
        return common::Status::Ok();
    }
    if (reservation.state == ReservationState::kReleased) {
        return common::Status::FailedPrecondition("released reservation cannot be confirmed");
    }
    auto& stock = stocks_.at(reservation.sku_id);
    const StockSnapshot previous_stock = stock;
    const ReservationState previous_state = reservation.state;
    stock.reserved -= reservation.quantity;
    stock.sold += reservation.quantity;
    reservation.state = ReservationState::kConfirmed;
    if (const auto status = persistStockAndReservation(reservation.sku_id, stock, business_operation_id, reservation); !status.ok()) {
        stock = previous_stock;
        reservation.state = previous_state;
        return status;
    }
    return common::Status::Ok();
}

common::Status InventoryService::releaseStock(const std::string& business_operation_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto reservation_it = reservations_.find(business_operation_id);
    if (reservation_it == reservations_.end()) {
        return common::Status::NotFound("reservation not found");
    }
    auto& reservation = reservation_it->second;
    if (reservation.state == ReservationState::kReleased) {
        return common::Status::Ok();
    }
    if (reservation.state == ReservationState::kConfirmed) {
        return common::Status::FailedPrecondition("confirmed reservation cannot be released");
    }
    auto& stock = stocks_.at(reservation.sku_id);
    const StockSnapshot previous_stock = stock;
    const ReservationState previous_state = reservation.state;
    stock.reserved -= reservation.quantity;
    stock.available += reservation.quantity;
    reservation.state = ReservationState::kReleased;
    if (const auto status = persistStockAndReservation(reservation.sku_id, stock, business_operation_id, reservation); !status.ok()) {
        stock = previous_stock;
        reservation.state = previous_state;
        return status;
    }
    return common::Status::Ok();
}

common::Status InventoryService::rollbackConfirmation(const std::string& business_operation_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto reservation_it = reservations_.find(business_operation_id);
    if (reservation_it == reservations_.end()) return common::Status::NotFound("reservation not found");
    auto& reservation = reservation_it->second;
    if (reservation.state != ReservationState::kConfirmed) return common::Status::FailedPrecondition("reservation is not confirmed");
    auto& stock = stocks_.at(reservation.sku_id);
    const auto previous = stock;
    stock.reserved += reservation.quantity;
    stock.sold -= reservation.quantity;
    reservation.state = ReservationState::kReserved;
    if (const auto status = persistStockAndReservation(reservation.sku_id, stock, business_operation_id, reservation); !status.ok()) {
        stock = previous;
        reservation.state = ReservationState::kConfirmed;
        return status;
    }
    return common::Status::Ok();
}

common::Status InventoryService::rollbackRelease(const std::string& business_operation_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto reservation_it = reservations_.find(business_operation_id);
    if (reservation_it == reservations_.end()) return common::Status::NotFound("reservation not found");
    auto& reservation = reservation_it->second;
    if (reservation.state != ReservationState::kReleased) return common::Status::FailedPrecondition("reservation is not released");
    auto& stock = stocks_.at(reservation.sku_id);
    const auto previous = stock;
    stock.available -= reservation.quantity;
    stock.reserved += reservation.quantity;
    reservation.state = ReservationState::kReserved;
    if (const auto status = persistStockAndReservation(reservation.sku_id, stock, business_operation_id, reservation); !status.ok()) {
        stock = previous;
        reservation.state = ReservationState::kReleased;
        return status;
    }
    return common::Status::Ok();
}

common::Status InventoryService::releaseOrphanReservations(
    const std::unordered_set<std::string>& active_operation_ids, std::size_t* released_count) {
    if (released_count != nullptr) *released_count = 0;
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& [operation_id, reservation] : reservations_) {
        if (reservation.state != ReservationState::kReserved ||
            active_operation_ids.find(operation_id) != active_operation_ids.end()) {
            continue;
        }
        const auto stock_it = stocks_.find(reservation.sku_id);
        if (stock_it == stocks_.end()) return common::Status::Internal("orphan reservation references missing stock");
        auto& stock = stock_it->second;
        const auto previous_stock = stock;
        const auto previous_state = reservation.state;
        stock.available += reservation.quantity;
        stock.reserved -= reservation.quantity;
        reservation.state = ReservationState::kReleased;
        if (const auto status = persistStockAndReservation(reservation.sku_id, stock, operation_id, reservation); !status.ok()) {
            stock = previous_stock;
            reservation.state = previous_state;
            return status;
        }
        if (released_count != nullptr) ++(*released_count);
    }
    return common::Status::Ok();
}

common::Status InventoryService::queryStock(const std::string& sku_id,
                                            StockSnapshot* snapshot) const {
    if (snapshot == nullptr) {
        return common::Status::InvalidArgument("snapshot must not be null");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = stocks_.find(sku_id);
    if (it == stocks_.end()) {
        return common::Status::NotFound("sku not found");
    }
    *snapshot = it->second;
    return common::Status::Ok();
}

common::Status InventoryService::replaceStock(const std::string& sku_id, const StockSnapshot& snapshot) {
    if (sku_id.empty() || snapshot.available < 0 || snapshot.reserved < 0 || snapshot.sold < 0) {
        return common::Status::InvalidArgument("invalid replacement stock");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (stocks_.find(sku_id) == stocks_.end()) return common::Status::NotFound("sku not found");
    stocks_[sku_id] = snapshot;
    if (storage_ != nullptr) return storage_->set("inventory/stock/" + sku_id, encodeStock(snapshot));
    return common::Status::Ok();
}

common::Status InventoryService::validateInvariants() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::unordered_map<std::string, std::int64_t> reserved_by_sku;
    std::unordered_map<std::string, std::int64_t> sold_by_sku;
    for (const auto& [sku_id, stock] : stocks_) {
        (void)sku_id;
        if (stock.available < 0 || stock.reserved < 0 || stock.sold < 0) {
            return common::Status::Internal("negative inventory invariant");
        }
    }
    for (const auto& [operation_id, reservation] : reservations_) {
        (void)operation_id;
        const auto stock_it = stocks_.find(reservation.sku_id);
        if (stock_it == stocks_.end() || reservation.quantity <= 0) {
            return common::Status::Internal("inventory reservation references invalid stock");
        }
        if (reservation.state == ReservationState::kReserved) reserved_by_sku[reservation.sku_id] += reservation.quantity;
        if (reservation.state == ReservationState::kConfirmed) sold_by_sku[reservation.sku_id] += reservation.quantity;
    }
    for (const auto& [sku_id, stock] : stocks_) {
        if (reserved_by_sku[sku_id] != stock.reserved || sold_by_sku[sku_id] != stock.sold) {
            return common::Status::Internal("inventory reservation totals do not match stock snapshot");
        }
    }
    return common::Status::Ok();
}

}  // namespace live::business::inventory
