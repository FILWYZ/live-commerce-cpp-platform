#include "business/inventory/inventory_service.h"

#include "storage/local_engine/local_kv_engine.h"

#include <functional>
#include <sstream>
#include <utility>

namespace live::business::inventory {

std::size_t InventoryService::shardFor(const std::string& sku_id) {
    return std::hash<std::string>{}(sku_id) % kShardCount;
}

std::string InventoryService::encodeStock(const StockSnapshot& stock) {
    return std::to_string(stock.available) + "|" + std::to_string(stock.reserved) + "|" + std::to_string(stock.sold);
}

bool InventoryService::decodeStock(const std::string& value, StockSnapshot* stock) {
    if (stock == nullptr) return false;
    std::istringstream input(value); char separator = 0;
    return static_cast<bool>(input >> stock->available >> separator) && separator == '|' &&
           static_cast<bool>(input >> stock->reserved >> separator) && separator == '|' &&
           static_cast<bool>(input >> stock->sold);
}

std::string InventoryService::encodeReservation(const Reservation& reservation) {
    return reservation.sku_id + "|" + std::to_string(reservation.quantity) + "|" + std::to_string(static_cast<int>(reservation.state));
}

bool InventoryService::decodeReservation(const std::string& value, Reservation* reservation) {
    if (reservation == nullptr) return false;
    std::istringstream input(value); char separator = 0; int state = 0;
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
    std::unordered_map<std::string, StockSnapshot> stocks;
    std::unordered_map<std::string, Reservation> reservations;
    static const std::string stock_prefix = "inventory/stock/";
    static const std::string reservation_prefix = "inventory/reservation/";
    for (const auto& [key, value] : storage_->scanPrefix(stock_prefix)) {
        StockSnapshot stock;
        if (!decodeStock(value, &stock)) return common::Status::Internal("invalid persisted stock");
        stocks[key.substr(stock_prefix.size())] = stock;
    }
    for (const auto& [key, value] : storage_->scanPrefix(reservation_prefix)) {
        Reservation reservation;
        if (!decodeReservation(value, &reservation)) return common::Status::Internal("invalid persisted reservation");
        reservations[key.substr(reservation_prefix.size())] = reservation;
    }
    std::unique_lock<std::shared_mutex> structure_lock(structure_mutex_);
    std::lock_guard<std::mutex> reservation_lock(reservations_mutex_);
    stocks_ = std::move(stocks);
    reservations_ = std::move(reservations);
    return common::Status::Ok();
}

common::Status InventoryService::addSku(std::string sku_id, std::int64_t quantity) {
    if (sku_id.empty() || quantity <= 0) return common::Status::InvalidArgument("sku_id and quantity must be valid");
    const auto shard = shardFor(sku_id);
    std::lock_guard<std::mutex> persistence_lock(persistence_shards_[shard]);
    {
        std::unique_lock<std::shared_mutex> structure_lock(structure_mutex_);
        std::lock_guard<std::mutex> shard_lock(stock_shards_[shard]);
        if (stocks_.find(sku_id) != stocks_.end()) return common::Status::AlreadyExists("sku already exists");
        stocks_.emplace(sku_id, StockSnapshot{quantity, 0, 0});
    }
    if (storage_ != nullptr) {
        if (const auto status = storage_->set("inventory/stock/" + sku_id, encodeStock({quantity, 0, 0})); !status.ok()) {
            std::unique_lock<std::shared_mutex> structure_lock(structure_mutex_);
            std::lock_guard<std::mutex> shard_lock(stock_shards_[shard]);
            stocks_.erase(sku_id);
            return status;
        }
    }
    return common::Status::Ok();
}

common::Status InventoryService::reserveStock(const std::string& sku_id, std::int64_t quantity,
                                              const std::string& operation_id) {
    if (sku_id.empty() || quantity <= 0 || operation_id.empty()) return common::Status::InvalidArgument("invalid reserve arguments");
    const auto shard = shardFor(sku_id);
    std::lock_guard<std::mutex> persistence_lock(persistence_shards_[shard]);
    StockSnapshot stock;
    Reservation reservation;
    {
        std::shared_lock<std::shared_mutex> structure_lock(structure_mutex_);
        std::lock_guard<std::mutex> shard_lock(stock_shards_[shard]);
        std::lock_guard<std::mutex> reservation_lock(reservations_mutex_);
        const auto existing = reservations_.find(operation_id);
        if (existing != reservations_.end()) {
            if (existing->second.sku_id == sku_id && existing->second.quantity == quantity) return common::Status::Ok();
            return common::Status::FailedPrecondition("operation id was reused with different data");
        }
        const auto stock_it = stocks_.find(sku_id);
        if (stock_it == stocks_.end()) return common::Status::NotFound("sku not found");
        if (stock_it->second.available < quantity) return common::Status::ResourceExhausted("insufficient stock");
        stock_it->second.available -= quantity;
        stock_it->second.reserved += quantity;
        reservation = {sku_id, quantity, ReservationState::kReserved};
        reservations_.emplace(operation_id, reservation);
        stock = stock_it->second;
    }
    if (const auto status = persistStockAndReservation(sku_id, stock, operation_id, reservation); !status.ok()) {
        std::shared_lock<std::shared_mutex> structure_lock(structure_mutex_);
        std::lock_guard<std::mutex> shard_lock(stock_shards_[shard]);
        std::lock_guard<std::mutex> reservation_lock(reservations_mutex_);
        auto stock_it = stocks_.find(sku_id);
        auto reservation_it = reservations_.find(operation_id);
        if (stock_it != stocks_.end() && reservation_it != reservations_.end()) {
            stock_it->second.available += quantity;
            stock_it->second.reserved -= quantity;
            reservations_.erase(reservation_it);
        }
        return status;
    }
    return common::Status::Ok();
}

common::Status InventoryService::confirmStock(const std::string& operation_id) {
    std::string sku_id;
    {
        std::lock_guard<std::mutex> lock(reservations_mutex_);
        const auto it = reservations_.find(operation_id);
        if (it == reservations_.end()) return common::Status::NotFound("reservation not found");
        sku_id = it->second.sku_id;
    }
    const auto shard = shardFor(sku_id);
    std::lock_guard<std::mutex> persistence_lock(persistence_shards_[shard]);
    StockSnapshot previous_stock;
    Reservation previous_reservation;
    StockSnapshot stock;
    Reservation reservation;
    {
        std::shared_lock<std::shared_mutex> structure_lock(structure_mutex_);
        std::lock_guard<std::mutex> shard_lock(stock_shards_[shard]);
        std::lock_guard<std::mutex> reservation_lock(reservations_mutex_);
        auto reservation_it = reservations_.find(operation_id);
        auto stock_it = stocks_.find(sku_id);
        if (reservation_it == reservations_.end() || stock_it == stocks_.end()) return common::Status::NotFound("reservation not found");
        if (reservation_it->second.state == ReservationState::kConfirmed) return common::Status::Ok();
        if (reservation_it->second.state == ReservationState::kReleased) return common::Status::FailedPrecondition("released reservation cannot be confirmed");
        previous_stock = stock_it->second; previous_reservation = reservation_it->second;
        stock_it->second.reserved -= reservation_it->second.quantity;
        stock_it->second.sold += reservation_it->second.quantity;
        reservation_it->second.state = ReservationState::kConfirmed;
        stock = stock_it->second; reservation = reservation_it->second;
    }
    if (const auto status = persistStockAndReservation(sku_id, stock, operation_id, reservation); !status.ok()) {
        std::shared_lock<std::shared_mutex> structure_lock(structure_mutex_);
        std::lock_guard<std::mutex> shard_lock(stock_shards_[shard]);
        std::lock_guard<std::mutex> reservation_lock(reservations_mutex_);
        stocks_[sku_id] = previous_stock; reservations_[operation_id] = previous_reservation;
        return status;
    }
    return common::Status::Ok();
}

common::Status InventoryService::releaseStock(const std::string& operation_id) {
    std::string sku_id;
    {
        std::lock_guard<std::mutex> lock(reservations_mutex_);
        const auto it = reservations_.find(operation_id);
        if (it == reservations_.end()) return common::Status::NotFound("reservation not found");
        sku_id = it->second.sku_id;
    }
    const auto shard = shardFor(sku_id);
    std::lock_guard<std::mutex> persistence_lock(persistence_shards_[shard]);
    StockSnapshot previous_stock;
    Reservation previous_reservation;
    StockSnapshot stock;
    Reservation reservation;
    {
        std::shared_lock<std::shared_mutex> structure_lock(structure_mutex_);
        std::lock_guard<std::mutex> shard_lock(stock_shards_[shard]);
        std::lock_guard<std::mutex> reservation_lock(reservations_mutex_);
        auto reservation_it = reservations_.find(operation_id);
        auto stock_it = stocks_.find(sku_id);
        if (reservation_it == reservations_.end() || stock_it == stocks_.end()) return common::Status::NotFound("reservation not found");
        if (reservation_it->second.state == ReservationState::kReleased) return common::Status::Ok();
        if (reservation_it->second.state == ReservationState::kConfirmed) return common::Status::FailedPrecondition("confirmed reservation cannot be released");
        previous_stock = stock_it->second;
        previous_reservation = reservation_it->second;
        stock_it->second.reserved -= reservation_it->second.quantity;
        stock_it->second.available += reservation_it->second.quantity;
        reservation_it->second.state = ReservationState::kReleased;
        stock = stock_it->second;
        reservation = reservation_it->second;
    }
    if (const auto status = persistStockAndReservation(sku_id, stock, operation_id, reservation); !status.ok()) {
        std::shared_lock<std::shared_mutex> structure_lock(structure_mutex_);
        std::lock_guard<std::mutex> shard_lock(stock_shards_[shard]);
        std::lock_guard<std::mutex> reservation_lock(reservations_mutex_);
        stocks_[sku_id] = previous_stock;
        reservations_[operation_id] = previous_reservation;
        return status;
    }
    return common::Status::Ok();
}

common::Status InventoryService::rollbackConfirmation(const std::string& operation_id) {
    std::string sku_id;
    {
        std::lock_guard<std::mutex> lock(reservations_mutex_);
        const auto it = reservations_.find(operation_id);
        if (it == reservations_.end()) return common::Status::NotFound("reservation not found");
        sku_id = it->second.sku_id;
    }
    const auto shard = shardFor(sku_id);
    std::lock_guard<std::mutex> persistence_lock(persistence_shards_[shard]);
    StockSnapshot previous_stock;
    Reservation previous_reservation;
    StockSnapshot stock;
    Reservation reservation;
    {
        std::shared_lock<std::shared_mutex> structure_lock(structure_mutex_);
        std::lock_guard<std::mutex> shard_lock(stock_shards_[shard]);
        std::lock_guard<std::mutex> reservation_lock(reservations_mutex_);
        auto& current = reservations_.at(operation_id);
        auto& stock_ref = stocks_.at(sku_id);
        if (current.state != ReservationState::kConfirmed) return common::Status::FailedPrecondition("reservation is not confirmed");
        previous_stock = stock_ref;
        previous_reservation = current;
        stock_ref.reserved += current.quantity;
        stock_ref.sold -= current.quantity;
        current.state = ReservationState::kReserved;
        stock = stock_ref;
        reservation = current;
    }
    if (const auto status = persistStockAndReservation(sku_id, stock, operation_id, reservation); !status.ok()) {
        std::shared_lock<std::shared_mutex> structure_lock(structure_mutex_);
        std::lock_guard<std::mutex> shard_lock(stock_shards_[shard]);
        std::lock_guard<std::mutex> reservation_lock(reservations_mutex_);
        stocks_[sku_id] = previous_stock;
        reservations_[operation_id] = previous_reservation;
        return status;
    }
    return common::Status::Ok();
}

common::Status InventoryService::rollbackRelease(const std::string& operation_id) {
    std::string sku_id;
    {
        std::lock_guard<std::mutex> lock(reservations_mutex_);
        const auto it = reservations_.find(operation_id);
        if (it == reservations_.end()) return common::Status::NotFound("reservation not found");
        sku_id = it->second.sku_id;
    }
    const auto shard = shardFor(sku_id);
    std::lock_guard<std::mutex> persistence_lock(persistence_shards_[shard]);
    StockSnapshot previous_stock;
    Reservation previous_reservation;
    StockSnapshot stock;
    Reservation reservation;
    {
        std::shared_lock<std::shared_mutex> structure_lock(structure_mutex_);
        std::lock_guard<std::mutex> shard_lock(stock_shards_[shard]);
        std::lock_guard<std::mutex> reservation_lock(reservations_mutex_);
        auto& current = reservations_.at(operation_id);
        auto& stock_ref = stocks_.at(sku_id);
        if (current.state != ReservationState::kReleased) return common::Status::FailedPrecondition("reservation is not released");
        previous_stock = stock_ref;
        previous_reservation = current;
        stock_ref.available -= current.quantity;
        stock_ref.reserved += current.quantity;
        current.state = ReservationState::kReserved;
        stock = stock_ref;
        reservation = current;
    }
    if (const auto status = persistStockAndReservation(sku_id, stock, operation_id, reservation); !status.ok()) {
        std::shared_lock<std::shared_mutex> structure_lock(structure_mutex_);
        std::lock_guard<std::mutex> shard_lock(stock_shards_[shard]);
        std::lock_guard<std::mutex> reservation_lock(reservations_mutex_);
        stocks_[sku_id] = previous_stock;
        reservations_[operation_id] = previous_reservation;
        return status;
    }
    return common::Status::Ok();
}

common::Status InventoryService::releaseOrphanReservations(const std::unordered_set<std::string>& active_operation_ids,
                                                           std::size_t* released_count) {
    if (released_count != nullptr) *released_count = 0;
    struct Change {
        std::string operation_id;
        std::string sku_id;
        StockSnapshot previous_stock;
        Reservation previous_reservation;
    };
    std::vector<Change> changes;
    std::vector<::live::storage::WalRecord> mutations;
    {
        std::unique_lock<std::shared_mutex> structure_lock(structure_mutex_);
        std::lock_guard<std::mutex> reservation_lock(reservations_mutex_);
        for (auto& [operation_id, reservation] : reservations_) {
            if (reservation.state != ReservationState::kReserved ||
                active_operation_ids.find(operation_id) != active_operation_ids.end()) continue;
            auto stock_it = stocks_.find(reservation.sku_id);
            if (stock_it == stocks_.end()) return common::Status::Internal("orphan reservation references missing stock");
            changes.push_back({operation_id, reservation.sku_id, stock_it->second, reservation});
            stock_it->second.available += reservation.quantity;
            stock_it->second.reserved -= reservation.quantity;
            reservation.state = ReservationState::kReleased;
            mutations.push_back({::live::storage::WalOperation::kSet,
                                 "inventory/stock/" + reservation.sku_id,
                                 encodeStock(stock_it->second)});
            mutations.push_back({::live::storage::WalOperation::kSet,
                                 "inventory/reservation/" + operation_id,
                                 encodeReservation(reservation)});
        }
    }
    if (storage_ != nullptr && !mutations.empty()) {
        if (const auto status = storage_->writeBatch(mutations); !status.ok()) {
            std::unique_lock<std::shared_mutex> structure_lock(structure_mutex_);
            std::lock_guard<std::mutex> reservation_lock(reservations_mutex_);
            for (const auto& change : changes) {
                stocks_[change.sku_id] = change.previous_stock;
                reservations_[change.operation_id] = change.previous_reservation;
            }
            return status;
        }
    }
    if (released_count != nullptr) *released_count = changes.size();
    return common::Status::Ok();
}

common::Status InventoryService::queryStock(const std::string& sku_id, StockSnapshot* snapshot) const {
    if (snapshot == nullptr) return common::Status::InvalidArgument("snapshot must not be null");
    std::shared_lock<std::shared_mutex> structure_lock(structure_mutex_);
    std::lock_guard<std::mutex> shard_lock(stock_shards_[shardFor(sku_id)]);
    const auto it = stocks_.find(sku_id);
    if (it == stocks_.end()) return common::Status::NotFound("sku not found");
    *snapshot = it->second; return common::Status::Ok();
}

common::Status InventoryService::replaceStock(const std::string& sku_id, const StockSnapshot& snapshot) {
    if (sku_id.empty() || snapshot.available < 0 || snapshot.reserved < 0 || snapshot.sold < 0) {
        return common::Status::InvalidArgument("invalid replacement stock");
    }
    const auto shard = shardFor(sku_id);
    std::lock_guard<std::mutex> persistence_lock(persistence_shards_[shard]);
    StockSnapshot previous;
    {
        std::shared_lock<std::shared_mutex> structure_lock(structure_mutex_);
        std::lock_guard<std::mutex> shard_lock(stock_shards_[shard]);
        const auto it = stocks_.find(sku_id);
        if (it == stocks_.end()) return common::Status::NotFound("sku not found");
        previous = it->second;
        stocks_[sku_id] = snapshot;
    }
    if (storage_ != nullptr) {
        if (const auto status = storage_->set("inventory/stock/" + sku_id, encodeStock(snapshot)); !status.ok()) {
            std::shared_lock<std::shared_mutex> structure_lock(structure_mutex_);
            std::lock_guard<std::mutex> shard_lock(stock_shards_[shard]);
            stocks_[sku_id] = previous;
            return status;
        }
    }
    return common::Status::Ok();
}

common::Status InventoryService::validateInvariants() const {
    std::shared_lock<std::shared_mutex> structure_lock(structure_mutex_);
    std::vector<std::unique_lock<std::mutex>> shard_locks;
    shard_locks.reserve(kShardCount);
    for (auto& shard : stock_shards_) shard_locks.emplace_back(shard);
    std::lock_guard<std::mutex> reservation_lock(reservations_mutex_);
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
