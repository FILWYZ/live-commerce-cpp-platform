#pragma once

#include "common/error/status.h"

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_set>
#include <unordered_map>

namespace live::storage { class LocalKVEngine; }

namespace live::business::inventory {

struct StockSnapshot {
    std::int64_t available{0};
    std::int64_t reserved{0};
    std::int64_t sold{0};
};

class InventoryService {
public:
    explicit InventoryService(::live::storage::LocalKVEngine* storage = nullptr) : storage_(storage) {}

    common::Status addSku(std::string sku_id, std::int64_t quantity);
    common::Status restore();
    common::Status reserveStock(const std::string& sku_id,
                                std::int64_t quantity,
                                const std::string& business_operation_id);
    common::Status confirmStock(const std::string& business_operation_id);
    common::Status releaseStock(const std::string& business_operation_id);
    // Compensation operations used when the order state/event transaction fails.
    common::Status rollbackConfirmation(const std::string& business_operation_id);
    common::Status rollbackRelease(const std::string& business_operation_id);
    // Releases reservations that survived a process crash before the owning
    // order was durably written. The caller supplies the recovered order IDs.
    common::Status releaseOrphanReservations(const std::unordered_set<std::string>& active_operation_ids,
                                             std::size_t* released_count = nullptr);

    common::Status queryStock(const std::string& sku_id, StockSnapshot* snapshot) const;
    common::Status replaceStock(const std::string& sku_id, const StockSnapshot& snapshot);
    common::Status validateInvariants() const;

private:
    enum class ReservationState { kReserved, kConfirmed, kReleased };
    struct Reservation {
        std::string sku_id;
        std::int64_t quantity{0};
        ReservationState state{ReservationState::kReserved};
    };

    static std::string encodeStock(const StockSnapshot& stock);
    static bool decodeStock(const std::string& value, StockSnapshot* stock);
    static std::string encodeReservation(const Reservation& reservation);
    static bool decodeReservation(const std::string& value, Reservation* reservation);
    common::Status persistStockAndReservation(const std::string& sku_id,
                                              const StockSnapshot& stock,
                                              const std::string& operation_id,
                                              const Reservation& reservation);

    mutable std::mutex mutex_;
    std::unordered_map<std::string, StockSnapshot> stocks_;
    std::unordered_map<std::string, Reservation> reservations_;
    ::live::storage::LocalKVEngine* storage_{nullptr};
};

}  // namespace live::business::inventory
