#include "common/error/status.h"
#include "business/inventory/inventory_service.h"
#include "business/order/order_service.h"
#include "messaging/in_memory_broker.h"
#include "storage/local_engine/local_kv_engine.h"
#include "storage/wal/wal.h"

#include <cstdio>
#include <fstream>
#include <stdexcept>
#include <string>
#include <unistd.h>

namespace {

void check(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

std::string path(const char* suffix) {
    return std::string("/tmp/live-commerce-") + std::to_string(::getpid()) + suffix;
}

void testBatchRecoveryAndSingleWriterLock() {
    const auto snapshot = path("-storage.snapshot");
    const auto wal = path("-storage.wal");
    std::remove(snapshot.c_str());
    std::remove((snapshot + ".tmp").c_str());
    std::remove(wal.c_str());

    live::storage::LocalKVEngine first;
    check(first.open(snapshot, wal).ok(), "first storage open");
    check(first.writeBatch({
        {live::storage::WalOperation::kSet, "order/order-1", "PENDING"},
        {live::storage::WalOperation::kSet, "inventory/sku-1", "available=9;reserved=1"},
    }).ok(), "atomic batch write");

    live::storage::LocalKVEngine second;
    check(second.open(snapshot, wal).code() == live::common::ErrorCode::kResourceExhausted,
          "storage must reject a second writer");
    first.close();
    check(second.open(snapshot, wal).ok(), "storage re-open after writer release");
    std::string value;
    check(second.get("order/order-1", &value).ok() && value == "PENDING", "recovered order state");
    check(second.get("inventory/sku-1", &value).ok() && value == "available=9;reserved=1", "recovered inventory state");
    second.close();
    std::remove(snapshot.c_str());
    std::remove(wal.c_str());
}

void testWalChecksumRejectsCorruption() {
    const auto wal_path = path("-checksum.wal");
    std::remove(wal_path.c_str());
    {
        live::storage::Wal wal;
        check(wal.open(wal_path).ok(), "checksum WAL open");
        check(wal.append({live::storage::WalOperation::kSet, "key", "value"}).ok(), "checksum WAL append");
    }
    {
        std::fstream file(wal_path, std::ios::in | std::ios::out | std::ios::binary);
        check(file.good(), "checksum WAL mutate open");
        file.seekp(-1, std::ios::end);
        char byte = 0;
        file.read(&byte, 1);
        file.seekp(-1, std::ios::end);
        byte ^= static_cast<char>(0x7f);
        file.write(&byte, 1);
    }
    live::storage::Wal corrupted;
    check(corrupted.open(wal_path).ok(), "corrupted WAL open");
    const auto status = corrupted.replay([](const live::storage::WalRecord&) {
        return live::common::Status::Ok();
    });
    check(status.code() == live::common::ErrorCode::kInternal, "WAL checksum must reject corruption");
    std::remove(wal_path.c_str());
}

void testSnapshotChecksumRejectsCorruption() {
    const auto snapshot = path("-checksum.snapshot");
    const auto wal = path("-checksum-storage.wal");
    std::remove(snapshot.c_str());
    std::remove(wal.c_str());
    {
        live::storage::LocalKVEngine storage;
        check(storage.open(snapshot, wal).ok(), "snapshot storage open");
        check(storage.set("key", "value").ok(), "snapshot storage set");
        check(storage.snapshot().ok(), "snapshot save");
    }
    {
        std::fstream file(snapshot, std::ios::in | std::ios::out | std::ios::binary);
        check(file.good(), "snapshot mutate open");
        file.seekp(-1, std::ios::end);
        char byte = 0;
        file.read(&byte, 1);
        file.seekp(-1, std::ios::end);
        byte ^= static_cast<char>(0x33);
        file.write(&byte, 1);
    }
    live::storage::LocalKVEngine recovered;
    check(recovered.open(snapshot, wal).code() == live::common::ErrorCode::kInternal,
          "snapshot checksum must reject corruption");
    std::remove(snapshot.c_str());
    std::remove(wal.c_str());
}

void testOrphanReservationRecovery() {
    const auto snapshot = path("-orphan.snapshot");
    const auto wal = path("-orphan.wal");
    std::remove(snapshot.c_str());
    std::remove(wal.c_str());
    {
        live::storage::LocalKVEngine storage;
        check(storage.open(snapshot, wal).ok(), "orphan storage open");
        live::business::inventory::InventoryService inventory(&storage);
        check(inventory.addSku("sku-orphan", 10).ok(), "orphan inventory setup");
        check(inventory.reserveStock("sku-orphan", 3, "crashed-order-1").ok(), "orphan reservation setup");
    }
    {
        live::storage::LocalKVEngine storage;
        check(storage.open(snapshot, wal).ok(), "orphan recovery storage open");
        live::business::inventory::InventoryService inventory(&storage);
        check(inventory.restore().ok(), "orphan inventory restore");
        live::messaging::InMemoryBroker broker;
        live::business::order::OrderService orders(&inventory, &broker, &storage);
        check(orders.restore().ok(), "orphan order restore");
        live::business::inventory::StockSnapshot stock;
        check(inventory.queryStock("sku-orphan", &stock).ok() && stock.available == 10 && stock.reserved == 0,
              "orphan reservation was not released");
        check(inventory.validateInvariants().ok(), "orphan recovery invariant failure");
    }
    std::remove(snapshot.c_str());
    std::remove(wal.c_str());
}

}  // namespace

int main() {
    try {
        testBatchRecoveryAndSingleWriterLock();
        testWalChecksumRejectsCorruption();
        testSnapshotChecksumRejectsCorruption();
        testOrphanReservationRecovery();
        return 0;
    } catch (const std::exception& error) {
        std::fprintf(stderr, "storage_recovery_test failed: %s\n", error.what());
        return 1;
    }
}
