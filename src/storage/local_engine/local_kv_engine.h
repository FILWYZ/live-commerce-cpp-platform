#pragma once

#include "common/error/status.h"
#include "storage/snapshot/snapshot_store.h"
#include "storage/wal/wal.h"

#include <mutex>
#include <string>
#include <utility>
#include <unordered_map>
#include <vector>

namespace live::storage {

class LocalKVEngine {
public:
    ~LocalKVEngine();

    common::Status open(const std::string& snapshot_path, const std::string& wal_path);
    void close();
    common::Status get(const std::string& key, std::string* value) const;
    common::Status set(const std::string& key, const std::string& value);
    common::Status del(const std::string& key);
    // Applies a group of mutations as one WAL record. A torn write is either
    // ignored during recovery or replayed in full; callers never observe a
    // partially applied batch after restart.
    common::Status writeBatch(const std::vector<WalRecord>& mutations);
    common::Status snapshot();
    std::vector<std::pair<std::string, std::string>> scanPrefix(const std::string& prefix) const;

private:
    common::Status apply(const WalRecord& record);

    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::string> data_;
    std::string snapshot_path_;
    Wal wal_;
    SnapshotStore snapshots_;
    int lock_fd_{-1};
};

}  // namespace live::storage
