#pragma once

#include "common/error/status.h"

#include <cstdint>
#include <functional>
#include <string>

namespace live::storage {

enum class WalOperation : std::uint8_t { kSet = 1, kDelete = 2, kBatch = 3 };

struct WalRecord {
    WalOperation operation{WalOperation::kSet};
    std::string key;
    std::string value;
};

class Wal {
public:
    Wal() = default;
    ~Wal();

    common::Status open(const std::string& path);
    common::Status append(const WalRecord& record, bool sync = true);
    common::Status replay(const std::function<common::Status(const WalRecord&)>& apply) const;
    common::Status truncate();
    void close();

private:
    int fd_{-1};
    std::string path_;
};

}  // namespace live::storage
