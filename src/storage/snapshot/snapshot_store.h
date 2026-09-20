#pragma once

#include "common/error/status.h"

#include <string>
#include <unordered_map>

namespace live::storage {

class SnapshotStore {
public:
    common::Status save(const std::string& path, const std::unordered_map<std::string, std::string>& data) const;
    common::Status load(const std::string& path, std::unordered_map<std::string, std::string>* data) const;
};

}  // namespace live::storage
