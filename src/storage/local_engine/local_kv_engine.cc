#include "storage/local_engine/local_kv_engine.h"

#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

namespace {

void appendU32(std::string* output, std::uint32_t value) {
    output->append(reinterpret_cast<const char*>(&value), sizeof(value));
}

bool readU32(const std::string& input, std::size_t* offset, std::uint32_t* value) {
    if (offset == nullptr || value == nullptr || *offset + sizeof(*value) > input.size()) return false;
    std::memcpy(value, input.data() + *offset, sizeof(*value));
    *offset += sizeof(*value);
    return true;
}

bool appendBytes(std::string* output, const std::string& value) {
    if (value.size() > UINT32_MAX) return false;
    appendU32(output, static_cast<std::uint32_t>(value.size()));
    output->append(value);
    return true;
}

bool readBytes(const std::string& input, std::size_t* offset, std::string* value) {
    std::uint32_t size = 0;
    if (!readU32(input, offset, &size) || *offset + size > input.size()) return false;
    value->assign(input.data() + *offset, size);
    *offset += size;
    return true;
}

}  // namespace

namespace live::storage {

LocalKVEngine::~LocalKVEngine() { close(); }

common::Status LocalKVEngine::open(const std::string& snapshot_path, const std::string& wal_path) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (lock_fd_ >= 0) {
        ::flock(lock_fd_, LOCK_UN);
        ::close(lock_fd_);
        lock_fd_ = -1;
    }
    wal_.close();
    data_.clear();
    snapshot_path_ = snapshot_path;
    if (snapshot_path.empty() || wal_path.empty()) return common::Status::InvalidArgument("storage paths must not be empty");
    lock_fd_ = ::open(wal_path.c_str(), O_CREAT | O_RDWR, 0644);
    if (lock_fd_ < 0) return common::Status::Internal("failed to open storage lock");
    if (::flock(lock_fd_, LOCK_EX | LOCK_NB) != 0) {
        ::close(lock_fd_);
        lock_fd_ = -1;
        return common::Status::ResourceExhausted("storage is already open by another process");
    }
    const auto snapshot_status = snapshots_.load(snapshot_path, &data_);
    if (!snapshot_status.ok() && snapshot_status.code() != common::ErrorCode::kNotFound) {
        ::flock(lock_fd_, LOCK_UN);
        ::close(lock_fd_);
        lock_fd_ = -1;
        return snapshot_status;
    }
    auto status = wal_.open(wal_path);
    if (!status.ok()) {
        ::flock(lock_fd_, LOCK_UN);
        ::close(lock_fd_);
        lock_fd_ = -1;
        return status;
    }
    status = wal_.replay([this](const WalRecord& record) { return apply(record); });
    if (!status.ok()) {
        wal_.close();
        ::flock(lock_fd_, LOCK_UN);
        ::close(lock_fd_);
        lock_fd_ = -1;
    }
    return status;
}

void LocalKVEngine::close() {
    std::lock_guard<std::mutex> lock(mutex_);
    wal_.close();
    if (lock_fd_ >= 0) {
        ::flock(lock_fd_, LOCK_UN);
        ::close(lock_fd_);
        lock_fd_ = -1;
    }
}

common::Status LocalKVEngine::get(const std::string& key, std::string* value) const {
    if (value == nullptr || key.empty()) return common::Status::InvalidArgument("invalid local KV get");
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = data_.find(key);
    if (it == data_.end()) return common::Status::NotFound("key not found");
    *value = it->second;
    return common::Status::Ok();
}

common::Status LocalKVEngine::set(const std::string& key, const std::string& value) {
    if (key.empty()) return common::Status::InvalidArgument("key must not be empty");
    std::lock_guard<std::mutex> lock(mutex_);
    auto status = wal_.append({WalOperation::kSet, key, value});
    if (!status.ok()) return status;
    data_[key] = value;
    return common::Status::Ok();
}

common::Status LocalKVEngine::del(const std::string& key) {
    if (key.empty()) return common::Status::InvalidArgument("key must not be empty");
    std::lock_guard<std::mutex> lock(mutex_);
    auto status = wal_.append({WalOperation::kDelete, key, {}});
    if (!status.ok()) return status;
    data_.erase(key);
    return common::Status::Ok();
}

common::Status LocalKVEngine::writeBatch(const std::vector<WalRecord>& mutations) {
    if (mutations.empty() || mutations.size() > UINT32_MAX) return common::Status::InvalidArgument("invalid WAL batch size");
    std::string encoded;
    appendU32(&encoded, static_cast<std::uint32_t>(mutations.size()));
    for (const auto& mutation : mutations) {
        if ((mutation.operation != WalOperation::kSet && mutation.operation != WalOperation::kDelete) ||
            mutation.key.empty() || mutation.key.size() > 16 * 1024 * 1024 || mutation.value.size() > 64 * 1024 * 1024) {
            return common::Status::InvalidArgument("invalid WAL batch mutation");
        }
        encoded.push_back(static_cast<char>(mutation.operation));
        if (!appendBytes(&encoded, mutation.key) || !appendBytes(&encoded, mutation.value)) {
            return common::Status::InvalidArgument("WAL batch mutation is too large");
        }
    }
    std::lock_guard<std::mutex> lock(mutex_);
    const auto status = wal_.append({WalOperation::kBatch, "__batch__", encoded});
    if (!status.ok()) return status;
    for (const auto& mutation : mutations) {
        if (mutation.operation == WalOperation::kSet) data_[mutation.key] = mutation.value;
        else data_.erase(mutation.key);
    }
    return common::Status::Ok();
}

common::Status LocalKVEngine::snapshot() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (const auto status = snapshots_.save(snapshot_path_, data_); !status.ok()) return status;
    return wal_.truncate();
}

std::vector<std::pair<std::string, std::string>> LocalKVEngine::scanPrefix(const std::string& prefix) const {
    std::vector<std::pair<std::string, std::string>> result;
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [key, value] : data_) {
        if (key.compare(0, prefix.size(), prefix) == 0) result.emplace_back(key, value);
    }
    return result;
}

common::Status LocalKVEngine::apply(const WalRecord& record) {
    if (record.operation == WalOperation::kSet) data_[record.key] = record.value;
    else if (record.operation == WalOperation::kDelete) data_.erase(record.key);
    else if (record.operation == WalOperation::kBatch) {
        std::size_t offset = 0;
        std::uint32_t count = 0;
        if (record.key != "__batch__" || !readU32(record.value, &offset, &count) || count == 0 || count > 100000) {
            return common::Status::Internal("invalid WAL batch");
        }
        std::vector<WalRecord> mutations;
        mutations.reserve(count);
        for (std::uint32_t i = 0; i < count; ++i) {
            if (offset >= record.value.size()) return common::Status::Internal("truncated WAL batch");
            const auto operation = static_cast<WalOperation>(static_cast<std::uint8_t>(record.value[offset++]));
            std::string key;
            std::string value;
            if ((operation != WalOperation::kSet && operation != WalOperation::kDelete) ||
                !readBytes(record.value, &offset, &key) || !readBytes(record.value, &offset, &value) || key.empty()) {
                return common::Status::Internal("invalid WAL batch mutation");
            }
            mutations.push_back({operation, std::move(key), std::move(value)});
        }
        if (offset != record.value.size()) return common::Status::Internal("trailing bytes in WAL batch");
        for (const auto& mutation : mutations) {
            if (mutation.operation == WalOperation::kSet) data_[mutation.key] = mutation.value;
            else data_.erase(mutation.key);
        }
    }
    else return common::Status::Internal("unknown WAL operation");
    return common::Status::Ok();
}

}  // namespace live::storage
