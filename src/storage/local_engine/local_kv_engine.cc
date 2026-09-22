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
    std::unique_lock<std::shared_mutex> gate(operation_gate_);
    stopWriter();
    {
        std::lock_guard<std::mutex> lock(data_mutex_);
        wal_.close();
        data_.clear();
        snapshot_path_ = snapshot_path;
        opened_ = false;
    }
    if (snapshot_path.empty() || wal_path.empty()) return common::Status::InvalidArgument("storage paths must not be empty");
    lock_fd_ = ::open(wal_path.c_str(), O_CREAT | O_RDWR, 0644);
    if (lock_fd_ < 0) return common::Status::Internal("failed to open storage lock");
    if (::flock(lock_fd_, LOCK_EX | LOCK_NB) != 0) {
        ::close(lock_fd_); lock_fd_ = -1;
        return common::Status::ResourceExhausted("storage is already open by another process");
    }
    auto fail = [this](common::Status status) {
        wal_.close();
        if (lock_fd_ >= 0) { ::flock(lock_fd_, LOCK_UN); ::close(lock_fd_); lock_fd_ = -1; }
        return status;
    };
    {
        std::lock_guard<std::mutex> lock(data_mutex_);
        const auto snapshot_status = snapshots_.load(snapshot_path, &data_);
        if (!snapshot_status.ok() && snapshot_status.code() != common::ErrorCode::kNotFound) return fail(snapshot_status);
        auto status = wal_.open(wal_path);
        if (!status.ok()) return fail(status);
        status = wal_.replay([this](const WalRecord& record) { return apply(record); });
        if (!status.ok()) return fail(status);
        opened_ = true;
    }
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        writer_stop_ = false;
        writer_error_ = common::Status::Ok();
    }
    writer_ = std::thread(&LocalKVEngine::runWriter, this);
    return common::Status::Ok();
}

void LocalKVEngine::close() {
    std::unique_lock<std::shared_mutex> gate(operation_gate_);
    stopWriter();
    std::lock_guard<std::mutex> lock(data_mutex_);
    wal_.close();
    opened_ = false;
    if (lock_fd_ >= 0) { ::flock(lock_fd_, LOCK_UN); ::close(lock_fd_); lock_fd_ = -1; }
}

void LocalKVEngine::stopWriter() {
    if (!writer_.joinable()) return;
    auto completion = std::make_shared<std::promise<common::Status>>();
    auto future = completion->get_future();
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        queue_.push_back({{}, completion, true});
        writer_stop_ = true;
    }
    queue_condition_.notify_one();
    (void)future.get();
    if (writer_.joinable()) writer_.join();
    std::lock_guard<std::mutex> lock(queue_mutex_);
    queue_.clear();
}

common::Status LocalKVEngine::submit(std::vector<WalRecord> records) {
    auto completion = std::make_shared<std::promise<common::Status>>();
    auto future = completion->get_future();
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (!opened_) return common::Status::FailedPrecondition("local KV is not open");
        if (!writer_error_.ok()) return writer_error_;
        queue_.push_back({std::move(records), completion, false});
    }
    queue_condition_.notify_one();
    return future.get();
}

common::Status LocalKVEngine::submitBarrier() {
    auto completion = std::make_shared<std::promise<common::Status>>();
    auto future = completion->get_future();
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (!opened_) return common::Status::FailedPrecondition("local KV is not open");
        queue_.push_back({{}, completion, true});
    }
    queue_condition_.notify_one();
    return future.get();
}

void LocalKVEngine::runWriter() {
    constexpr std::size_t kMaxBatch = 64;
    while (true) {
        std::vector<WriteRequest> batch;
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_condition_.wait(lock, [this] { return writer_stop_ || !queue_.empty(); });
            if (queue_.empty() && writer_stop_) return;
            batch.push_back(std::move(queue_.front()));
            queue_.pop_front();
            queue_condition_.wait_for(lock, std::chrono::milliseconds(1), [this] {
                return queue_.size() >= kMaxBatch || writer_stop_;
            });
            while (!queue_.empty() && batch.size() < kMaxBatch) {
                batch.push_back(std::move(queue_.front()));
                queue_.pop_front();
                if (batch.back().barrier) break;
            }
        }

        std::vector<WalRecord> records;
        for (const auto& request : batch) records.insert(records.end(), request.records.begin(), request.records.end());
        common::Status status = records.empty() ? common::Status::Ok() : wal_.appendBatch(records, true);
        if (status.ok()) {
            std::lock_guard<std::mutex> lock(data_mutex_);
            for (const auto& request : batch) {
                for (const auto& record : request.records) {
                    if (!(status = apply(record)).ok()) break;
                }
                if (!status.ok()) break;
            }
        }
        if (!status.ok()) {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            writer_error_ = status;
        }
        for (const auto& request : batch) request.completion->set_value(status);
        bool should_stop = false;
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            should_stop = writer_stop_ && queue_.empty();
        }
        if (should_stop) return;
    }
}

common::Status LocalKVEngine::get(const std::string& key, std::string* value) const {
    if (value == nullptr || key.empty()) return common::Status::InvalidArgument("invalid local KV get");
    std::lock_guard<std::mutex> lock(data_mutex_);
    const auto it = data_.find(key);
    if (it == data_.end()) return common::Status::NotFound("key not found");
    *value = it->second;
    return common::Status::Ok();
}

common::Status LocalKVEngine::set(const std::string& key, const std::string& value) {
    if (key.empty()) return common::Status::InvalidArgument("key must not be empty");
    std::shared_lock<std::shared_mutex> gate(operation_gate_);
    return submit({{WalOperation::kSet, key, value}});
}

common::Status LocalKVEngine::del(const std::string& key) {
    if (key.empty()) return common::Status::InvalidArgument("key must not be empty");
    std::shared_lock<std::shared_mutex> gate(operation_gate_);
    return submit({{WalOperation::kDelete, key, {}}});
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
    std::shared_lock<std::shared_mutex> gate(operation_gate_);
    return submit({{WalOperation::kBatch, "__batch__", encoded}});
}

common::Status LocalKVEngine::snapshot() {
    std::unique_lock<std::shared_mutex> gate(operation_gate_);
    if (const auto status = submitBarrier(); !status.ok()) return status;
    std::lock_guard<std::mutex> lock(data_mutex_);
    if (const auto status = snapshots_.save(snapshot_path_, data_); !status.ok()) return status;
    return wal_.truncate();
}

std::vector<std::pair<std::string, std::string>> LocalKVEngine::scanPrefix(const std::string& prefix) const {
    std::vector<std::pair<std::string, std::string>> result;
    std::lock_guard<std::mutex> lock(data_mutex_);
    for (const auto& [key, value] : data_) if (key.compare(0, prefix.size(), prefix) == 0) result.emplace_back(key, value);
    return result;
}

common::Status LocalKVEngine::apply(const WalRecord& record) {
    if (record.operation == WalOperation::kSet) data_[record.key] = record.value;
    else if (record.operation == WalOperation::kDelete) data_.erase(record.key);
    else if (record.operation == WalOperation::kBatch) {
        std::size_t offset = 0;
        std::uint32_t count = 0;
        if (record.key != "__batch__" || !readU32(record.value, &offset, &count) ||
            count == 0 || count > 100000) {
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
                !readBytes(record.value, &offset, &key) ||
                !readBytes(record.value, &offset, &value) || key.empty()) {
                return common::Status::Internal("invalid WAL batch mutation");
            }
            mutations.push_back({operation, std::move(key), std::move(value)});
        }
        if (offset != record.value.size()) return common::Status::Internal("trailing bytes in WAL batch");
        for (const auto& mutation : mutations) {
            if (mutation.operation == WalOperation::kSet) data_[mutation.key] = mutation.value;
            else data_.erase(mutation.key);
        }
    } else return common::Status::Internal("unknown WAL operation");
    return common::Status::Ok();
}

}  // namespace live::storage
