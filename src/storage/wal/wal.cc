#include "storage/wal/wal.h"

#include "common/serialization/checksum.h"

#include <cerrno>
#include <fcntl.h>
#include <limits>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

namespace live::storage {
namespace {

constexpr std::uint32_t kMagic = 0x4c564b32;  // LVK2
constexpr std::uint8_t kVersion = 1;
constexpr std::uint32_t kMaxKeySize = 16 * 1024 * 1024;
constexpr std::uint32_t kMaxValueSize = 64 * 1024 * 1024;

bool writeAll(int fd, const void* data, std::size_t size) {
    const auto* bytes = static_cast<const char*>(data);
    while (size > 0) {
        const ssize_t written = ::write(fd, bytes, size);
        if (written < 0 && errno == EINTR) continue;
        if (written <= 0) return false;
        bytes += written;
        size -= static_cast<std::size_t>(written);
    }
    return true;
}

bool readAll(int fd, void* data, std::size_t size) {
    auto* bytes = static_cast<char*>(data);
    while (size > 0) {
        const ssize_t received = ::read(fd, bytes, size);
        if (received < 0 && errno == EINTR) continue;
        if (received <= 0) return false;
        bytes += received;
        size -= static_cast<std::size_t>(received);
    }
    return true;
}

}  // namespace

Wal::~Wal() { close(); }

common::Status Wal::open(const std::string& path) {
    close();
    if (path.empty()) return common::Status::InvalidArgument("WAL path must not be empty");
    fd_ = ::open(path.c_str(), O_CREAT | O_APPEND | O_RDWR, 0644);
    if (fd_ < 0) return common::Status::Internal("failed to open WAL");
    path_ = path;
    return common::Status::Ok();
}

common::Status Wal::append(const WalRecord& record, bool sync) {
    if (fd_ < 0 || record.key.empty()) return common::Status::FailedPrecondition("WAL is not open");
    if (record.key.size() > UINT32_MAX || record.value.size() > UINT32_MAX) {
        return common::Status::InvalidArgument("WAL record is too large");
    }
    const std::uint32_t magic = kMagic;
    const std::uint8_t operation = static_cast<std::uint8_t>(record.operation);
    const std::uint8_t version = kVersion;
    const std::uint8_t reserved = 0;
    const std::uint32_t key_size = static_cast<std::uint32_t>(record.key.size());
    const std::uint32_t value_size = static_cast<std::uint32_t>(record.value.size());
    std::vector<std::uint8_t> checksum_input;
    checksum_input.reserve(sizeof(version) + sizeof(operation) + sizeof(reserved) +
                           sizeof(key_size) + sizeof(value_size) + record.key.size() + record.value.size());
    checksum_input.insert(checksum_input.end(), &version, &version + sizeof(version));
    checksum_input.insert(checksum_input.end(), &operation, &operation + sizeof(operation));
    checksum_input.insert(checksum_input.end(), &reserved, &reserved + sizeof(reserved));
    const auto appendBytes = [&checksum_input](const auto& value) {
        const auto* bytes = reinterpret_cast<const std::uint8_t*>(&value);
        checksum_input.insert(checksum_input.end(), bytes, bytes + sizeof(value));
    };
    appendBytes(key_size);
    appendBytes(value_size);
    checksum_input.insert(checksum_input.end(), record.key.begin(), record.key.end());
    checksum_input.insert(checksum_input.end(), record.value.begin(), record.value.end());
    const std::uint32_t checksum = ::live::common::serialization::crc32(checksum_input.data(), checksum_input.size());
    if (!writeAll(fd_, &magic, sizeof(magic)) || !writeAll(fd_, &version, sizeof(version)) ||
        !writeAll(fd_, &operation, sizeof(operation)) || !writeAll(fd_, &reserved, sizeof(reserved)) ||
        !writeAll(fd_, &key_size, sizeof(key_size)) || !writeAll(fd_, &value_size, sizeof(value_size)) ||
        !writeAll(fd_, record.key.data(), record.key.size()) || !writeAll(fd_, record.value.data(), record.value.size()) ||
        !writeAll(fd_, &checksum, sizeof(checksum))) {
        return common::Status::Internal("failed to append WAL record");
    }
    if (sync && ::fsync(fd_) != 0) return common::Status::Internal("failed to fsync WAL");
    return common::Status::Ok();
}

common::Status Wal::appendBatch(const std::vector<WalRecord>& records, bool sync) {
    if (records.empty()) return common::Status::InvalidArgument("WAL batch must not be empty");
    for (const auto& record : records) {
        if (const auto status = append(record, false); !status.ok()) return status;
    }
    if (sync && ::fsync(fd_) != 0) return common::Status::Internal("failed to fsync WAL batch");
    return common::Status::Ok();
}

common::Status Wal::replay(const std::function<common::Status(const WalRecord&)>& apply) const {
    if (fd_ < 0 || !apply) return common::Status::InvalidArgument("invalid WAL replay arguments");
    const int read_fd = ::open(path_.c_str(), O_RDONLY);
    if (read_fd < 0) return common::Status::Internal("failed to open WAL for replay");
    while (true) {
        const off_t record_start = ::lseek(read_fd, 0, SEEK_CUR);
        std::uint32_t magic = 0;
        const ssize_t first = ::read(read_fd, &magic, sizeof(magic));
        if (first == 0) break;
        if (first != static_cast<ssize_t>(sizeof(magic))) {
            // A process may crash after writing only part of the final
            // record. Earlier complete records are still safe to replay.
            ::ftruncate(fd_, record_start);
            ::fsync(fd_);
            break;
        }
        if (magic != kMagic) {
            ::close(read_fd);
            return common::Status::Internal("corrupt WAL header");
        }
        std::uint8_t version = 0;
        std::uint8_t operation = 0;
        std::uint8_t reserved = 0;
        std::uint32_t key_size = 0;
        std::uint32_t value_size = 0;
        if (!readAll(read_fd, &version, sizeof(version)) || !readAll(read_fd, &operation, sizeof(operation)) ||
            !readAll(read_fd, &reserved, sizeof(reserved)) || !readAll(read_fd, &key_size, sizeof(key_size)) ||
            !readAll(read_fd, &value_size, sizeof(value_size))) {
            ::ftruncate(fd_, record_start);
            ::fsync(fd_);
            ::close(read_fd);
            break;
        }
        if (version != kVersion || reserved != 0 || key_size > kMaxKeySize || value_size > kMaxValueSize) {
            ::close(read_fd);
            return common::Status::Internal("corrupt WAL record");
        }
        WalRecord record;
        record.operation = static_cast<WalOperation>(operation);
        record.key.resize(key_size);
        record.value.resize(value_size);
        if (!readAll(read_fd, record.key.data(), key_size) || !readAll(read_fd, record.value.data(), value_size)) {
            ::ftruncate(fd_, record_start);
            ::fsync(fd_);
            ::close(read_fd);
            break;
        }
        std::uint32_t stored_checksum = 0;
        if (!readAll(read_fd, &stored_checksum, sizeof(stored_checksum))) {
            ::ftruncate(fd_, record_start);
            ::fsync(fd_);
            ::close(read_fd);
            break;
        }
        std::vector<std::uint8_t> checksum_input;
        checksum_input.reserve(sizeof(version) + sizeof(operation) + sizeof(reserved) +
                               sizeof(key_size) + sizeof(value_size) + record.key.size() + record.value.size());
        checksum_input.insert(checksum_input.end(), &version, &version + sizeof(version));
        checksum_input.insert(checksum_input.end(), &operation, &operation + sizeof(operation));
        checksum_input.insert(checksum_input.end(), &reserved, &reserved + sizeof(reserved));
        const auto appendBytes = [&checksum_input](const auto& value) {
            const auto* bytes = reinterpret_cast<const std::uint8_t*>(&value);
            checksum_input.insert(checksum_input.end(), bytes, bytes + sizeof(value));
        };
        appendBytes(key_size);
        appendBytes(value_size);
        checksum_input.insert(checksum_input.end(), record.key.begin(), record.key.end());
        checksum_input.insert(checksum_input.end(), record.value.begin(), record.value.end());
        if (::live::common::serialization::crc32(checksum_input.data(), checksum_input.size()) != stored_checksum) {
            ::close(read_fd);
            return common::Status::Internal("corrupt WAL checksum");
        }
        const auto status = apply(record);
        if (!status.ok()) {
            ::close(read_fd);
            return status;
        }
    }
    ::close(read_fd);
    return common::Status::Ok();
}

common::Status Wal::truncate() {
    if (fd_ < 0) return common::Status::FailedPrecondition("WAL is not open");
    if (::ftruncate(fd_, 0) != 0 || ::lseek(fd_, 0, SEEK_END) < 0 || ::fsync(fd_) != 0) {
        return common::Status::Internal("failed to truncate WAL");
    }
    return common::Status::Ok();
}

void Wal::close() {
    if (fd_ >= 0) ::close(fd_);
    fd_ = -1;
    path_.clear();
}

}  // namespace live::storage
