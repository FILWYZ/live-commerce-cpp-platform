#include "storage/snapshot/snapshot_store.h"

#include "common/serialization/checksum.h"

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iterator>
#include <limits>
#include <unistd.h>
#include <utility>

namespace live::storage {
namespace {

constexpr std::uint32_t kMagic = 0x4c565332;  // LVS2
constexpr std::uint32_t kMaxKeySize = 16 * 1024 * 1024;
constexpr std::uint32_t kMaxValueSize = 64 * 1024 * 1024;

template <typename T>
bool writeValue(std::ofstream& output, const T& value) {
    output.write(reinterpret_cast<const char*>(&value), sizeof(value));
    return output.good();
}

template <typename T>
bool readValue(std::ifstream& input, T* value) {
    input.read(reinterpret_cast<char*>(value), sizeof(*value));
    return input.good();
}

}  // namespace

common::Status SnapshotStore::save(const std::string& path,
                                   const std::unordered_map<std::string, std::string>& data) const {
    if (path.empty()) return common::Status::InvalidArgument("snapshot path must not be empty");
    const std::string temporary = path + ".tmp";
    std::ofstream output(temporary, std::ios::binary | std::ios::trunc);
    if (!output) return common::Status::Internal("failed to create snapshot");
    const std::uint32_t magic = kMagic;
    const std::uint64_t count = data.size();
    std::string body;
    body.reserve(sizeof(count) + data.size() * (sizeof(std::uint32_t) * 2));
    const auto appendValue = [&body](const auto& value) {
        body.append(reinterpret_cast<const char*>(&value), sizeof(value));
    };
    appendValue(count);
    for (const auto& [key, value] : data) {
        if (key.size() > kMaxKeySize || value.size() > kMaxValueSize) {
            return common::Status::InvalidArgument("snapshot value is too large");
        }
        const auto key_size = static_cast<std::uint32_t>(key.size());
        const auto value_size = static_cast<std::uint32_t>(value.size());
        appendValue(key_size);
        appendValue(value_size);
        body.append(key);
        body.append(value);
    }
    const auto checksum = ::live::common::serialization::crc32(body.data(), body.size());
    if (!writeValue(output, magic) || !output.write(body.data(), static_cast<std::streamsize>(body.size())) ||
        !writeValue(output, checksum)) return common::Status::Internal("failed to write snapshot");
    output.close();
    if (!output) return common::Status::Internal("failed to flush snapshot");
    const int fd = ::open(temporary.c_str(), O_RDONLY);
    if (fd < 0 || ::fsync(fd) != 0) {
        if (fd >= 0) ::close(fd);
        return common::Status::Internal("failed to fsync snapshot");
    }
    ::close(fd);
    if (std::rename(temporary.c_str(), path.c_str()) != 0) return common::Status::Internal("failed to publish snapshot");
    return common::Status::Ok();
}

common::Status SnapshotStore::load(const std::string& path,
                                   std::unordered_map<std::string, std::string>* data) const {
    if (data == nullptr || path.empty()) return common::Status::InvalidArgument("invalid snapshot arguments");
    std::ifstream input(path, std::ios::binary);
    if (!input) return common::Status::NotFound("snapshot not found");
    std::uint32_t magic = 0;
    if (!readValue(input, &magic) || magic != kMagic) {
        return common::Status::Internal("corrupt snapshot header");
    }
    const std::string body((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
    if (body.size() < sizeof(std::uint64_t) + sizeof(std::uint32_t)) {
        return common::Status::Internal("truncated snapshot");
    }
    std::uint32_t stored_checksum = 0;
    std::memcpy(&stored_checksum, body.data() + body.size() - sizeof(stored_checksum), sizeof(stored_checksum));
    const std::string payload = body.substr(0, body.size() - sizeof(stored_checksum));
    if (::live::common::serialization::crc32(payload.data(), payload.size()) != stored_checksum) {
        return common::Status::Internal("corrupt snapshot checksum");
    }
    std::size_t offset = 0;
    auto readPayload = [&payload, &offset](void* destination, std::size_t size) {
        if (destination == nullptr || offset + size > payload.size()) return false;
        std::memcpy(destination, payload.data() + offset, size);
        offset += size;
        return true;
    };
    std::uint64_t count = 0;
    if (!readPayload(&count, sizeof(count)) || count > 10'000'000) {
        return common::Status::Internal("corrupt snapshot header");
    }
    data->clear();
    for (std::uint64_t i = 0; i < count; ++i) {
        std::uint32_t key_size = 0;
        std::uint32_t value_size = 0;
        if (!readPayload(&key_size, sizeof(key_size)) || !readPayload(&value_size, sizeof(value_size)) ||
            key_size > kMaxKeySize || value_size > kMaxValueSize) {
            return common::Status::Internal("corrupt snapshot record");
        }
        std::string key(key_size, '\0');
        std::string value(value_size, '\0');
        if (!readPayload(key.data(), key.size()) || !readPayload(value.data(), value.size())) {
            return common::Status::Internal("truncated snapshot");
        }
        (*data)[std::move(key)] = std::move(value);
    }
    if (offset != payload.size()) return common::Status::Internal("trailing snapshot bytes");
    return common::Status::Ok();
}

}  // namespace live::storage
