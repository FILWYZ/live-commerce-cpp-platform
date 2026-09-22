#pragma once

#include "common/serialization/checksum.h"
#include "messaging/in_memory_broker.h"

#include <functional>
#include <fstream>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <fcntl.h>
#include <mutex>
#include <cstring>
#include <string>
#include <string_view>
#include <unistd.h>
#include <utility>
#include <vector>

namespace live::messaging {

class TransactionalOutbox {
public:
    void append(Event event) {
        std::lock_guard<std::mutex> lock(mutex_);
        pending_.push_back(std::move(event));
    }

    common::Status drain(const std::function<common::Status(const Event&)>& publish,
                         std::size_t limit = 128) {
        if (!publish) return common::Status::InvalidArgument("outbox publisher must not be empty");
        if (limit == 0) return common::Status::InvalidArgument("outbox drain limit must be positive");
        std::vector<Event> batch;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            const auto count = std::min(limit, pending_.size());
            batch.reserve(count);
            for (std::size_t i = 0; i < count; ++i) batch.push_back(std::move(pending_[i]));
            pending_.erase(pending_.begin(), pending_.begin() + static_cast<std::ptrdiff_t>(count));
        }
        std::vector<Event> failed;
        for (const auto& event : batch) {
            const auto status = publish(event);
            if (!status.ok()) failed.push_back(event);
        }
        if (!failed.empty()) {
            std::lock_guard<std::mutex> lock(mutex_);
            pending_.insert(pending_.begin(), failed.begin(), failed.end());
            return common::Status::Internal("outbox publish partially failed");
        }
        return common::Status::Ok();
    }

    std::size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return pending_.size();
    }

private:
    mutable std::mutex mutex_;
    std::vector<Event> pending_;
};

// A small durable outbox used by the single-process service. Each record has
// length framing and a CRC so a torn/corrupted event file is rejected instead
// of being silently interpreted as a valid business event.
class FileOutbox {
public:
    common::Status open(std::string path) {
        if (path.empty()) return common::Status::InvalidArgument("outbox path must not be empty");
        std::lock_guard<std::mutex> lock(mutex_);
        path_ = std::move(path);
        pending_.clear();
        std::ifstream input(path_, std::ios::binary);
        if (!input.good()) {
            std::ofstream create(path_, std::ios::binary);
            return create.good() ? common::Status::Ok() : common::Status::Internal("cannot create outbox file");
        }
        while (true) {
            Event event;
            bool clean_eof = false;
            if (!readRecord(input, &event, &clean_eof)) {
                if (clean_eof) break;
                return common::Status::Internal("corrupted outbox record");
            }
            pending_.push_back(std::move(event));
        }
        return common::Status::Ok();
    }

    common::Status append(const Event& event) {
        if (event.topic.empty()) return common::Status::InvalidArgument("outbox topic must not be empty");
        std::lock_guard<std::mutex> lock(mutex_);
        if (path_.empty()) return common::Status::FailedPrecondition("outbox is not open");
        Event durable = event;
        if (durable.event_id.empty()) {
            durable.event_id = durable.topic + "-" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
                "-" + std::to_string(pending_.size());
        }
        std::ofstream output(path_, std::ios::binary | std::ios::app);
        if (!output.good() || !writeRecord(output, durable)) {
            return common::Status::Internal("cannot append outbox record");
        }
        output.flush();
        if (!output.good()) return common::Status::Internal("cannot flush outbox record");
        if (!syncFile(path_)) return common::Status::Internal("cannot fsync outbox record");
        pending_.push_back(std::move(durable));
        return common::Status::Ok();
    }

    common::Status drain(const std::function<common::Status(const Event&)>& publish,
                         std::size_t limit = 128) {
        if (!publish) return common::Status::InvalidArgument("outbox publisher must not be empty");
        if (limit == 0) return common::Status::InvalidArgument("outbox drain limit must be positive");
        std::vector<Event> batch;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            const auto count = std::min(limit, pending_.size());
            batch.reserve(count);
            for (std::size_t i = 0; i < count; ++i) batch.push_back(std::move(pending_[i]));
            pending_.erase(pending_.begin(), pending_.begin() + static_cast<std::ptrdiff_t>(count));
        }
        std::vector<Event> failed;
        for (const auto& event : batch) if (!publish(event).ok()) failed.push_back(event);
        std::lock_guard<std::mutex> lock(mutex_);
        pending_.insert(pending_.begin(), failed.begin(), failed.end());
        const auto status = rewriteLocked();
        if (!status.ok()) return status;
        return failed.empty() ? common::Status::Ok() : common::Status::Internal("outbox publish partially failed");
    }

    std::size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return pending_.size();
    }

private:
    static constexpr std::uint32_t kMagic = 0x4f425832;  // OBX2
    static constexpr std::uint32_t kMaxRecordSize = 64 * 1024 * 1024;

    static bool appendField(std::string* output, const std::string& value) {
        if (output == nullptr || value.size() > 16 * 1024 * 1024) return false;
        const auto size = static_cast<std::uint32_t>(value.size());
        output->append(reinterpret_cast<const char*>(&size), sizeof(size));
        output->append(value);
        return true;
    }
    static bool appendNumber(std::string* output, std::uint64_t value) {
        if (output == nullptr) return false;
        output->append(reinterpret_cast<const char*>(&value), sizeof(value));
        return true;
    }
    static bool readField(std::string_view input, std::size_t* offset, std::string* value) {
        if (offset == nullptr || value == nullptr || *offset + sizeof(std::uint32_t) > input.size()) return false;
        std::uint32_t size = 0;
        std::memcpy(&size, input.data() + *offset, sizeof(size));
        *offset += sizeof(size);
        if (size > 16 * 1024 * 1024 || *offset + size > input.size()) return false;
        value->resize(size);
        std::memcpy(value->data(), input.data() + *offset, size);
        *offset += size;
        return true;
    }
    static bool readNumber(std::string_view input, std::size_t* offset, std::uint64_t* value) {
        if (offset == nullptr || value == nullptr || *offset + sizeof(*value) > input.size()) return false;
        std::memcpy(value, input.data() + *offset, sizeof(*value));
        *offset += sizeof(*value);
        return true;
    }
    static bool encodeEvent(const Event& event, std::string* payload) {
        return appendField(payload, event.event_id) && appendField(payload, event.topic) &&
               appendField(payload, event.key) && appendField(payload, event.payload) &&
               appendNumber(payload, event.sequence);
    }
    static bool decodeEvent(std::string_view payload, Event* event) {
        if (event == nullptr) return false;
        std::size_t offset = 0;
        return readField(payload, &offset, &event->event_id) && readField(payload, &offset, &event->topic) &&
               readField(payload, &offset, &event->key) && readField(payload, &offset, &event->payload) &&
               readNumber(payload, &offset, &event->sequence) && offset == payload.size() && !event->topic.empty();
    }
    static bool writeRecord(std::ofstream& output, const Event& event) {
        std::string payload;
        if (!encodeEvent(event, &payload) || payload.size() > kMaxRecordSize) return false;
        const auto size = static_cast<std::uint32_t>(payload.size());
        const auto checksum = ::live::common::serialization::crc32(payload.data(), payload.size());
        output.write(reinterpret_cast<const char*>(&kMagic), sizeof(kMagic));
        output.write(reinterpret_cast<const char*>(&size), sizeof(size));
        output.write(payload.data(), static_cast<std::streamsize>(payload.size()));
        output.write(reinterpret_cast<const char*>(&checksum), sizeof(checksum));
        return output.good();
    }
    static bool readRecord(std::ifstream& input, Event* event, bool* clean_eof) {
        if (clean_eof != nullptr) *clean_eof = false;
        std::uint32_t magic = 0;
        input.read(reinterpret_cast<char*>(&magic), sizeof(magic));
        if (input.eof() && input.gcount() == 0) {
            if (clean_eof != nullptr) *clean_eof = true;
            return false;
        }
        std::uint32_t size = 0;
        if (!input.good() || magic != kMagic || !input.read(reinterpret_cast<char*>(&size), sizeof(size)) ||
            size > kMaxRecordSize) return false;
        std::string payload(size, '\0');
        std::uint32_t checksum = 0;
        if (!input.read(payload.data(), static_cast<std::streamsize>(payload.size())) ||
            !input.read(reinterpret_cast<char*>(&checksum), sizeof(checksum)) ||
            ::live::common::serialization::crc32(payload.data(), payload.size()) != checksum) return false;
        return decodeEvent(payload, event);
    }
    common::Status rewriteLocked() const {
        if (path_.empty()) return common::Status::FailedPrecondition("outbox is not open");
        const std::string temp = path_ + ".tmp";
        std::ofstream output(temp, std::ios::binary | std::ios::trunc);
        if (!output.good()) return common::Status::Internal("cannot rewrite outbox");
        for (const auto& event : pending_) if (!writeRecord(output, event)) return common::Status::Internal("cannot write outbox snapshot");
        output.flush();
        output.close();
        if (!output || !syncFile(temp) || std::rename(temp.c_str(), path_.c_str()) != 0) return common::Status::Internal("cannot replace outbox file");
        return common::Status::Ok();
    }

    static bool syncFile(const std::string& path) {
        const int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) return false;
        const bool ok = ::fsync(fd) == 0;
        ::close(fd);
        return ok;
    }

    mutable std::mutex mutex_;
    std::string path_;
    std::vector<Event> pending_;
};

class OutboxPublisher final : public IEventPublisher {
public:
    explicit OutboxPublisher(FileOutbox* outbox) : outbox_(outbox) {}

    common::Status publish(const Event& event) override {
        if (outbox_ == nullptr) return common::Status::FailedPrecondition("outbox publisher is not configured");
        return outbox_->append(event);
    }

private:
    FileOutbox* outbox_;
};

}  // namespace live::messaging
