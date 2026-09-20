#pragma once

#include "common/error/status.h"

#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace live::messaging {

struct Event {
    std::string event_id;
    std::string topic;
    std::string key;
    std::string payload;
    std::uint64_t sequence{0};
};

class IEventPublisher {
public:
    virtual ~IEventPublisher() = default;
    virtual common::Status publish(const Event& event) = 0;
};

class InMemoryBroker final : public IEventPublisher {
public:
    using Handler = std::function<common::Status(const Event&)>;
    common::Status publish(std::string topic, std::string key, std::string payload, Event* event = nullptr);
    common::Status publish(const Event& event) override;
    std::uint64_t subscribe(const std::string& topic, Handler handler,
                            std::size_t max_attempts = 5, std::string dead_letter_topic = {});
    common::Status deliver(const std::string& topic, std::size_t max_events = 0);
    std::size_t pending(const std::string& topic) const;
    std::size_t deadLetterPending(const std::string& topic) const;

private:
    struct Subscription {
        std::uint64_t id;
        Handler handler;
        std::size_t max_attempts{5};
        std::string dead_letter_topic;
    };
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::vector<Event>> events_;
    std::unordered_map<std::string, std::vector<Subscription>> subscriptions_;
    std::unordered_map<std::string, std::size_t> delivery_attempts_;
    std::uint64_t next_sequence_{1};
    std::uint64_t next_subscription_{1};
};

}  // namespace live::messaging
