#include "messaging/in_memory_broker.h"

#include <algorithm>
#include <sstream>

namespace live::messaging {

common::Status InMemoryBroker::publish(std::string topic, std::string key, std::string payload, Event* event) {
    if (topic.empty()) return common::Status::InvalidArgument("topic must not be empty");
    Event item{"", std::move(topic), std::move(key), std::move(payload), 0};
    const auto status = publish(item);
    if (status.ok() && event != nullptr) *event = item;
    return status;
}

common::Status InMemoryBroker::publish(const Event& event) {
    if (event.topic.empty()) return common::Status::InvalidArgument("topic must not be empty");
    std::lock_guard<std::mutex> lock(mutex_);
    Event item = event;
    item.event_id = event.event_id.empty() ? event.topic + "-" + std::to_string(next_sequence_) : event.event_id;
    item.sequence = next_sequence_++;
    events_[item.topic].push_back(item);
    return common::Status::Ok();
}

std::uint64_t InMemoryBroker::subscribe(const std::string& topic, Handler handler,
                                        std::size_t max_attempts, std::string dead_letter_topic) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto id = next_subscription_++;
    if (dead_letter_topic.empty()) dead_letter_topic = topic + ".DLQ";
    subscriptions_[topic].push_back(Subscription{id, std::move(handler), std::max<std::size_t>(1, max_attempts),
                                                   std::move(dead_letter_topic)});
    return id;
}

common::Status InMemoryBroker::deliver(const std::string& topic, std::size_t max_events) {
    std::vector<Event> events;
    std::vector<Subscription> subscriptions;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto event_it = events_.find(topic);
        if (event_it == events_.end()) return common::Status::Ok();
        const std::size_t count = max_events == 0 ? event_it->second.size() : std::min(max_events, event_it->second.size());
        events.assign(event_it->second.begin(), event_it->second.begin() + count);
        subscriptions = subscriptions_[topic];
    }
    if (subscriptions.empty()) return common::Status::FailedPrecondition("no consumer subscribed to topic");
    std::vector<std::string> delivered;
    std::vector<Event> dead_letters;
    common::Status result = common::Status::Ok();
    for (const auto& event : events) {
        bool event_ok = true;
        for (const auto& subscription : subscriptions) {
            if (!subscription.handler) continue;
            const auto status = subscription.handler(event);
            const std::string attempt_key = topic + "|" + std::to_string(subscription.id) + "|" + event.event_id;
            if (status.ok()) {
                std::lock_guard<std::mutex> lock(mutex_);
                delivery_attempts_.erase(attempt_key);
                continue;
            }
            result = common::Status::Internal("message delivery failed; event retained or dead-lettered");
            std::size_t attempts = 0;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                attempts = ++delivery_attempts_[attempt_key];
                if (attempts >= subscription.max_attempts) delivery_attempts_.erase(attempt_key);
            }
            if (attempts >= subscription.max_attempts) {
                Event dead_letter = event;
                dead_letter.topic = subscription.dead_letter_topic;
                dead_letter.event_id = dead_letter.topic + "-" + event.event_id + "-" + std::to_string(attempts);
                dead_letter.sequence = 0;
                dead_letters.push_back(std::move(dead_letter));
            } else {
                event_ok = false;
            }
        }
        if (event_ok) delivered.push_back(event.event_id);
    }
    if (!delivered.empty() || !dead_letters.empty()) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto& queue = events_[topic];
        queue.erase(std::remove_if(queue.begin(), queue.end(), [&](const Event& event) {
            return std::find(delivered.begin(), delivered.end(), event.event_id) != delivered.end();
        }), queue.end());
        for (auto& dead_letter : dead_letters) {
            dead_letter.sequence = next_sequence_++;
            events_[dead_letter.topic].push_back(std::move(dead_letter));
        }
    }
    return result;
}

std::size_t InMemoryBroker::pending(const std::string& topic) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = events_.find(topic);
    return it == events_.end() ? 0 : it->second.size();
}

std::size_t InMemoryBroker::deadLetterPending(const std::string& topic) const {
    return pending(topic + ".DLQ");
}

}  // namespace live::messaging
