#pragma once

#include "common/error/status.h"
#include "messaging/in_memory_broker.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

struct rd_kafka_s;
typedef struct rd_kafka_s rd_kafka_t;

namespace live::messaging {

class KafkaProducer : public IEventPublisher {
public:
    KafkaProducer() = default;
    ~KafkaProducer();
    common::Status start(const std::string& brokers, const std::string& client_id);
    common::Status publish(const Event& event) override;
    common::Status flush(std::uint32_t timeout_ms);
    void stop();

private:
    rd_kafka_t* client_{nullptr};
};

class KafkaConsumer {
public:
    using Handler = std::function<common::Status(const Event&)>;
    KafkaConsumer() = default;
    ~KafkaConsumer();
    common::Status start(const std::string& brokers, const std::string& group_id,
                         const std::vector<std::string>& topics);
    common::Status poll(std::uint32_t timeout_ms, const Handler& handler);
    void stop();

private:
    rd_kafka_t* client_{nullptr};
};

}  // namespace live::messaging
