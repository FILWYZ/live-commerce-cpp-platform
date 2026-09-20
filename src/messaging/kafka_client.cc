#include "messaging/kafka_client.h"

#include <librdkafka/rdkafka.h>

#include <cstring>

namespace live::messaging {
namespace {

common::Status error(const char* message) { return common::Status::Internal(message == nullptr ? "Kafka error" : message); }

}  // namespace

KafkaProducer::~KafkaProducer() { stop(); }

common::Status KafkaProducer::start(const std::string& brokers, const std::string& client_id) {
    stop();
    char error_buffer[256]{};
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers.c_str(), error_buffer, sizeof(error_buffer)) != RD_KAFKA_CONF_OK ||
        rd_kafka_conf_set(conf, "client.id", client_id.c_str(), error_buffer, sizeof(error_buffer)) != RD_KAFKA_CONF_OK ||
        rd_kafka_conf_set(conf, "enable.idempotence", "true", error_buffer, sizeof(error_buffer)) != RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        return error(error_buffer);
    }
    client_ = rd_kafka_new(RD_KAFKA_PRODUCER, conf, error_buffer, sizeof(error_buffer));
    return client_ == nullptr ? error(error_buffer) : common::Status::Ok();
}

common::Status KafkaProducer::publish(const Event& event) {
    if (client_ == nullptr || event.topic.empty()) return common::Status::FailedPrecondition("Kafka producer is not started");
    const auto result = rd_kafka_producev(client_, RD_KAFKA_V_TOPIC(event.topic.c_str()),
        RD_KAFKA_V_KEY(const_cast<char*>(event.key.data()), event.key.size()),
        RD_KAFKA_V_VALUE(const_cast<char*>(event.payload.data()), event.payload.size()),
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_END);
    if (result != RD_KAFKA_RESP_ERR_NO_ERROR) return error(rd_kafka_err2str(result));
    rd_kafka_poll(client_, 0);
    return common::Status::Ok();
}

common::Status KafkaProducer::flush(std::uint32_t timeout_ms) {
    if (client_ == nullptr) return common::Status::FailedPrecondition("Kafka producer is not started");
    return rd_kafka_flush(client_, timeout_ms) == RD_KAFKA_RESP_ERR_NO_ERROR ? common::Status::Ok() : error("Kafka flush timeout");
}

void KafkaProducer::stop() {
    if (client_ != nullptr) {
        rd_kafka_flush(client_, 5000);
        rd_kafka_destroy(client_);
        client_ = nullptr;
    }
}

KafkaConsumer::~KafkaConsumer() { stop(); }

common::Status KafkaConsumer::start(const std::string& brokers, const std::string& group_id,
                                    const std::vector<std::string>& topics) {
    stop();
    if (topics.empty()) return common::Status::InvalidArgument("Kafka topics must not be empty");
    char error_buffer[256]{};
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers.c_str(), error_buffer, sizeof(error_buffer)) != RD_KAFKA_CONF_OK ||
        rd_kafka_conf_set(conf, "group.id", group_id.c_str(), error_buffer, sizeof(error_buffer)) != RD_KAFKA_CONF_OK ||
        rd_kafka_conf_set(conf, "enable.auto.commit", "false", error_buffer, sizeof(error_buffer)) != RD_KAFKA_CONF_OK ||
        rd_kafka_conf_set(conf, "auto.offset.reset", "earliest", error_buffer, sizeof(error_buffer)) != RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        return error(error_buffer);
    }
    client_ = rd_kafka_new(RD_KAFKA_CONSUMER, conf, error_buffer, sizeof(error_buffer));
    if (client_ == nullptr) return error(error_buffer);
    rd_kafka_poll_set_consumer(client_);
    rd_kafka_topic_partition_list_t* list = rd_kafka_topic_partition_list_new(topics.size());
    for (const auto& topic : topics) rd_kafka_topic_partition_list_add(list, topic.c_str(), RD_KAFKA_PARTITION_UA);
    const auto result = rd_kafka_subscribe(client_, list);
    rd_kafka_topic_partition_list_destroy(list);
    if (result != RD_KAFKA_RESP_ERR_NO_ERROR) {
        stop();
        return error(rd_kafka_err2str(result));
    }
    return common::Status::Ok();
}

common::Status KafkaConsumer::poll(std::uint32_t timeout_ms, const Handler& handler) {
    if (client_ == nullptr || !handler) return common::Status::FailedPrecondition("Kafka consumer is not ready");
    rd_kafka_message_t* message = rd_kafka_consumer_poll(client_, timeout_ms);
    if (message == nullptr) return common::Status::Ok();
    common::Status status = common::Status::Ok();
    if (message->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        status = error(rd_kafka_message_errstr(message));
    } else {
        Event event;
        event.topic = rd_kafka_topic_name(message->rkt);
        if (message->key != nullptr) event.key.assign(static_cast<const char*>(message->key), message->key_len);
        if (message->payload != nullptr) event.payload.assign(static_cast<const char*>(message->payload), message->len);
        event.sequence = static_cast<std::uint64_t>(message->offset);
        event.event_id = event.topic + "-" + std::to_string(event.sequence);
        status = handler(event);
        if (status.ok()) rd_kafka_commit_message(client_, message, 0);
    }
    rd_kafka_message_destroy(message);
    return status;
}

void KafkaConsumer::stop() {
    if (client_ != nullptr) {
        rd_kafka_consumer_close(client_);
        rd_kafka_destroy(client_);
        client_ = nullptr;
    }
}

}  // namespace live::messaging
