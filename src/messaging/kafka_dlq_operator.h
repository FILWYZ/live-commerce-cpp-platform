#pragma once

#include "common/error/status.h"

#include <cstddef>
#include <string>

namespace live::messaging {

class KafkaDlqOperator {
public:
    static common::Status replay(const std::string& brokers, const std::string& dlq_topic,
                                 const std::string& target_topic, std::size_t max_messages,
                                 std::size_t* replayed);
};

}  // namespace live::messaging
