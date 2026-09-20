#pragma once

#include "gateway/http/http_parser.h"
#include "gateway/http/http_response.h"

#include <cstdint>
#include <mutex>
#include <ostream>

namespace live::observability {

class StructuredLogger {
public:
    explicit StructuredLogger(std::ostream* output);

    void logHttpError(const gateway::http::HttpRequest& request,
                      const gateway::http::HttpResponse& response,
                      std::uint64_t elapsed_us);

private:
    static std::string escape(const std::string& value);

    std::ostream* output_;
    std::mutex mutex_;
};

}  // namespace live::observability
