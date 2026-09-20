#include "observability/logging/structured_logger.h"

#include <iomanip>

namespace live::observability {

StructuredLogger::StructuredLogger(std::ostream* output) : output_(output) {}

std::string StructuredLogger::escape(const std::string& value) {
    std::string escaped;
    escaped.reserve(value.size());
    for (const char c : value) {
        switch (c) {
            case '"': escaped += "\\\""; break;
            case '\\': escaped += "\\\\"; break;
            case '\n': escaped += "\\n"; break;
            case '\r': escaped += "\\r"; break;
            default: escaped.push_back(c); break;
        }
    }
    return escaped;
}

void StructuredLogger::logHttpError(const gateway::http::HttpRequest& request,
                                    const gateway::http::HttpResponse& response,
                                    std::uint64_t elapsed_us) {
    if (output_ == nullptr) return;
    std::lock_guard<std::mutex> lock(mutex_);
    (*output_) << "{\"event\":\"http_request_error\",\"request_id\":\""
               << escape(request.request_id) << "\",\"method\":\"" << escape(request.method)
               << "\",\"target\":\"" << escape(request.target) << "\",\"status\":"
               << response.status_code << ",\"elapsed_us\":" << elapsed_us << "}\n";
    output_->flush();
}

}  // namespace live::observability
