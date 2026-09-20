#include "gateway/http/http_response.h"

#include <sstream>

namespace live::gateway::http {

std::string HttpResponse::serialize() const {
    std::ostringstream output;
    output << "HTTP/1.1 " << status_code << ' ' << reason << "\r\n";
    for (const auto& [key, value] : headers) {
        output << key << ": " << value << "\r\n";
    }
    output << "Content-Length: " << body.size() << "\r\n\r\n" << body;
    return output.str();
}

}  // namespace live::gateway::http
