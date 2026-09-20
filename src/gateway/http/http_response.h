#pragma once

#include "gateway/http/http_parser.h"

#include <map>
#include <string>

namespace live::gateway::http {

struct HttpResponse {
    int status_code{200};
    std::string reason{"OK"};
    std::map<std::string, std::string> headers;
    std::string body;

    std::string serialize() const;
};

}  // namespace live::gateway::http
