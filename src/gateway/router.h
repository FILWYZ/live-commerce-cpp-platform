#pragma once

#include "gateway/http/http_parser.h"
#include "gateway/http/http_response.h"

#include <functional>
#include <string>
#include <unordered_map>

namespace live::gateway {

class Router {
public:
    using Handler = std::function<http::HttpResponse(const http::HttpRequest&)>;

    void addRoute(std::string method, std::string path, Handler handler);
    http::HttpResponse dispatch(const http::HttpRequest& request) const;

private:
    static std::string key(const std::string& method, const std::string& path);

    std::unordered_map<std::string, Handler> handlers_;
};

}  // namespace live::gateway
