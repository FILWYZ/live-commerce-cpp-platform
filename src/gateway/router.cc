#include "gateway/router.h"

namespace live::gateway {

void Router::addRoute(std::string method, std::string path, Handler handler) {
    handlers_[key(method, path)] = std::move(handler);
}

http::HttpResponse Router::dispatch(const http::HttpRequest& request) const {
    const auto query_start = request.target.find('?');
    const std::string path = query_start == std::string::npos ? request.target : request.target.substr(0, query_start);
    const auto it = handlers_.find(key(request.method, path));
    if (it == handlers_.end()) {
        return http::HttpResponse{404, "Not Found", {}, "route not found"};
    }
    return it->second(request);
}

std::string Router::key(const std::string& method, const std::string& path) {
    return method + " " + path;
}

}  // namespace live::gateway
