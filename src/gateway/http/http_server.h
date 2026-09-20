#pragma once

#include "common/error/status.h"
#include "common/thread_pool.h"
#include "gateway/router.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace live::network { class Buffer; class Channel; class EventLoop; class TcpAcceptor; }

namespace live::gateway::http {

class HttpServer {
public:
    using RequestObserver = std::function<void(const http::HttpRequest&, const http::HttpResponse&, std::uint64_t)>;

    HttpServer(network::EventLoop* loop, std::string host, std::uint16_t port,
               std::size_t max_connections = 10000, RequestObserver observer = {},
               std::size_t worker_threads = 0, std::size_t worker_queue_capacity = 4096);
    ~HttpServer();

    common::Status start();
    void stop();
    // Shutdown helpers used by the owner to drain worker callbacks before the
    // event loop is stopped. The *InLoop methods must run on the event-loop thread.
    void stopWorkers();
    void stopAcceptingInLoop();
    void stopInLoop();
    Router& router() { return router_; }
    std::uint16_t port() const;

private:
    struct Connection;
    void onConnection(int fd);
    void removeConnection(int fd);
    void observe(const http::HttpRequest& request, const http::HttpResponse& response, std::int64_t elapsed_us);

    network::EventLoop* loop_;
    std::size_t max_connections_;
    RequestObserver observer_;
    std::unique_ptr<common::ThreadPool> worker_pool_;
    Router router_;
    std::unique_ptr<network::TcpAcceptor> acceptor_;
    std::unordered_map<int, std::shared_ptr<Connection>> connections_;
};

}  // namespace live::gateway::http
