#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace {

struct ScenarioStats {
    std::uint64_t completed{0};
    std::uint64_t success_2xx{0};
    std::uint64_t client_4xx{0};
    std::uint64_t server_5xx{0};
    std::uint64_t other{0};
    std::vector<double> latency_ms;
};

struct WorkerStats {
    std::array<ScenarioStats, 5> scenarios;
};

bool sendAll(int fd, const std::string& request) {
    std::size_t sent = 0;
    while (sent < request.size()) {
        const ssize_t result = ::send(fd, request.data() + sent, request.size() - sent, 0);
        if (result <= 0) return false;
        sent += static_cast<std::size_t>(result);
    }
    return true;
}

int responseStatus(const std::string& response) {
    const auto end = response.find("\r\n");
    const auto line = response.substr(0, end == std::string::npos ? response.size() : end);
    if (line.size() < 12 || line.rfind("HTTP/1.1 ", 0) != 0) return 0;
    return std::atoi(line.substr(9, 3).c_str());
}

double percentile(std::vector<double> values, double ratio) {
    if (values.empty()) return 0.0;
    std::sort(values.begin(), values.end());
    const auto index = static_cast<std::size_t>(ratio * static_cast<double>(values.size() - 1));
    return values[index];
}

void record(ScenarioStats* stats, int status, double latency_ms) {
    ++stats->completed;
    if (status >= 200 && status < 300) ++stats->success_2xx;
    else if (status >= 400 && status < 500) ++stats->client_4xx;
    else if (status >= 500 && status < 600) ++stats->server_5xx;
    else ++stats->other;
    stats->latency_ms.push_back(latency_ms);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 5) {
        std::cerr << "usage: http_business_load_generator <host> <port> <requests> <concurrency>\n";
        return 2;
    }
    const std::string host = argv[1];
    const int port = std::atoi(argv[2]);
    const int requests = std::atoi(argv[3]);
    const int requested_concurrency = std::atoi(argv[4]);
    if (port <= 0 || port > 65535 || requests <= 0 || requested_concurrency <= 0) {
        std::cerr << "invalid arguments\n";
        return 2;
    }

    const std::array<std::string, 5> names = {
        "health", "product_read", "stock_read", "promotion_read", "metrics_read"};
    const std::array<std::string, 5> paths = {
        "/health", "/products/sku-1", "/stock/sku-1", "/promotions/promo-1", "/metrics"};
    const int worker_count = std::min(requests, requested_concurrency);
    std::vector<WorkerStats> workers(static_cast<std::size_t>(worker_count));
    const auto started_at = std::chrono::steady_clock::now();
    std::vector<std::thread> threads;
    threads.reserve(static_cast<std::size_t>(worker_count));

    for (int worker = 0; worker < worker_count; ++worker) {
        threads.emplace_back([&, worker] {
            auto& local = workers[static_cast<std::size_t>(worker)];
            for (int request_index = worker; request_index < requests; request_index += worker_count) {
                const std::size_t scenario = static_cast<std::size_t>(request_index) % names.size();
                const std::string request = "GET " + paths[scenario] +
                    " HTTP/1.1\r\nHost: business-load-generator\r\nConnection: close\r\n\r\n";
                const auto request_started = std::chrono::steady_clock::now();
                int status = 0;
                const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
                if (fd >= 0) {
                    timeval timeout{5, 0};
                    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
                    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
                    sockaddr_in address{};
                    address.sin_family = AF_INET;
                    address.sin_port = htons(static_cast<std::uint16_t>(port));
                    const bool connected = ::inet_pton(AF_INET, host.c_str(), &address.sin_addr) == 1 &&
                        ::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0;
                    if (connected && sendAll(fd, request)) {
                        std::string response;
                        char buffer[4096];
                        while (true) {
                            const ssize_t received = ::recv(fd, buffer, sizeof(buffer), 0);
                            if (received <= 0) break;
                            response.append(buffer, static_cast<std::size_t>(received));
                        }
                        status = responseStatus(response);
                    }
                    ::close(fd);
                }
                const auto request_finished = std::chrono::steady_clock::now();
                record(&local.scenarios[scenario], status,
                    std::chrono::duration<double, std::milli>(request_finished - request_started).count());
            }
        });
    }
    for (auto& thread : threads) thread.join();

    std::array<ScenarioStats, 5> totals;
    std::uint64_t completed = 0;
    std::uint64_t success = 0;
    for (const auto& worker : workers) {
        for (std::size_t i = 0; i < totals.size(); ++i) {
            totals[i].completed += worker.scenarios[i].completed;
            totals[i].success_2xx += worker.scenarios[i].success_2xx;
            totals[i].client_4xx += worker.scenarios[i].client_4xx;
            totals[i].server_5xx += worker.scenarios[i].server_5xx;
            totals[i].other += worker.scenarios[i].other;
            totals[i].latency_ms.insert(totals[i].latency_ms.end(), worker.scenarios[i].latency_ms.begin(),
                worker.scenarios[i].latency_ms.end());
        }
    }
    for (const auto& total : totals) {
        completed += total.completed;
        success += total.success_2xx;
    }
    const auto finished_at = std::chrono::steady_clock::now();
    const double elapsed_ms = std::chrono::duration<double, std::milli>(finished_at - started_at).count();
    std::cout << "business_requests=" << requests << " completed=" << completed << " 2xx=" << success
              << " non_2xx=" << (completed - success) << " concurrency=" << worker_count
              << " elapsed_ms=" << std::fixed << std::setprecision(2) << elapsed_ms
              << " throughput_rps=" << (static_cast<double>(completed) * 1000.0 / elapsed_ms) << '\n';
    for (std::size_t i = 0; i < names.size(); ++i) {
        const auto& total = totals[i];
        std::cout << names[i] << " requests=" << total.completed
                  << " 2xx=" << total.success_2xx << " 4xx=" << total.client_4xx
                  << " 5xx=" << total.server_5xx << " other=" << total.other
                  << " p50_ms=" << percentile(total.latency_ms, 0.50)
                  << " p95_ms=" << percentile(total.latency_ms, 0.95)
                  << " p99_ms=" << percentile(total.latency_ms, 0.99) << '\n';
    }
    return success == completed ? 0 : 1;
}
