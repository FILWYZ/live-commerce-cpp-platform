#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace {

struct WorkerStats {
    int completed{0};
    int success{0};
    std::vector<double> latencies_ms;
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

bool isHttp200(const std::string& response) {
    const auto line_end = response.find("\r\n");
    const std::string status_line = response.substr(0, line_end == std::string::npos ? response.size() : line_end);
    return status_line.rfind("HTTP/1.1 200 ", 0) == 0 || status_line.rfind("HTTP/1.0 200 ", 0) == 0;
}

double percentile(std::vector<double> values, double ratio) {
    if (values.empty()) return 0.0;
    std::sort(values.begin(), values.end());
    const auto index = static_cast<std::size_t>(ratio * static_cast<double>(values.size() - 1));
    return values[index];
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 5) {
        std::cerr << "usage: http_load_generator <host> <port> <path> <requests> [concurrency]\n";
        return 2;
    }
    const std::string host = argv[1];
    const int port = std::atoi(argv[2]);
    const std::string path = argv[3];
    const int requests = std::atoi(argv[4]);
    if (port <= 0 || port > 65535 || requests <= 0) {
        std::cerr << "port and requests must be positive\n";
        return 2;
    }
    const int requested_concurrency = argc >= 6 ? std::atoi(argv[5]) : std::min(32, requests);
    const int worker_count = std::min(requests, std::max(1, requested_concurrency));
    const std::string request = "GET " + path + " HTTP/1.1\r\nHost: load-generator\r\nConnection: close\r\n\r\n";
    const auto started_at = std::chrono::steady_clock::now();
    std::vector<WorkerStats> stats(static_cast<std::size_t>(worker_count));
    std::vector<std::thread> workers;
    workers.reserve(static_cast<std::size_t>(worker_count));

    for (int worker = 0; worker < worker_count; ++worker) {
        workers.emplace_back([&, worker] {
            auto& local = stats[static_cast<std::size_t>(worker)];
            local.latencies_ms.reserve(static_cast<std::size_t>((requests + worker_count - 1) / worker_count));
            for (int i = worker; i < requests; i += worker_count) {
                const auto request_started = std::chrono::steady_clock::now();
                const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
                if (fd < 0) continue;
                timeval timeout{5, 0};
                ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
                ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
                sockaddr_in address{};
                address.sin_family = AF_INET;
                address.sin_port = htons(static_cast<std::uint16_t>(port));
                const bool connected = ::inet_pton(AF_INET, host.c_str(), &address.sin_addr) == 1 &&
                    ::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0;
                bool ok = false;
                if (connected && sendAll(fd, request)) {
                    std::string response;
                    char buffer[4096];
                    while (true) {
                        const ssize_t received = ::recv(fd, buffer, sizeof(buffer), 0);
                        if (received <= 0) break;
                        response.append(buffer, static_cast<std::size_t>(received));
                    }
                    ok = isHttp200(response);
                }
                ::close(fd);
                const auto request_finished = std::chrono::steady_clock::now();
                const double latency = std::chrono::duration<double, std::milli>(request_finished - request_started).count();
                local.completed++;
                if (ok) local.success++;
                local.latencies_ms.push_back(latency);
            }
        });
    }
    for (auto& worker : workers) worker.join();

    int completed = 0;
    int success = 0;
    std::vector<double> latencies;
    for (auto& local : stats) {
        completed += local.completed;
        success += local.success;
        latencies.insert(latencies.end(), local.latencies_ms.begin(), local.latencies_ms.end());
    }
    const auto finished_at = std::chrono::steady_clock::now();
    const double elapsed_ms = std::chrono::duration<double, std::milli>(finished_at - started_at).count();
    const double throughput = elapsed_ms <= 0.0 ? 0.0 : static_cast<double>(completed) * 1000.0 / elapsed_ms;
    const double max_latency = latencies.empty() ? 0.0 : *std::max_element(latencies.begin(), latencies.end());
    std::cout << "requests=" << requests
              << " completed=" << completed
              << " success=" << success
              << " failed=" << (requests - success)
              << " concurrency=" << worker_count
              << " elapsed_ms=" << std::fixed << std::setprecision(2) << elapsed_ms
              << " throughput_rps=" << throughput
              << " p50_ms=" << percentile(latencies, 0.50)
              << " p95_ms=" << percentile(latencies, 0.95)
              << " p99_ms=" << percentile(latencies, 0.99)
              << " max_ms=" << max_latency << '\n';
    return success == requests ? 0 : 1;
}
