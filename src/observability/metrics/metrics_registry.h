#pragma once

#include <cstdint>
#include <array>
#include <mutex>
#include <string>
#include <unordered_map>

namespace live::observability {

class MetricsRegistry {
public:
    void increment(const std::string& name, std::uint64_t value = 1);
    void setGauge(const std::string& name, double value);
    void observe(const std::string& name, double value);
    std::uint64_t counter(const std::string& name) const;
    std::string renderPrometheus() const;

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::uint64_t> counters_;
    std::unordered_map<std::string, double> gauges_;
    struct Histogram {
        std::array<std::uint64_t, 8> buckets{};
        std::uint64_t count{0};
        double sum{0.0};
    };
    std::unordered_map<std::string, Histogram> histograms_;
};

}  // namespace live::observability
