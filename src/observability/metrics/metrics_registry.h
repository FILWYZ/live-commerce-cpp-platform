#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

namespace live::observability {

class MetricsRegistry {
private:
    struct HistogramMetric;

public:
    class CounterHandle {
    public:
        CounterHandle() = default;
        void add(std::uint64_t value = 1) const;
        std::uint64_t value() const;
        explicit operator bool() const { return value_ != nullptr; }

    private:
        explicit CounterHandle(std::shared_ptr<std::atomic<std::uint64_t>> value) : value_(std::move(value)) {}
        std::shared_ptr<std::atomic<std::uint64_t>> value_;
        friend class MetricsRegistry;
    };

    class GaugeHandle {
    public:
        GaugeHandle() = default;
        void set(double value) const;
        double value() const;
        explicit operator bool() const { return value_ != nullptr; }

    private:
        explicit GaugeHandle(std::shared_ptr<std::atomic<double>> value) : value_(std::move(value)) {}
        std::shared_ptr<std::atomic<double>> value_;
        friend class MetricsRegistry;
    };

    class HistogramHandle {
    public:
        HistogramHandle() = default;
        void observe(double value) const;
        explicit operator bool() const { return metric_ != nullptr; }

    private:
        explicit HistogramHandle(std::shared_ptr<HistogramMetric> metric) : metric_(std::move(metric)) {}
        std::shared_ptr<HistogramMetric> metric_;
        friend class MetricsRegistry;
    };

    explicit MetricsRegistry(std::chrono::milliseconds merge_interval = std::chrono::seconds(1));
    ~MetricsRegistry();

    MetricsRegistry(const MetricsRegistry&) = delete;
    MetricsRegistry& operator=(const MetricsRegistry&) = delete;

    CounterHandle counterHandle(const std::string& name);
    GaugeHandle gaugeHandle(const std::string& name);
    HistogramHandle histogramHandle(const std::string& name);

    // These name-based methods are kept for compatibility. Hot paths should
    // keep a handle and update the atomic value directly.
    void increment(const std::string& name, std::uint64_t value = 1);
    void setGauge(const std::string& name, double value);
    void observe(const std::string& name, double value);
    std::uint64_t counter(const std::string& name) const;
    std::string renderPrometheus() const;

private:
    void mergeHistograms() const;
    void runMerger();
    static std::string counterName(const std::string& name);

    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<std::atomic<std::uint64_t>>> counters_;
    std::unordered_map<std::string, std::shared_ptr<std::atomic<double>>> gauges_;
    std::unordered_map<std::string, std::shared_ptr<HistogramMetric>> histograms_;
    const std::chrono::milliseconds merge_interval_;
    mutable std::mutex merge_mutex_;
    std::condition_variable merge_condition_;
    bool stop_merger_{false};
    std::thread merger_;
};

}  // namespace live::observability
