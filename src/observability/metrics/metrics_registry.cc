#include "observability/metrics/metrics_registry.h"

#include <sstream>

namespace live::observability {

namespace {

constexpr std::array<double, 8> kHistogramBounds{0.5, 1.0, 2.0, 5.0, 10.0, 50.0, 100.0, 500.0};

}  // namespace

void MetricsRegistry::increment(const std::string& name, std::uint64_t value) {
    std::lock_guard<std::mutex> lock(mutex_);
    counters_[name] += value;
}

void MetricsRegistry::setGauge(const std::string& name, double value) {
    std::lock_guard<std::mutex> lock(mutex_);
    gauges_[name] = value;
}

void MetricsRegistry::observe(const std::string& name, double value) {
    if (name.empty() || value < 0.0) return;
    std::lock_guard<std::mutex> lock(mutex_);
    auto& histogram = histograms_[name];
    ++histogram.count;
    histogram.sum += value;
    for (std::size_t i = 0; i < kHistogramBounds.size(); ++i) {
        if (value <= kHistogramBounds[i]) ++histogram.buckets[i];
    }
}

std::uint64_t MetricsRegistry::counter(const std::string& name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = counters_.find(name);
    return it == counters_.end() ? 0 : it->second;
}

std::string MetricsRegistry::renderPrometheus() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::ostringstream output;
    for (const auto& [name, value] : counters_) output << name << "_total " << value << '\n';
    for (const auto& [name, value] : gauges_) output << name << ' ' << value << '\n';
    for (const auto& [name, histogram] : histograms_) {
        for (std::size_t i = 0; i < kHistogramBounds.size(); ++i) {
            output << name << "_bucket{le=\"" << kHistogramBounds[i] << "\"} " << histogram.buckets[i] << '\n';
        }
        output << name << "_bucket{le=\"+Inf\"} " << histogram.count << '\n';
        output << name << "_sum " << histogram.sum << '\n';
        output << name << "_count " << histogram.count << '\n';
    }
    return output.str();
}

}  // namespace live::observability
