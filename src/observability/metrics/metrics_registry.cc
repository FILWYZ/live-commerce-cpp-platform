#include "observability/metrics/metrics_registry.h"

#include <array>
#include <sstream>
#include <utility>
#include <vector>

namespace live::observability {
namespace {

constexpr std::array<double, 8> kHistogramBounds{0.5, 1.0, 2.0, 5.0, 10.0, 50.0, 100.0, 500.0};

struct HistogramShard {
    std::mutex mutex;
    std::array<std::uint64_t, 8> buckets{};
    std::uint64_t count{0};
    double sum{0.0};
};

thread_local std::unordered_map<const void*, std::shared_ptr<HistogramShard>> local_histograms;

}  // namespace

struct MetricsRegistry::HistogramMetric {
    mutable std::mutex shards_mutex;
    std::vector<std::shared_ptr<HistogramShard>> shards;
    mutable std::mutex global_mutex;
    std::array<std::uint64_t, 8> buckets{};
    std::uint64_t count{0};
    double sum{0.0};
};

void MetricsRegistry::CounterHandle::add(std::uint64_t value) const {
    if (value_ != nullptr) value_->fetch_add(value, std::memory_order_relaxed);
}

std::uint64_t MetricsRegistry::CounterHandle::value() const {
    return value_ == nullptr ? 0 : value_->load(std::memory_order_relaxed);
}

void MetricsRegistry::GaugeHandle::set(double value) const {
    if (value_ != nullptr) value_->store(value, std::memory_order_relaxed);
}

double MetricsRegistry::GaugeHandle::value() const {
    return value_ == nullptr ? 0.0 : value_->load(std::memory_order_relaxed);
}

void MetricsRegistry::HistogramHandle::observe(double value) const {
    if (metric_ == nullptr || value < 0.0) return;
    auto it = local_histograms.find(metric_.get());
    if (it == local_histograms.end()) {
        auto shard = std::make_shared<HistogramShard>();
        {
            std::lock_guard<std::mutex> lock(metric_->shards_mutex);
            metric_->shards.push_back(shard);
        }
        it = local_histograms.emplace(metric_.get(), std::move(shard)).first;
    }
    auto& shard = *it->second;
    std::lock_guard<std::mutex> lock(shard.mutex);
    ++shard.count;
    shard.sum += value;
    for (std::size_t i = 0; i < kHistogramBounds.size(); ++i) {
        if (value <= kHistogramBounds[i]) ++shard.buckets[i];
    }
}

MetricsRegistry::MetricsRegistry(std::chrono::milliseconds merge_interval)
    : merge_interval_(merge_interval <= std::chrono::milliseconds::zero() ? std::chrono::seconds(1) : merge_interval),
      merger_(&MetricsRegistry::runMerger, this) {}

MetricsRegistry::~MetricsRegistry() {
    {
        std::lock_guard<std::mutex> lock(merge_mutex_);
        stop_merger_ = true;
    }
    merge_condition_.notify_one();
    if (merger_.joinable()) merger_.join();
    mergeHistograms();
}

MetricsRegistry::CounterHandle MetricsRegistry::counterHandle(const std::string& name) {
    if (name.empty()) return {};
    std::lock_guard<std::mutex> lock(mutex_);
    auto& value = counters_[name];
    if (value == nullptr) value = std::make_shared<std::atomic<std::uint64_t>>(0);
    return CounterHandle(value);
}

MetricsRegistry::GaugeHandle MetricsRegistry::gaugeHandle(const std::string& name) {
    if (name.empty()) return {};
    std::lock_guard<std::mutex> lock(mutex_);
    auto& value = gauges_[name];
    if (value == nullptr) value = std::make_shared<std::atomic<double>>(0.0);
    return GaugeHandle(value);
}

MetricsRegistry::HistogramHandle MetricsRegistry::histogramHandle(const std::string& name) {
    if (name.empty()) return {};
    std::lock_guard<std::mutex> lock(mutex_);
    auto& metric = histograms_[name];
    if (metric == nullptr) metric = std::make_shared<HistogramMetric>();
    return HistogramHandle(metric);
}

void MetricsRegistry::increment(const std::string& name, std::uint64_t value) {
    counterHandle(name).add(value);
}

void MetricsRegistry::setGauge(const std::string& name, double value) {
    gaugeHandle(name).set(value);
}

void MetricsRegistry::observe(const std::string& name, double value) {
    histogramHandle(name).observe(value);
}

std::uint64_t MetricsRegistry::counter(const std::string& name) const {
    std::shared_ptr<std::atomic<std::uint64_t>> value;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto it = counters_.find(name);
        if (it == counters_.end()) return 0;
        value = it->second;
    }
    return value->load(std::memory_order_relaxed);
}

void MetricsRegistry::mergeHistograms() const {
    std::vector<std::shared_ptr<HistogramMetric>> metrics;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& [name, metric] : histograms_) metrics.push_back(metric);
    }
    for (const auto& metric : metrics) {
        std::vector<std::shared_ptr<HistogramShard>> shards;
        {
            std::lock_guard<std::mutex> lock(metric->shards_mutex);
            shards = metric->shards;
        }
        std::lock_guard<std::mutex> global_lock(metric->global_mutex);
        for (const auto& shard : shards) {
            std::lock_guard<std::mutex> shard_lock(shard->mutex);
            for (std::size_t i = 0; i < kHistogramBounds.size(); ++i) {
                metric->buckets[i] += shard->buckets[i];
                shard->buckets[i] = 0;
            }
            metric->count += shard->count;
            metric->sum += shard->sum;
            shard->count = 0;
            shard->sum = 0.0;
        }
    }
}

void MetricsRegistry::runMerger() {
    std::unique_lock<std::mutex> lock(merge_mutex_);
    while (!stop_merger_) {
        if (merge_condition_.wait_for(lock, merge_interval_, [this] { return stop_merger_; })) break;
        lock.unlock();
        mergeHistograms();
        lock.lock();
    }
}

std::string MetricsRegistry::counterName(const std::string& name) {
    return name.size() >= 6 && name.compare(name.size() - 6, 6, "_total") == 0 ? name : name + "_total";
}

std::string MetricsRegistry::renderPrometheus() const {
    mergeHistograms();
    std::vector<std::pair<std::string, std::shared_ptr<std::atomic<std::uint64_t>>>> counters;
    std::vector<std::pair<std::string, std::shared_ptr<std::atomic<double>>>> gauges;
    std::vector<std::pair<std::string, std::shared_ptr<HistogramMetric>>> histograms;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& item : counters_) counters.push_back(item);
        for (const auto& item : gauges_) gauges.push_back(item);
        for (const auto& item : histograms_) histograms.push_back(item);
    }
    std::ostringstream output;
    for (const auto& [name, value] : counters) output << counterName(name) << ' ' << value->load(std::memory_order_relaxed) << '\n';
    for (const auto& [name, value] : gauges) output << name << ' ' << value->load(std::memory_order_relaxed) << '\n';
    for (const auto& [name, histogram] : histograms) {
        std::lock_guard<std::mutex> lock(histogram->global_mutex);
        for (std::size_t i = 0; i < kHistogramBounds.size(); ++i) {
            output << name << "_bucket{le=\"" << kHistogramBounds[i] << "\"} "
                   << histogram->buckets[i] << '\n';
        }
        output << name << "_bucket{le=\"+Inf\"} " << histogram->count << '\n';
        output << name << "_sum " << histogram->sum << '\n';
        output << name << "_count " << histogram->count << '\n';
    }
    return output.str();
}

}  // namespace live::observability
