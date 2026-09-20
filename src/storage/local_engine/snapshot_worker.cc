#include "storage/local_engine/snapshot_worker.h"

#include "storage/local_engine/local_kv_engine.h"

namespace live::storage {

SnapshotWorker::SnapshotWorker(LocalKVEngine* storage, std::chrono::seconds interval)
    : storage_(storage), interval_(interval <= std::chrono::seconds::zero() ? std::chrono::seconds(30) : interval) {}

SnapshotWorker::~SnapshotWorker() { stop(); }

common::Status SnapshotWorker::start() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (storage_ == nullptr) return common::Status::InvalidArgument("snapshot storage must not be null");
    if (started_) return common::Status::AlreadyExists("snapshot worker already started");
    stop_requested_ = false;
    started_ = true;
    worker_ = std::thread(&SnapshotWorker::run, this);
    return common::Status::Ok();
}

void SnapshotWorker::stop() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!started_) return;
        stop_requested_ = true;
    }
    condition_.notify_all();
    if (worker_.joinable()) worker_.join();
    if (storage_ != nullptr) (void)storage_->snapshot();
    std::lock_guard<std::mutex> lock(mutex_);
    started_ = false;
}

void SnapshotWorker::run() {
    while (true) {
        std::unique_lock<std::mutex> lock(mutex_);
        condition_.wait_for(lock, interval_, [this] { return stop_requested_; });
        if (stop_requested_) return;
        lock.unlock();
        if (storage_ != nullptr) (void)storage_->snapshot();
    }
}

}  // namespace live::storage
