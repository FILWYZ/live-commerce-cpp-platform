#pragma once

#include "common/error/status.h"

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace live::storage {

class LocalKVEngine;

class SnapshotWorker {
public:
    SnapshotWorker(LocalKVEngine* storage, std::chrono::seconds interval = std::chrono::seconds(30));
    ~SnapshotWorker();

    SnapshotWorker(const SnapshotWorker&) = delete;
    SnapshotWorker& operator=(const SnapshotWorker&) = delete;

    common::Status start();
    void stop();

private:
    void run();

    LocalKVEngine* storage_;
    std::chrono::seconds interval_;
    std::mutex mutex_;
    std::condition_variable condition_;
    bool started_{false};
    bool stop_requested_{false};
    std::thread worker_;
};

}  // namespace live::storage
