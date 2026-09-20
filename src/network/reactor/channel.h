#pragma once

#include <functional>
#include <poll.h>

namespace live::network {

class EventLoop;

class Channel {
public:
    using EventCallback = std::function<void()>;

    Channel(EventLoop* loop, int fd);
    ~Channel() = default;

    int fd() const { return fd_; }
    int events() const { return events_; }
    void setRevents(int revents) { revents_ = revents; }

    void setReadCallback(EventCallback callback) { read_callback_ = std::move(callback); }
    void setWriteCallback(EventCallback callback) { write_callback_ = std::move(callback); }
    void setCloseCallback(EventCallback callback) { close_callback_ = std::move(callback); }
    void setErrorCallback(EventCallback callback) { error_callback_ = std::move(callback); }

    void enableReading();
    void disableReading();
    void enableWriting();
    void disableWriting();
    void disableAll();
    void handleEvent();

private:
    void update();

    EventLoop* loop_;
    int fd_;
    int events_{0};
    int revents_{0};
    EventCallback read_callback_;
    EventCallback write_callback_;
    EventCallback close_callback_;
    EventCallback error_callback_;
};

}  // namespace live::network
