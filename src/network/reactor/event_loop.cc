#include "network/reactor/event_loop.h"

#include "network/reactor/channel.h"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

#ifdef __linux__
#include <sys/epoll.h>
#include <sys/eventfd.h>
#else
#include <poll.h>
#endif

namespace live::network {
namespace {

void setNonBlocking(int fd) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

}  // namespace

EventLoop::EventLoop() {
#ifdef __linux__
    poller_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    wakeup_read_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    wakeup_write_fd_ = wakeup_read_fd_;
#else
    int pipe_fds[2];
    if (::pipe(pipe_fds) == 0) {
        wakeup_read_fd_ = pipe_fds[0];
        wakeup_write_fd_ = pipe_fds[1];
        setNonBlocking(wakeup_read_fd_);
        setNonBlocking(wakeup_write_fd_);
    }
#endif
}

EventLoop::~EventLoop() {
    if (poller_fd_ >= 0) {
        ::close(poller_fd_);
    }
    if (wakeup_read_fd_ >= 0) {
        ::close(wakeup_read_fd_);
    }
#ifndef __linux__
    if (wakeup_write_fd_ >= 0) {
        ::close(wakeup_write_fd_);
    }
#endif
}

void EventLoop::loop() {
    // The loop may be constructed on one thread and started on another. The
    // owner thread is the thread that actually runs callbacks.
    {
        std::lock_guard<std::mutex> lock(mutex_);
        thread_id_ = std::this_thread::get_id();
    }
    quit_.store(false);
#ifdef __linux__
    epoll_event events[64];
    while (!quit_.load()) {
        const int count = ::epoll_wait(poller_fd_, events, 64, 1000);
        if (count < 0 && errno == EINTR) {
            continue;
        }
        for (int i = 0; i < count; ++i) {
            const int fd = events[i].data.fd;
            if (fd == wakeup_read_fd_) {
                handleWakeup();
                continue;
            }
            const auto it = channels_.find(fd);
            if (it != channels_.end()) {
                it->second->setRevents(events[i].events);
                it->second->handleEvent();
            }
        }
        doPendingFunctors();
    }
#else
    while (!quit_.load()) {
        std::vector<pollfd> poll_fds;
        poll_fds.push_back({wakeup_read_fd_, POLLIN, 0});
        for (const auto& [fd, channel] : channels_) {
            poll_fds.push_back({fd, static_cast<short>(channel->events()), 0});
        }
        const int count = ::poll(poll_fds.data(), poll_fds.size(), 1000);
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (!poll_fds.empty() && poll_fds[0].revents != 0) {
            handleWakeup();
        }
        for (std::size_t i = 1; i < poll_fds.size(); ++i) {
            if (poll_fds[i].revents == 0) {
                continue;
            }
            const auto it = channels_.find(poll_fds[i].fd);
            if (it != channels_.end()) {
                it->second->setRevents(poll_fds[i].revents);
                it->second->handleEvent();
            }
        }
        doPendingFunctors();
    }
#endif
}

void EventLoop::quit() {
    quit_.store(true);
    wakeup();
}

void EventLoop::runInLoop(Functor callback) {
    if (isInLoopThread()) {
        callback();
        return;
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pending_functors_.push_back(std::move(callback));
    }
    wakeup();
}

void EventLoop::updateChannel(Channel* channel) {
    channels_[channel->fd()] = channel;
#ifdef __linux__
    epoll_event event{};
    event.events = static_cast<std::uint32_t>(channel->events());
    event.data.fd = channel->fd();
    const int operation = channel->events() == 0 ? EPOLL_CTL_DEL : EPOLL_CTL_ADD;
    if (operation == EPOLL_CTL_DEL) {
        ::epoll_ctl(poller_fd_, operation, channel->fd(), nullptr);
    } else if (::epoll_ctl(poller_fd_, operation, channel->fd(), &event) < 0 && errno == EEXIST) {
        ::epoll_ctl(poller_fd_, EPOLL_CTL_MOD, channel->fd(), &event);
    }
#endif
}

void EventLoop::removeChannel(Channel* channel) {
    channels_.erase(channel->fd());
#ifdef __linux__
    ::epoll_ctl(poller_fd_, EPOLL_CTL_DEL, channel->fd(), nullptr);
#endif
}

void EventLoop::wakeup() {
#ifdef __linux__
    const std::uint64_t one = 1;
    ::write(wakeup_write_fd_, &one, sizeof(one));
#else
    const char one = 1;
    ::write(wakeup_write_fd_, &one, sizeof(one));
#endif
}

void EventLoop::handleWakeup() {
#ifdef __linux__
    std::uint64_t value = 0;
    while (::read(wakeup_read_fd_, &value, sizeof(value)) > 0) {
    }
#else
    char buffer[64];
    while (::read(wakeup_read_fd_, buffer, sizeof(buffer)) > 0) {
    }
#endif
}

void EventLoop::doPendingFunctors() {
    std::vector<Functor> callbacks;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        callbacks.swap(pending_functors_);
    }
    for (auto& callback : callbacks) {
        callback();
    }
}

}  // namespace live::network
