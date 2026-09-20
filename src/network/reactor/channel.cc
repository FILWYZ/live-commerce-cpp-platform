#include "network/reactor/channel.h"

#include "network/reactor/event_loop.h"

namespace live::network {

Channel::Channel(EventLoop* loop, int fd) : loop_(loop), fd_(fd) {}

void Channel::enableReading() {
    events_ |= POLLIN;
    update();
}

void Channel::enableWriting() {
    events_ |= POLLOUT;
    update();
}

void Channel::disableReading() {
    events_ &= ~POLLIN;
    update();
}

void Channel::disableWriting() {
    events_ &= ~POLLOUT;
    update();
}

void Channel::disableAll() {
    events_ = 0;
    update();
}

void Channel::update() {
    loop_->updateChannel(this);
}

void Channel::handleEvent() {
    if ((revents_ & (POLLERR | POLLNVAL)) != 0) {
        if (error_callback_) {
            error_callback_();
        }
        return;
    }
    int peer_closed_events = POLLHUP;
#ifdef POLLRDHUP
    peer_closed_events |= POLLRDHUP;
#endif
    if ((revents_ & peer_closed_events) != 0 && (revents_ & POLLIN) == 0) {
        if (close_callback_) {
            close_callback_();
        }
        return;
    }
    if ((revents_ & (POLLIN | POLLPRI)) != 0 && read_callback_) {
        read_callback_();
        // A read callback is allowed to close and destroy its owning
        // connection. Do not touch this Channel again in the same dispatch;
        // a pending POLLOUT event will be picked up on the next poll cycle.
        return;
    }
    if ((revents_ & POLLOUT) != 0 && write_callback_) {
        write_callback_();
    }
}

}  // namespace live::network
