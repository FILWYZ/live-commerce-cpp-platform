#include "network/buffer.h"

#include <cerrno>
#include <cstring>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>

namespace live::network {

Buffer::Buffer() : buffer_(kCheapPrepend + kInitialSize) {}

void Buffer::retrieve(std::size_t len) {
    if (len < readableBytes()) {
        reader_index_ += len;
    } else {
        retrieveAll();
    }
}

void Buffer::retrieveAll() {
    reader_index_ = kCheapPrepend;
    writer_index_ = kCheapPrepend;
}

std::string Buffer::retrieveAsString() {
    std::string result(peek(), readableBytes());
    retrieveAll();
    return result;
}

void Buffer::append(const char* data, std::size_t len) {
    ensureWritableBytes(len);
    std::memcpy(beginWrite(), data, len);
    writer_index_ += len;
}

void Buffer::ensureWritableBytes(std::size_t len) {
    if (writableBytes() < len) {
        makeSpace(len);
    }
}

void Buffer::makeSpace(std::size_t len) {
    if (writableBytes() + prependableBytes() < len + kCheapPrepend) {
        buffer_.resize(writer_index_ + len);
    } else {
        const std::size_t readable = readableBytes();
        std::copy(begin() + reader_index_, begin() + writer_index_, begin() + kCheapPrepend);
        reader_index_ = kCheapPrepend;
        writer_index_ = reader_index_ + readable;
    }
}

ssize_t Buffer::readFd(int fd, int* saved_errno) {
    char extra_buffer[65536];
    struct iovec vec[2];
    const std::size_t writable = writableBytes();

    vec[0].iov_base = begin() + writer_index_;
    vec[0].iov_len = writable;
    vec[1].iov_base = extra_buffer;
    vec[1].iov_len = sizeof(extra_buffer);

    const ssize_t n = ::readv(fd, vec, 2);
    if (n < 0) {
        if (saved_errno != nullptr) {
            *saved_errno = errno;
        }
        return n;
    }

    if (static_cast<std::size_t>(n) <= writable) {
        writer_index_ += static_cast<std::size_t>(n);
    } else {
        writer_index_ = buffer_.size();
        append(extra_buffer, static_cast<std::size_t>(n) - writable);
    }
    return n;
}

}  // namespace live::network
