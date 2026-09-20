#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace live::network {

class Buffer {
public:
    static constexpr std::size_t kCheapPrepend = 8;
    static constexpr std::size_t kInitialSize = 1024;

    Buffer();

    std::size_t readableBytes() const { return writer_index_ - reader_index_; }
    std::size_t writableBytes() const { return buffer_.size() - writer_index_; }
    std::size_t prependableBytes() const { return reader_index_; }

    const char* peek() const { return begin() + reader_index_; }
    char* beginWrite() { return begin() + writer_index_; }

    void retrieve(std::size_t len);
    void retrieveAll();
    std::string retrieveAsString();

    void append(const char* data, std::size_t len);
    void append(std::string_view data) { append(data.data(), data.size()); }

    // Reads from a non-blocking fd. saved_errno is populated only on failure.
    ssize_t readFd(int fd, int* saved_errno);

private:
    char* begin() { return buffer_.data(); }
    const char* begin() const { return buffer_.data(); }
    void ensureWritableBytes(std::size_t len);
    void makeSpace(std::size_t len);

    std::vector<char> buffer_;
    std::size_t reader_index_{kCheapPrepend};
    std::size_t writer_index_{kCheapPrepend};
};

}  // namespace live::network
