#include "gateway/http/http_parser.h"

#include <algorithm>
#include <charconv>
#include <cctype>
#include <limits>
#include <sstream>

namespace live::gateway::http {
namespace {

std::string trim(std::string_view value) {
    std::size_t begin = 0;
    std::size_t end = value.size();
    while (begin < end && std::isspace(static_cast<unsigned char>(value[begin]))) {
        ++begin;
    }
    while (end > begin && std::isspace(static_cast<unsigned char>(value[end - 1]))) {
        --end;
    }
    return std::string(value.substr(begin, end - begin));
}

std::string lower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return value;
}

}  // namespace

common::Status HttpRequestParser::parse(std::string_view raw, HttpRequest* request) {
    return parse(raw, request, nullptr);
}

common::Status HttpRequestParser::parse(std::string_view raw, HttpRequest* request,
                                        std::size_t* consumed) {
    if (request == nullptr) {
        return common::Status::InvalidArgument("request must not be null");
    }
    if (consumed != nullptr) *consumed = 0;
    constexpr std::size_t kMaxHeaderBytes = 64 * 1024;
    constexpr std::size_t kMaxBodyBytes = 8 * 1024 * 1024;
    const std::size_t header_end = raw.find("\r\n\r\n");
    if (header_end == std::string_view::npos) {
        if (raw.size() > kMaxHeaderBytes) return common::Status::InvalidArgument("HTTP headers are too large");
        return common::Status::Incomplete("HTTP headers are incomplete");
    }
    if (header_end + 4 > kMaxHeaderBytes) return common::Status::InvalidArgument("HTTP headers are too large");

    const std::string_view header_block = raw.substr(0, header_end);
    const std::size_t line_end = header_block.find("\r\n");
    if (line_end == std::string_view::npos) {
        return common::Status::InvalidArgument("missing HTTP request line");
    }

    std::istringstream request_line(std::string(header_block.substr(0, line_end)));
    std::string extra;
    if (!(request_line >> request->method >> request->target >> request->version) ||
        (request_line >> extra) || request->method.empty() || request->target.empty() || request->target.size() > 8192 ||
        request->version != "HTTP/1.1") {
        return common::Status::InvalidArgument("invalid HTTP request line");
    }

    request->headers.clear();
    std::size_t cursor = line_end + 2;
    while (cursor < header_block.size()) {
        const std::size_t end = header_block.find("\r\n", cursor);
        const std::size_t actual_end = end == std::string_view::npos ? header_block.size() : end;
        const std::string_view line = header_block.substr(cursor, actual_end - cursor);
        const std::size_t colon = line.find(':');
        if (colon == std::string_view::npos || colon == 0) {
            return common::Status::InvalidArgument("invalid HTTP header");
        }
        const std::string name = lower(trim(line.substr(0, colon)));
        const std::string value = trim(line.substr(colon + 1));
        if (name.empty() || value.size() > kMaxHeaderBytes) return common::Status::InvalidArgument("invalid HTTP header");
        if (request->headers.find(name) != request->headers.end()) {
            return common::Status::InvalidArgument("duplicate HTTP header");
        }
        request->headers[name] = name == "connection" ? lower(value) : value;
        cursor = actual_end + 2;
    }

    if (request->headers.find("host") == request->headers.end()) {
        return common::Status::InvalidArgument("Host header is required");
    }
    const auto transfer_encoding = request->headers.find("transfer-encoding");
    if (transfer_encoding != request->headers.end() && lower(transfer_encoding->second) != "identity") {
        return common::Status::InvalidArgument("Transfer-Encoding is not supported");
    }
    std::size_t content_length = 0;
    const auto content_length_it = request->headers.find("content-length");
    if (content_length_it != request->headers.end()) {
        const auto text = content_length_it->second;
        const auto result = std::from_chars(text.data(), text.data() + text.size(), content_length);
        if (result.ec != std::errc{} || result.ptr != text.data() + text.size()) {
            return common::Status::InvalidArgument("invalid Content-Length");
        }
    }
    if (content_length > kMaxBodyBytes) return common::Status::InvalidArgument("HTTP body is too large");
    const std::size_t body_begin = header_end + 4;
    if (raw.size() - body_begin < content_length) {
        return common::Status::Incomplete("HTTP body is incomplete");
    }
    request->body.assign(raw.substr(body_begin, content_length));
    if (consumed != nullptr) *consumed = body_begin + content_length;
    return common::Status::Ok();
}

}  // namespace live::gateway::http
