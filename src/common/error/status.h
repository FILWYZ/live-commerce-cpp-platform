#pragma once

#include <string>
#include <string_view>

namespace live::common {

enum class ErrorCode {
    kOk = 0,
    kInvalidArgument,
    kIncomplete,
    kNotFound,
    kAlreadyExists,
    kUnauthenticated,
    kResourceExhausted,
    kFailedPrecondition,
    kAborted,
    kInternal,
};

class Status {
public:
    Status() = default;

    static Status Ok() { return {}; }
    static Status InvalidArgument(std::string_view message) {
        return Status(ErrorCode::kInvalidArgument, message);
    }
    static Status Incomplete(std::string_view message) {
        return Status(ErrorCode::kIncomplete, message);
    }
    static Status NotFound(std::string_view message) {
        return Status(ErrorCode::kNotFound, message);
    }
    static Status AlreadyExists(std::string_view message) {
        return Status(ErrorCode::kAlreadyExists, message);
    }
    static Status Unauthenticated(std::string_view message) {
        return Status(ErrorCode::kUnauthenticated, message);
    }
    static Status ResourceExhausted(std::string_view message) {
        return Status(ErrorCode::kResourceExhausted, message);
    }
    static Status FailedPrecondition(std::string_view message) {
        return Status(ErrorCode::kFailedPrecondition, message);
    }
    static Status Aborted(std::string_view message) {
        return Status(ErrorCode::kAborted, message);
    }
    static Status Internal(std::string_view message) {
        return Status(ErrorCode::kInternal, message);
    }

    bool ok() const { return code_ == ErrorCode::kOk; }
    bool isIncomplete() const { return code_ == ErrorCode::kIncomplete; }
    ErrorCode code() const { return code_; }
    const std::string& message() const { return message_; }

private:
    Status(ErrorCode code, std::string_view message) : code_(code), message_(message) {}

    ErrorCode code_{ErrorCode::kOk};
    std::string message_;
};

}  // namespace live::common
