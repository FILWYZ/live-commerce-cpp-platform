#pragma once

#include "common/error/status.h"

#include <string>

namespace live::common::security {

// PBKDF2-HMAC-SHA256 password storage. The encoded value contains the
// algorithm, work factor, random salt and derived key so it can be migrated
// without changing the User model again.
class PasswordHasher {
public:
    static common::Status hash(const std::string& password, std::string* encoded);
    static common::Status verify(const std::string& password, const std::string& encoded, bool* matches);
};

}  // namespace live::common::security
