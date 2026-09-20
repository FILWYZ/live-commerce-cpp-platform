#include "common/security/password_hasher.h"

#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/rand.h>

#include <array>
#include <charconv>
#include <cstdint>

namespace live::common::security {
namespace {

constexpr int kIterations = 210000;
constexpr std::size_t kSaltBytes = 16;
constexpr std::size_t kDerivedKeyBytes = 32;

char hexDigit(unsigned char value) {
    return value < 10 ? static_cast<char>('0' + value) : static_cast<char>('a' + value - 10);
}

std::string hexEncode(const unsigned char* data, std::size_t size) {
    std::string result;
    result.reserve(size * 2);
    for (std::size_t i = 0; i < size; ++i) {
        result.push_back(hexDigit(static_cast<unsigned char>(data[i] >> 4)));
        result.push_back(hexDigit(static_cast<unsigned char>(data[i] & 0x0f)));
    }
    return result;
}

int hexValue(char value) {
    if (value >= '0' && value <= '9') return value - '0';
    if (value >= 'a' && value <= 'f') return value - 'a' + 10;
    if (value >= 'A' && value <= 'F') return value - 'A' + 10;
    return -1;
}

bool hexDecode(const std::string& input, unsigned char* output, std::size_t output_size) {
    if (input.size() != output_size * 2) return false;
    for (std::size_t i = 0; i < output_size; ++i) {
        const int high = hexValue(input[i * 2]);
        const int low = hexValue(input[i * 2 + 1]);
        if (high < 0 || low < 0) return false;
        output[i] = static_cast<unsigned char>((high << 4) | low);
    }
    return true;
}

common::Status derive(const std::string& password, const unsigned char* salt, std::size_t salt_size,
                      int iterations, unsigned char* output, std::size_t output_size) {
    if (password.empty() || password.size() > 1024 || salt == nullptr || output == nullptr ||
        salt_size == 0 || iterations <= 0) {
        return common::Status::InvalidArgument("invalid password hash arguments");
    }
    if (PKCS5_PBKDF2_HMAC(password.data(), static_cast<int>(password.size()), salt,
                          static_cast<int>(salt_size), iterations, EVP_sha256(),
                          static_cast<int>(output_size), output) != 1) {
        return common::Status::Internal("password derivation failed");
    }
    return common::Status::Ok();
}

}  // namespace

common::Status PasswordHasher::hash(const std::string& password, std::string* encoded) {
    if (encoded == nullptr || password.empty() || password.size() > 1024) {
        return common::Status::InvalidArgument("password must contain 1 to 1024 bytes");
    }
    std::array<unsigned char, kSaltBytes> salt{};
    std::array<unsigned char, kDerivedKeyBytes> derived{};
    if (RAND_bytes(salt.data(), static_cast<int>(salt.size())) != 1) {
        return common::Status::Internal("secure random generator failed");
    }
    if (const auto status = derive(password, salt.data(), salt.size(), kIterations,
                                   derived.data(), derived.size()); !status.ok()) {
        return status;
    }
    *encoded = "pbkdf2-sha256$" + std::to_string(kIterations) + "$" +
               hexEncode(salt.data(), salt.size()) + "$" +
               hexEncode(derived.data(), derived.size());
    return common::Status::Ok();
}

common::Status PasswordHasher::verify(const std::string& password, const std::string& encoded, bool* matches) {
    if (matches == nullptr || password.empty()) return common::Status::InvalidArgument("invalid password verification arguments");
    *matches = false;
    const auto first = encoded.find('$');
    const auto second = first == std::string::npos ? std::string::npos : encoded.find('$', first + 1);
    const auto third = second == std::string::npos ? std::string::npos : encoded.find('$', second + 1);
    if (first == std::string::npos || second == std::string::npos || third == std::string::npos ||
        encoded.substr(0, first) != "pbkdf2-sha256") {
        return common::Status::InvalidArgument("invalid password hash format");
    }
    int iterations = 0;
    const auto iteration_text = encoded.substr(first + 1, second - first - 1);
    const auto parse = std::from_chars(iteration_text.data(), iteration_text.data() + iteration_text.size(), iterations);
    if (parse.ec != std::errc{} || parse.ptr != iteration_text.data() + iteration_text.size() || iterations < 10000 || iterations > 10000000) {
        return common::Status::InvalidArgument("invalid password work factor");
    }
    const std::string salt_text = encoded.substr(second + 1, third - second - 1);
    const std::string derived_text = encoded.substr(third + 1);
    std::array<unsigned char, kSaltBytes> salt{};
    std::array<unsigned char, kDerivedKeyBytes> expected{};
    std::array<unsigned char, kDerivedKeyBytes> actual{};
    if (!hexDecode(salt_text, salt.data(), salt.size()) ||
        !hexDecode(derived_text, expected.data(), expected.size())) {
        return common::Status::InvalidArgument("invalid password hash encoding");
    }
    if (const auto status = derive(password, salt.data(), salt.size(), iterations,
                                   actual.data(), actual.size()); !status.ok()) {
        return status;
    }
    *matches = CRYPTO_memcmp(actual.data(), expected.data(), actual.size()) == 0;
    return common::Status::Ok();
}

}  // namespace live::common::security
