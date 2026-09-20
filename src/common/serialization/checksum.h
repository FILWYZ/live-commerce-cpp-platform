#pragma once

#include <cstddef>
#include <cstdint>

namespace live::common::serialization {

// CRC-32 (IEEE 802.3) is used only to detect torn/corrupted local records.
// It is not a cryptographic checksum and must not be used for authentication.
inline std::uint32_t crc32(const void* data, std::size_t size) {
    const auto* bytes = static_cast<const std::uint8_t*>(data);
    std::uint32_t crc = 0xffffffffU;
    for (std::size_t i = 0; i < size; ++i) {
        crc ^= bytes[i];
        for (int bit = 0; bit < 8; ++bit) {
            crc = (crc & 1U) != 0U ? (crc >> 1U) ^ 0xedb88320U : crc >> 1U;
        }
    }
    return crc ^ 0xffffffffU;
}

}  // namespace live::common::serialization
