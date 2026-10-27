// SHA-256 (FIPS 180-4) and base64 - small self-contained implementations so the
// project needs no OpenSSL. Used for block checksums and block transfer.
#pragma once

#include <cstdint>
#include <string>

namespace nexus {

std::string sha256Hex(const std::string& data);
std::string sha256Hex(const void* data, size_t len);

std::string base64Encode(const std::string& data);
// Returns false on malformed input.
bool base64Decode(const std::string& text, std::string& out);

}  // namespace nexus
