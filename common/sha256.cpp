#include "sha256.h"

#include <cstring>
#include <vector>

namespace nexus {

namespace {

const uint32_t K[64] = {
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2};

inline uint32_t rotr(uint32_t x, int n) { return (x >> n) | (x << (32 - n)); }

void compress(uint32_t h[8], const unsigned char* p) {
    uint32_t w[64];
    for (int i = 0; i < 16; ++i)
        w[i] = (uint32_t(p[i * 4]) << 24) | (uint32_t(p[i * 4 + 1]) << 16) |
               (uint32_t(p[i * 4 + 2]) << 8) | uint32_t(p[i * 4 + 3]);
    for (int i = 16; i < 64; ++i) {
        uint32_t s0 = rotr(w[i - 15], 7) ^ rotr(w[i - 15], 18) ^ (w[i - 15] >> 3);
        uint32_t s1 = rotr(w[i - 2], 17) ^ rotr(w[i - 2], 19) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16] + s0 + w[i - 7] + s1;
    }
    uint32_t a = h[0], b = h[1], c = h[2], d = h[3], e = h[4], f = h[5], g = h[6], hh = h[7];
    for (int i = 0; i < 64; ++i) {
        uint32_t S1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25);
        uint32_t ch = (e & f) ^ (~e & g);
        uint32_t t1 = hh + S1 + ch + K[i] + w[i];
        uint32_t S0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22);
        uint32_t mj = (a & b) ^ (a & c) ^ (b & c);
        uint32_t t2 = S0 + mj;
        hh = g; g = f; f = e; e = d + t1; d = c; c = b; b = a; a = t1 + t2;
    }
    h[0] += a; h[1] += b; h[2] += c; h[3] += d; h[4] += e; h[5] += f; h[6] += g; h[7] += hh;
}

}  // namespace

std::string sha256Hex(const void* data, size_t len) {
    uint32_t h[8] = {0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
                     0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};
    const unsigned char* p = static_cast<const unsigned char*>(data);
    size_t full = len / 64;
    for (size_t i = 0; i < full; ++i) compress(h, p + i * 64);

    unsigned char tail[128];
    size_t rem = len - full * 64;
    std::memset(tail, 0, sizeof tail);
    if (rem) std::memcpy(tail, p + full * 64, rem);
    tail[rem] = 0x80;
    size_t tailLen = (rem < 56) ? 64 : 128;
    uint64_t bits = static_cast<uint64_t>(len) * 8;
    for (int i = 0; i < 8; ++i) tail[tailLen - 1 - i] = static_cast<unsigned char>(bits >> (8 * i));
    compress(h, tail);
    if (tailLen == 128) compress(h, tail + 64);

    static const char* hex = "0123456789abcdef";
    std::string out;
    for (int i = 0; i < 8; ++i)
        for (int s = 28; s >= 0; s -= 4) out += hex[(h[i] >> s) & 0xF];
    return out;
}

std::string sha256Hex(const std::string& data) { return sha256Hex(data.data(), data.size()); }

// ------------------------------------------------------------------ base64 --

std::string base64Encode(const std::string& in) {
    static const char* tbl = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve((in.size() + 2) / 3 * 4);
    size_t i = 0;
    while (i + 2 < in.size()) {
        uint32_t v = (uint8_t(in[i]) << 16) | (uint8_t(in[i + 1]) << 8) | uint8_t(in[i + 2]);
        out += tbl[(v >> 18) & 63]; out += tbl[(v >> 12) & 63]; out += tbl[(v >> 6) & 63]; out += tbl[v & 63];
        i += 3;
    }
    if (i + 1 == in.size()) {
        uint32_t v = uint8_t(in[i]) << 16;
        out += tbl[(v >> 18) & 63]; out += tbl[(v >> 12) & 63]; out += "==";
    } else if (i + 2 == in.size()) {
        uint32_t v = (uint8_t(in[i]) << 16) | (uint8_t(in[i + 1]) << 8);
        out += tbl[(v >> 18) & 63]; out += tbl[(v >> 12) & 63]; out += tbl[(v >> 6) & 63]; out += '=';
    }
    return out;
}

bool base64Decode(const std::string& text, std::string& out) {
    static int8_t rev[256];
    static bool init = false;
    if (!init) {
        std::memset(rev, -1, sizeof rev);
        const char* tbl = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        for (int i = 0; i < 64; ++i) rev[uint8_t(tbl[i])] = static_cast<int8_t>(i);
        init = true;
    }
    out.clear();
    if (text.size() % 4 != 0) return false;
    out.reserve(text.size() / 4 * 3);
    for (size_t i = 0; i < text.size(); i += 4) {
        int pad = 0;
        uint32_t v = 0;
        for (int k = 0; k < 4; ++k) {
            char c = text[i + k];
            if (c == '=') {
                if (i + 4 != text.size() || k < 2) return false;   // '=' only at the very end
                ++pad;
                v <<= 6;
            } else {
                if (pad) return false;
                int8_t r = rev[uint8_t(c)];
                if (r < 0) return false;
                v = (v << 6) | static_cast<uint32_t>(r);
            }
        }
        out += static_cast<char>((v >> 16) & 0xFF);
        if (pad < 2) out += static_cast<char>((v >> 8) & 0xFF);
        if (pad < 1) out += static_cast<char>(v & 0xFF);
    }
    return true;
}

}  // namespace nexus
