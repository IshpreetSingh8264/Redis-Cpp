/**
 * sha256.cpp -- FIPS 180-4 SHA-256, hex encoded.
 *
 * Straight from the specification: a 64-byte block compressor over eight
 * 32-bit words of state. Only the standard round constants and initial state
 * are hard-coded; there is nothing Redis-specific in this file.
 */
#include "auth/sha256.hpp"

#include <array>
#include <cstdint>
#include <cstdlib>

namespace redis::auth {

namespace {

/// Round constants: the first 32 bits of the fractional parts of the cube
/// roots of the first 64 primes.
constexpr std::array<uint32_t, 64> kRoundConstants = {
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1,
    0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
    0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
    0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
    0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
    0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
    0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
    0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
    0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
};

/// Initial state: the first 32 bits of the fractional parts of the square
/// roots of the first 8 primes.
constexpr std::array<uint32_t, 8> kInitialState = {
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
    0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
};

constexpr size_t kBlockSize = 64;

uint32_t rotateRight(uint32_t word, int bits) {
    return (word >> bits) | (word << (32 - bits));
}

}  // namespace

std::string sha256Hex(const std::string& data) {
    std::array<uint32_t, 8> state = kInitialState;

    // Pad: a 0x80 byte, then zeros, then the message length in bits as a
    // 64-bit big-endian integer. The result is a whole number of blocks.
    const uint64_t bitLength = static_cast<uint64_t>(data.size()) * 8;
    std::string padded = data;
    padded.push_back(static_cast<char>(0x80));
    while (padded.size() % kBlockSize != kBlockSize - 8) padded.push_back('\0');
    for (int shift = 56; shift >= 0; shift -= 8) {
        padded.push_back(static_cast<char>((bitLength >> shift) & 0xFF));
    }

    for (size_t offset = 0; offset < padded.size(); offset += kBlockSize) {
        // The message schedule: the first 16 words are the block read as
        // big-endian 32-bit integers, the rest are derived from them.
        std::array<uint32_t, 64> schedule{};
        for (size_t i = 0; i < 16; i++) {
            const size_t at = offset + i * 4;
            schedule[i] = (static_cast<uint32_t>(static_cast<unsigned char>(padded[at])) << 24) |
                          (static_cast<uint32_t>(static_cast<unsigned char>(padded[at + 1])) << 16) |
                          (static_cast<uint32_t>(static_cast<unsigned char>(padded[at + 2])) << 8) |
                          static_cast<uint32_t>(static_cast<unsigned char>(padded[at + 3]));
        }
        for (size_t i = 16; i < 64; i++) {
            const uint32_t s0 = rotateRight(schedule[i - 15], 7) ^
                                rotateRight(schedule[i - 15], 18) ^ (schedule[i - 15] >> 3);
            const uint32_t s1 = rotateRight(schedule[i - 2], 17) ^
                                rotateRight(schedule[i - 2], 19) ^ (schedule[i - 2] >> 10);
            schedule[i] = schedule[i - 16] + s0 + schedule[i - 7] + s1;
        }

        uint32_t a = state[0], b = state[1], c = state[2], d = state[3];
        uint32_t e = state[4], f = state[5], g = state[6], h = state[7];
        for (size_t i = 0; i < 64; i++) {
            const uint32_t sigma1 = rotateRight(e, 6) ^ rotateRight(e, 11) ^ rotateRight(e, 25);
            const uint32_t choose = (e & f) ^ (~e & g);
            const uint32_t temp1 = h + sigma1 + choose + kRoundConstants[i] + schedule[i];
            const uint32_t sigma0 = rotateRight(a, 2) ^ rotateRight(a, 13) ^ rotateRight(a, 22);
            const uint32_t majority = (a & b) ^ (a & c) ^ (b & c);
            const uint32_t temp2 = sigma0 + majority;
            h = g;
            g = f;
            f = e;
            e = d + temp1;
            d = c;
            c = b;
            b = a;
            a = temp1 + temp2;
        }
        state[0] += a; state[1] += b; state[2] += c; state[3] += d;
        state[4] += e; state[5] += f; state[6] += g; state[7] += h;
    }

    static const char kHex[] = "0123456789abcdef";
    std::string out;
    out.reserve(64);
    for (uint32_t word : state) {
        for (int shift = 28; shift >= 0; shift -= 4) {
            out.push_back(kHex[(word >> shift) & 0xF]);
        }
    }
    return out;
}

}  // namespace redis::auth
