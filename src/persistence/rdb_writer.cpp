#include "persistence/rdb_writer.hpp"

#include <cstring>

#include "persistence/rdb_reader.hpp"  // crc64
#include "utils/time.hpp"

namespace redis::rdb {

void RdbWriter::putByte(uint8_t b) { body_.push_back(static_cast<char>(b)); }

void RdbWriter::putLength(uint64_t n) {
    if (n < (1ULL << 6)) {
        putByte(static_cast<uint8_t>(n));
    } else if (n < (1ULL << 14)) {
        putByte(static_cast<uint8_t>(((n >> 8) & 0x3F) | 0x40));
        putByte(static_cast<uint8_t>(n & 0xFF));
    } else {
        putByte(0x80);
        for (int i = 3; i >= 0; i--) putByte(static_cast<uint8_t>((n >> (i * 8)) & 0xFF));
    }
}

void RdbWriter::putString(const std::string& s) {
    putLength(s.size());
    body_ += s;
}

void RdbWriter::putLittleEndianDouble(double d) {
    uint64_t bits = 0;
    std::memcpy(&bits, &d, sizeof(bits));
    for (int i = 0; i < 8; i++) putByte(static_cast<uint8_t>((bits >> (i * 8)) & 0xFF));
}

void RdbWriter::writeEntry(const std::string& key, const RedisValue& value) {
    const int64_t now = timeutil::nowMs();
    if (value.isExpired(now)) return;  // nothing to persist

    // Expiry goes *before* the type byte and applies to exactly one key.
    if (value.expiryMs >= 0) {
        putByte(OP_EXPIRETIME_MS);
        for (int i = 7; i >= 0; i--) {
            putByte(static_cast<uint8_t>((static_cast<uint64_t>(value.expiryMs) >> (i * 8)) & 0xFF));
        }
    }

    switch (value.type) {
        case DataType::STRING:
            putByte(TYPE_STRING);
            putString(key);
            putString(value.stringValue);
            break;

        case DataType::LIST:
            putByte(TYPE_LIST);
            putString(key);
            putLength(value.listValue.size());
            for (const auto& e : value.listValue) putString(e);
            break;

        case DataType::SET:
            putByte(TYPE_SET);
            putString(key);
            putLength(value.setValue.size());
            for (const auto& e : value.setValue) putString(e);
            break;

        case DataType::HASH:
            putByte(TYPE_HASH);
            putString(key);
            putLength(value.hashValue.size());
            for (const auto& [f, v] : value.hashValue) {
                putString(f);
                putString(v);
            }
            break;

        case DataType::ZSET: {
            // TYPE_ZSET_2 stores the score as a raw little-endian double,
            // which is what Redis 7 writes.
            putByte(TYPE_ZSET_2);
            putString(key);
            putLength(value.zsetScores.size());
            // Walk the score-ordered map so the file is deterministic.
            for (const auto& [score, member] : value.zsetByScore) {
                putString(member);
                putLittleEndianDouble(score);
            }
            break;
        }

        case DataType::STREAM:
            // Streams are not serialised. Writing a partial stream would be
            // worse than not writing it: the key would come back empty and
            // XLEN would report a number that never existed.
            break;

        default:
            break;
    }
}

std::string RdbWriter::finish() const {
    std::string out(kMagic, 5);
    std::string version = std::to_string(kVersion);
    version.insert(version.begin(), 4 - version.size(), '0');  // "REDIS0011"
    out += version;

    out += body_;

    // A single database, so declare it rather than relying on a default.
    out.push_back(static_cast<char>(OP_SELECTDB));
    out.push_back(0);
    out.push_back(static_cast<char>(OP_RESIZEDB));
    out.push_back(0);
    out.push_back(0);

    out.push_back(static_cast<char>(OP_EOF));

    const uint64_t sum = crc64(reinterpret_cast<const uint8_t*>(out.data()), out.size());
    for (int i = 0; i < 8; i++) out.push_back(static_cast<char>((sum >> (i * 8)) & 0xFF));
    return out;
}

}  // namespace redis::rdb
