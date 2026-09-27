#include "persistence/rdb_reader.hpp"

#include <cmath>
#include <cstring>

#include "utils/time.hpp"

namespace redis::rdb {

namespace {

/// LZF decompression, as used inside RDB string payloads.
///
/// A control byte whose upper 3 bits select a literal run (7) or a
/// back-reference. Every copy source lies strictly behind the write cursor,
/// which is what makes the overlapping case -- a run longer than its window --
/// legal.
bool lzfDecompress(const uint8_t* in, size_t inLen, size_t expectedOut, std::string& out) {
    out.clear();
    out.reserve(expectedOut);
    size_t i = 0;
    while (i < inLen) {
        unsigned ctrl = in[i++];
        if (ctrl < 32) {
            size_t len = ctrl + 1;
            if (i + len > inLen) return false;
            out.append(reinterpret_cast<const char*>(in + i), len);
            i += len;
        } else {
            size_t len = ctrl >> 5;
            if (len == 7) {
                if (i >= inLen) return false;
                len += in[i++];
            }
            if (i >= inLen) return false;
            size_t ref = out.size() - ((ctrl & 0x1f) << 8) - in[i++] - 1;
            if (ref >= out.size()) return false;
            for (size_t k = 0; k <= len + 1; k++) out.push_back(out[ref + k]);
        }
    }
    return out.size() == expectedOut;
}

/// Read a ziplist entry-length prefix, advancing `p`.
bool ziplistEntryLen(const uint8_t*& p, const uint8_t* end, uint32_t& len) {
    if (p >= end) return false;
    uint8_t first = *p;
    if ((first & 0xC0) == 0xC0) {
        if (p + 4 > end) return false;
        len = (static_cast<uint32_t>(first & 0x3F) << 24) |
              (static_cast<uint32_t>(p[1]) << 16) | (static_cast<uint32_t>(p[2]) << 8) |
              static_cast<uint32_t>(p[3]);
        p += 4;
    } else if ((first & 0x80) == 0x80) {
        if (p + 2 > end) return false;
        len = ((static_cast<uint32_t>(first) & 0x7F) << 8) | p[1];
        p += 2;
    } else {
        len = first & 0x7F;
        p += 1;
    }
    return true;
}

}  // namespace

uint64_t crc64(const uint8_t* data, size_t size, uint64_t seed) {
    uint64_t crc = seed;
    for (size_t j = 0; j < size; j++) {
        crc ^= data[j];
        for (int k = 0; k < 8; k++) crc = (crc >> 1) ^ (kCrcPoly & (~((crc & 1) - 1)));
    }
    return crc;
}

bool RdbReader::looksLikeRdb(const uint8_t* data, size_t size) {
    return size >= 9 && std::memcmp(data, kMagic, 5) == 0;
}

uint32_t RdbReader::peekVersion(const uint8_t* data, size_t size) {
    if (!looksLikeRdb(data, size)) return 0;
    uint32_t v = 0;
    for (int i = 0; i < 4; i++) {
        if (data[5 + i] < '0' || data[5 + i] > '9') return 0;
        v = v * 10 + static_cast<uint32_t>(data[5 + i] - '0');
    }
    return v;
}

bool RdbReader::readUint(uint64_t& out, int bytes) {
    if (pos_ + static_cast<size_t>(bytes) > size_) return false;
    // Big-endian, which is what every multi-byte integer in an RDB uses.
    uint64_t v = 0;
    for (int i = 0; i < bytes; i++) v = (v << 8) | data_[pos_ + static_cast<size_t>(i)];
    pos_ += static_cast<size_t>(bytes);
    out = v;
    return true;
}

bool RdbReader::readLength(uint64_t& out) {
    if (pos_ >= size_) return false;
    uint8_t first = data_[pos_++];
    uint8_t kind = (first >> 6) & 0x03;
    if (kind == LEN_6BIT) {
        out = first & 0x3F;
        return true;
    }
    if (kind == LEN_14BIT) {
        if (pos_ >= size_) return false;
        out = (static_cast<uint64_t>(first & 0x3F) << 8) | data_[pos_++];
        return true;
    }
    if (kind == LEN_32BIT) return readUint(out, 4);

    uint8_t spec = first & 0x3F;
    if (spec == SPECIAL_ZERO) { out = 0; return true; }
    if (spec == SPECIAL_ONE) { out = 1; return true; }
    if (spec == SPECIAL_TWO) { out = 2; return true; }

    if (spec == SPECIAL_ENC_STR) {
        if (pos_ >= size_) return false;
        int bytes;
        switch (data_[pos_++]) {
            case 0: bytes = 1; break;  // int8
            case 1: bytes = 2; break;  // int16
            case 2: bytes = 4; break;  // int32
            default: return false;
        }
        uint64_t raw = 0;
        if (!readUint(raw, bytes)) return false;
        int bits = bytes * 8;
        if (raw & (1ULL << (bits - 1))) raw |= ~0ULL << bits;  // sign extend
        out = raw;
        return true;
    }

    if (spec == SPECIAL_LZF) {
        uint64_t compressedLen = 0, uncompressedLen = 0;
        if (!readLength(compressedLen) || !readLength(uncompressedLen)) return false;
        if (compressedLen > size_ - pos_) return false;
        std::string decoded;
        if (!lzfDecompress(data_ + pos_, static_cast<size_t>(compressedLen),
                           static_cast<size_t>(uncompressedLen), decoded)) {
            return false;
        }
        pos_ += static_cast<size_t>(compressedLen);
        // The decoded bytes are not in the buffer, so park them for readString.
        lzfPending_ = std::move(decoded);
        out = static_cast<uint64_t>(decoded.size());
        return true;
    }
    return false;
}

bool RdbReader::readString(std::string& out) {
    if (!lzfPending_.empty()) {
        out = std::move(lzfPending_);
        lzfPending_.clear();
        return true;
    }
    uint64_t len = 0;
    if (!readLength(len)) return false;
    if (len > size_ - pos_) return false;
    out.assign(reinterpret_cast<const char*>(data_ + pos_), static_cast<size_t>(len));
    pos_ += static_cast<size_t>(len);
    return true;
}

bool RdbReader::readScoreAsString(double& out) {
    // RDB_TYPE_ZSET: the score is a length-prefixed 8-byte little-endian
    // IEEE754 bit pattern.
    std::string raw;
    if (!readString(raw) || raw.size() != 8) return false;
    uint64_t bits = 0;
    for (int i = 7; i >= 0; i--) bits = (bits << 8) | static_cast<uint8_t>(raw[static_cast<size_t>(i)]);
    std::memcpy(&out, &bits, sizeof(out));
    return true;
}

bool RdbReader::readScoreAsDouble(double& out) {
    // RDB_TYPE_ZSET_2 stores the score as a raw IEEE754 bit pattern in the
    // machine's native byte order, so this is a straight 8-byte copy rather
    // than a readUint -- unlike every other multi-byte field in the format.
    if (pos_ + 8 > size_) return false;
    std::memcpy(&out, data_ + pos_, sizeof(out));
    pos_ += 8;
    return true;
}

bool RdbReader::readPlainList(RedisValue& out) {
    out.type = DataType::LIST;
    uint64_t n = 0;
    if (!readLength(n)) return false;
    for (uint64_t i = 0; i < n; i++) {
        std::string e;
        if (!readString(e)) return false;
        out.listValue.push_back(std::move(e));
    }
    return true;
}

bool RdbReader::readPlainSet(RedisValue& out) {
    out.type = DataType::SET;
    uint64_t n = 0;
    if (!readLength(n)) return false;
    for (uint64_t i = 0; i < n; i++) {
        std::string e;
        if (!readString(e)) return false;
        out.setValue.insert(std::move(e));
    }
    return true;
}

bool RdbReader::readPlainHash(RedisValue& out) {
    out.type = DataType::HASH;
    uint64_t pairs = 0;
    if (!readLength(pairs)) return false;
    for (uint64_t i = 0; i < pairs; i++) {
        std::string f, v;
        if (!readString(f) || !readString(v)) return false;
        out.hashValue[std::move(f)] = std::move(v);
    }
    return true;
}

bool RdbReader::readZset(RedisValue& out, bool scoreIsString) {
    out.type = DataType::ZSET;
    uint64_t n = 0;
    if (!readLength(n)) return false;
    for (uint64_t i = 0; i < n; i++) {
        std::string member;
        if (!readString(member)) return false;
        double score = 0;
        bool ok = scoreIsString ? readScoreAsString(score) : readScoreAsDouble(score);
        if (!ok) return false;
        out.zsetScores[member] = score;
        out.zsetByScore.insert({score, member});
    }
    return true;
}

bool RdbReader::readIntset(RedisValue& out) {
    out.type = DataType::SET;
    std::string blob;
    if (!readString(blob)) return false;
    const uint8_t* p = reinterpret_cast<const uint8_t*>(blob.data());
    const uint8_t* end = p + blob.size();
    if (blob.size() < 4) return false;
    uint32_t encoding = 0;
    for (int i = 3; i >= 0; i--) encoding = (encoding << 8) | p[i];
    p += 4;

    int width;
    switch (encoding) {
        case 2: width = 2; break;
        case 4: width = 4; break;
        case 8: width = 8; break;
        default: return false;
    }
    while (p < end) {
        if (static_cast<size_t>(end - p) < static_cast<size_t>(width)) return false;
        uint64_t raw = 0;
        for (int i = width - 1; i >= 0; i--) raw = (raw << 8) | p[i];
        p += width;
        int bits = width * 8;
        if (raw & (1ULL << (bits - 1))) raw |= ~0ULL << bits;  // sign extend
        out.setValue.insert(std::to_string(raw));
    }
    return true;
}

bool RdbReader::readZiplist(const uint8_t* p, size_t len, std::vector<std::string>& out) {
    const uint8_t* end = p + len;
    uint32_t zlbytes = 0, zltail = 0;
    if (!ziplistEntryLen(p, end, zlbytes)) return false;
    if (!ziplistEntryLen(p, end, zltail)) return false;
    uint32_t zllen = 0;
    if (!ziplistEntryLen(p, end, zllen)) return false;
    (void)zltail;
    if (p >= end || (*p & 0xF0) != 0xF0) return false;  // prevlen must be present
    p += static_cast<size_t>(*p == 0xFE ? 5 : (*p == 0xFF ? 9 : 1));
    for (uint32_t i = 0; i < zllen; i++) {
        if (p >= end) return false;
        if (*p != 0xFE && *p != 0xFF) p += static_cast<size_t>(*p < 254 ? *p : (*p == 254 ? 5 : 9));
        if (p >= end) return false;
        uint32_t entryLen = 0;
        if (!ziplistEntryLen(p, end, entryLen)) return false;
        if (static_cast<size_t>(end - p) < entryLen) return false;
        out.emplace_back(reinterpret_cast<const char*>(p), entryLen);
        p += entryLen;
    }
    return true;
}

bool RdbReader::readQuicklist(RedisValue& out) {
    out.type = DataType::LIST;
    uint64_t nodes = 0;
    if (!readLength(nodes)) return false;
    for (uint64_t i = 0; i < nodes; i++) {
        std::string node;
        if (!readString(node)) return false;
        if (node.empty()) return false;
        // 0 = plain string element, 1 = PLAIN ziplist node, 2 = PACKED ziplist.
        uint8_t container = static_cast<uint8_t>(node[0]);
        if (container == 0) {
            out.listValue.push_back(node.substr(1));
        } else if (container == 1 || container == 2) {
            std::vector<std::string> items;
            if (!readZiplist(reinterpret_cast<const uint8_t*>(node.data()) + 1, node.size() - 1,
                             items)) {
                return false;
            }
            for (auto& item : items) out.listValue.push_back(std::move(item));
        } else {
            return false;
        }
    }
    return true;
}

bool RdbReader::readValue(uint8_t type, RedisValue& out) {
    switch (type) {
        case TYPE_STRING:    out.type = DataType::STRING; return readString(out.stringValue);
        case TYPE_LIST:      return readPlainList(out);
        case TYPE_SET:       return readPlainSet(out);
        case TYPE_SET_INTSET:return readIntset(out);
        case TYPE_HASH:      return readPlainHash(out);
        case TYPE_ZSET:      return readZset(out, true);
        case TYPE_ZSET_2:    return readZset(out, false);
        case TYPE_LIST_QUICKLIST:
        case TYPE_LIST_QUICKLIST_2: return readQuicklist(out);
        default:
            return false;
    }
}

bool RdbReader::readHeader(std::string& error) {
    if (!looksLikeRdb(data_, size_)) {
        error = "not an RDB file (bad magic)";
        return false;
    }
    uint32_t version = peekVersion(data_, size_);
    if (version == 0) {
        error = "unreadable RDB version field";
        return false;
    }
    if (version > kVersion) {
        error = "RDB version " + std::to_string(version) + " is newer than the " +
                std::to_string(kVersion) + " this build understands";
        return false;
    }
    pos_ = 9;
    return true;
}

bool RdbReader::load(Entries& out, std::string& error) {
    error.clear();
    if (!readHeader(error)) return false;

    const int64_t now = timeutil::nowMs();

    while (pos_ < size_) {
        uint8_t opcode = data_[pos_++];

        if (opcode == OP_EOF) break;

        switch (opcode) {
            case OP_SELECTDB: {
                uint64_t db = 0;
                if (!readUint(db, 1)) { error = "truncated SELECTDB"; return false; }
                continue;
            }
            case OP_RESIZEDB: {
                uint64_t a = 0, b = 0;
                if (!readLength(a) || !readLength(b)) { error = "truncated RESIZEDB"; return false; }
                continue;
            }
            case OP_AUX: {
                std::string k, v;
                if (!readString(k) || !readString(v)) { error = "truncated AUX"; return false; }
                continue;
            }
            case OP_MODULE_AUX:
            case OP_IDLE:
            case OP_FREQ:
            case OP_FUNCTION:
            case OP_FUNCTION2:
            case OP_SLOT_INFO: {
                // Metadata this server never writes. Skip the payload and keep
                // going rather than aborting the load.
                std::string ignored;
                if (!readString(ignored)) { error = "truncated metadata opcode"; return false; }
                continue;
            }
            case OP_EXPIRETIME: {
                uint64_t secs = 0;
                if (!readUint(secs, 4)) { error = "truncated EXPIRETIME"; return false; }
                pendingExpiryMs_ = static_cast<int64_t>(secs) * 1000;
                continue;
            }
            case OP_EXPIRETIME_MS: {
                uint64_t ms = 0;
                if (!readUint(ms, 8)) { error = "truncated EXPIRETIME_MS"; return false; }
                pendingExpiryMs_ = static_cast<int64_t>(ms);
                continue;
            }
            default:
                break;
        }

        // Everything else is a value type byte introducing a key.
        std::string key;
        if (!readString(key)) { error = "truncated key"; return false; }

        RedisValue value;
        if (!readValue(opcode, value)) {
            error = "key '" + key + "': unsupported or corrupt encoding, type byte 0x" +
                    std::to_string(static_cast<int>(opcode));
            return false;
        }

        if (pendingExpiryMs_ >= 0) {
            if (pendingExpiryMs_ <= now) {
                // Already dead by the time we read it -- drop it, as Redis does.
                pendingExpiryMs_ = -1;
                continue;
            }
            value.expiryMs = pendingExpiryMs_;
        }
        pendingExpiryMs_ = -1;

        out.emplace_back(std::move(key), std::move(value));
    }

    // The 8 bytes after EOF are a CRC64 over everything before it. Verified,
    // but a mismatch is reported rather than fatal: refusing to boot on a
    // checksum for a file we could otherwise read is worse than loading it.
    if (pos_ + kChecksumSize <= size_) {
        uint64_t expected = 0;
        for (int i = 7; i >= 0; i--) {
            expected = (expected << 8) | data_[pos_ + static_cast<size_t>(i)];
        }
        uint64_t actual = crc64(data_, pos_);
        if (actual != expected) {
            error = "RDB checksum mismatch (file says " + std::to_string(expected) +
                    ", contents hash to " + std::to_string(actual) + ")";
        }
    }

    return true;
}

}  // namespace redis::rdb
