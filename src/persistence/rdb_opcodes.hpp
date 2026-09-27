/**
 * rdb_opcodes.hpp -- the RDB file format's byte vocabulary.
 *
 * Split out from the reader and writer because both halves need the same
 * numbers and neither should own them.
 */
#ifndef REDIS_PERSISTENCE_RDB_OPCODES_HPP
#define REDIS_PERSISTENCE_RDB_OPCODES_HPP

#include <cstdint>

namespace redis::rdb {

/// File header. "REDIS" + a 4-digit version.
inline constexpr char kMagic[5] = {'R', 'E', 'D', 'I', 'S'};
/// Version 11 is the last format Redis 7.x emits and every version since 7
/// reads, so it is the right thing to write.
inline constexpr uint32_t kVersion = 11;

/// Structural opcodes.
enum Opcode : uint8_t {
    OP_SLOT_INFO = 0xF4,
    OP_FUNCTION2 = 0xF5,
    OP_FUNCTION = 0xF6,
    OP_MODULE_AUX = 0xF7,
    OP_IDLE = 0xF8,
    OP_FREQ = 0xF9,
    OP_AUX = 0xFA,
    OP_RESIZEDB = 0xFB,
    OP_EXPIRETIME_MS = 0xFC,
    OP_EXPIRETIME = 0xFD,
    OP_SELECTDB = 0xFE,
    OP_EOF = 0xFF,
};

/// Value type bytes, i.e. the first byte of a key-value pair.
enum Type : uint8_t {
    TYPE_STRING = 0,
    TYPE_LIST = 1,
    TYPE_SET = 2,
    TYPE_ZSET = 3,          // score stored as a length-prefixed string
    TYPE_HASH = 4,
    TYPE_ZSET_2 = 5,        // score stored as a raw little-endian double
    TYPE_HASH_ZIPMAP = 9,
    TYPE_LIST_ZIPLIST = 10,
    TYPE_SET_INTSET = 11,
    TYPE_ZSET_ZIPLIST = 12,
    TYPE_HASH_ZIPLIST = 13,
    TYPE_LIST_QUICKLIST = 14,
    TYPE_STREAM_LISTPACKS = 15,
    TYPE_HASH_LISTPACK = 16,
    TYPE_ZSET_LISTPACK = 17,
    TYPE_LIST_QUICKLIST_2 = 18,
    TYPE_STREAM_LISTPACKS_2 = 19,
    TYPE_SET_LISTPACK = 20,
};

/// Top two bits of a length byte. The 11 case is a special encoding, not a
/// width.
enum LengthKind : uint8_t { LEN_6BIT = 0, LEN_14BIT = 1, LEN_32BIT = 2, LEN_SPECIAL = 3 };

/// The six special encodings that share the 0b11 prefix.
enum SpecialLength : uint8_t {
    SPECIAL_ZERO = 0,
    SPECIAL_ONE = 1,
    SPECIAL_TWO = 2,
    SPECIAL_ENC_STR = 3,   // an 8/16/32-bit integer, then that many bytes
    SPECIAL_LZF = 4,      // compressed length, uncompressed length, payload
};

/// Length of the trailing CRC64 that follows the EOF opcode.
inline constexpr size_t kChecksumSize = 8;

/// Redis's CRC64: the Jones polynomial, reflected, seeded with zero.
inline constexpr uint64_t kCrcPoly = 0xad93d23594c935a9ULL;

}  // namespace redis::rdb

#endif  // REDIS_PERSISTENCE_RDB_OPCODES_HPP
