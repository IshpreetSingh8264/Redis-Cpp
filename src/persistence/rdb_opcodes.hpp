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
/// Version 12 is the format Redis 7.4 emits (RDB_VERSION in src/rdb.h). We
/// only *write* the type bytes Redis has understood since 2.6, so declaring 12
/// costs nothing and is what lets a dump produced by a current redis-server be
/// loaded at all -- a v11 ceiling rejects every 7.4 file outright, expiry
/// handling included.
inline constexpr uint32_t kVersion = 12;

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

/// What the 0b11 prefix means, and it depends on where the byte appears:
/// in a *length* position (a collection's cardinality) it is the value 0, 1
/// or 2; in a *string* position it is an encoding selector. One byte value, two
/// meanings, which is the single most confusing thing about the RDB format.
enum SpecialLength : uint8_t {
    /// Length context only: the cardinality is 0, 1 or 2.
    SPECIAL_ZERO = 0,
    SPECIAL_ONE = 1,
    SPECIAL_TWO = 2,
};

/// String context only: which encoding a payload uses. The sub-code is also
/// the width in bytes, and no separate width byte follows.
enum SpecialEncoding : uint8_t {
    SPECIAL_ENC_INT8 = 0,
    SPECIAL_ENC_INT16 = 1,
    SPECIAL_ENC_INT32 = 2,
    SPECIAL_LZF = 3,
};

/// Length of the trailing CRC64 that follows the EOF opcode.
inline constexpr size_t kChecksumSize = 8;

/// Redis's CRC64 polynomial in the form a right-shifting (reflected) loop
/// needs. The published CRC-64/REDIS constant is 0xad93d23594c935a9, which is
/// the most-significant-bit-first form; the reflected polynomial a CRC loop
/// that shifts right has to use is its bit reversal, 0x95ac9329ac4bc9b5.
/// Using the published constant directly produces a checksum that is
/// self-consistent -- our own writer and reader agree -- and wrong for
/// everything else, which is the worst kind of wrong: every RDB we wrote had a
/// checksum redis-server would reject.
///
/// Check value: crc64("123456789") == 0xe9c6d914c4b8d9ca.
inline constexpr uint64_t kCrcPoly = 0x95ac9329ac4bc9b5ULL;

}  // namespace redis::rdb

#endif  // REDIS_PERSISTENCE_RDB_OPCODES_HPP
