/**
 * rdb_writer.hpp -- key/value pairs into RDB bytes.
 *
 * The reader's counterpart. It writes the plain type opcodes (0/1/2/4/5) that
 * every Redis since 2.6 can load, and a correct CRC64, so a file produced here
 * is readable by real redis-server and not just by our own loader.
 */
#ifndef REDIS_PERSISTENCE_RDB_WRITER_HPP
#define REDIS_PERSISTENCE_RDB_WRITER_HPP

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "persistence/rdb_opcodes.hpp"
#include "types/values.hpp"

namespace redis::rdb {

class RdbWriter {
public:
    /// Append a key. `now` is only used to decide whether an expired key is
    /// worth writing at all; expiry itself is absolute, so the file is
    /// self-describing.
    void writeEntry(const std::string& key, const RedisValue& value);

    /// Serialise everything written so far, complete with magic, footer and
    /// checksum.
    std::string finish() const;

    bool empty() const { return body_.empty(); }

private:
    void putByte(uint8_t b);
    void putString(const std::string& s);
    void putLength(uint64_t n);
    void putLittleEndianUint64(uint64_t v);
    void putLittleEndianDouble(double d);

    std::string body_;
};

}  // namespace redis::rdb

#endif  // REDIS_PERSISTENCE_RDB_WRITER_HPP
