/**
 * rdb_reader.hpp -- RDB bytes into key/value pairs.
 */
#ifndef REDIS_PERSISTENCE_RDB_READER_HPP
#define REDIS_PERSISTENCE_RDB_READER_HPP

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "persistence/rdb_opcodes.hpp"
#include "types/values.hpp"

namespace redis::rdb {

using Entries = std::vector<std::pair<std::string, RedisValue>>;

class RdbReader {
public:
    RdbReader(const uint8_t* data, size_t size) : data_(data), size_(size) {}

    /// Decode the whole file. Returns false only on a hard structural error or
    /// an encoding this reader does not implement; `error` then says which.
    /// Anything decoded before the failure is still appended, which is what
    /// Redis does with a truncated RDB.
    ///
    /// Keys whose TTL elapsed before the file was read are dropped, so loading
    /// restores "what was live at save time" rather than "what was ever
    /// written".
    bool load(Entries& out, std::string& error);

    static bool looksLikeRdb(const uint8_t* data, size_t size);
    static uint32_t peekVersion(const uint8_t* data, size_t size);

private:
    bool readHeader(std::string& error);
    bool readLength(uint64_t& out);
    bool readString(std::string& out);
    bool readUint(uint64_t& out, int bytes);
    bool readScoreAsString(double& out);
    bool readScoreAsDouble(double& out);
    bool readValue(uint8_t type, RedisValue& out);
    bool readPlainList(RedisValue& out);
    bool readPlainSet(RedisValue& out);
    bool readPlainHash(RedisValue& out);
    bool readZset(RedisValue& out, bool scoreIsString);
    bool readIntset(RedisValue& out);
    bool readQuicklist(RedisValue& out);
    bool readZiplist(const uint8_t* p, size_t len, std::vector<std::string>& out);

    const uint8_t* data_ = nullptr;
    size_t size_ = 0;
    size_t pos_ = 0;
    int64_t pendingExpiryMs_ = -1;
    /// Set by readLength() when the previous string was LZF-compressed; the
    /// decompressed bytes live outside the buffer so readString() drains them.
    std::string lzfPending_;
};

/// Reflected CRC64 over a byte range, as Redis computes it.
uint64_t crc64(const uint8_t* data, size_t size, uint64_t seed = 0);

}  // namespace redis::rdb

#endif  // REDIS_PERSISTENCE_RDB_READER_HPP
