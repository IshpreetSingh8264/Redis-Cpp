/**
 * values.hpp -- the data shapes the store holds.
 *
 * One concept per type. StreamEntry and XaddResult are stream-specific and
 * would otherwise be lost inside the 2,400 line data_store header they came
 * out of.
 */
#ifndef REDIS_TYPES_VALUES_HPP
#define REDIS_TYPES_VALUES_HPP

#include <cstdint>
#include <deque>
#include <limits>
#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "types/enums.hpp"

namespace redis {

/// Sentinel for "this key has no expiry". Matches Redis's own convention of
/// treating any negative expiry as absent.
inline constexpr int64_t kNoExpiry = -1;

/// One entry in a stream. `id` is always the canonical "<ms>-<seq>" rendering
/// of (timestamp, sequence); it is stored rather than recomputed so that what
/// the client sent and what we hand back cannot drift apart.
struct StreamEntry {
    std::string id;
    uint64_t timestamp = 0;
    uint64_t sequence = 0;
    std::vector<std::pair<std::string, std::string>> fields;
};

/// Outcome of an XADD, so the handler can distinguish "made a new stream" from
/// "appended to an existing one" (NOMKSTREAM) and know the id it produced.
struct XaddResult {
    bool created = false;   // true when XADD had to create the key
    std::string id;         // the id actually stored
};

/// One entry in a sorted set. Held in a multimap keyed by score so that
/// ZRANGE/ZCOUNT are ordered scans and ZRANK is a position.
using ZsetEntry = std::pair<double, std::string>;

/// Everything Redis can store under one key.
///
/// One struct with one member per type rather than a variant: the command
/// handlers need cheap type checks (`if (v.type != DataType::LIST) wrongType`)
/// far more often than they need to move a value between representations.
struct RedisValue {
    DataType type = DataType::NONE;

    std::string stringValue;
    std::deque<std::string> listValue;
    std::unordered_set<std::string> setValue;
    std::unordered_map<std::string, std::string> hashValue;
    std::multimap<double, std::string> zsetByScore;
    std::unordered_map<std::string, double> zsetScores;
    std::vector<StreamEntry> streamValue;

    uint64_t streamLastTimestamp = 0;
    uint64_t streamLastSequence = 0;

    /// Absolute wall-clock expiry in milliseconds since the epoch, or kNoExpiry.
    int64_t expiryMs = kNoExpiry;

    bool isExpired(int64_t nowMs) const {
        return expiryMs >= 0 && nowMs >= expiryMs;
    }

    /// TTL in milliseconds, or -1 when the key never expires (Redis semantics).
    int64_t ttlMs(int64_t nowMs) const {
        if (expiryMs < 0) return -1;
        int64_t remaining = expiryMs - nowMs;
        return remaining > 0 ? remaining : 0;
    }
};

/// Result of resolving an ID string of the form "<ms>-<seq>".
struct StreamId {
    uint64_t ms = 0;
    uint64_t seq = 0;
    bool valid = false;
};

/// True when (a) sorts strictly before (b). Stream IDs order lexicographically
/// on (ms, seq) and nothing else, so this is the single comparison every
/// stream command needs.
inline bool streamIdLess(const StreamId& a, const StreamId& b) {
    if (a.ms != b.ms) return a.ms < b.ms;
    return a.seq < b.seq;
}

/// The largest representable stream ID, i.e. the "+" of XRANGE.
inline StreamId streamIdMax() {
    return StreamId{std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max(), true};
}

}  // namespace redis

#endif  // REDIS_TYPES_VALUES_HPP
