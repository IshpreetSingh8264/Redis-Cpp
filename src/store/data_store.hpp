/**
 * data_store.hpp -- the keyspace.
 *
 * One class, one concept. The 2,400 line header this replaces had a DataStore
 * that owned the value type, the blocking-queue bookkeeping, the pub/sub
 * directory, the RDB codec and a copy of the command handlers; those are now
 * separate modules, and this file is just the map plus its lock.
 *
 * Lock discipline: handlers never see the lock. They call read() or write()
 * with a closure and the store decides the lock. That is the only way to keep
 * a shared_mutex from being held across a notify() that re-enters the store.
 */
#ifndef REDIS_STORE_DATA_STORE_HPP
#define REDIS_STORE_DATA_STORE_HPP

#include <functional>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "types/values.hpp"

namespace redis {

class DataStore {
public:
    using Map = std::unordered_map<std::string, RedisValue>;

    /// Run `fn` under a shared lock. Return type is preserved.
    template <class Fn>
    decltype(auto) read(Fn&& fn) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return std::forward<Fn>(fn)(data_);
    }

    /// Run `fn` under an exclusive lock. Return type is preserved.
    template <class Fn>
    decltype(auto) write(Fn&& fn) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        return std::forward<Fn>(fn)(data_);
    }

    /// Number of live (non-expired) keys.
    size_t size() const;

    /// Drop every key.
    void clear();

    /// A consistent copy of every live key, for the RDB and AOF layers.
    std::vector<std::pair<std::string, RedisValue>> snapshot() const;

    /// Keys currently expired. The active expiry cycle in net/ calls this.
    std::vector<std::string> collectExpired() const;
    void removeKeys(const std::vector<std::string>& keys);

private:
    mutable std::shared_mutex mutex_;
    Map data_;
};

/// Erase `key` if its TTL has passed. Must be called with the write lock held.
/// Returns true if something was actually removed, which is what the callers
/// use to decide between "not found" and "found but stale".
bool purgeIfExpired(DataStore::Map& data, const std::string& key, int64_t nowMs);

/// The WRONGTYPE guard, in the one place it is spelled out.
bool typeMatches(const RedisValue& v, DataType expected);

}  // namespace redis

#endif  // REDIS_STORE_DATA_STORE_HPP
