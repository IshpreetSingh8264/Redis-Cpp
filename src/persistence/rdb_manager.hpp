/**
 * rdb_manager.hpp -- SAVE / BGSAVE / DEBUG RELOAD, and the startup load.
 *
 * Owns the file, not the format: rdb_reader and rdb_writer own the format.
 */
#ifndef REDIS_PERSISTENCE_RDB_MANAGER_HPP
#define REDIS_PERSISTENCE_RDB_MANAGER_HPP

#include <string>

namespace redis {

class DataStore;
class ServerConfig;

class RdbManager {
public:
    explicit RdbManager(ServerConfig* config) : config_(config) {}

    /// Serialise the whole keyspace. Returns false if the file could not be
    /// written; the previous dump is left alone in that case, which is what
    /// makes SAVE atomic from a restart's point of view.
    bool save(const DataStore& store, std::string& error) const;

    /// Replace the keyspace with the contents of the dump file. Returns false
    /// if the file is missing or unreadable; `loaded` says how many keys came
    /// back, `error` carries any warning (a bad checksum, say).
    bool load(DataStore& store, int& loaded, std::string& error) const;

    /// Replace the keyspace with the contents of an in-memory dump, i.e. the
    /// payload a master sends after +FULLRESYNC.
    bool loadFromBuffer(const std::string& blob, DataStore& store, int& loaded,
                        std::string& error) const;

    std::string path() const;

private:
    const ServerConfig* config_;
};

}  // namespace redis

#endif  // REDIS_PERSISTENCE_RDB_MANAGER_HPP
