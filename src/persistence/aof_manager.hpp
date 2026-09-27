/**
 * aof_manager.hpp -- the append-only file.
 *
 * Redis 7 layout, which is what CodeCrafters checks for:
 *
 *   <dir>/<appenddirname>/
 *       appendonly.aof.manifest      -- the list of parts
 *       appendonly.aof.1.base.rdb    -- RDB snapshot, full keyspace
 *       appendonly.aof.1.incr.aof    -- RESP commands appended since the snapshot
 *
 * The manifest is what makes this restartable: without it there is no way to
 * know which parts are current, and a half-written snapshot would be
 * indistinguishable from a good one.
 */
#ifndef REDIS_PERSISTENCE_AOF_MANAGER_HPP
#define REDIS_PERSISTENCE_AOF_MANAGER_HPP

#include <functional>
#include <mutex>
#include <string>
#include <vector>

#include "types/server_config.hpp"

namespace redis {

class DataStore;
class ServerConfig;
class Services;

class AofManager {
public:
    AofManager(ServerConfig* config, Services* services)
        : config_(config), services_(services) {}

    /// Read the configuration commands for CONFIG GET appendonly /
    /// appendfilename / appenddirname. Case-insensitive in `param`.
    std::string configGet(const std::string& param) const;

    /// CONFIG SET appendonly|appendfilename|appenddirname. Returns false and
    /// fills `error` if the change could not be made.
    bool configSet(const std::string& param, const std::string& value, std::string& error);

    /// Bring the AOF up. When it is enabled this creates the directory and
    /// files if they are missing, then loads the base snapshot and replays the
    /// incremental part. When disabled it is a no-op.
    ///
    /// `applyCommand` must be set first: replaying an incremental part means
    /// actually running the commands in it.
    bool open(DataStore& store, std::string& error);

    /// Fold the whole keyspace into a fresh base part and start a new
    /// incremental one. This is what BGREWRITEAOF does; without it the
    /// incremental part grows forever and every restart replays all of it.
    bool rewrite(DataStore& store, std::string& error);

    /// Append one write command. Silently does nothing when AOF is off, so the
    /// caller does not have to branch.
    void append(const std::vector<std::string>& args);

    /// Flush and close. Called on shutdown and by SHUTDOWN.
    void close();

    bool enabled() const { return config_->appendonly; }
    bool isOpen() const;
    std::string dirPath() const;
    std::string manifestPath() const;
    std::string basePath() const;
    std::string incrPath() const;
    /// Part filenames currently listed in the manifest, in order.
    std::vector<std::string> manifestEntries() const;
    size_t bufferedBytes() const;

    /// Runs a command read back out of the incremental part. Installed by
    /// net/server, because replaying a command is exactly dispatching one.
    std::function<void(const std::vector<std::string>&)> applyCommand;

private:
    bool writeManifest(std::string& error) const;
    bool writeFreshBase(DataStore& store, std::string& error) const;
    /// The base part of the next rewrite. `sequence` is the manifest sequence.
    bool writeBasePart(const std::string& name, DataStore& store, std::string& error) const;
    bool readManifest(std::vector<std::string>& parts) const;
    bool replayIncrement(const std::string& path, std::string& error) const;

    ServerConfig* config_;
    Services* services_;
    mutable std::mutex mutex_;
    int fd_ = -1;  // the incremental part, held open for appends
};

}  // namespace redis

#endif  // REDIS_PERSISTENCE_AOF_MANAGER_HPP
