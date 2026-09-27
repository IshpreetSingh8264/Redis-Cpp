/**
 * services.hpp -- the aggregate handed to every command handler.
 *
 * Layered by role (not by class), the same way Shell's `builtins` are handed
 * nothing at all and `dispatcher` is handed the world: the command layer
 * receives pointers to the layers below it and never constructs them, and
 * never reaches back up into net/ to find them.
 *
 * The one exception is `execute`, a callback the net layer installs so that
 * EXEC can run its queue through exactly the same path a top-level command
 * takes -- including the AOF append and the replica propagation. A queued
 * write that skipped those would be a write that vanishes on restart and never
 * reaches a replica, and giving the command layer a reference to the dispatcher
 * to achieve that would turn the include graph back into a cycle.
 *
 * Forward declarations only, so this header has no dependencies of its own.
 */
#ifndef REDIS_TYPES_SERVICES_HPP
#define REDIS_TYPES_SERVICES_HPP

#include <functional>
#include <string>
#include <vector>

namespace redis {

class ServerConfig;
class DataStore;
class BlockedClients;
class ReplicationManager;
class RdbManager;
class AofManager;
class AuthManager;
class PubSubDirectory;
class ClientSession;

struct Services {
    ServerConfig* config = nullptr;
    DataStore* store = nullptr;
    BlockedClients* blocked = nullptr;
    ReplicationManager* replication = nullptr;
    RdbManager* rdb = nullptr;
    AofManager* aof = nullptr;
    AuthManager* auth = nullptr;
    PubSubDirectory* pubsub = nullptr;

    /// Run one command as if it had arrived on `client`. Installed by
    /// net/server.cpp. See the note at the top of this file.
    std::function<std::string(const std::vector<std::string>&, ClientSession*)> execute;
};

}  // namespace redis

#endif  // REDIS_TYPES_SERVICES_HPP
