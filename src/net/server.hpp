/**
 * server.hpp -- the event loop and the dispatcher.
 *
 * One layer owns every cross-cutting concern: socket lifetime, the read
 * buffer, the MULTI queueing rule, the AOF append, the replica propagation and
 * the READONLY check on a replica. The command modules know none of this --
 * they are a map from name to function, and they cannot influence anything
 * outside their own reply except by mutating the store.
 */
#ifndef REDIS_NET_SERVER_HPP
#define REDIS_NET_SERVER_HPP

#include <memory>
#include <string>
#include <unordered_map>

#include "commands/pubsub_directory.hpp"
#include "persistence/aof_manager.hpp"
#include "persistence/rdb_manager.hpp"
#include "replication/replication_manager.hpp"
#include "store/blocked_clients.hpp"
#include "store/data_store.hpp"
#include "types/handler.hpp"
#include "types/server_config.hpp"

namespace redis {

class RedisServer {
public:
    explicit RedisServer(ServerConfig config);
    ~RedisServer();

    /// Load the dump, open the AOF, start replication, then serve forever.
    void run();

    /// Parse argv into `config`. Returns false and prints usage on a bad flag.
    static bool parseArguments(int argc, char** argv, ServerConfig& config);

private:
    void listen();
    void addToPoll(int fd);
    void acceptConnection();
    void handleReadable(int fd);
    void processInput(ClientSession& session);
    void ensureMasterSession();
    void closeConnection(int fd);
    void tick();

    /// The single path every command takes: registry lookup, MULTI queueing,
    /// auth check, replica read-only check, handler call, AOF append,
    /// propagation. EXEC re-enters this, which is why it is a function.
    std::string execute(const std::vector<std::string>& args, ClientSession& session);

    ServerConfig config_;
    DataStore store_;
    BlockedClients blocked_;
    RdbManager rdb_;
    std::unique_ptr<AofManager> aof_;
    std::unique_ptr<AuthManager> auth_;
    ReplicationManager replication_;
    PubSubDirectory pubsub_;
    CommandRegistry registry_;
    Services services_;

    /// The session that stands for the upstream master link on a replica. It is
    /// a session like any other, so the command layer needs no special case;
    /// it just never gets a reply written back.
    std::unique_ptr<ClientSession> masterSession_;

    int listenFd_ = -1;
    int epollFd_ = -1;
    std::unordered_map<int, std::unique_ptr<ClientSession>> clients_;
    bool stopping_ = false;
    bool masterLinkRegistered_ = false;
    int64_t lastResyncAttemptMs_ = 0;
    bool linkErrorReported_ = false;
};

}  // namespace redis

#endif  // REDIS_NET_SERVER_HPP
