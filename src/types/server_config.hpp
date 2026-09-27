/**
 * server_config.hpp -- process configuration, parsed from argv.
 *
 * Pure data so that CONFIG GET, the RDB layer and the net layer can all read
 * it without depending on each other.
 */
#ifndef REDIS_TYPES_SERVER_CONFIG_HPP
#define REDIS_TYPES_SERVER_CONFIG_HPP

#include <string>

namespace redis {

struct ServerConfig {
    int port = 6379;
    std::string dir = ".";
    std::string dbfilename = "dump.rdb";

    // Replication
    bool isReplica = false;
    std::string masterHost;
    int masterPort = 0;

    // AOF
    bool appendonly = false;
    std::string appendfilename = "appendonly.aof";
    std::string appenddirname = "appendonlydir";
    // Redis accepts always/everysec/no. This server writes every command
    // immediately and fsyncs on close and on rewrite, which is `everysec`
    // behaviour in the sense that matters here: a crash can lose at most the
    // tail, never a whole file. Reported verbatim by CONFIG GET, so it must
    // reflect the policy actually in force.
    std::string appendfsync = "everysec";

    // Auth
    std::string requirepass;
    std::string user = "default";
    // Password this replica offers to its master. Separate from requirepass:
    // one is who we are, the other is who we trust.
    std::string masterauth;
};

}  // namespace redis

#endif  // REDIS_TYPES_SERVER_CONFIG_HPP
