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

    // Auth
    std::string requirepass;
    std::string user = "default";
    // Password this replica offers to its master. Separate from requirepass:
    // one is who we are, the other is who we trust.
    std::string masterauth;
};

}  // namespace redis

#endif  // REDIS_TYPES_SERVER_CONFIG_HPP
