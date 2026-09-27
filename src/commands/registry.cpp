/**
 * registry.cpp -- the dispatch table.
 *
 * This file is the only place in the project that knows the complete list of
 * commands. Every group file exposes one `registerXCommands()`; nothing else
 * needs editing to add a command, and nothing else branches on command names.
 */
#include "types/handler.hpp"

#include <unordered_set>

#include "protocol/resp.hpp"
#include "utils/strutil.hpp"

namespace redis {

const char* const kWrongTypeError = "WRONGTYPE Operation against a key holding the wrong kind of value";

std::string wrongArity(const std::string& command) {
    return resp::error("ERR wrong number of arguments for '" + strutil::toLower(command) +
                       "' command");
}

CommandRegistry buildRegistry() {
    // Memoised: the registry is a set of stateless function objects, so one
    // instance can be shared. COMMAND reads it to report the command list.
    static CommandRegistry registry = [] {
    CommandRegistry built;
    built.reserve(160);

    registerStringCommands(built);
    registerKeyCommands(built);
    registerListCommands(built);
    registerSetCommands(built);
    registerHashCommands(built);
    registerZsetCommands(built);
    registerStreamCommands(built);
    registerTransactionCommands(built);
    registerPubsubCommands(built);
    registerGeoCommands(built);
    registerAuthCommands(built);
    registerServerCommands(built);
    registerReplicationCommands(built);
    registerPersistenceCommands(built);
    return built;
    }();
    return registry;
}

bool isWriteCommand(const std::string& upperName) {
    // Kept as a set rather than inferred from a prefix so that "which commands
    // go to the AOF and to the replicas" is a single answerable question.
    static const std::unordered_set<std::string> kWrites = {
        // strings
        "SET", "SETNX", "SETXX", "GETSET", "SETEX", "PSETEX", "SETRANGE", "APPEND",
        "INCR", "DECR", "INCRBY", "DECRBY", "INCRBYFLOAT", "MSET", "MSETNX",
        // keys
        "DEL", "UNLINK", "EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT", "PERSIST",
        "RENAME", "RENAMENX", "COPY", "FLUSHDB", "FLUSHALL", "RESTORE",
        // lists
        "LPUSH", "RPUSH", "LPUSHX", "RPUSHX", "LPOP", "RPOP", "LSET", "LREM",
        "LTRIM", "LINSERT", "RPOPLPUSH", "LMOVE", "BLPOP", "BRPOP",
        // sets
        "SADD", "SREM", "SPOP", "SMOVE", "SDIFFSTORE", "SINTERSTORE", "SUNIONSTORE",
        // hashes
        "HSET", "HSETNX", "HMSET", "HDEL", "HINCRBY", "HINCRBYFLOAT",
        // sorted sets
        "ZADD", "ZINCRBY", "ZREM", "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE",
        "ZREMRANGEBYLEX", "ZPOPMIN", "ZPOPMAX", "ZUNIONSTORE", "ZINTERSTORE",
        "ZDIFFSTORE",
        // streams
        "XADD", "XDEL", "XTRIM", "XSETID", "XGROUP", "XACK",
        // geo
        "GEOADD",
    };
    return kWrites.count(upperName) != 0;
}

}  // namespace redis
