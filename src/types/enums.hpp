/**
 * enums.hpp -- core enumerations shared by every layer.
 *
 * Pure declarations. No logic, no dependencies. Everything else in the project
 * includes this file to talk about "what kind of thing is this" without
 * including a module to find out.
 */
#ifndef REDIS_TYPES_ENUMS_HPP
#define REDIS_TYPES_ENUMS_HPP

#include <string>

namespace redis {

/// The type tag stored alongside every value in the data store.
enum class DataType { NONE, STRING, LIST, SET, HASH, ZSET, STREAM };

/// Which side of the replication link this process is on.
enum class ReplicationRole { MASTER, REPLICA };

/// Per-connection mode. Drives what the dispatcher is allowed to do next.
enum class ClientState {
    NORMAL,      // ordinary request/response
    MULTI,       // inside a MULTI block, commands are queued
    SUBSCRIBED,  // at least one channel, only a restricted command set is legal
};

/// Human-readable name of a data type, as returned by the TYPE command.
inline const char* dataTypeName(DataType t) {
    switch (t) {
        case DataType::STRING: return "string";
        case DataType::LIST:   return "list";
        case DataType::SET:    return "set";
        case DataType::HASH:   return "hash";
        case DataType::ZSET:   return "zset";
        case DataType::STREAM: return "stream";
        default:               return "none";
    }
}

}  // namespace redis

#endif  // REDIS_TYPES_ENUMS_HPP
