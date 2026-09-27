/**
 * handler.hpp -- the one function signature every Redis command has.
 *
 * This is the whole dispatch contract. Adding a command means adding one
 * `std::string someCommand(CommandContext&)` function and one line in
 * commands/registry.cpp -- no branch anywhere, no edit to any other file.
 */
#ifndef REDIS_TYPES_HANDLER_HPP
#define REDIS_TYPES_HANDLER_HPP

#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

#include "types/client_session.hpp"
#include "types/services.hpp"

namespace redis {

/// Everything a handler is allowed to see. Built once per command by the
/// dispatcher in net/server.cpp and passed by reference, so handlers stay
/// plain functions and the registry stays a flat map.
struct CommandContext {
    Services* services = nullptr;
    ClientSession* client = nullptr;
    std::vector<std::string> args;

    size_t size() const { return args.size(); }
    const std::string& operator[](size_t i) const { return args[i]; }

    /// Upper-cased command name, for error messages.
    std::string name() const { return args.empty() ? std::string() : args[0]; }

    /// The fd of the connection this command arrived on. Needed by the handful
    /// of commands that answer later (BLPOP, XREAD BLOCK) or take over the
    /// socket (PSYNC).
    int fd() const { return client ? client->fd() : -1; }
};

/// Rule 6: one signature, used by every command in the server.
using Handler = std::function<std::string(CommandContext&)>;

/// Rule 5: dispatch is a table, not control flow.
using CommandRegistry = std::unordered_map<std::string, Handler>;

// One registration function per command group. Each is defined in its own
// file and called exactly once by buildRegistry().
void registerStringCommands(CommandRegistry& r);
void registerKeyCommands(CommandRegistry& r);
void registerListCommands(CommandRegistry& r);
void registerSetCommands(CommandRegistry& r);
void registerHashCommands(CommandRegistry& r);
void registerZsetCommands(CommandRegistry& r);
void registerStreamCommands(CommandRegistry& r);
void registerTransactionCommands(CommandRegistry& r);
void registerPubsubCommands(CommandRegistry& r);
void registerGeoCommands(CommandRegistry& r);
void registerAuthCommands(CommandRegistry& r);
void registerServerCommands(CommandRegistry& r);
void registerReplicationCommands(CommandRegistry& r);
void registerPersistenceCommands(CommandRegistry& r);

/// Build the full registry. Called once at startup.
CommandRegistry buildRegistry();

/// Commands that mutate the keyspace. The single source of truth for "should
/// this go to the AOF and to the replicas", consulted by net/ and by the AOF
/// layer rather than duplicated as a second if-chain.
bool isWriteCommand(const std::string& upperName);

/// Convenience: the argument-count error Redis uses everywhere.
std::string wrongArity(const std::string& command);

/// Convenience: the WRONGTYPE error, identical to real Redis's.
extern const char* const kWrongTypeError;

}  // namespace redis

#endif  // REDIS_TYPES_HANDLER_HPP
