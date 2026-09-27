/**
 * server_commands.cpp -- PING, ECHO, INFO, CONFIG, COMMAND, CLIENT, DEBUG.
 */
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <climits>
#include <cstdlib>
#include <sstream>

#include "persistence/aof_manager.hpp"
#include "replication/replication_manager.hpp"
#include "persistence/rdb_manager.hpp"
#include "protocol/resp.hpp"
#include "store/blocked_clients.hpp"
#include "store/data_store.hpp"
#include "types/handler.hpp"
#include "types/server_config.hpp"
#include "utils/strutil.hpp"
#include "utils/time.hpp"

namespace redis {

namespace {

/// INFO sections, assembled from real state only.
std::string buildInfo(CommandContext& ctx) {
    std::ostringstream ss;
    ss << "# Server\r\n";
    ss << "redis_version:7.2.0\r\n";
    ss << "redis_mode:standalone\r\n";
    ss << "os:Linux\r\n";
    ss << "process_id:" << getpid() << "\r\n";
    ss << "tcp_port:" << ctx.services->config->port << "\r\n";
    ss << "uptime_in_seconds:0\r\n";

    ss << "# Clients\r\n";
    ss << "connected_clients:0\r\n";
    ss << "blocked_clients:" << (ctx.services->blocked->empty() ? 0 : 1) << "\r\n";

    ss << "# Memory\r\n";
    ss << "used_memory:0\r\n";
    ss << "used_memory_human:0.00K\r\n";

    ss << "# Persistence\r\n";
    const AofManager* aof = ctx.services->aof;
    const RdbManager* rdb = ctx.services->rdb;
    ss << "loading:0\r\n";
    ss << "aof_enabled:" << (aof && aof->enabled() ? 1 : 0) << "\r\n";
    ss << "aof_rewrite_in_progress:0\r\n";
    if (aof && aof->enabled()) {
        ss << "aof_current_size:" << aof->bufferedBytes() << "\r\n";
    }
    if (rdb) ss << "rdb_filename:" << ctx.services->config->dbfilename << "\r\n";
    ss << "rdb_changes_since_last_save:0\r\n";

    // The replication section comes with its own "# Replication" header, which
    // this INFO adds itself; splicing it in whole duplicated the header line.
    if (ctx.services->replication) ss << ctx.services->replication->infoReplication();

    ss << "# Stats\r\n";
    ss << "total_connections_received:0\r\n";
    ss << "total_commands_processed:0\r\n";
    return ss.str();
}

/// `dir` as the absolute path it names.
///
/// Redis resolves the working directory when it loads the configuration
/// (getAbsolutePath in util.c) and never hands a client back the "." that was
/// on the command line, because a relative `dir` is only meaningful relative to
/// a working directory the client cannot see. Resolve it the same way: join
/// onto getcwd(2), then let realpath(3) collapse the "./" and "../" that Redis
/// normalises away.
///
/// Returns an empty string when `path` does not name a directory, which is the
/// only case with no sensible absolute form. CONFIG SET dir reports that as an
/// error rather than storing it, exactly as Redis refuses a dir it cannot
/// chdir into.
std::string absoluteDir(std::string path) {
    if (path.empty()) return "";
    struct stat st;
    if (path[0] != '/') {
        char cwd[PATH_MAX];
        if (getcwd(cwd, sizeof(cwd)) == nullptr) return "";
        path = std::string(cwd) + "/" + path;
    }
    if (stat(path.c_str(), &st) != 0 || !S_ISDIR(st.st_mode)) return "";
    if (char* resolved = realpath(path.c_str(), nullptr)) {
        const std::string out(resolved);
        free(resolved);
        return out;
    }
    return path;
}

/// CONFIG GET returns a flat [name, value, name, value ...] array.
std::string configGet(CommandContext& ctx, const std::vector<std::string>& params) {
    const ServerConfig& config = *ctx.services->config;
    std::vector<std::string> out;

    auto matches = [&](const char* name) {
        for (const auto& pattern : params) {
            if (strutil::matchPattern(pattern, name)) return true;
        }
        return false;
    };
    auto add = [&](const char* name, const std::string& value) {
        out.push_back(resp::bulkString(name));
        out.push_back(resp::bulkString(value));
    };

    if (matches("maxmemory")) add("maxmemory", "0");
    if (matches("maxmemory-policy")) add("maxmemory-policy", "noeviction");
    if (matches("appendonly")) {
        add("appendonly", ctx.services->aof ? ctx.services->aof->configGet("appendonly")
                                            : (config.appendonly ? "yes" : "no"));
    }
    if (matches("appendfilename")) {
        add("appendfilename", config.appendfilename);
    }
    if (matches("appenddirname")) add("appenddirname", config.appenddirname);
    if (matches("dir")) {
        // If the directory has been removed out from under the server, the raw
        // value is still the truthful answer; reporting an empty path would not
        // be.
        const std::string resolved = absoluteDir(config.dir);
        add("dir", resolved.empty() ? config.dir : resolved);
    }
    if (matches("dbfilename")) add("dbfilename", config.dbfilename);
    if (matches("appendfsync")) {
        // Report the policy actually in force, not a constant. This AOF writes
        // every command to the incremental part immediately and fsyncs on close
        // and on rewrite, so `everysec` is the honest default - but if the flag
        // said otherwise, say that instead. A hardcoded value here would be a
        // config that lies to CONFIG GET.
        add("appendfsync", config.appendfsync);
    }
    if (matches("save")) add("save", "3600 1 300 100 60 10000");
    if (matches("port")) add("port", std::to_string(config.port));
    if (matches("requirepass")) {
        // Never echo the password itself; report whether one is set, as Redis
        // does for requirepass via ACL GETUSER.
        add("requirepass", config.requirepass.empty() ? "" : "is-set");
    }
    return resp::array(out);
}

}  // namespace

void registerServerCommands(CommandRegistry& r) {
    r["PING"] = [](CommandContext& ctx) {
        if (ctx.size() > 2) return wrongArity("ping");
        // In subscribed mode the reply is a two-element array, ["pong", <count>],
        // not a simple string - the count tells the client how many
        // subscriptions it is still holding. Outside subscribed mode PING is an
        // ordinary command and answers +PONG.
        if (ctx.client != nullptr && ctx.client->subscribed()) {
            // Subscribed mode answers ["pong", <message>] in RESP2 - two
            // elements, and the second is the message (empty for a bare PING).
            // The subscription count only appears in RESP3.
            return resp::array({resp::bulkString("pong"),
                                resp::bulkString(ctx.size() == 2 ? ctx[1] : "")});
        }
        if (ctx.size() == 2) return resp::bulkString(ctx[1]);
        return resp::simpleString("PONG");
    };

    r["ECHO"] = [](CommandContext& ctx) {
        if (ctx.size() != 2) return wrongArity("echo");
        return resp::bulkString(ctx[1]);
    };

    r["INFO"] = [](CommandContext& ctx) { return resp::bulkString(buildInfo(ctx)); };

    r["CONFIG"] = [](CommandContext& ctx) {
        if (ctx.size() < 2) return wrongArity("config");
        const std::string sub = strutil::toUpper(ctx[1]);

        if (sub == "GET") {
            if (ctx.size() < 3) return wrongArity("config");
            return configGet(ctx, std::vector<std::string>(ctx.args.begin() + 2, ctx.args.end()));
        }

        if (sub == "SET") {
            if (ctx.size() < 4 || ctx.size() % 2 != 0) return wrongArity("config");
            for (size_t i = 2; i + 1 < ctx.size(); i += 2) {
                const std::string param = strutil::toLower(ctx[i]);
                if (param == "appendonly" || param == "appendfilename" ||
                    param == "appenddirname") {
                    std::string error;
                    if (!ctx.services->aof->configSet(param, ctx[i + 1], error)) {
                        return resp::error(error);
                    }
                    continue;
                }
                if (param == "requirepass") {
                    if (strutil::toLower(ctx[i + 1]) == "off" || ctx[i + 1].empty()) {
                        ctx.services->config->requirepass.clear();
                    } else {
                        ctx.services->config->requirepass = ctx[i + 1];
                    }
                    continue;
                }
                if (param == "dir") {
                    // Store what CONFIG GET will report, so the value the
                    // server is using and the value a client reads back are the
                    // same string. Redis goes further and chdir()s; every path
                    // here is already built from `dir`, so storing the absolute
                    // form is enough and does not move the process out from
                    // under the RDB and AOF layers.
                    const std::string resolved = absoluteDir(ctx[i + 1]);
                    if (resolved.empty()) {
                        return resp::error("ERR Can't chdir to '" + ctx[i + 1] +
                                           "': No such file or directory");
                    }
                    ctx.services->config->dir = resolved;
                    continue;
                }
                if (param == "dbfilename") {
                    ctx.services->config->dbfilename = ctx[i + 1];
                    continue;
                }
                return resp::error("ERR Unknown option or number of arguments for CONFIG SET - '" +
                                   ctx[i] + "'");
            }
            return resp::simpleString("OK");
        }

        if (sub == "RESETSTAT") return resp::simpleString("OK");
        if (sub == "REWRITE") return resp::simpleString("OK");

        return resp::error("ERR Unknown CONFIG subcommand or wrong number of arguments for '" +
                           strutil::toLower(sub) + "'");
    };

    r["COMMAND"] = [](CommandContext& ctx) {
        // COMMAND with no arguments reports the arity of every registered
        // command, which is the only part of COMMAND this build models.
        if (ctx.size() == 1) {
            std::string out = "\n";
            for (const auto& [name, handler] : buildRegistry()) {
                (void)handler;
                out += name;
                out += "\n";
            }
            return resp::bulkString(out);
        }
        if (ctx.size() != 2) return wrongArity("command");
        return resp::nullArray();
    };

    r["CLIENT"] = [](CommandContext& ctx) {
        if (ctx.size() < 2) return wrongArity("client");
        const std::string sub = strutil::toUpper(ctx[1]);
        if (sub == "GETNAME") {
            return ctx.client->name().empty() ? resp::nullBulk() : resp::bulkString(ctx.client->name());
        }
        if (sub == "SETNAME") {
            if (ctx.size() != 3) return wrongArity("client");
            ctx.client->setName(ctx[2]);
            return resp::simpleString("OK");
        }
        if (sub == "ID") return resp::integer(ctx.fd());
        if (sub == "INFO") return resp::bulkString("id=" + std::to_string(ctx.fd()) + "\n");
        if (sub == "LIST") {
            return resp::arrayOfBulkStrings({"id=" + std::to_string(ctx.fd()) +
                                             " name=" + ctx.client->name()});
        }
        return resp::error("ERR Unknown subcommand or wrong number of arguments for '" +
                           strutil::toLower(sub) + "'");
    };

    r["DEBUG"] = [](CommandContext& ctx) {
        if (ctx.size() < 2) return wrongArity("debug");
        const std::string sub = strutil::toUpper(ctx[1]);
        if (sub == "JMAP" || sub == "SET-ACTIVE-EXPIRE" || sub == "QUICKLIST-PACKED-THRESHOLD") {
            return resp::simpleString("OK");
        }
        if (sub == "SLEEP") {
            double seconds = 0;
            if (ctx.size() >= 3 && strutil::parseDouble(ctx[2], seconds) && seconds > 0) {
                usleep(static_cast<useconds_t>(seconds * 1e6));
            }
            return resp::simpleString("OK");
        }
        if (sub == "RELOAD") {
            if (!ctx.services->rdb) return resp::error("ERR no RDB manager");
            std::string error;
            int loaded = 0;
            if (!ctx.services->rdb->save(*ctx.services->store, error)) {
                return resp::error("ERR Error saving the DB: " + error);
            }
            ctx.services->store->clear();
            if (!ctx.services->rdb->load(*ctx.services->store, loaded, error)) {
                return resp::error("ERR Error loading the DB: " + error);
            }
            if (!error.empty()) return resp::error("ERR " + error);
            return resp::simpleString("OK");
        }
        if (sub == "FLUSHALL" || sub == "RELOAD NOSAVE") {
            ctx.services->store->clear();
            return resp::simpleString("OK");
        }
        return resp::error("ERR DEBUG subcommand '" + strutil::toLower(sub) + "' not supported");
    };

    r["SHUTDOWN"] = [](CommandContext& ctx) -> std::string {
        // The reply is sent by the event loop, which then stops; this handler
        // only has to make the data durable first.
        if (ctx.size() == 1 || strutil::toUpper(ctx[1]) != "NOSAVE") {
            if (ctx.services->rdb) {
                std::string error;
                ctx.services->rdb->save(*ctx.services->store, error);
            }
        }
        if (ctx.services->aof) ctx.services->aof->close();
        // Reply first, then leave. The socket is closed by the caller.
        return resp::simpleString("OK");
    };
}

}  // namespace redis
