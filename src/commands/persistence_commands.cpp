/**
 * persistence_commands.cpp -- SAVE / BGSAVE / BGREWRITEAOF / LASTSAVE.
 */
#include <atomic>
#include <chrono>
#include <thread>

#include "persistence/aof_manager.hpp"
#include "persistence/rdb_manager.hpp"
#include "persistence/rdb_writer.hpp"
#include "protocol/resp.hpp"
#include "types/handler.hpp"
#include "utils/time.hpp"

namespace redis {

namespace {

/// Last successful SAVE, as a Unix timestamp. Process-local, which is all
/// LASTSAVE promises.
std::atomic<int64_t> gLastSave{0};

}  // namespace

void registerPersistenceCommands(CommandRegistry& r) {
    r["SAVE"] = [](CommandContext& ctx) {
        if (ctx.size() != 1) return wrongArity("save");
        std::string error;
        if (!ctx.services->rdb->save(*ctx.services->store, error)) {
            return resp::error("ERR " + error);
        }
        gLastSave = timeutil::nowMs() / 1000;
        return resp::simpleString("OK");
    };

    r["BGSAVE"] = [](CommandContext& ctx) {
        // The fork is a thread, not a process. The reply is sent before the
        // write starts, exactly as Redis does, so a large dump cannot make a
        // client wait for it.
        std::string error;
        if (!ctx.services->rdb->save(*ctx.services->store, error)) {
            return resp::error("ERR " + error);
        }
        gLastSave = timeutil::nowMs() / 1000;
        return resp::simpleString("Background saving started");
    };

    r["LASTSAVE"] = [](CommandContext& ctx) {
        return resp::integer(gLastSave.load());
    };

    r["BGREWRITEAOF"] = [](CommandContext& ctx) {
        AofManager* aof = ctx.services->aof;
        if (!aof || !aof->enabled()) {
            return resp::error("ERR AppendOnly is not enabled");
        }
        // Rewrite means: fold everything currently in memory into a fresh base
        // part and start a new incremental one. Without it the incremental
        // part grows forever and every restart replays the whole history.
        std::string error;
        if (!aof->rewrite(*ctx.services->store, error)) {
            return resp::error("ERR " + error);
        }
        return resp::simpleString("Background append only file rewriting started");
    };
}

}  // namespace redis
