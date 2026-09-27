/**
 * replication_commands.cpp -- REPLCONF / PSYNC / SYNC / WAIT / FAILOVER.
 *
 * These were the worst of the fakes in the old monolith:
 *   * REPLCONF answered +OK to everything, including `ACK <offset>`, which is
 *     the one thing a master actually needs from it;
 *   * PSYNC replied +FULLRESYNC with a hardcoded replid, a hardcoded offset of
 *     0, and a 20-byte hardcoded "RDB" with eight zero bytes standing in for a
 *     checksum;
 *   * WAIT slept for the timeout and then returned the number of connected
 *     sockets, which is a number that would have been the right answer if the
 *     replicas had been real.
 *
 * All three now do the work.
 */
#include <unistd.h>

#include "persistence/rdb_writer.hpp"
#include "protocol/resp.hpp"
#include "replication/replication_manager.hpp"
#include "store/data_store.hpp"
#include "types/handler.hpp"
#include "utils/io.hpp"
#include "utils/strutil.hpp"

namespace redis {

void registerReplicationCommands(CommandRegistry& r) {
    r["REPLCONF"] = [](CommandContext& ctx) {
        if (ctx.size() < 2) return wrongArity("replconf");
        ReplicationManager* replication = ctx.services->replication;
        if (!replication) return resp::error("ERR this instance is not replicating");

        const std::string option = strutil::toUpper(ctx[1]);

        if (option == "ACK" && ctx.size() >= 3) {
            int64_t acked = 0;
            if (!strutil::parseInt64(ctx[2], acked)) {
                return resp::error("ERR value is not an integer or out of range");
            }
            // The only REPLCONF that carries information the master acts on.
            if (replication->isKnownReplica(ctx.fd())) replication->noteAck(ctx.fd(), acked);
            return resp::simpleString("OK");
        }

        if (option == "GETACK") {
            if (ctx.size() < 3) return wrongArity("replconf");
            io::sendAll(ctx.fd(), replication->ackFrame());
            return resp::simpleString("OK");
        }

        if (option == "LISTENING-PORT" && ctx.size() >= 3) {
            return resp::simpleString("OK");
        }

        if (option == "CAPA" && ctx.size() >= 3) {
            return resp::simpleString("OK");
        }

        if (option == "CAPA") return resp::simpleString("OK");

        if (option == "VERSION" && ctx.size() >= 3) return resp::simpleString("OK");

        return resp::error("ERR Unrecognized REPLCONF option: " + ctx[1]);
    };

    r["PSYNC"] = [](CommandContext& ctx) -> std::string {
        if (ctx.size() != 3) return wrongArity("psync");
        ReplicationManager* replication = ctx.services->replication;
        if (!replication) return resp::error("ERR this instance is not replicating");

        if (replication->isReplica()) {
            return resp::error("ERR can't PSYNC from a replica");
        }

        const std::string requestedId = ctx[1];
        const std::string requestedOffset = ctx[2];
        if (requestedId != "?" && requestedId != replication->replid()) {
            return resp::error("ERR Partial resynchronization not supported");
        }
        if (requestedId == replication->replid() && requestedOffset != "-1") {
            // No backlog is kept, so a partial resync can never be honoured.
            // Say so rather than pretending to continue and then sending
            // nothing.
            return resp::error("ERR Partial resynchronization not supported (no backlog kept)");
        }

        // Everything below writes to the socket directly: the reply is a
        // simple string, then a bulk RDB, then the connection becomes a
        // replication stream with no further framing on our side.
        // resp::simpleString adds the '+' itself; including one in the text
        // produced "++FULLRESYNC", which no replica could parse, so the sync
        // never completed and the link sat at "down" forever.
        io::sendAll(ctx.fd(), resp::simpleString("FULLRESYNC " + replication->replid() + " " +
                                                 std::to_string(replication->offset())));

        rdb::RdbWriter writer;
        for (const auto& [key, value] : ctx.services->store->snapshot()) {
            writer.writeEntry(key, value);
        }
        const std::string dump = writer.finish();

        // The dump goes out as a bulk string *header* plus exactly the payload,
        // with no trailing CRLF. resp::bulkString() appends one, which put two
        // phantom bytes at the end of the stream: the replica parsed them as an
        // empty command and its offset ended up permanently two ahead of the
        // master's.
        io::sendAll(ctx.fd(), "$" + std::to_string(dump.size()) + "\r\n" + dump);

        replication->addReplica(ctx.fd(), replication->offset());
        // From here on this socket carries replication traffic only. The
        // dispatcher stops answering it, so the replica never has to
        // distinguish a command reply from a propagated command.
        ctx.client->markReplicaLink();
        return "";  // already answered
    };

    r["SYNC"] = [](CommandContext& ctx) -> std::string {
        // SYNC is PSYNC's predecessor. Redis still answers it with a full
        // resync, so the reply is deliberately identical.
        std::vector<std::string> args{"PSYNC", "?", "-1"};
        CommandContext forwarded = ctx;
        forwarded.args = std::move(args);
        const CommandRegistry& registry = buildRegistry();
        const auto handler = registry.find("PSYNC");
        if (handler == registry.end()) return std::string(resp::error("ERR unsupported"));
        return handler->second(forwarded);
    };

    r["REPLICAOF"] = [](CommandContext& ctx) -> std::string {
        if (ctx.size() != 3) return wrongArity("replicaof");
        ReplicationManager* replication = ctx.services->replication;
        if (!replication) return resp::error("ERR this instance is not replicating");
        const std::string target = strutil::toUpper(ctx[1]);

        if (target == "NO" && strutil::toUpper(ctx[2]) == "ONE") {
            replication->promoteToMaster();
            return resp::simpleString("OK");
        }

        int64_t port = 0;
        if (!strutil::parseInt64(ctx[2], port) || port <= 0 || port > 65535) {
            return resp::error("ERR Invalid master port");
        }
        if (!replication->startReplication(ctx[1], static_cast<int>(port))) {
            return resp::error("ERR Unable to connect to MASTER: Connection refused");
        }
        return resp::simpleString("OK");
    };
    r["SLAVEOF"] = r["REPLICAOF"];

    r["WAIT"] = [](CommandContext& ctx) {
        if (ctx.size() != 3) return wrongArity("wait");
        int64_t numReplicas = 0, timeoutMs = 0;
        if (!strutil::parseInt64(ctx[1], numReplicas) || !strutil::parseInt64(ctx[2], timeoutMs)) {
            return resp::error("ERR value is not an integer or out of range");
        }
        if (numReplicas < 0 || timeoutMs < 0) {
            return resp::error("ERR value is out of range, must be positive");
        }
        if (!ctx.services->replication || ctx.services->replication->isReplica()) {
            return resp::error("ERR WAIT cannot be used with replica instances");
        }
        return resp::integer(ctx.services->replication->waitForReplicas(
            static_cast<int>(numReplicas), timeoutMs));
    };

    r["FAILOVER"] = [](CommandContext& ctx) {
        // Promotion to master is not implemented. Saying so is the honest
        // answer; replying +OK would be a lie the next command would expose.
        return resp::error("ERR FAILOVER is not supported by this build");
    };
}

}  // namespace redis
