/**
 * transaction_commands.cpp -- MULTI / EXEC / DISCARD / WATCH.
 *
 * WATCH is optimistic locking. The net layer asks `watchedKeysChanged()` before
 * EXEC runs the queue, so the abort decision is made with the same lock the
 * writes took, rather than by polling a flag a writer might not have set yet.
 */
#include <algorithm>

#include "protocol/resp.hpp"
#include "store/data_store.hpp"
#include "types/handler.hpp"
#include "utils/strutil.hpp"
#include "utils/time.hpp"

namespace redis {

namespace {

/// Fingerprint of a key: the value plus the presence of a TTL. Any change to
/// either is a change to the key as far as a watcher is concerned.
struct WatchStamp {
    bool present = false;
    uint64_t fingerprint = 0;
};

uint64_t hashCombine(uint64_t seed, const std::string& s) {
    uint64_t h = seed ^ 0xcbf29ce484222325ULL;
    for (unsigned char c : s) {
        h ^= c;
        h *= 0x100000001b3ULL;
    }
    return h;
}

WatchStamp stampOf(const DataStore::Map& data, const std::string& key, int64_t now) {
    auto it = data.find(key);
    if (it == data.end() || it->second.isExpired(now)) return WatchStamp{};
    WatchStamp stamp;
    stamp.present = true;
    uint64_t h = hashCombine(0, key);
    h = hashCombine(h, std::to_string(it->second.expiryMs));
    h = hashCombine(h, std::to_string(static_cast<int>(it->second.type)));
    switch (it->second.type) {
        case DataType::STRING:
            h = hashCombine(h, it->second.stringValue);
            break;
        case DataType::LIST:
            for (const auto& e : it->second.listValue) h = hashCombine(h, e);
            break;
        case DataType::SET:
            for (const auto& e : it->second.setValue) h = hashCombine(h, e);
            break;
        case DataType::HASH:
            for (const auto& [f, v] : it->second.hashValue) {
                h = hashCombine(h, f);
                h = hashCombine(h, v);
            }
            break;
        case DataType::ZSET:
            for (const auto& [score, member] : it->second.zsetByScore) {
                h = hashCombine(h, member);
                h = hashCombine(h, strutil::formatScore(score));
            }
            break;
        case DataType::STREAM:
            for (const auto& e : it->second.streamValue) h = hashCombine(h, e.id);
            break;
        default:
            break;
    }
    stamp.fingerprint = h;
    return stamp;
}

}  // namespace

void registerTransactionCommands(CommandRegistry& r) {
    r["MULTI"] = [](CommandContext& ctx) {
        if (ctx.client->inMulti()) return resp::error("ERR MULTI calls can not be nested");
        ctx.client->beginMulti();
        ctx.client->clearQueue();
        return resp::simpleString("OK");
    };

    r["DISCARD"] = [](CommandContext& ctx) {
        if (!ctx.client->inMulti()) return resp::error("ERR DISCARD without MULTI");
        ctx.client->endMulti();
        ctx.client->clearQueue();
        ctx.client->clearWatched();
        return resp::simpleString("OK");
    };

    r["WATCH"] = [](CommandContext& ctx) {
        if (ctx.size() < 2) return wrongArity("watch");
        // Inside MULTI there is no single client whose view could go stale
        // between here and EXEC -- the whole queue is the unit, and EXEC
        // already checks the watches. Refuse before touching any state, so the
        // refused WATCH leaves no half-registered snapshot behind and the open
        // MULTI stays usable.
        if (ctx.client->inMulti()) {
            return resp::error("ERR WATCH inside MULTI is not allowed");
        }
        // Fingerprint every key once, under one read lock, so the snapshot is
        // internally consistent even if another client writes between two keys.
        ctx.services->store->read([&](const DataStore::Map& data) {
            const int64_t now = timeutil::nowMs();
            for (size_t i = 1; i < ctx.size(); i++) {
                ctx.client->watch(ctx[i], stampOf(data, ctx[i], now).fingerprint);
            }
        });
        return resp::simpleString("OK");
    };

    r["UNWATCH"] = [](CommandContext& ctx) {
        ctx.client->clearWatched();
        return resp::simpleString("OK");
    };

    r["RESET"] = [](CommandContext& ctx) {
        ctx.client->endMulti();
        ctx.client->clearQueue();
        ctx.client->clearWatched();
        return resp::simpleString("RESET");
    };

    // EXEC runs the queue through the net layer's own execute() so the AOF
    // append and the replica propagation happen exactly as they would for the
    // same command sent on its own.
    r["EXEC"] = [](CommandContext& ctx) {
        if (!ctx.client->inMulti()) return resp::error("ERR EXEC without MULTI");

        // Optimistic locking: a watched key whose fingerprint moved since
        // WATCH means this client's view is stale, so nothing runs at all.
        if (ctx.client->watchesAnything()) {
            const bool dirty = ctx.services->store->read([&](const DataStore::Map& data) {
                const int64_t now = timeutil::nowMs();
                for (const auto& [key, was] : ctx.client->watched()) {
                    if (stampOf(data, key, now).fingerprint != was) return true;
                }
                return false;
            });
            if (dirty) {
                ctx.client->endMulti();
                ctx.client->clearQueue();
                ctx.client->clearWatched();
                return resp::error("EXECABORT Transaction discarded because of previous errors.");
            }
        }

        auto queue = std::move(ctx.client->queue());
        ctx.client->clearQueue();
        ctx.client->endMulti();
        ctx.client->clearWatched();

        if (queue.empty()) return resp::emptyArray();

        std::vector<std::string> replies;
        replies.reserve(queue.size());
        for (const auto& args : queue) {
            std::string reply =
                ctx.services->execute ? ctx.services->execute(args, ctx.client) : resp::error(
                    "ERR EXEC could not run this command");
            // A handler that deferred its answer (BLPOP) has nothing to put
            // in this slot. An empty element would truncate the RESP array, so
            // say "nothing" instead.
            if (reply.empty()) reply = resp::nullArray();
            replies.push_back(std::move(reply));
        }
        return resp::array(replies);
    };
}

}  // namespace redis
