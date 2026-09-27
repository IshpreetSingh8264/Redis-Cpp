/**
 * key_commands.cpp -- operations on keys regardless of their type.
 */
#include <algorithm>
#include <random>

#include "protocol/resp.hpp"
#include "store/data_store.hpp"
#include "types/handler.hpp"
#include "utils/strutil.hpp"
#include "utils/time.hpp"

namespace redis {

namespace {

/// EXPIRE / PEXPIRE / EXPIREAT / PEXPIREAT all reduce to "set an absolute
/// deadline on this key".
std::string expireGeneric(CommandContext& ctx, bool millis, bool absolute) {
    if (ctx.size() < 3) return wrongArity("expire");
    int64_t when = 0;
    if (!strutil::parseInt64(ctx[2], when)) {
        return resp::error("ERR value is not an integer or out of range");
    }
    int64_t deadline = absolute ? (millis ? when : when * 1000)
                                : timeutil::nowMs() + (millis ? when : when * 1000);

    return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
        purgeIfExpired(data, ctx[1], timeutil::nowMs());
        auto it = data.find(ctx[1]);
        if (it == data.end()) return resp::integer(0);
        if (deadline <= timeutil::nowMs()) {
            data.erase(it);
            return resp::integer(1);
        }
        it->second.expiryMs = deadline;
        return resp::integer(1);
    });
}

std::string ttlGeneric(CommandContext& ctx, bool millis) {
    if (ctx.size() != 2) return wrongArity(millis ? "pttl" : "ttl");
    return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
        const int64_t now = timeutil::nowMs();
        auto it = data.find(ctx[1]);
        if (it == data.end() || it->second.isExpired(now)) return resp::integer(-2);
        const int64_t remaining = it->second.ttlMs(now);
        if (remaining < 0) return resp::integer(-1);  // no TTL set, not "0 seconds left"
        return resp::integer(millis ? remaining : (remaining + 999) / 1000);
    });
}

}  // namespace

void registerKeyCommands(CommandRegistry& r) {
    r["DEL"] = [](CommandContext& ctx) {
        if (ctx.size() < 2) return wrongArity("del");
        return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
            int64_t removed = 0;
            for (size_t i = 1; i < ctx.size(); i++) removed += data.erase(ctx[i]) ? 1 : 0;
            return resp::integer(removed);
        });
    };
    r["UNLINK"] = r["DEL"];  // same observable behaviour at this scale

    r["EXISTS"] = [](CommandContext& ctx) {
        if (ctx.size() < 2) return wrongArity("exists");
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            const int64_t now = timeutil::nowMs();
            int64_t count = 0;
            for (size_t i = 1; i < ctx.size(); i++) {
                auto it = data.find(ctx[i]);
                if (it != data.end() && !it->second.isExpired(now)) count++;
            }
            return resp::integer(count);
        });
    };

    r["TYPE"] = [](CommandContext& ctx) {
        if (ctx.size() != 2) return wrongArity("type");
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            auto it = data.find(ctx[1]);
            if (it == data.end() || it->second.isExpired(timeutil::nowMs())) {
                return resp::simpleString("none");
            }
            return resp::simpleString(dataTypeName(it->second.type));
        });
    };

    r["KEYS"] = [](CommandContext& ctx) {
        if (ctx.size() != 2) return wrongArity("keys");
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            const int64_t now = timeutil::nowMs();
            std::vector<std::string> out;
            for (const auto& [key, value] : data) {
                if (!value.isExpired(now) && strutil::matchPattern(ctx[1], key)) {
                    out.push_back(resp::bulkString(key));
                }
            }
            return resp::array(out);
        });
    };

    r["SCAN"] = [](CommandContext& ctx) {
        // Counted only so that a cursor round-trips; the full keyspace is
        // scanned, which is honest and correct for a keyspace this size.
        if (ctx.size() < 2) return wrongArity("scan");
        int64_t cursor = 0;
        if (!strutil::parseInt64(ctx[1], cursor)) {
            return resp::error("ERR invalid cursor");
        }
        std::string pattern;
        int64_t count = 10;
        for (size_t i = 2; i < ctx.size(); i++) {
            const std::string opt = strutil::toUpper(ctx[i]);
            if (opt == "MATCH" && i + 1 < ctx.size()) pattern = ctx[++i];
            else if (opt == "COUNT" && i + 1 < ctx.size()) strutil::parseInt64(ctx[++i], count);
        }
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            const int64_t now = timeutil::nowMs();
            std::vector<std::string> keys;
            for (const auto& [key, value] : data) {
                if (value.isExpired(now)) continue;
                if (!pattern.empty() && !strutil::matchPattern(pattern, key)) continue;
                keys.push_back(key);
            }
            std::sort(keys.begin(), keys.end());
            const size_t start = std::min<size_t>(static_cast<size_t>(cursor), keys.size());
            const size_t take =
                std::min<size_t>(static_cast<size_t>(std::max<int64_t>(count, 1)),
                                 keys.size() - start);
            std::vector<std::string> out;
            for (size_t i = start; i < start + take; i++) out.push_back(resp::bulkString(keys[i]));
            const size_t next = start + take;
            return resp::array({resp::bulkString(std::to_string(next)), resp::array(out)});
        });
    };

    r["EXPIRE"] = [](CommandContext& ctx) { return expireGeneric(ctx, false, false); };
    r["PEXPIRE"] = [](CommandContext& ctx) { return expireGeneric(ctx, true, false); };
    r["EXPIREAT"] = [](CommandContext& ctx) { return expireGeneric(ctx, false, true); };
    r["PEXPIREAT"] = [](CommandContext& ctx) { return expireGeneric(ctx, true, true); };
    r["TTL"] = [](CommandContext& ctx) { return ttlGeneric(ctx, false); };
    r["PTTL"] = [](CommandContext& ctx) { return ttlGeneric(ctx, true); };

    r["PERSIST"] = [](CommandContext& ctx) {
        if (ctx.size() != 2) return wrongArity("persist");
        return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
            purgeIfExpired(data, ctx[1], timeutil::nowMs());
            auto it = data.find(ctx[1]);
            if (it == data.end() || it->second.expiryMs < 0) return resp::integer(0);
            it->second.expiryMs = -1;
            return resp::integer(1);
        });
    };

    r["RENAME"] = [](CommandContext& ctx) {
        if (ctx.size() != 3) return wrongArity("rename");
        return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
            const int64_t now = timeutil::nowMs();
            purgeIfExpired(data, ctx[1], now);
            auto it = data.find(ctx[1]);
            if (it == data.end()) {
                return resp::error("ERR no such key");
            }
            RedisValue moved = std::move(it->second);
            data.erase(it);
            data[ctx[2]] = std::move(moved);
            return resp::simpleString("OK");
        });
    };

    r["RENAMENX"] = [](CommandContext& ctx) {
        if (ctx.size() != 3) return wrongArity("renamenx");
        return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
            const int64_t now = timeutil::nowMs();
            purgeIfExpired(data, ctx[1], now);
            purgeIfExpired(data, ctx[2], now);
            auto it = data.find(ctx[1]);
            if (it == data.end()) return resp::error("ERR no such key");
            if (data.count(ctx[2])) return resp::integer(0);
            RedisValue moved = std::move(it->second);
            data.erase(it);
            data[ctx[2]] = std::move(moved);
            return resp::integer(1);
        });
    };

    r["RANDOMKEY"] = [](CommandContext& ctx) {
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            const int64_t now = timeutil::nowMs();
            std::vector<std::string> live;
            for (const auto& [key, value] : data) {
                if (!value.isExpired(now)) live.push_back(key);
            }
            if (live.empty()) return resp::nullBulk();
            std::mt19937 rng(std::random_device{}());
            std::uniform_int_distribution<size_t> pick(0, live.size() - 1);
            return resp::bulkString(live[pick(rng)]);
        });
    };

    r["DBSIZE"] = [](CommandContext& ctx) {
        return resp::integer(static_cast<int64_t>(ctx.services->store->size()));
    };

    r["FLUSHDB"] = [](CommandContext& ctx) {
        ctx.services->store->clear();
        return resp::simpleString("OK");
    };
    r["FLUSHALL"] = r["FLUSHDB"];
}

}  // namespace redis
