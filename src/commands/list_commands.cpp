/**
 * list_commands.cpp -- the list type, including the two that block.
 */
#include <algorithm>

#include "protocol/resp.hpp"
#include "store/blocked_clients.hpp"
#include "store/data_store.hpp"
#include "types/handler.hpp"
#include "utils/strutil.hpp"
#include "utils/time.hpp"

namespace redis {

namespace {

/// LPUSH / RPUSH, which also has to wake anyone parked on this key.
std::string pushGeneric(CommandContext& ctx, bool left, bool requireExisting) {
    if (ctx.size() < 3) return wrongArity(left ? "lpush" : "rpush");
    const std::string key = ctx[1];

    std::string result = ctx.services->store->write([&](DataStore::Map& data) -> std::string {
        purgeIfExpired(data, key, timeutil::nowMs());
        auto it = data.find(key);
        if (it != data.end() && it->second.type != DataType::LIST) return resp::error(kWrongTypeError);
        if (requireExisting && it == data.end()) return resp::integer(0);

        RedisValue& slot = data[key];
        slot.type = DataType::LIST;
        for (size_t i = 2; i < ctx.size(); i++) {
            if (left) slot.listValue.push_front(ctx[i]);
            else slot.listValue.push_back(ctx[i]);
        }
        return resp::integer(static_cast<int64_t>(slot.listValue.size()));
    });
    if (!result.empty() && result[0] == '-') return result;

    ctx.services->blocked->notifyList(key, *ctx.services->store);
    return result;
}

std::string popGeneric(CommandContext& ctx, bool left) {
    if (ctx.size() < 2) return wrongArity(left ? "lpop" : "rpop");
    const std::string key = ctx[1];

    int64_t count = 1;
    bool haveCount = false;
    if (ctx.size() >= 3) {
        if (!strutil::parseInt64(ctx[2], count) || count < 0) {
            return resp::error("ERR value is out of range, must be positive");
        }
        haveCount = true;
    }

    return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
        purgeIfExpired(data, key, timeutil::nowMs());
        auto it = data.find(key);
        if (it == data.end()) return resp::nullBulk();
        if (it->second.type != DataType::LIST) return resp::error(kWrongTypeError);
        if (it->second.listValue.empty()) {
            data.erase(it);
            return resp::nullArray();
        }

        auto takeOne = [&]() {
            std::string v;
            if (left) {
                v = it->second.listValue.front();
                it->second.listValue.pop_front();
            } else {
                v = it->second.listValue.back();
                it->second.listValue.pop_back();
            }
            return v;
        };

        if (!haveCount) {
            std::string v = takeOne();
            if (it->second.listValue.empty()) data.erase(it);
            return resp::bulkString(v);
        }

        if (count == 0) return resp::emptyArray();
        std::vector<std::string> out;
        const int64_t take = std::min<int64_t>(count, static_cast<int64_t>(it->second.listValue.size()));
        for (int64_t i = 0; i < take; i++) out.push_back(resp::bulkString(takeOne()));
        if (it->second.listValue.empty()) data.erase(it);
        return resp::array(out);
    });
}

/// BLPOP / BRPOP. Answers immediately if it can, otherwise parks the session
/// and returns the empty string, which tells the net layer not to reply at all.
std::string blockingPopGeneric(CommandContext& ctx, bool left) {
    if (ctx.size() < 3) return wrongArity(left ? "blpop" : "brpop");

    double timeout = 0;
    if (!strutil::parseDouble(ctx[ctx.size() - 1], timeout) || timeout < 0) {
        return resp::error("ERR timeout is not a float or out of range");
    }

    std::vector<std::string> keys;
    for (size_t i = 1; i + 1 < ctx.size(); i++) keys.push_back(ctx[i]);
    if (keys.empty()) return wrongArity(left ? "blpop" : "brpop");

    // Try each key in turn. `answered` distinguishes "served now" from "there
    // is nothing here", which matters because the reply for the second case is
    // not a valid RESP frame at all -- nothing is sent until a push arrives.
    std::string served;
    const bool answered = ctx.services->store->write([&](DataStore::Map& data) {
        for (const auto& key : keys) {
            purgeIfExpired(data, key, timeutil::nowMs());
            auto it = data.find(key);
            if (it == data.end() || it->second.type != DataType::LIST) continue;
            if (it->second.listValue.empty()) continue;
            std::string value;
            if (left) {
                value = it->second.listValue.front();
                it->second.listValue.pop_front();
            } else {
                value = it->second.listValue.back();
                it->second.listValue.pop_back();
            }
            if (it->second.listValue.empty()) data.erase(it);
            served = resp::array({resp::bulkString(key), resp::bulkString(value)});
            return true;
        }
        return false;
    });
    if (answered) return served;

    // 0 means "wait forever", which is -1 in the queue's terms.
    const int64_t timeoutMs = (timeout == 0) ? -1 : static_cast<int64_t>(timeout * 1000);
    ctx.services->blocked->blockOnList(ctx.client, keys, left, timeoutMs);
    return "";  // no reply yet
}

}  // namespace

void registerListCommands(CommandRegistry& r) {
    r["LPUSH"] = [](CommandContext& ctx) { return pushGeneric(ctx, true, false); };
    r["RPUSH"] = [](CommandContext& ctx) { return pushGeneric(ctx, false, false); };
    r["LPUSHX"] = [](CommandContext& ctx) { return pushGeneric(ctx, true, true); };
    r["RPUSHX"] = [](CommandContext& ctx) { return pushGeneric(ctx, false, true); };

    r["LPOP"] = [](CommandContext& ctx) { return popGeneric(ctx, true); };
    r["RPOP"] = [](CommandContext& ctx) { return popGeneric(ctx, false); };
    r["BLPOP"] = [](CommandContext& ctx) { return blockingPopGeneric(ctx, true); };
    r["BRPOP"] = [](CommandContext& ctx) { return blockingPopGeneric(ctx, false); };

    r["LLEN"] = [](CommandContext& ctx) {
        if (ctx.size() != 2) return wrongArity("llen");
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            auto it = data.find(ctx[1]);
            if (it == data.end() || it->second.isExpired(timeutil::nowMs())) return resp::integer(0);
            if (it->second.type != DataType::LIST) return resp::error(kWrongTypeError);
            return resp::integer(static_cast<int64_t>(it->second.listValue.size()));
        });
    };

    r["LRANGE"] = [](CommandContext& ctx) {
        if (ctx.size() != 4) return wrongArity("lrange");
        int64_t start = 0, stop = 0;
        if (!strutil::parseInt64(ctx[2], start) || !strutil::parseInt64(ctx[3], stop)) {
            return resp::error("ERR value is not an integer or out of range");
        }
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            auto it = data.find(ctx[1]);
            if (it == data.end() || it->second.isExpired(timeutil::nowMs())) {
                return resp::emptyArray();
            }
            if (it->second.type != DataType::LIST) return resp::error(kWrongTypeError);
            const auto& list = it->second.listValue;
            const int64_t len = static_cast<int64_t>(list.size());
            if (start < 0) start = len + start;
            if (stop < 0) stop = len + stop;
            if (start < 0) start = 0;
            if (stop >= len) stop = len - 1;
            std::vector<std::string> out;
            for (int64_t i = start; i <= stop && i < len; i++) {
                out.push_back(resp::bulkString(list[static_cast<size_t>(i)]));
            }
            return resp::array(out);
        });
    };

    r["LINDEX"] = [](CommandContext& ctx) {
        if (ctx.size() != 3) return wrongArity("lindex");
        int64_t index = 0;
        if (!strutil::parseInt64(ctx[2], index)) {
            return resp::error("ERR value is not an integer or out of range");
        }
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            auto it = data.find(ctx[1]);
            if (it == data.end() || it->second.isExpired(timeutil::nowMs())) {
                return resp::nullBulk();
            }
            if (it->second.type != DataType::LIST) return resp::error(kWrongTypeError);
            const auto& list = it->second.listValue;
            const int64_t len = static_cast<int64_t>(list.size());
            if (index < 0) index += len;
            if (index < 0 || index >= len) return resp::nullBulk();
            return resp::bulkString(list[static_cast<size_t>(index)]);
        });
    };

    r["LSET"] = [](CommandContext& ctx) {
        if (ctx.size() != 4) return wrongArity("lset");
        int64_t index = 0;
        if (!strutil::parseInt64(ctx[2], index)) {
            return resp::error("ERR value is not an integer or out of range");
        }
        return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
            purgeIfExpired(data, ctx[1], timeutil::nowMs());
            auto it = data.find(ctx[1]);
            if (it == data.end()) return resp::error("ERR no such key");
            if (it->second.type != DataType::LIST) return resp::error(kWrongTypeError);
            auto& list = it->second.listValue;
            const int64_t len = static_cast<int64_t>(list.size());
            if (index < 0) index += len;
            if (index < 0 || index >= len) return resp::error("ERR index out of range");
            list[static_cast<size_t>(index)] = ctx[3];
            return resp::simpleString("OK");
        });
    };

    r["LREM"] = [](CommandContext& ctx) {
        if (ctx.size() != 4) return wrongArity("lrem");
        int64_t count = 0;
        if (!strutil::parseInt64(ctx[2], count)) {
            return resp::error("ERR value is not an integer or out of range");
        }
        return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
            purgeIfExpired(data, ctx[1], timeutil::nowMs());
            auto it = data.find(ctx[1]);
            if (it == data.end()) return resp::integer(0);
            if (it->second.type != DataType::LIST) return resp::error(kWrongTypeError);
            auto& list = it->second.listValue;
            const bool fromHead = count >= 0;
            const int64_t limit = count < 0 ? -count : count;
            int64_t removed = 0;

            if (fromHead) {
                for (auto i = list.begin(); i != list.end();) {
                    if (*i == ctx[3] && (limit == 0 || removed < limit)) {
                        i = list.erase(i);
                        removed++;
                    } else {
                        ++i;
                    }
                }
            } else {
                // Negative count: scan from the tail. std::deque's iterators
                // are not reversible, so walk forwards and remember where to
                // stop.
                std::deque<std::string> kept;
                for (const auto& value : list) {
                    if (value == ctx[3] && (limit == 0 || removed < limit)) {
                        removed++;
                        continue;
                    }
                    kept.push_back(value);
                }
                list.swap(kept);
            }
            if (list.empty()) data.erase(it);
            return resp::integer(removed);
        });
    };

    r["LTRIM"] = [](CommandContext& ctx) {
        if (ctx.size() != 4) return wrongArity("ltrim");
        int64_t start = 0, stop = 0;
        if (!strutil::parseInt64(ctx[2], start) || !strutil::parseInt64(ctx[3], stop)) {
            return resp::error("ERR value is not an integer or out of range");
        }
        return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
            purgeIfExpired(data, ctx[1], timeutil::nowMs());
            auto it = data.find(ctx[1]);
            if (it == data.end()) return resp::simpleString("OK");
            if (it->second.type != DataType::LIST) return resp::error(kWrongTypeError);
            auto& list = it->second.listValue;
            const int64_t len = static_cast<int64_t>(list.size());
            if (start < 0) start = len + start;
            if (stop < 0) stop = len + stop;
            if (start < 0) start = 0;
            if (stop >= len) stop = len - 1;
            if (start > stop) {
                list.clear();
            } else {
                std::deque<std::string> kept;
                for (int64_t i = start; i <= stop; i++) kept.push_back(list[static_cast<size_t>(i)]);
                list = std::move(kept);
            }
            if (list.empty()) data.erase(it);
            return resp::simpleString("OK");
        });
    };

    r["LINSERT"] = [](CommandContext& ctx) -> std::string {
        // LINSERT key BEFORE|AFTER pivot element -- four arguments after the
        // command name, not three.
        if (ctx.size() != 5) return wrongArity("linsert");
        const bool before = strutil::toUpper(ctx[2]) == "BEFORE";
        if (!before && strutil::toUpper(ctx[2]) != "AFTER") {
            return resp::error("ERR syntax error");
        }
        return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
            purgeIfExpired(data, ctx[1], timeutil::nowMs());
            auto it = data.find(ctx[1]);
            if (it == data.end()) return resp::integer(0);
            if (it->second.type != DataType::LIST) return resp::error(kWrongTypeError);
            auto& list = it->second.listValue;
            auto at = std::find(list.begin(), list.end(), ctx[3]);
            if (at == list.end()) return resp::integer(-1);
            list.insert(before ? at : std::next(at), ctx[4]);
            return resp::integer(static_cast<int64_t>(list.size()));
        });
    };

    r["RPOPLPUSH"] = [](CommandContext& ctx) {
        if (ctx.size() != 3) return wrongArity("rpoplpush");
        return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
            auto src = data.find(ctx[1]);
            if (src == data.end() || src->second.type != DataType::LIST) return resp::nullBulk();
            if (src->second.listValue.empty()) return resp::nullBulk();
            std::string value = src->second.listValue.back();
            src->second.listValue.pop_back();
            if (src->second.listValue.empty()) data.erase(src);

            auto& dst = data[ctx[2]];
            dst.type = DataType::LIST;
            dst.listValue.push_front(value);
            return resp::bulkString(value);
        });
    };
}

}  // namespace redis
