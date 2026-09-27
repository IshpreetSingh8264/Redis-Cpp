/**
 * string_commands.cpp -- the string type.
 */
#include <cmath>

#include "protocol/resp.hpp"
#include "store/data_store.hpp"
#include "types/handler.hpp"
#include "utils/strutil.hpp"
#include "utils/time.hpp"

namespace redis {

namespace {

std::string setGeneric(CommandContext& ctx) {
    if (ctx.size() < 3) return wrongArity("set");

    const std::string& key = ctx[1];
    const std::string& value = ctx[2];
    int64_t expiryMs = -1;
    bool nx = false, xx = false, keepttl = false, get = false;

    for (size_t i = 3; i < ctx.size(); i++) {
        const std::string opt = strutil::toUpper(ctx[i]);
        auto needValue = [&](int64_t& slot) -> bool {
            if (i + 1 >= ctx.size()) return false;
            return strutil::parseInt64(ctx[i + 1], slot);
        };
        if (opt == "EX" || opt == "PX" || opt == "EXAT" || opt == "PXAT" || opt == "KEEPTTL") {
            if (opt == "KEEPTTL") {
                keepttl = true;
                continue;
            }
            int64_t raw = 0;
            if (!needValue(raw)) return resp::error("ERR syntax error");
            i++;
            const int64_t now = timeutil::nowMs();
            if (opt == "EX") expiryMs = now + raw * 1000;
            else if (opt == "PX") expiryMs = now + raw;
            else if (opt == "EXAT") expiryMs = raw * 1000;
            else expiryMs = raw;
        } else if (opt == "NX") {
            nx = true;
        } else if (opt == "XX") {
            xx = true;
        } else if (opt == "GET") {
            get = true;
        } else {
            return resp::error("ERR syntax error");
        }
    }

    // Real Redis rejects NX and XX on the same SET. Accepting both silently
    // meant SET k v NX XX on a missing key returned +OK having done nothing.
    if (nx && xx) {
        return resp::error("ERR syntax error");
    }

    return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
        const int64_t now = timeutil::nowMs();
        auto it = data.find(key);
        if (it != data.end() && it->second.isExpired(now)) {
            data.erase(it);
            it = data.end();
        }
        const bool exists = it != data.end();

        std::string previous = resp::nullBulk();
        if (get) {
            if (exists && it->second.type != DataType::STRING) {
                return resp::error(std::string("GET ") + kWrongTypeError);
            }
            if (exists) previous = resp::bulkString(it->second.stringValue);
        }

        if (nx && exists) return get ? previous : resp::nullBulk();
        if (xx && !exists) return get ? resp::nullBulk() : resp::nullBulk();

        if (exists && it->second.type != DataType::STRING && it->second.type != DataType::NONE) {
            return resp::error(std::string(kWrongTypeError));
        }

        RedisValue& slot = data[key];
        const int64_t keep = slot.expiryMs;
        slot.type = DataType::STRING;
        slot.stringValue = value;
        slot.expiryMs = keepttl ? keep : expiryMs;
        return get ? previous : resp::simpleString("OK");
    });
}

std::string getGeneric(CommandContext& ctx) {
    if (ctx.size() != 2) return wrongArity("get");
    return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
        auto it = data.find(ctx[1]);
        if (it == data.end() || it->second.isExpired(timeutil::nowMs())) return resp::nullBulk();
        if (it->second.type != DataType::STRING) {
            return resp::error("GET " + std::string(kWrongTypeError));
        }
        return resp::bulkString(it->second.stringValue);
    });
}

/// Shared by INCR / DECR / INCRBY / DECRBY. `delta` of 0 with `by` unset means
/// step by one.
std::string incrementBy(CommandContext& ctx, int64_t delta, bool haveDelta) {
    if (ctx.size() < 2 || (haveDelta && ctx.size() != 3)) return wrongArity("incrby");

    if (haveDelta) {
        int64_t parsed = 0;
        if (!strutil::parseInt64(ctx[2], parsed)) {
            return resp::error("ERR value is not an integer or out of range");
        }
        delta = parsed;
    }

    return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
        const int64_t now = timeutil::nowMs();
        purgeIfExpired(data, ctx[1], now);
        auto it = data.find(ctx[1]);

        int64_t value = 0;
        if (it != data.end()) {
            if (it->second.type != DataType::STRING) return resp::error(kWrongTypeError);
            if (!strutil::parseInt64(it->second.stringValue, value)) {
                return resp::error("ERR value is not an integer or out of range");
            }
        }
        if ((delta > 0 && value > INT64_MAX - delta) || (delta < 0 && value < INT64_MIN - delta)) {
            return resp::error("ERR increment or decrement would overflow");
        }
        value += delta;

        RedisValue& slot = data[ctx[1]];
        slot.type = DataType::STRING;
        slot.stringValue = std::to_string(value);
        return resp::integer(value);
    });
}

std::string setexGeneric(CommandContext& ctx, bool millis) {
    if (ctx.size() != 4) return wrongArity(millis ? "psetex" : "setex");
    int64_t ttl = 0;
    if (!strutil::parseInt64(ctx[2], ttl) || ttl <= 0) {
        return resp::error("ERR invalid expire time in 'setex' command");
    }
    const std::string key = ctx[1];
    const std::string value = ctx[3];
    const int64_t expiry = timeutil::nowMs() + (millis ? ttl : ttl * 1000);

    return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
        const int64_t now = timeutil::nowMs();
        purgeIfExpired(data, key, now);
        auto it = data.find(key);
        if (it != data.end() && it->second.type != DataType::STRING &&
            it->second.type != DataType::NONE) {
            return resp::error(kWrongTypeError);
        }
        RedisValue& slot = data[key];
        slot.type = DataType::STRING;
        slot.stringValue = value;
        slot.expiryMs = expiry;
        return resp::simpleString("OK");
    });
}

}  // namespace

void registerStringCommands(CommandRegistry& r) {
    r["SET"] = setGeneric;
    r["GET"] = getGeneric;

    // SETNX and SETXX are SET with a condition, but they answer with 1/0
    // rather than +OK, so they cannot just forward to setGeneric and translate
    // the reply -- they have to decide the integer themselves.
    auto conditionalSet = [](bool onlyIfNew) {
        return [onlyIfNew](CommandContext& ctx) -> std::string {
            if (ctx.size() < 3) return wrongArity(onlyIfNew ? "setnx" : "setxx");
            const std::string key = ctx[1];
            const std::string value = ctx[2];

            const std::string written = ctx.services->store->write([&](DataStore::Map& data) {
                const int64_t now = timeutil::nowMs();
                purgeIfExpired(data, key, now);
                auto it = data.find(key);
                if (it != data.end() && it->second.type != DataType::STRING &&
                    it->second.type != DataType::NONE) {
                    return resp::error(kWrongTypeError);
                }
                const bool exists = it != data.end();
                if (exists == onlyIfNew) return std::string();  // nothing to do

                RedisValue& slot = data[key];
                slot.type = DataType::STRING;
                slot.stringValue = value;
                slot.expiryMs = -1;
                return resp::integer(1);
            });
            return written.empty() ? resp::integer(0) : written;
        };
    };
    r["SETNX"] = conditionalSet(true);
    r["SETXX"] = conditionalSet(false);

    r["GETSET"] = [](CommandContext& ctx) -> std::string {
        if (ctx.size() < 3) return wrongArity("getset");
        const std::string previous = getGeneric(ctx);
        if (!previous.empty() && previous[0] == '-') return previous;
        std::vector<std::string> args{"SET", ctx[1], ctx[2]};
        ctx.args = std::move(args);
        std::string written = setGeneric(ctx);
        if (!written.empty() && written[0] == '-') return written;
        return previous;
    };
    r["SETEX"] = [](CommandContext& ctx) { return setexGeneric(ctx, false); };
    r["PSETEX"] = [](CommandContext& ctx) { return setexGeneric(ctx, true); };

    r["APPEND"] = [](CommandContext& ctx) {
        if (ctx.size() < 3) return wrongArity("append");
        return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
            const int64_t now = timeutil::nowMs();
            purgeIfExpired(data, ctx[1], now);
            auto it = data.find(ctx[1]);
            if (it != data.end() && it->second.type != DataType::STRING &&
                it->second.type != DataType::NONE) {
                return resp::error(kWrongTypeError);
            }
            RedisValue& slot = data[ctx[1]];
            if (slot.type == DataType::NONE) slot.type = DataType::STRING;
            const size_t before = slot.stringValue.size();
            slot.stringValue += ctx[2];
            return resp::integer(static_cast<int64_t>(slot.stringValue.size() - before));
        });
    };

    r["STRLEN"] = [](CommandContext& ctx) {
        if (ctx.size() != 2) return wrongArity("strlen");
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            auto it = data.find(ctx[1]);
            if (it == data.end() || it->second.isExpired(timeutil::nowMs())) return resp::integer(0);
            if (it->second.type != DataType::STRING) return resp::error(kWrongTypeError);
            return resp::integer(static_cast<int64_t>(it->second.stringValue.size()));
        });
    };

    r["GETRANGE"] = [](CommandContext& ctx) {
        if (ctx.size() != 4) return wrongArity("getrange");
        int64_t start = 0, end = 0;
        if (!strutil::parseInt64(ctx[2], start) || !strutil::parseInt64(ctx[3], end)) {
            return resp::error("ERR value is not an integer or out of range");
        }
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            auto it = data.find(ctx[1]);
            if (it == data.end() || it->second.isExpired(timeutil::nowMs())) {
                return resp::bulkString("");
            }
            if (it->second.type != DataType::STRING) return resp::error(kWrongTypeError);
            const std::string& s = it->second.stringValue;
            int64_t len = static_cast<int64_t>(s.size());
            if (start < 0) start = std::max<int64_t>(0, len + start);
            if (end < 0) end = len + end;
            if (end >= len) end = len - 1;
            if (start > end || start >= len) return resp::bulkString("");
            return resp::bulkString(s.substr(static_cast<size_t>(start),
                                             static_cast<size_t>(end - start + 1)));
        });
    };

    r["SETRANGE"] = [](CommandContext& ctx) {
        if (ctx.size() != 4) return wrongArity("setrange");
        int64_t offset = 0;
        if (!strutil::parseInt64(ctx[2], offset) || offset < 0) {
            return resp::error("ERR offset is out of range");
        }
        return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
            purgeIfExpired(data, ctx[1], timeutil::nowMs());
            auto it = data.find(ctx[1]);
            if (it != data.end() && it->second.type != DataType::STRING &&
                it->second.type != DataType::NONE) {
                return resp::error(kWrongTypeError);
            }
            RedisValue& slot = data[ctx[1]];
            if (slot.type == DataType::NONE) slot.type = DataType::STRING;
            if (static_cast<size_t>(offset) + ctx[3].size() > 512UL * 1024 * 1024) {
                return resp::error("ERR string exceeds maximum allowed size (proto-max-bulk-len)");
            }
            if (slot.stringValue.size() < static_cast<size_t>(offset)) {
                slot.stringValue.resize(static_cast<size_t>(offset), '\0');
            }
            slot.stringValue.replace(static_cast<size_t>(offset), ctx[3].size(), ctx[3]);
            return resp::integer(static_cast<int64_t>(slot.stringValue.size()));
        });
    };

    r["MGET"] = [](CommandContext& ctx) {
        if (ctx.size() < 2) return wrongArity("mget");
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            const int64_t now = timeutil::nowMs();
            std::vector<std::string> out;
            for (size_t i = 1; i < ctx.size(); i++) {
                auto it = data.find(ctx[i]);
                if (it == data.end() || it->second.isExpired(now) ||
                    it->second.type != DataType::STRING) {
                    out.push_back(resp::nullBulk());
                } else {
                    out.push_back(resp::bulkString(it->second.stringValue));
                }
            }
            return resp::array(out);
        });
    };

    r["MSET"] = [](CommandContext& ctx) {
        if (ctx.size() < 3 || ctx.size() % 2 == 0) return wrongArity("mset");
        return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
            const int64_t now = timeutil::nowMs();
            for (size_t i = 1; i + 1 < ctx.size(); i += 2) {
                purgeIfExpired(data, ctx[i], now);
                auto it = data.find(ctx[i]);
                if (it != data.end() && it->second.type != DataType::STRING &&
                    it->second.type != DataType::NONE) {
                    return resp::error(kWrongTypeError);
                }
            }
            for (size_t i = 1; i + 1 < ctx.size(); i += 2) {
                RedisValue& slot = data[ctx[i]];
                slot.type = DataType::STRING;
                slot.stringValue = ctx[i + 1];
                slot.expiryMs = -1;
            }
            return resp::simpleString("OK");
        });
    };

    r["INCR"] = [](CommandContext& ctx) { return incrementBy(ctx, 1, false); };
    r["DECR"] = [](CommandContext& ctx) { return incrementBy(ctx, -1, false); };
    r["INCRBY"] = [](CommandContext& ctx) { return incrementBy(ctx, 0, true); };
    r["DECRBY"] = [](CommandContext& ctx) {
        if (ctx.size() != 3) return wrongArity("decrby");
        int64_t delta = 0;
        if (!strutil::parseInt64(ctx[2], delta)) {
            return resp::error("ERR value is not an integer or out of range");
        }
        return incrementBy(ctx, -delta, true);
    };

    r["INCRBYFLOAT"] = [](CommandContext& ctx) {
        if (ctx.size() != 3) return wrongArity("incrbyfloat");
        double delta = 0;
        if (!strutil::parseDouble(ctx[2], delta)) {
            return resp::error("ERR value is not a valid float");
        }
        return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
            purgeIfExpired(data, ctx[1], timeutil::nowMs());
            auto it = data.find(ctx[1]);
            double value = 0;
            if (it != data.end()) {
                if (it->second.type != DataType::STRING) return resp::error(kWrongTypeError);
                if (!strutil::parseDouble(it->second.stringValue, value)) {
                    return resp::error("ERR value is not a valid float");
                }
            }
            value += delta;
            if (std::isinf(value)) return resp::error("ERR increment would produce NaN or Infinity");
            RedisValue& slot = data[ctx[1]];
            slot.type = DataType::STRING;
            const std::string rendered = strutil::formatScore(value);
            slot.stringValue = rendered;
            return resp::bulkString(rendered);
        });
    };
}

}  // namespace redis
