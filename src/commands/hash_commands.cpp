/**
 * hash_commands.cpp -- the hash type.
 */
#include <algorithm>

#include "protocol/resp.hpp"
#include "store/data_store.hpp"
#include "types/handler.hpp"
#include "utils/strutil.hpp"
#include "utils/time.hpp"

namespace redis {

void registerHashCommands(CommandRegistry& r) {
    r["HSET"] = [](CommandContext& ctx) {
        if (ctx.size() < 4 || (ctx.size() - 2) % 2 != 0) return wrongArity("hset");
        return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
            purgeIfExpired(data, ctx[1], timeutil::nowMs());
            auto it = data.find(ctx[1]);
            if (it != data.end() && it->second.type != DataType::HASH) {
                return resp::error(kWrongTypeError);
            }
            RedisValue& slot = data[ctx[1]];
            slot.type = DataType::HASH;
            int64_t added = 0;
            for (size_t i = 2; i + 1 < ctx.size(); i += 2) {
                if (slot.hashValue.emplace(ctx[i], ctx[i + 1]).second) added++;
            }
            return resp::integer(added);
        });
    };

    r["HMSET"] = r["HSET"];

    r["HSETNX"] = [](CommandContext& ctx) {
        if (ctx.size() != 4) return wrongArity("hsetnx");
        return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
            purgeIfExpired(data, ctx[1], timeutil::nowMs());
            auto it = data.find(ctx[1]);
            if (it != data.end() && it->second.type != DataType::HASH) {
                return resp::error(kWrongTypeError);
            }
            RedisValue& slot = data[ctx[1]];
            slot.type = DataType::HASH;
            if (slot.hashValue.count(ctx[2])) return resp::integer(0);
            slot.hashValue[ctx[2]] = ctx[3];
            return resp::integer(1);
        });
    };

    r["HGET"] = [](CommandContext& ctx) {
        if (ctx.size() != 3) return wrongArity("hget");
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            auto it = data.find(ctx[1]);
            if (it == data.end() || it->second.isExpired(timeutil::nowMs())) {
                return resp::nullBulk();
            }
            if (it->second.type != DataType::HASH) return resp::error(kWrongTypeError);
            auto fit = it->second.hashValue.find(ctx[2]);
            if (fit == it->second.hashValue.end()) return resp::nullBulk();
            return resp::bulkString(fit->second);
        });
    };

    r["HMGET"] = [](CommandContext& ctx) {
        if (ctx.size() < 3) return wrongArity("hmget");
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            auto it = data.find(ctx[1]);
            const bool live =
                it != data.end() && !it->second.isExpired(timeutil::nowMs()) &&
                it->second.type == DataType::HASH;
            std::vector<std::string> out;
            for (size_t i = 2; i < ctx.size(); i++) {
                if (!live) {
                    out.push_back(resp::nullBulk());
                    continue;
                }
                auto fit = it->second.hashValue.find(ctx[i]);
                out.push_back(fit == it->second.hashValue.end() ? resp::nullBulk()
                                                                : resp::bulkString(fit->second));
            }
            return resp::array(out);
        });
    };

    r["HGETALL"] = [](CommandContext& ctx) {
        if (ctx.size() != 2) return wrongArity("hgetall");
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            auto it = data.find(ctx[1]);
            if (it == data.end() || it->second.isExpired(timeutil::nowMs())) {
                return resp::emptyArray();
            }
            if (it->second.type != DataType::HASH) return resp::error(kWrongTypeError);
            std::vector<std::string> out;
            for (const auto& [field, value] : it->second.hashValue) {
                out.push_back(resp::bulkString(field));
                out.push_back(resp::bulkString(value));
            }
            return resp::array(out);
        });
    };

    r["HKEYS"] = [](CommandContext& ctx) {
        if (ctx.size() != 2) return wrongArity("hkeys");
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            auto it = data.find(ctx[1]);
            if (it == data.end() || it->second.isExpired(timeutil::nowMs())) {
                return resp::emptyArray();
            }
            if (it->second.type != DataType::HASH) return resp::error(kWrongTypeError);
            std::vector<std::string> out;
            for (const auto& [field, value] : it->second.hashValue) {
                out.push_back(resp::bulkString(field));
            }
            return resp::array(out);
        });
    };

    r["HVALS"] = [](CommandContext& ctx) {
        if (ctx.size() != 2) return wrongArity("hvals");
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            auto it = data.find(ctx[1]);
            if (it == data.end() || it->second.isExpired(timeutil::nowMs())) {
                return resp::emptyArray();
            }
            if (it->second.type != DataType::HASH) return resp::error(kWrongTypeError);
            std::vector<std::string> out;
            for (const auto& [field, value] : it->second.hashValue) {
                out.push_back(resp::bulkString(value));
            }
            return resp::array(out);
        });
    };

    r["HDEL"] = [](CommandContext& ctx) {
        if (ctx.size() < 3) return wrongArity("hdel");
        return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
            auto it = data.find(ctx[1]);
            if (it == data.end()) return resp::integer(0);
            if (it->second.type != DataType::HASH) return resp::error(kWrongTypeError);
            int64_t removed = 0;
            for (size_t i = 2; i < ctx.size(); i++) removed += it->second.hashValue.erase(ctx[i]);
            if (it->second.hashValue.empty()) data.erase(it);
            return resp::integer(removed);
        });
    };

    r["HEXISTS"] = [](CommandContext& ctx) {
        if (ctx.size() != 3) return wrongArity("hexists");
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            auto it = data.find(ctx[1]);
            if (it == data.end() || it->second.isExpired(timeutil::nowMs())) {
                return resp::integer(0);
            }
            if (it->second.type != DataType::HASH) return resp::error(kWrongTypeError);
            return resp::integer(it->second.hashValue.count(ctx[2]) ? 1 : 0);
        });
    };

    r["HLEN"] = [](CommandContext& ctx) {
        if (ctx.size() != 2) return wrongArity("hlen");
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            auto it = data.find(ctx[1]);
            if (it == data.end() || it->second.isExpired(timeutil::nowMs())) {
                return resp::integer(0);
            }
            if (it->second.type != DataType::HASH) return resp::error(kWrongTypeError);
            return resp::integer(static_cast<int64_t>(it->second.hashValue.size()));
        });
    };

    r["HSTRLEN"] = [](CommandContext& ctx) {
        if (ctx.size() != 3) return wrongArity("hstrlen");
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            auto it = data.find(ctx[1]);
            if (it == data.end() || it->second.isExpired(timeutil::nowMs())) {
                return resp::integer(0);
            }
            if (it->second.type != DataType::HASH) return resp::error(kWrongTypeError);
            auto fit = it->second.hashValue.find(ctx[2]);
            if (fit == it->second.hashValue.end()) return resp::integer(0);
            return resp::integer(static_cast<int64_t>(fit->second.size()));
        });
    };

    r["HINCRBY"] = [](CommandContext& ctx) {
        if (ctx.size() != 4) return wrongArity("hincrby");
        int64_t delta = 0;
        if (!strutil::parseInt64(ctx[3], delta)) {
            return resp::error("ERR value is not an integer or out of range");
        }
        return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
            purgeIfExpired(data, ctx[1], timeutil::nowMs());
            auto it = data.find(ctx[1]);
            if (it != data.end() && it->second.type != DataType::HASH) {
                return resp::error(kWrongTypeError);
            }
            RedisValue& slot = data[ctx[1]];
            slot.type = DataType::HASH;
            int64_t value = 0;
            auto fit = slot.hashValue.find(ctx[2]);
            if (fit != slot.hashValue.end() && !strutil::parseInt64(fit->second, value)) {
                return resp::error("ERR hash value is not an integer");
            }
            value += delta;
            slot.hashValue[ctx[2]] = std::to_string(value);
            return resp::integer(value);
        });
    };
}

}  // namespace redis
