/**
 * set_commands.cpp -- the set type.
 */
#include <algorithm>
#include <random>

#include "protocol/resp.hpp"
#include "store/data_store.hpp"
#include "types/handler.hpp"
#include "utils/strutil.hpp"
#include "utils/time.hpp"

namespace redis {

void registerSetCommands(CommandRegistry& r) {
    r["SADD"] = [](CommandContext& ctx) {
        if (ctx.size() < 3) return wrongArity("sadd");
        return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
            purgeIfExpired(data, ctx[1], timeutil::nowMs());
            auto it = data.find(ctx[1]);
            if (it != data.end() && it->second.type != DataType::SET) {
                return resp::error(kWrongTypeError);
            }
            RedisValue& slot = data[ctx[1]];
            slot.type = DataType::SET;
            int64_t added = 0;
            for (size_t i = 2; i < ctx.size(); i++) {
                if (slot.setValue.insert(ctx[i]).second) added++;
            }
            return resp::integer(added);
        });
    };

    r["SREM"] = [](CommandContext& ctx) {
        if (ctx.size() < 3) return wrongArity("srem");
        return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
            auto it = data.find(ctx[1]);
            if (it == data.end()) return resp::integer(0);
            if (it->second.type != DataType::SET) return resp::error(kWrongTypeError);
            int64_t removed = 0;
            for (size_t i = 2; i < ctx.size(); i++) removed += it->second.setValue.erase(ctx[i]);
            if (it->second.setValue.empty()) data.erase(it);
            return resp::integer(removed);
        });
    };

    r["SMEMBERS"] = [](CommandContext& ctx) {
        if (ctx.size() != 2) return wrongArity("smembers");
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            auto it = data.find(ctx[1]);
            if (it == data.end() || it->second.isExpired(timeutil::nowMs())) {
                return resp::emptyArray();
            }
            if (it->second.type != DataType::SET) return resp::error(kWrongTypeError);
            return resp::arrayOfBulkStrings({it->second.setValue.begin(), it->second.setValue.end()});
        });
    };

    r["SISMEMBER"] = [](CommandContext& ctx) {
        if (ctx.size() != 3) return wrongArity("sismember");
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            auto it = data.find(ctx[1]);
            if (it == data.end() || it->second.isExpired(timeutil::nowMs())) {
                return resp::integer(0);
            }
            if (it->second.type != DataType::SET) return resp::error(kWrongTypeError);
            return resp::integer(it->second.setValue.count(ctx[2]) ? 1 : 0);
        });
    };

    r["SCARD"] = [](CommandContext& ctx) {
        if (ctx.size() != 2) return wrongArity("scard");
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            auto it = data.find(ctx[1]);
            if (it == data.end() || it->second.isExpired(timeutil::nowMs())) {
                return resp::integer(0);
            }
            if (it->second.type != DataType::SET) return resp::error(kWrongTypeError);
            return resp::integer(static_cast<int64_t>(it->second.setValue.size()));
        });
    };

    r["SPOP"] = [](CommandContext& ctx) {
        if (ctx.size() < 2 || ctx.size() > 3) return wrongArity("spop");
        return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
            purgeIfExpired(data, ctx[1], timeutil::nowMs());
            auto it = data.find(ctx[1]);
            if (it == data.end()) return resp::nullArray();
            if (it->second.type != DataType::SET) return resp::error(kWrongTypeError);

            std::vector<std::string> members(it->second.setValue.begin(),
                                            it->second.setValue.end());
            int64_t want = 1;
            bool haveCount = false;
            if (ctx.size() == 3) {
                if (!strutil::parseInt64(ctx[2], want) || want < 0) {
                    return resp::error("ERR value is out of range, must be positive");
                }
                haveCount = true;
            }
            if (!haveCount) {
                if (members.empty()) return resp::nullBulk();
                std::mt19937 rng(std::random_device{}());
                std::uniform_int_distribution<size_t> pick(0, members.size() - 1);
                std::string chosen = members[pick(rng)];
                it->second.setValue.erase(chosen);
                if (it->second.setValue.empty()) data.erase(it);
                return resp::bulkString(chosen);
            }

            const size_t take = std::min<size_t>(static_cast<size_t>(want), members.size());
            std::shuffle(members.begin(), members.end(),
                         std::mt19937(std::random_device{}()));
            std::vector<std::string> out;
            for (size_t i = 0; i < take; i++) {
                it->second.setValue.erase(members[i]);
                out.push_back(resp::bulkString(members[i]));
            }
            if (it->second.setValue.empty()) data.erase(it);
            return resp::array(out);
        });
    };

    r["SRANDMEMBER"] = [](CommandContext& ctx) {
        if (ctx.size() < 2 || ctx.size() > 3) return wrongArity("srandmember");
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            auto it = data.find(ctx[1]);
            if (it == data.end() || it->second.isExpired(timeutil::nowMs())) {
                return ctx.size() == 3 ? resp::emptyArray() : resp::nullBulk();
            }
            if (it->second.type != DataType::SET) return resp::error(kWrongTypeError);
            std::vector<std::string> members(it->second.setValue.begin(),
                                            it->second.setValue.end());
            if (members.empty()) return ctx.size() == 3 ? resp::emptyArray() : resp::nullBulk();
            std::shuffle(members.begin(), members.end(),
                         std::mt19937(std::random_device{}()));
            if (ctx.size() == 2) return resp::bulkString(members[0]);
            int64_t count = 0;
            strutil::parseInt64(ctx[2], count);
            const bool allowRepeat = count < 0;
            const size_t want = static_cast<size_t>(std::abs(count));
            std::vector<std::string> out;
            if (allowRepeat) {
                for (size_t i = 0; i < want; i++) out.push_back(resp::bulkString(members[i % members.size()]));
            } else {
                for (size_t i = 0; i < want && i < members.size(); i++) {
                    out.push_back(resp::bulkString(members[i]));
                }
            }
            return resp::array(out);
        });
    };

    r["SMOVE"] = [](CommandContext& ctx) {
        if (ctx.size() != 4) return wrongArity("smove");
        return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
            auto src = data.find(ctx[1]);
            if (src == data.end()) return resp::integer(0);
            if (src->second.type != DataType::SET) return resp::error(kWrongTypeError);
            if (src->second.setValue.erase(ctx[3]) == 0) return resp::integer(0);
            if (src->second.setValue.empty()) data.erase(src);
            RedisValue& dst = data[ctx[2]];
            dst.type = DataType::SET;
            dst.setValue.insert(ctx[3]);
            return resp::integer(1);
        });
    };

    // SDIFF / SINTER / SUNION over N sets. `op` is the single letter the
    // behaviour keys off, so the three share one traversal.
    auto setOperation = [](char op) {
        return [op](CommandContext& ctx) -> std::string {
            if (ctx.size() < 2) return wrongArity(op == 'D' ? "sdiff" : (op == 'I' ? "sinter" : "sunion"));
            return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
                std::vector<std::unordered_set<std::string>> sets;
                for (size_t i = 1; i < ctx.size(); i++) {
                    auto it = data.find(ctx[i]);
                    if (it == data.end()) {
                        sets.emplace_back();
                        continue;
                    }
                    if (it->second.type != DataType::SET) return resp::error(kWrongTypeError);
                    sets.push_back(it->second.setValue);
                }
                std::unordered_set<std::string> result;
                if (!sets.empty()) result = sets[0];
                if (op == 'D') {
                    // SDIFF: keep only what none of the others has.
                    for (size_t i = 1; i < sets.size(); i++) {
                        for (auto it = result.begin(); it != result.end();) {
                            it = sets[i].count(*it) ? result.erase(it) : std::next(it);
                        }
                    }
                } else if (op == 'I') {
                    // SINTER: keep only what every one of the others has.
                    for (size_t i = 1; i < sets.size(); i++) {
                        for (auto it = result.begin(); it != result.end();) {
                            it = sets[i].count(*it) ? std::next(it) : result.erase(it);
                        }
                    }
                } else {
                    // SUNION: add the others in. Pruning the accumulator first
                    // and unioning afterwards collapses it to an intersection,
                    // because the pruning step is what an intersection is.
                    for (size_t i = 1; i < sets.size(); i++) {
                        result.insert(sets[i].begin(), sets[i].end());
                    }
                }
                return resp::arrayOfBulkStrings({result.begin(), result.end()});
            });
        };
    };
    r["SDIFF"] = setOperation('D');
    r["SINTER"] = setOperation('I');
    r["SUNION"] = setOperation('U');

    r["SDIFFSTORE"] = [](CommandContext& ctx) -> std::string {
        if (ctx.size() < 3) return wrongArity("sdiffstore");
        const std::string destination = ctx[1];
        return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
            std::unordered_set<std::string> result;
            auto first = data.find(ctx[2]);
            if (first != data.end() && first->second.type == DataType::SET) {
                result = first->second.setValue;
                for (size_t i = 3; i < ctx.size(); i++) {
                    auto it = data.find(ctx[i]);
                    if (it == data.end() || it->second.type != DataType::SET) {
                        // A missing or wrongly-typed operand empties the result,
                        // because every member is "in" the difference set.
                        for (auto x = result.begin(); x != result.end();) x = result.erase(x);
                        continue;
                    }
                    for (auto x = result.begin(); x != result.end();) {
                        x = it->second.setValue.count(*x) ? result.erase(x) : std::next(x);
                    }
                }
            }
            const int64_t size = static_cast<int64_t>(result.size());
            data.erase(destination);
            if (!result.empty()) {
                RedisValue& slot = data[destination];
                slot.type = DataType::SET;
                slot.setValue = std::move(result);
            }
            return resp::integer(size);
        });
    };
}

}  // namespace redis
