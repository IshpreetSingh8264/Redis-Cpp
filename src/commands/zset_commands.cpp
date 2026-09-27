/**
 * zset_commands.cpp -- the sorted set type.
 *
 * A zset is two views of one thing: a multimap keyed by score (for lookups
 * and for persistence) and a member -> score map. Redis's own order -- score
 * ascending, then member bytewise -- is only partly the multimap's, because
 * that is keyed on the score alone; `orderedByScore()` completes it, and every
 * ranked read goes through it.
 */
#include <algorithm>
#include <cmath>
#include <sstream>

#include "protocol/resp.hpp"
#include "store/data_store.hpp"
#include "types/handler.hpp"
#include "utils/strutil.hpp"
#include "utils/time.hpp"

namespace redis {

namespace {

/// Turn a (start, stop) pair from the client into a half-open [from, to)
/// window over ranks [0, cardinality), or an empty window if it lies outside.
///
/// Every rank-based command goes through this, so ZRANGE key -1 -1 and
/// ZRANGE key -100 100 cannot disagree about what "the last element" means.
bool normalizeRange(int64_t cardinality, int64_t start, int64_t stop, int64_t& from,
                    int64_t& to) {
    if (start < 0) start += cardinality;
    if (stop < 0) stop += cardinality;
    if (start < 0) start = 0;
    if (stop >= cardinality) stop = cardinality - 1;
    if (start > stop || start >= cardinality) {
        from = to = 0;
        return false;
    }
    from = start;
    to = stop + 1;
    return true;
}

/// Parse "min"/"max" with an optional leading '(' for exclusive bounds.
bool parseScoreBound(const std::string& raw, double& value, bool& exclusive) {
    exclusive = false;
    std::string text = raw;
    if (!text.empty() && text[0] == '(') {
        exclusive = true;
        text = text.substr(1);
    }
    if (text == "+inf" || text == "inf") {
        value = std::numeric_limits<double>::infinity();
        return true;
    }
    if (text == "-inf") {
        value = -std::numeric_limits<double>::infinity();
        return true;
    }
    return strutil::parseDouble(text, value);
}

bool withinScore(double score, double min, bool minExclusive, double max, bool maxExclusive) {
    if (minExclusive ? !(score > min) : !(score >= min)) return false;
    if (maxExclusive ? !(score < max) : !(score <= max)) return false;
    return true;
}

std::string zaddGeneric(CommandContext& ctx) {
    if (ctx.size() < 4) return wrongArity("zadd");

    bool nx = false, xx = false, ch = false, gt = false, lt = false;
    size_t i = 2;
    for (; i < ctx.size(); i++) {
        const std::string opt = strutil::toUpper(ctx[i]);
        if (opt == "NX") nx = true;
        else if (opt == "XX") xx = true;
        else if (opt == "CH") ch = true;
        else if (opt == "GT") gt = true;
        else if (opt == "LT") lt = true;
        else break;
    }
    if ((ctx.size() - i) % 2 != 0) return wrongArity("zadd");
    if (nx && (xx || gt || lt)) return resp::error(
        "ERR GT, LT, and/or NX options at the same time are not compatible");
    if (gt && lt) return resp::error("ERR GT, LT, and/or NX options at the same time are not "
                                     "compatible");

    return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
        purgeIfExpired(data, ctx[1], timeutil::nowMs());
        auto it = data.find(ctx[1]);
        if (it != data.end() && it->second.type != DataType::ZSET) {
            return resp::error(kWrongTypeError);
        }
        RedisValue& slot = data[ctx[1]];
        slot.type = DataType::ZSET;

        int64_t added = 0, changed = 0;
        for (size_t j = i; j + 1 < ctx.size(); j += 2) {
            double score = 0;
            if (!strutil::parseDouble(ctx[j], score)) {
                return resp::error("ERR value is not a valid float");
            }
            const std::string& member = ctx[j + 1];
            auto existing = slot.zsetScores.find(member);

            if (existing == slot.zsetScores.end()) {
                if (xx) continue;  // XX only updates existing members
                added++;
                changed++;
            } else {
                if (nx) continue;  // NX only adds new members
                if ((gt && existing->second >= score) || (lt && existing->second <= score)) {
                    continue;
                }
                if (existing->second != score) changed++;
            }
            // The old row goes whether or not the score moved. Dropping it only
            // on a change left a second (score, member) row behind whenever the
            // same score was re-added, and every rank-based read then counted
            // that member twice.
            if (existing != slot.zsetScores.end()) {
                auto range = slot.zsetByScore.equal_range(existing->second);
                for (auto sit = range.first; sit != range.second;) {
                    if (sit->second == member) {
                        sit = slot.zsetByScore.erase(sit);
                    } else {
                        ++sit;
                    }
                }
            }
            slot.zsetScores[member] = score;
            slot.zsetByScore.insert({score, member});
        }

        if (ch) return resp::integer(changed);
        return resp::integer(added);
    });
}

/// Redis's sorted-set order: score ascending, and among equal scores the member
/// compared bytewise. `zsetByScore` is keyed on the score alone, so its own
/// order is only half of that and leaves tied members in whatever order they
/// were inserted -- which is not what a "rank" means to Redis. Every read that
/// ranks or windows by score goes through this, so ZRANGE, ZRANK,
/// ZREMRANGEBYRANK and ZPOPMIN cannot disagree about who comes first.
using OrderedMembers = std::vector<std::pair<double, std::string>>;

OrderedMembers orderedByScore(const RedisValue& slot) {
    OrderedMembers ordered(slot.zsetByScore.begin(), slot.zsetByScore.end());
    std::sort(ordered.begin(), ordered.end(), [](const auto& a, const auto& b) {
        if (a.first != b.first) return a.first < b.first;
        return a.second < b.second;
    });
    return ordered;
}

/// Drop the row for `member` sitting at `score` from the score index.
void eraseFromIndex(RedisValue& slot, double score, const std::string& member) {
    auto range = slot.zsetByScore.equal_range(score);
    for (auto it = range.first; it != range.second;) {
        if (it->second == member) {
            it = slot.zsetByScore.erase(it);
        } else {
            ++it;
        }
    }
}

/// Collect the members in ranks [from, to), optionally with their scores.
std::vector<std::string> sliceByRank(const OrderedMembers& ordered, int64_t from, int64_t to,
                                     bool withScores) {
    std::vector<std::string> out;
    const int64_t last = std::min<int64_t>(to, static_cast<int64_t>(ordered.size()));
    for (int64_t rank = std::max<int64_t>(from, 0); rank < last; rank++) {
        const auto& [score, member] = ordered[static_cast<size_t>(rank)];
        out.push_back(resp::bulkString(member));
        if (withScores) out.push_back(resp::bulkString(strutil::formatScore(score)));
    }
    return out;
}

std::string zrangeGeneric(CommandContext& ctx, bool byScore, bool reverse) {
    if (ctx.size() < 4) return wrongArity(reverse ? "zrevrange" : (byScore ? "zrangebyscore" : "zrange"));

    const bool withScores = ctx.size() >= 5 && strutil::toUpper(ctx[4]) == "WITHSCORES";

    return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
        auto it = data.find(ctx[1]);
        if (it == data.end() || it->second.isExpired(timeutil::nowMs())) {
            return resp::emptyArray();
        }
        if (it->second.type != DataType::ZSET) return resp::error(kWrongTypeError);
        const RedisValue& slot = it->second;
        const OrderedMembers ordered = orderedByScore(slot);

        if (byScore) {
            // ZRANGEBYSCORE key min max, or ZRANGE key min max BYSCORE.
            const size_t minAt = reverse ? 3 : 2;
            const size_t maxAt = reverse ? 2 : 3;
            double min = 0, max = 0;
            bool minEx = false, maxEx = false;
            if (!parseScoreBound(ctx[minAt], min, minEx) ||
                !parseScoreBound(ctx[maxAt], max, maxEx)) {
                return resp::error("ERR min or max is not a float");
            }
            if (minEx) min = std::nextafter(min, std::numeric_limits<double>::infinity());
            if (maxEx) max = std::nextafter(max, -std::numeric_limits<double>::infinity());
            if (min > max) return resp::emptyArray();

            int64_t offset = 0, count = -1;
            for (size_t i = 4; i + 1 < ctx.size(); i++) {
                if (strutil::toUpper(ctx[i]) == "LIMIT") {
                    strutil::parseInt64(ctx[i + 1], offset);
                    if (i + 2 < ctx.size()) strutil::parseInt64(ctx[i + 2], count);
                }
            }

            std::vector<std::pair<double, std::string>> hits;
            for (const auto& entry : ordered) {
                if (withinScore(entry.first, min, minEx, max, maxEx)) hits.push_back(entry);
            }
            if (reverse) std::reverse(hits.begin(), hits.end());
            if (offset < static_cast<int64_t>(hits.size())) {
                hits.erase(hits.begin(), hits.begin() + offset);
            } else {
                hits.clear();
            }
            if (count >= 0 && static_cast<size_t>(count) < hits.size()) {
                hits.resize(static_cast<size_t>(count));
            }
            std::vector<std::string> out;
            for (const auto& [score, member] : hits) {
                out.push_back(resp::bulkString(member));
                if (withScores) out.push_back(resp::bulkString(strutil::formatScore(score)));
            }
            return resp::array(out);
        }

        int64_t start = 0, stop = 0;
        if (!strutil::parseInt64(ctx[2], start) || !strutil::parseInt64(ctx[3], stop)) {
            return resp::error("ERR value is not an integer or out of range");
        }
        const int64_t cardinality = static_cast<int64_t>(ordered.size());
        int64_t from = 0, to = 0;
        if (!normalizeRange(cardinality, start, stop, from, to)) return resp::emptyArray();

        // A rank window is a window on the same ordered set either way round, so
        // the ranks do not change -- only the order the members come back in
        // does. Mirroring the window instead (as an earlier version did) is a
        // no-op over a symmetric range, which is why ZREVRANGE used to answer
        // in ascending order.
        std::vector<std::string> members = sliceByRank(ordered, from, to, withScores);
        if (reverse && !members.empty()) {
            const size_t width = withScores ? 2 : 1;
            const size_t pairs = members.size() / width;
            for (size_t i = 0; i < pairs / 2; i++) {
                const size_t lo = i * width;
                const size_t hi = (pairs - 1 - i) * width;
                for (size_t k = 0; k < width; k++) std::swap(members[lo + k], members[hi + k]);
            }
        }
        return resp::array(members);
    });
}

std::string zremrangeGeneric(CommandContext& ctx, bool byScore) {
    if (ctx.size() < 4) return wrongArity(byScore ? "zremrangebyscore" : "zremrangebyrank");
    return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
        purgeIfExpired(data, ctx[1], timeutil::nowMs());
        auto it = data.find(ctx[1]);
        if (it == data.end() || it->second.isExpired(timeutil::nowMs())) return resp::integer(0);
        if (it->second.type != DataType::ZSET) return resp::error(kWrongTypeError);
        RedisValue& slot = it->second;
        const OrderedMembers ordered = orderedByScore(slot);

        std::vector<std::string> doomed;
        if (byScore) {
            double min = 0, max = 0;
            bool minEx = false, maxEx = false;
            if (!parseScoreBound(ctx[2], min, minEx) || !parseScoreBound(ctx[3], max, maxEx)) {
                return resp::error("ERR min or max is not a float");
            }
            if (minEx) min = std::nextafter(min, std::numeric_limits<double>::infinity());
            if (maxEx) max = std::nextafter(max, -std::numeric_limits<double>::infinity());
            for (const auto& [score, member] : ordered) {
                if (withinScore(score, min, minEx, max, maxEx)) doomed.push_back(member);
            }
        } else {
            int64_t start = 0, stop = 0;
            if (!strutil::parseInt64(ctx[2], start) || !strutil::parseInt64(ctx[3], stop)) {
                return resp::error("ERR value is not an integer or out of range");
            }
            const int64_t cardinality = static_cast<int64_t>(ordered.size());
            int64_t from = 0, to = 0;
            if (!normalizeRange(cardinality, start, stop, from, to)) return resp::integer(0);
            for (int64_t rank = from; rank < to; rank++) {
                doomed.push_back(ordered[static_cast<size_t>(rank)].second);
            }
        }

        for (const auto& member : doomed) {
            auto score = slot.zsetScores.find(member);
            if (score == slot.zsetScores.end()) continue;
            eraseFromIndex(slot, score->second, member);
            slot.zsetScores.erase(score);
        }
        if (slot.zsetScores.empty()) data.erase(it);
        return resp::integer(static_cast<int64_t>(doomed.size()));
    });
}

}  // namespace

void registerZsetCommands(CommandRegistry& r) {
    r["ZADD"] = zaddGeneric;

    r["ZSCORE"] = [](CommandContext& ctx) {
        if (ctx.size() != 3) return wrongArity("zscore");
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            auto it = data.find(ctx[1]);
            if (it == data.end() || it->second.isExpired(timeutil::nowMs())) {
                return resp::nullBulk();
            }
            if (it->second.type != DataType::ZSET) return resp::error(kWrongTypeError);
            auto sit = it->second.zsetScores.find(ctx[2]);
            if (sit == it->second.zsetScores.end()) return resp::nullBulk();
            return resp::bulkString(strutil::formatScore(sit->second));
        });
    };

    r["ZMSCORE"] = [](CommandContext& ctx) {
        if (ctx.size() < 3) return wrongArity("zmscore");
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            auto it = data.find(ctx[1]);
            const bool live = it != data.end() && !it->second.isExpired(timeutil::nowMs()) &&
                              it->second.type == DataType::ZSET;
            std::vector<std::string> out;
            for (size_t i = 2; i < ctx.size(); i++) {
                if (!live) {
                    out.push_back(resp::nullBulk());
                    continue;
                }
                auto sit = it->second.zsetScores.find(ctx[i]);
                out.push_back(sit == it->second.zsetScores.end()
                                   ? resp::nullBulk()
                                   : resp::bulkString(strutil::formatScore(sit->second)));
            }
            return resp::array(out);
        });
    };

    auto rankOf = [](CommandContext& ctx, bool reverse) {
        if (ctx.size() < 3) return wrongArity(reverse ? "zrevrank" : "zrank");
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            auto it = data.find(ctx[1]);
            if (it == data.end() || it->second.isExpired(timeutil::nowMs())) {
                return resp::nullBulk();
            }
            if (it->second.type != DataType::ZSET) return resp::error(kWrongTypeError);
            if (it->second.zsetScores.count(ctx[2]) == 0) return resp::nullBulk();

            const OrderedMembers ordered = orderedByScore(it->second);
            const int64_t cardinality = static_cast<int64_t>(ordered.size());
            for (int64_t rank = 0; rank < cardinality; rank++) {
                if (ordered[static_cast<size_t>(rank)].second == ctx[2]) {
                    return resp::integer(reverse ? cardinality - 1 - rank : rank);
                }
            }
            return resp::nullBulk();
        });
    };
    r["ZRANK"] = [rankOf](CommandContext& ctx) { return rankOf(ctx, false); };
    r["ZREVRANK"] = [rankOf](CommandContext& ctx) { return rankOf(ctx, true); };

    r["ZRANGE"] = [](CommandContext& ctx) {
        // ZRANGE key min max [BYSCORE|BYLEX] [REV] [WITHSCORES] [LIMIT o c]
        if (ctx.size() >= 5 && strutil::toUpper(ctx[4]) == "BYSCORE") {
            return zrangeGeneric(ctx, true, false);
        }
        return zrangeGeneric(ctx, false, false);
    };
    r["ZRANGEBYSCORE"] = [](CommandContext& ctx) { return zrangeGeneric(ctx, true, false); };
    r["ZREVRANGEBYSCORE"] = [](CommandContext& ctx) { return zrangeGeneric(ctx, true, true); };
    r["ZREVRANGE"] = [](CommandContext& ctx) { return zrangeGeneric(ctx, false, true); };

    r["ZCOUNT"] = [](CommandContext& ctx) {
        if (ctx.size() != 4) return wrongArity("zcount");
        double min = 0, max = 0;
        bool minEx = false, maxEx = false;
        if (!parseScoreBound(ctx[2], min, minEx) || !parseScoreBound(ctx[3], max, maxEx)) {
            return resp::error("ERR min or max is not a float");
        }
        if (minEx) min = std::nextafter(min, std::numeric_limits<double>::infinity());
        if (maxEx) max = std::nextafter(max, -std::numeric_limits<double>::infinity());
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            auto it = data.find(ctx[1]);
            if (it == data.end() || it->second.isExpired(timeutil::nowMs())) {
                return resp::integer(0);
            }
            if (it->second.type != DataType::ZSET) return resp::error(kWrongTypeError);
            int64_t count = 0;
            for (const auto& [score, member] : it->second.zsetByScore) {
                if (withinScore(score, min, minEx, max, maxEx)) count++;
            }
            return resp::integer(count);
        });
    };

    r["ZCARD"] = [](CommandContext& ctx) {
        if (ctx.size() != 2) return wrongArity("zcard");
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            auto it = data.find(ctx[1]);
            if (it == data.end() || it->second.isExpired(timeutil::nowMs())) {
                return resp::integer(0);
            }
            if (it->second.type != DataType::ZSET) return resp::error(kWrongTypeError);
            return resp::integer(static_cast<int64_t>(it->second.zsetScores.size()));
        });
    };

    r["ZREM"] = [](CommandContext& ctx) {
        if (ctx.size() < 3) return wrongArity("zrem");
        return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
            auto it = data.find(ctx[1]);
            if (it == data.end()) return resp::integer(0);
            if (it->second.type != DataType::ZSET) return resp::error(kWrongTypeError);
            int64_t removed = 0;
            for (size_t i = 2; i < ctx.size(); i++) {
                auto score = it->second.zsetScores.find(ctx[i]);
                if (score == it->second.zsetScores.end()) continue;
                eraseFromIndex(it->second, score->second, ctx[i]);
                it->second.zsetScores.erase(score);
                removed++;
            }
            if (it->second.zsetScores.empty()) data.erase(it);
            return resp::integer(removed);
        });
    };

    r["ZREMRANGEBYRANK"] = [](CommandContext& ctx) { return zremrangeGeneric(ctx, false); };
    r["ZREMRANGEBYSCORE"] = [](CommandContext& ctx) { return zremrangeGeneric(ctx, true); };

    r["ZINCRBY"] = [](CommandContext& ctx) {
        if (ctx.size() != 4) return wrongArity("zincrby");
        double delta = 0;
        if (!strutil::parseDouble(ctx[2], delta)) {
            return resp::error("ERR value is not a valid float");
        }
        return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
            purgeIfExpired(data, ctx[1], timeutil::nowMs());
            auto it = data.find(ctx[1]);
            if (it != data.end() && it->second.type != DataType::ZSET) {
                return resp::error(kWrongTypeError);
            }
            RedisValue& slot = data[ctx[1]];
            slot.type = DataType::ZSET;
            const std::string& member = ctx[3];
            double score = delta;
            auto existing = slot.zsetScores.find(member);
            if (existing != slot.zsetScores.end()) {
                score = existing->second + delta;
                eraseFromIndex(slot, existing->second, member);
            }
            slot.zsetScores[member] = score;
            slot.zsetByScore.insert({score, member});
            return resp::bulkString(strutil::formatScore(score));
        });
    };

    auto popExtreme = [](bool smallest) {
        return [smallest](CommandContext& ctx) -> std::string {
            if (ctx.size() < 2 || ctx.size() > 3) {
                return wrongArity(smallest ? "zpopmin" : "zpopmax");
            }
            int64_t want = 1;
            if (ctx.size() == 3 && !strutil::parseInt64(ctx[2], want)) {
                return resp::error("ERR value is not an integer or out of range");
            }
            return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
                auto it = data.find(ctx[1]);
                if (it == data.end() || it->second.isExpired(timeutil::nowMs())) {
                    return resp::emptyArray();
                }
                if (it->second.type != DataType::ZSET) return resp::error(kWrongTypeError);
                RedisValue& slot = it->second;

                OrderedMembers ordered = orderedByScore(slot);
                // The last element of the canonical order is the largest score,
                // and among tied scores the largest member -- which is what
                // ZPOPMAX has to hand back first.
                if (!smallest) std::reverse(ordered.begin(), ordered.end());
                const size_t take = std::min<size_t>(static_cast<size_t>(std::max<int64_t>(want, 0)),
                                                     ordered.size());
                std::vector<std::string> out;
                for (size_t i = 0; i < take; i++) {
                    out.push_back(resp::bulkString(ordered[i].second));
                    out.push_back(resp::bulkString(strutil::formatScore(ordered[i].first)));
                    auto score = slot.zsetScores.find(ordered[i].second);
                    if (score != slot.zsetScores.end()) slot.zsetScores.erase(score);
                }
                for (size_t i = 0; i < take; i++) {
                    eraseFromIndex(slot, ordered[i].first, ordered[i].second);
                }
                if (slot.zsetScores.empty()) data.erase(it);
                return resp::array(out);
            });
        };
    };
    r["ZPOPMIN"] = popExtreme(true);
    r["ZPOPMAX"] = popExtreme(false);
}

}  // namespace redis
