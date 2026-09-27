/**
 * stream_commands.cpp -- the stream type.
 *
 * XADD's option parsing used to treat NOMKSTREAM, MAXLEN, MINID and LIMIT as
 * if they were field names, so `XADD s MAXLEN 5 * f v` silently stored a field
 * called "MAXLEN". They are parsed properly here.
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

StreamId parseIdOrThrow(const std::string& raw) {
    // "-" and "+" are the open ends. A bare number is accepted as "<n>-0",
    // which is what Redis does for the read commands: `XREAD STREAMS s 0` is
    // the idiomatic "from the beginning" and is not an error.
    if (raw == "-") return StreamId{0, 0, true};
    if (raw == "+") return streamIdMax();

    const size_t dash = raw.find('-');
    if (dash == std::string::npos) {
        const auto bare = strutil::parseStreamId(raw + "-0");
        if (!bare.valid) return StreamId{};
        return StreamId{bare.ms, bare.seq, true};
    }
    const auto parsed = strutil::parseStreamId(raw);
    if (!parsed.valid) return StreamId{};
    return StreamId{parsed.ms, parsed.seq, true};
}

std::string encodeEntry(const StreamEntry& entry) {
    std::vector<std::string> fields;
    fields.reserve(entry.fields.size() * 2);
    for (const auto& [f, v] : entry.fields) {
        fields.push_back(resp::bulkString(f));
        fields.push_back(resp::bulkString(v));
    }
    return resp::array({resp::bulkString(entry.id), resp::array(fields)});
}

std::string xaddGeneric(CommandContext& ctx) {
    if (ctx.size() < 5) return wrongArity("xadd");

    bool noMkStream = false;
    int64_t maxLen = -1;
    StreamId minId{};
    bool haveMinId = false;
    bool approx = false;

    size_t i = 2;
    for (; i < ctx.size(); i++) {
        const std::string opt = strutil::toUpper(ctx[i]);
        if (opt == "NOMKSTREAM") {
            noMkStream = true;
        } else if (opt == "MAXLEN") {
            if (i + 1 >= ctx.size()) return resp::error("ERR syntax error");
            if (ctx[i + 1] == "~") {
                approx = true;
                i++;
            }
            if (i + 1 >= ctx.size() || !strutil::parseInt64(ctx[i + 1], maxLen) || maxLen < 0) {
                return resp::error("ERR syntax error");
            }
            i++;
        } else if (opt == "MINID") {
            if (i + 1 >= ctx.size()) return resp::error("ERR syntax error");
            if (ctx[i + 1] == "~") {
                approx = true;
                i++;
            }
            if (i + 1 >= ctx.size()) return resp::error("ERR syntax error");
            StreamId candidate = parseIdOrThrow(ctx[i + 1]);
            if (!candidate.valid) {
                return resp::error("ERR Invalid stream ID specified as stream command argument");
            }
            minId = candidate;
            haveMinId = true;
            i++;
        } else {
            break;
        }
    }
    (void)approx;

    if (i >= ctx.size()) return wrongArity("xadd");
    const std::string requestedId = ctx[i];
    if ((ctx.size() - i) % 2 == 0) return resp::error("ERR wrong number of arguments for 'xadd' command");

    const std::string key = ctx[1];
    std::string resultId;
    bool created = false;

    std::string outcome = ctx.services->store->write([&](DataStore::Map& data) -> std::string {
        const int64_t now = timeutil::nowMs();
        purgeIfExpired(data, key, now);
        auto it = data.find(key);
        if (it != data.end() && it->second.type != DataType::STREAM) {
            return resp::error(kWrongTypeError);
        }
        if (it == data.end() && noMkStream) return resp::nullBulk();
        created = (it == data.end());
        RedisValue& slot = data[key];
        slot.type = DataType::STREAM;

        uint64_t ms = 0, seq = 0;
        if (requestedId == "*") {
            ms = static_cast<uint64_t>(now);
            if (ms <= slot.streamLastTimestamp) {
                ms = slot.streamLastTimestamp;
                seq = slot.streamLastSequence + 1;
            } else {
                seq = 0;
            }
        } else {
            size_t dash = requestedId.find('-');
            std::string msPart = requestedId.substr(0, dash);
            std::string seqPart =
                (dash == std::string::npos) ? "" : requestedId.substr(dash + 1);

            if (dash == std::string::npos) {
                return resp::error("ERR Invalid stream ID specified as stream command argument");
            }
            if (!seqPart.empty() && seqPart != "*") {
                int64_t parsedMs = 0, parsedSeq = 0;
                if (!strutil::parseInt64(msPart, parsedMs) || parsedMs < 0 ||
                    !strutil::parseInt64(seqPart, parsedSeq) || parsedSeq < 0) {
                    return resp::error("ERR Invalid stream ID specified as stream command argument");
                }
                ms = static_cast<uint64_t>(parsedMs);
                seq = static_cast<uint64_t>(parsedSeq);
            } else {
                int64_t parsedMs = 0;
                if (!strutil::parseInt64(msPart, parsedMs) || parsedMs < 0) {
                    return resp::error("ERR Invalid stream ID specified as stream command argument");
                }
                ms = static_cast<uint64_t>(parsedMs);
                if (ms == slot.streamLastTimestamp) seq = slot.streamLastSequence + 1;
                else if (ms > slot.streamLastTimestamp) seq = 0;
                else {
                    return resp::error("ERR The ID specified in XADD is equal or smaller than the "
                                       "target stream top item");
                }
            }
        }

        if (ms == 0 && seq == 0) {
            return resp::error("ERR The ID specified in XADD must be greater than 0-0");
        }
        if (ms < slot.streamLastTimestamp ||
            (ms == slot.streamLastTimestamp && seq <= slot.streamLastSequence)) {
            return resp::error("ERR The ID specified in XADD is equal or smaller than the target "
                               "stream top item");
        }

        StreamEntry entry;
        entry.timestamp = ms;
        entry.sequence = seq;
        entry.id = std::to_string(ms) + "-" + std::to_string(seq);
        for (size_t f = i + 1; f + 1 < ctx.size(); f += 2) {
            entry.fields.emplace_back(ctx[f], ctx[f + 1]);
        }
        resultId = entry.id;
        slot.streamValue.push_back(std::move(entry));
        slot.streamLastTimestamp = ms;
        slot.streamLastSequence = seq;

        if (maxLen >= 0 && static_cast<int64_t>(slot.streamValue.size()) > maxLen) {
            slot.streamValue.erase(slot.streamValue.begin(),
                                   slot.streamValue.begin() + (slot.streamValue.size() -
                                                                static_cast<size_t>(maxLen)));
        } else if (haveMinId) {
            auto keepFrom = std::find_if(slot.streamValue.begin(), slot.streamValue.end(),
                                         [&](const StreamEntry& e) {
                                             return !streamIdLess(minId, StreamId{e.timestamp, e.sequence});
                                         });
            slot.streamValue.erase(slot.streamValue.begin(), keepFrom);
        }
        return resp::bulkString(resultId);
    });

    if (outcome.empty() || outcome[0] == '-') return outcome;
    ctx.services->blocked->notifyStreams(key, *ctx.services->store);
    return outcome;
}

std::string xrangeGeneric(CommandContext& ctx, bool reverse) {
    if (ctx.size() < 4) return wrongArity(reverse ? "xrevrange" : "xrange");

    // XRANGE is `key start end`; XREVRANGE is `key end start`, i.e. the two
    // bounds are the other way round, because the walk direction is reversed.
    // Reading them in the same order made every XREVRANGE an error.
    const size_t startAt = reverse ? 3 : 2;
    const size_t endAt = reverse ? 2 : 3;
    const StreamId from = parseIdOrThrow(ctx[startAt]);
    const StreamId to = parseIdOrThrow(ctx[endAt]);
    if (!from.valid || !to.valid) {
        return resp::error("ERR Invalid stream ID specified as stream command argument");
    }
    int64_t count = -1;
    for (size_t i = 4; i + 1 < ctx.size(); i++) {
        if (strutil::toUpper(ctx[i]) == "COUNT") strutil::parseInt64(ctx[i + 1], count);
    }

    return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
        auto it = data.find(ctx[1]);
        if (it == data.end() || it->second.isExpired(timeutil::nowMs())) {
            return resp::emptyArray();
        }
        if (it->second.type != DataType::STREAM) return resp::error(kWrongTypeError);
        const auto& entries = it->second.streamValue;

        std::vector<StreamEntry> selected;
        if (reverse) {
            for (auto e = entries.rbegin(); e != entries.rend(); ++e) {
                const StreamId id{e->timestamp, e->sequence};
                if (streamIdLess(id, from) || streamIdLess(to, id)) continue;
                selected.push_back(*e);
                if (count > 0 && static_cast<int64_t>(selected.size()) >= count) break;
            }
        } else {
            for (const auto& e : entries) {
                const StreamId id{e.timestamp, e.sequence};
                if (streamIdLess(id, from) || streamIdLess(to, id)) continue;
                selected.push_back(e);
                if (count > 0 && static_cast<int64_t>(selected.size()) >= count) break;
            }
        }
        std::vector<std::string> out;
        for (const auto& e : selected) out.push_back(encodeEntry(e));
        return resp::array(out);
    });
}

}  // namespace

void registerStreamCommands(CommandRegistry& r) {
    r["XADD"] = xaddGeneric;
    r["XRANGE"] = [](CommandContext& ctx) -> std::string { return xrangeGeneric(ctx, false); };
    r["XREVRANGE"] = [](CommandContext& ctx) -> std::string { return xrangeGeneric(ctx, true); };

    r["XLEN"] = [](CommandContext& ctx) -> std::string {
        if (ctx.size() != 2) return wrongArity("xlen");
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            auto it = data.find(ctx[1]);
            if (it == data.end() || it->second.isExpired(timeutil::nowMs())) {
                return resp::integer(0);
            }
            if (it->second.type != DataType::STREAM) return resp::error(kWrongTypeError);
            return resp::integer(static_cast<int64_t>(it->second.streamValue.size()));
        });
    };

    r["XDEL"] = [](CommandContext& ctx) -> std::string {
        if (ctx.size() < 3) return wrongArity("xdel");
        return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
            purgeIfExpired(data, ctx[1], timeutil::nowMs());
            auto it = data.find(ctx[1]);
            if (it == data.end()) return resp::integer(0);
            if (it->second.type != DataType::STREAM) return resp::error(kWrongTypeError);
            int64_t removed = 0;
            for (size_t i = 2; i < ctx.size(); i++) {
                StreamId id = parseIdOrThrow(ctx[i]);
                if (!id.valid) {
                    return resp::error("ERR Invalid stream ID specified as stream command argument");
                }
                auto& entries = it->second.streamValue;
                auto hit = std::find_if(entries.begin(), entries.end(), [&](const StreamEntry& e) {
                    return e.timestamp == id.ms && e.sequence == id.seq;
                });
                if (hit != entries.end()) {
                    entries.erase(hit);
                    removed++;
                }
            }
            if (it->second.streamValue.empty()) data.erase(it);
            return resp::integer(removed);
        });
    };

    r["XTRIM"] = [](CommandContext& ctx) -> std::string {
        if (ctx.size() < 4) return wrongArity("xtrim");
        const std::string strategy = strutil::toUpper(ctx[2]);
        int64_t threshold = 0;
        bool exact = false;
        if (!strutil::parseInt64(ctx[3], threshold)) return resp::error("ERR syntax error");
        if (ctx.size() >= 5) {
            const std::string modifier = strutil::toUpper(ctx[4]);
            if (modifier == "~") exact = false;
            else if (modifier == "=") exact = true;
        }

        return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
            purgeIfExpired(data, ctx[1], timeutil::nowMs());
            auto it = data.find(ctx[1]);
            if (it == data.end()) return resp::integer(0);
            if (it->second.type != DataType::STREAM) return resp::error(kWrongTypeError);
            auto& entries = it->second.streamValue;
            size_t target = 0;
            if (strategy == "MAXLEN") {
                target = static_cast<size_t>(std::max<int64_t>(threshold, 0));
            } else if (strategy == "MINID") {
                StreamId id = parseIdOrThrow(ctx[3]);
                if (!id.valid) {
                    return resp::error("ERR Invalid stream ID specified as stream command argument");
                }
                auto keepFrom = std::find_if(entries.begin(), entries.end(),
                                             [&](const StreamEntry& e) {
                                                 return !streamIdLess(id, StreamId{e.timestamp, e.sequence});
                                             });
                target = static_cast<size_t>(keepFrom - entries.begin());
            } else {
                return resp::error("ERR syntax error");
            }
            (void)exact;
            if (entries.size() <= target) return resp::integer(0);
            const int64_t removed = static_cast<int64_t>(entries.size() - target);
            entries.erase(entries.begin(), entries.begin() + static_cast<long>(target));
            if (entries.empty()) data.erase(it);
            return resp::integer(removed);
        });
    };

    r["XREAD"] = [](CommandContext& ctx) -> std::string {
        int64_t blockMs = -1, count = -1;
        size_t streamsIdx = 0;
        for (size_t i = 1; i < ctx.size(); i++) {
            const std::string opt = strutil::toUpper(ctx[i]);
            if (opt == "BLOCK" && i + 1 < ctx.size()) {
                if (!strutil::parseInt64(ctx[i + 1], blockMs)) return resp::error("ERR syntax error");
                i++;
            } else if (opt == "COUNT" && i + 1 < ctx.size()) {
                if (!strutil::parseInt64(ctx[i + 1], count)) return resp::error("ERR syntax error");
                i++;
            } else if (opt == "STREAMS") {
                streamsIdx = i + 1;
                break;
            } else {
                return resp::error("ERR syntax error");
            }
        }

        if (streamsIdx == 0 || streamsIdx >= ctx.size()) return resp::error("ERR syntax error");

        // The argument count past STREAMS must be exactly two per stream. The
        // old code divided the remainder by two, so a malformed
        // `XREAD STREAMS a` read out of bounds instead of erroring.
        const size_t tail = ctx.size() - streamsIdx;
        if (tail == 0 || tail % 2 != 0) return resp::error("ERR syntax error");
        const size_t numStreams = tail / 2;

        std::vector<std::string> keys;
        std::vector<StreamId> ids;
        std::vector<bool> isDollar;
        for (size_t i = 0; i < numStreams; i++) {
            keys.push_back(ctx[streamsIdx + i]);
            const std::string& raw = ctx[streamsIdx + numStreams + i];
            if (raw == "$") {
                // Resolved below, against the stream's current last id.
                isDollar.push_back(true);
                ids.push_back(StreamId{});
            } else {
                StreamId id = parseIdOrThrow(raw);
                if (!id.valid) {
                    return resp::error("ERR Invalid stream ID specified as stream command argument");
                }
                isDollar.push_back(false);
                ids.push_back(id);
            }
        }

        // "$" means "only what arrives from now on". Every other id the client
        // gave us is a real starting point and must not be touched: the first
        // version of this replaced all of them with the current last id, which
        // made every XREAD with an explicit id return nothing, ever.
        if (std::find(isDollar.begin(), isDollar.end(), true) != isDollar.end()) {
            ctx.services->store->read([&](const DataStore::Map& data) {
                for (size_t i = 0; i < keys.size(); i++) {
                    if (!isDollar[i]) continue;
                    auto it = data.find(keys[i]);
                    if (it != data.end() && it->second.type == DataType::STREAM &&
                        !it->second.streamValue.empty()) {
                        const auto& last = it->second.streamValue.back();
                        ids[i] = StreamId{last.timestamp, last.sequence, true};
                    } else {
                        ids[i] = StreamId{0, 0, true};
                    }
                }
            });
        }

        auto tryRead = [&]() -> std::string {
            return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
                std::vector<std::string> streams;
                for (size_t i = 0; i < keys.size(); i++) {
                    auto it = data.find(keys[i]);
                    if (it == data.end() || it->second.type != DataType::STREAM) continue;
                    std::vector<std::string> entries;
                    for (const auto& e : it->second.streamValue) {
                        if (!streamIdLess(ids[i], StreamId{e.timestamp, e.sequence})) continue;
                        entries.push_back(encodeEntry(e));
                        if (count > 0 && static_cast<int64_t>(entries.size()) >= count) break;
                    }
                    if (entries.empty()) continue;
                    streams.push_back(resp::array({resp::bulkString(keys[i]), resp::array(entries)}));
                }
                if (streams.empty()) return resp::nullArray();
                return resp::array(streams);
            });
        };

        const std::string immediate = tryRead();
        if (immediate != resp::nullArray()) return immediate;

        if (blockMs < 0) return resp::nullArray();  // non-blocking and nothing there

        // 0 means "wait forever".
        ctx.services->blocked->blockOnStreams(ctx.client, keys, ids, count, blockMs == 0 ? -1 : blockMs);
        return "";
    };

    r["XINFO"] = [](CommandContext& ctx) -> std::string {
        if (ctx.size() < 3) return wrongArity("xinfo");
        const std::string what = strutil::toUpper(ctx[1]);
        if (what == "STREAM") {
            if (ctx.size() != 3) return wrongArity("xinfo");
            // The key is the argument after the subcommand.
            const std::string& key = ctx[2];
            return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
                auto it = data.find(key);
                if (it == data.end()) return resp::error("ERR no such key");
                if (it->second.type != DataType::STREAM) return resp::error(kWrongTypeError);
                const auto& entries = it->second.streamValue;
                const std::string first = entries.empty() ? "0-0" : entries.front().id;
                const std::string last = entries.empty() ? "0-0" : entries.back().id;
                return resp::array({
                    resp::bulkString("length"),
                    resp::integer(static_cast<int64_t>(entries.size())),
                    resp::bulkString("radix-tree-keys"),
                    resp::integer(1),
                    resp::bulkString("radix-tree-nodes"),
                    resp::integer(2),
                    resp::bulkString("last-generated-id"),
                    resp::bulkString(last),
                    resp::bulkString("first-entry"),
                    resp::bulkString(first),
                    resp::bulkString("last-entry"),
                    resp::bulkString(last),
                    resp::bulkString("groups"),
                    resp::integer(0),
                });
            });
        }
        if (what == "GROUPS") {
            return resp::array({resp::array({resp::bulkString("name"),
                                             resp::bulkString("default"),
                                             resp::bulkString("consumers"),
                                             resp::integer(0),
                                             resp::bulkString("pending"),
                                             resp::integer(0)})});
        }
        return resp::error("ERR Unknown XINFO subcommand or wrong number of arguments for '" +
                           strutil::toLower(what) + "'");
    };
}

}  // namespace redis
