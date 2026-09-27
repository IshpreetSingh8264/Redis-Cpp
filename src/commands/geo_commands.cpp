/**
 * geo_commands.cpp -- GEOADD / GEOPOS / GEODIST / GEOHASH / GEOSEARCH.
 *
 * All five read and write one ordinary sorted set whose score is a geohash (see
 * geo_util.hpp). Nothing geo-specific is stored on the side, which is why
 * ZSCORE on a geo key returns a number that looks meaningless and is not.
 */
#include <algorithm>
#include <cmath>

#include "commands/geo_util.hpp"
#include "protocol/resp.hpp"
#include "store/data_store.hpp"
#include "types/handler.hpp"
#include "utils/strutil.hpp"
#include "utils/time.hpp"

namespace redis {

namespace {

const char* const kGeoIndexTypeError =
    "WRONGTYPE Operation against a key holding the wrong kind of value";

/// Redis's wording for a coordinate outside the valid range, with the offending
/// values filled in.
std::string invalidCoordinate(double lon, double lat) {
    return "ERR invalid longitude,latitude pair " + strutil::formatScore(lon) + "," +
           strutil::formatScore(lat);
}

/// One member's position, recovered from its score.
bool positionOf(const RedisValue& slot, const std::string& member, double& lon, double& lat) {
    auto it = slot.zsetScores.find(member);
    if (it == slot.zsetScores.end()) return false;
    return geo::decode(static_cast<uint64_t>(it->second), lon, lat);
}

}  // namespace

void registerGeoCommands(CommandRegistry& r) {
    r["GEOADD"] = [](CommandContext& ctx) {
        // GEOADD key [NX|XX] [CH] lon lat member [lon lat member ...]
        if (ctx.size() < 5) return wrongArity("geoadd");
        bool nx = false, xx = false, ch = false;
        size_t i = 2;
        for (; i < ctx.size(); i++) {
            const std::string opt = strutil::toUpper(ctx[i]);
            if (opt == "NX") nx = true;
            else if (opt == "XX") xx = true;
            else if (opt == "CH") ch = true;
            else break;
        }
        if (nx && xx) return resp::error("ERR XX and NX options at the same time are not compatible");

        std::vector<std::pair<double, double>> coordinates;
        std::vector<std::string> members;
        for (size_t j = i; j + 2 < ctx.size(); j += 3) {
            double lon = 0, lat = 0;
            if (!strutil::parseDouble(ctx[j], lon) || !strutil::parseDouble(ctx[j + 1], lat)) {
                return resp::error("ERR value is not a valid float");
            }
            if (!geo::isValidCoordinate(lon, lat)) {
                return resp::error(invalidCoordinate(lon, lat));
            }
            coordinates.emplace_back(lon, lat);
            members.push_back(ctx[j + 2]);
        }
        if (coordinates.empty()) return wrongArity("geoadd");

        return ctx.services->store->write([&](DataStore::Map& data) -> std::string {
            purgeIfExpired(data, ctx[1], timeutil::nowMs());
            auto slot = data.find(ctx[1]);
            if (slot != data.end() && slot->second.type != DataType::ZSET) {
                return resp::error(kGeoIndexTypeError);
            }
            RedisValue& value = data[ctx[1]];
            value.type = DataType::ZSET;

            int64_t added = 0, updated = 0;
            for (size_t k = 0; k < members.size(); k++) {
                uint64_t packed = 0;
                if (!geo::encode(coordinates[k].first, coordinates[k].second, packed)) {
                    return resp::error(
                        invalidCoordinate(coordinates[k].first, coordinates[k].second));
                }
                const double score = static_cast<double>(packed);
                auto existing = value.zsetScores.find(members[k]);

                if (existing == value.zsetScores.end()) {
                    if (xx) continue;
                    added++;
                } else {
                    if (nx) continue;
                    if (existing->second != score) updated++;
                    // Clear the old row whether or not the coordinates moved.
                    // Skipping this when they matched left a second row for one
                    // member, and GEOSEARCH walked the index and saw it twice.
                    auto range = value.zsetByScore.equal_range(existing->second);
                    for (auto sit = range.first; sit != range.second;) {
                        if (sit->second == members[k]) {
                            sit = value.zsetByScore.erase(sit);
                        } else {
                            ++sit;
                        }
                    }
                }
                value.zsetScores[members[k]] = score;
                value.zsetByScore.insert({score, members[k]});
            }
            return resp::integer(ch ? added + updated : added);
        });
    };

    r["GEOPOS"] = [](CommandContext& ctx) {
        // GEOPOS key member [member ...]. A key with no members is legal and
        // answers with an empty array; `GEOPOS` on its own is not.
        if (ctx.size() < 2) return wrongArity("geopos");
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            auto slot = data.find(ctx[1]);
            const bool live = slot != data.end() && !slot->second.isExpired(timeutil::nowMs());
            if (live && slot->second.type != DataType::ZSET) return resp::error(kGeoIndexTypeError);
            // A key that is not there is not a short answer and not an error:
            // Redis still answers once per member asked for, with a null each
            // time, so the reply length always matches the request.
            std::vector<std::string> out;
            for (size_t i = 2; i < ctx.size(); i++) {
                double lon = 0, lat = 0;
                if (!live || !positionOf(slot->second, ctx[i], lon, lat)) {
                    out.push_back(resp::nullArray());
                } else {
                    out.push_back(resp::array({resp::bulkString(strutil::formatScore(lon)),
                                               resp::bulkString(strutil::formatScore(lat))}));
                }
            }
            return resp::array(out);
        });
    };

    r["GEODIST"] = [](CommandContext& ctx) {
        if (ctx.size() < 4) return wrongArity("geodist");
        const std::string unit = ctx.size() >= 5 ? ctx[4] : "m";
        // Meters per one unit, so the result is a plain division. Scaling by
        // `unitToMeters(unit, value, out)` with value 0 would put a zero in the
        // denominator, and every distance would come back inf.
        double metersPerUnit = 1;
        if (!geo::unitToMeters(unit, 1.0, metersPerUnit)) {
            return resp::error("ERR unsupported unit provided. please use m, km, ft, mi");
        }

        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            auto slot = data.find(ctx[1]);
            if (slot == data.end()) return resp::nullBulk();
            if (slot->second.type != DataType::ZSET) return resp::error(kGeoIndexTypeError);
            double lon1 = 0, lat1 = 0, lon2 = 0, lat2 = 0;
            if (!positionOf(slot->second, ctx[2], lon1, lat1) ||
                !positionOf(slot->second, ctx[3], lon2, lat2)) {
                return resp::nullBulk();
            }
            const double meters = geo::distanceMeters(lon1, lat1, lon2, lat2) / metersPerUnit;
            return resp::bulkString(strutil::formatScore(meters));
        });
    };

    r["GEOHASH"] = [](CommandContext& ctx) {
        if (ctx.size() < 2) return wrongArity("geohash");
        return ctx.services->store->read([&](const DataStore::Map& data) -> std::string {
            auto slot = data.find(ctx[1]);
            if (slot == data.end()) return resp::nullArray();
            if (slot->second.type != DataType::ZSET) return resp::error(kGeoIndexTypeError);
            std::vector<std::string> out;
            for (size_t i = 2; i < ctx.size(); i++) {
                auto it = slot->second.zsetScores.find(ctx[i]);
                if (it == slot->second.zsetScores.end()) {
                    out.push_back(resp::nullBulk());
                    continue;
                }
                // A 52-bit geohash needs 11 base32 characters; the last one
                // uses only its top two bits.
                static const char* kBase32 = "0123456789bcdefghjkmnpqrstuvwxyz";
                const uint64_t bits = static_cast<uint64_t>(it->second);
                std::string text(11, '0');
                for (int i = 0; i < 11; i++) {
                    const int shift = 50 - 5 * i;
                    text[static_cast<size_t>(i)] = kBase32[(bits >> shift) & 0x1F];
                }
                out.push_back(resp::bulkString(text));
            }
            return resp::array(out);
        });
    };

    r["GEOSEARCH"] = [](CommandContext& ctx) {
        // GEOSEARCH key <FROMLONLAT|FROMMEMBER> lon lat|member
        //         [BYRADIUS radius unit] [BYBOX w h unit] [ASC|DESC]
        //         [COUNT n [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]
        if (ctx.size() < 5) return wrongArity("geosearch");

        size_t i = 2;
        const std::string centreKind = strutil::toUpper(ctx[i]);
        if (centreKind != "FROMLONLAT" && centreKind != "FROMMEMBER") {
            return resp::error("ERR syntax error");
        }
        i++;

        double centreLon = 0, centreLat = 0;
        if (centreKind == "FROMLONLAT") {
            if (i + 1 >= ctx.size()) return resp::error("ERR syntax error");
            if (!strutil::parseDouble(ctx[i], centreLon) ||
                !strutil::parseDouble(ctx[i + 1], centreLat)) {
                return resp::error("ERR value is not a valid float");
            }
            if (!geo::isValidCoordinate(centreLon, centreLat)) {
                return resp::error(invalidCoordinate(centreLon, centreLat));
            }
            i += 2;
        } else {
            const std::string member = ctx[i];
            bool found = false;
            ctx.services->store->read([&](const DataStore::Map& data) {
                auto slot = data.find(ctx[1]);
                if (slot == data.end() || slot->second.type != DataType::ZSET) return;
                found = positionOf(slot->second, member, centreLon, centreLat);
            });
            if (!found) return resp::error("ERR could not decode requested zset member");
            i += 1;
        }

        // Only a radius search is supported. BYBOX is not answered rather than
        // answered approximately.
        if (i >= ctx.size() || strutil::toUpper(ctx[i]) != "BYRADIUS") {
            return resp::error("ERR only BYRADIUS is supported by this build");
        }
        i++;
        if (i + 1 >= ctx.size()) return resp::error("ERR syntax error");
        double radius = 0;
        if (!strutil::parseDouble(ctx[i], radius)) {
            return resp::error("ERR value is not a valid float");
        }
        const std::string unit = ctx[i + 1];
        double radiusMeters = 0;
        if (!geo::unitToMeters(unit, radius, radiusMeters)) {
            return resp::error("ERR unsupported unit provided. please use m, km, ft, mi");
        }
        i += 2;

        bool ascending = true;
        int64_t want = -1;
        bool withCoord = false, withDist = false, withHash = false;
        for (; i < ctx.size(); i++) {
            const std::string opt = strutil::toUpper(ctx[i]);
            if (opt == "ASC") ascending = true;
            else if (opt == "DESC") ascending = false;
            else if (opt == "COUNT" && i + 1 < ctx.size()) {
                strutil::parseInt64(ctx[i + 1], want);
                i++;
            } else if (opt == "ANY") {
                // Accepted and ignored: results are already exact here.
            } else if (opt == "WITHCOORD") {
                withCoord = true;
            } else if (opt == "WITHDIST") {
                withDist = true;
            } else if (opt == "WITHHASH") {
                withHash = true;
            } else {
                return resp::error("ERR syntax error");
            }
        }
        if (withDist || withCoord || withHash) ascending = false;  // Redis always sorts far-to-near

        struct Hit {
            std::string member;
            double distanceMeters;
            double lon;
            double lat;
            uint64_t hash;
        };
        // Meters per unit of the search radius, so WITHDIST reports in the unit
        // the caller asked about.
        double metersPerUnit = 1;
        geo::unitToMeters(unit, 1.0, metersPerUnit);

        std::vector<Hit> hits;

        ctx.services->store->read([&](const DataStore::Map& data) {
            auto slot = data.find(ctx[1]);
            if (slot == data.end() || slot->second.type != DataType::ZSET) return;
            for (const auto& [score, member] : slot->second.zsetByScore) {
                uint64_t hash = static_cast<uint64_t>(score);
                double lon = 0, lat = 0;
                geo::decode(hash, lon, lat);
                const double d = geo::distanceMeters(centreLon, centreLat, lon, lat);
                if (d > radiusMeters) continue;
                hits.push_back(Hit{member, d, lon, lat, hash});
            }
        });

        std::sort(hits.begin(), hits.end(), [&](const Hit& a, const Hit& b) {
            return ascending ? a.distanceMeters < b.distanceMeters
                             : a.distanceMeters > b.distanceMeters;
        });
        if (want >= 0 && static_cast<size_t>(want) < hits.size()) {
            hits.resize(static_cast<size_t>(want));
        }

        std::vector<std::string> out;
        const bool hasExtras = withDist || withHash || withCoord;
        for (const auto& hit : hits) {
            std::vector<std::string> fields;
            fields.push_back(resp::bulkString(hit.member));
            if (withDist) {
                fields.push_back(
                    resp::bulkString(strutil::formatScore(hit.distanceMeters / metersPerUnit)));
            }
            if (withHash) {
                fields.push_back(resp::integer(static_cast<int64_t>(hit.hash)));
            }
            if (withCoord) {
                fields.push_back(resp::array({resp::bulkString(strutil::formatScore(hit.lon)),
                                              resp::bulkString(strutil::formatScore(hit.lat))}));
            }
            // Redis returns a flat array of member names when no WITH* option is
            // given, and one nested array per hit when one is. Wrapping
            // unconditionally made a plain GEOSEARCH answer [[member]].
            out.push_back(hasExtras ? resp::array(fields) : fields[0]);
        }
        return resp::array(out);
    };
}

}  // namespace redis
