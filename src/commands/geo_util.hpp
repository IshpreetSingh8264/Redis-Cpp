/**
 * geo_util.hpp -- geospatial maths.
 *
 * A geo index is a sorted set whose score is a 52-bit geohash: 26 bits of
 * longitude and 26 bits of latitude, interleaved longitude-first, most
 * significant bit first. That is the whole trick, and getting it wrong is
 * invisible until a stage asserts a score, which is why it lives in its own
 * file with its own test rather than inline in a handler.
 *
 * Verified against real Redis: GEOADD Sicily 13.361389 38.115556 Palermo
 * produces the score 3479099956230698, and so does this.
 */
#ifndef REDIS_COMMANDS_GEO_UTIL_HPP
#define REDIS_COMMANDS_GEO_UTIL_HPP

#include <cstdint>
#include <string>
#include <utility>

namespace redis::geo {

/// Latitude is clamped to Mercator's usable range, not to +/-90, because
/// Web Mercator is undefined past ~85.051129 and Redis refuses those.
inline constexpr double kLatMin = -85.05112878;
inline constexpr double kLatMax = 85.05112878;
inline constexpr double kLonMin = -180.0;
inline constexpr double kLonMax = 180.0;

/// Bits per axis in the packed score.
inline constexpr int kStep = 26;
/// Earth radius Redis uses, in metres.
inline constexpr double kEarthRadiusMeters = 6372797.560856;
/// 0.5 m / 0.5 km, the default GEODIST and GEOADD unit.
inline constexpr double kMetersPerMile = 1609.344;
inline constexpr double kMetersPerFeet = 0.3048;

bool isValidLongitude(double lon);
bool isValidLatitude(double lat);
bool isValidCoordinate(double lon, double lat);

/// Pack (lon, lat) into the 52-bit score. Returns false if the coordinates are
/// out of range rather than silently clamping them, so GEOADD can say so.
bool encode(double lon, double lat, uint64_t& score);

/// Unpack a score back to coordinates. This is lossy: 26 bits over 360 degrees
/// is about 5 micrometres of longitude, which is why GEOPOS on a real Redis
/// agrees with the input only to that precision.
bool decode(uint64_t score, double& lon, double& lat);

/// Great-circle distance in metres (haversine).
double distanceMeters(double lon1, double lat1, double lon2, double lat2);

/// "m", "km", "mi", "ft" to metres. Unknown units are rejected by the caller.
bool unitToMeters(const std::string& unit, double value, double& meters);

/// Redis's error text for a coordinate outside the valid range.
extern const char* const kInvalidCoordinateError;

}  // namespace redis::geo

#endif  // REDIS_COMMANDS_GEO_UTIL_HPP
