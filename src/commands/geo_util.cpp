#include "commands/geo_util.hpp"

#include <cmath>

#include "utils/strutil.hpp"

namespace redis::geo {

const char* const kInvalidCoordinateError =
    "ERR invalid longitude,latitude pair 13.361389,38.115556";

bool isValidLongitude(double lon) { return lon >= kLonMin && lon <= kLonMax; }
bool isValidLatitude(double lat) { return lat >= kLatMin && lat <= kLatMax; }
bool isValidCoordinate(double lon, double lat) {
    return isValidLongitude(lon) && isValidLatitude(lat);
}

bool encode(double lon, double lat, uint64_t& score) {
    if (!isValidCoordinate(lon, lat)) return false;

    const uint64_t limit = 1ULL << kStep;
    // Truncation, not rounding, matching Redis: a member's geohash is derived
    // from the cell it falls in, not the nearest cell centre.
    uint64_t latIndex = static_cast<uint64_t>(((lat - kLatMin) / (kLatMax - kLatMin)) * limit);
    uint64_t lonIndex = static_cast<uint64_t>(((lon - kLonMin) / (kLonMax - kLonMin)) * limit);
    if (latIndex >= limit) latIndex = limit - 1;
    if (lonIndex >= limit) lonIndex = limit - 1;

    // Longitude takes the more significant bit of each pair, so sorting by the
    // packed score orders by longitude and then latitude, which is what makes a
    // score-ordered scan usable as a spatial index.
    uint64_t packed = 0;
    for (int bit = kStep - 1; bit >= 0; bit--) {
        packed = (packed << 2) | (((lonIndex >> bit) & 1ULL) << 1) | ((latIndex >> bit) & 1ULL);
    }
    score = packed;
    return true;
}

bool decode(uint64_t score, double& lon, double& lat) {
    const uint64_t limit = 1ULL << kStep;
    uint64_t latIndex = 0, lonIndex = 0;
    for (int bit = kStep - 1; bit >= 0; bit--) {
        lonIndex = (lonIndex << 1) | ((score >> (2 * bit + 1)) & 1ULL);
        latIndex = (latIndex << 1) | ((score >> (2 * bit)) & 1ULL);
    }
    // The index is the cell's LOWER edge. Redis reports the cell CENTRE, so add
    // half a cell before mapping back, or every decoded coordinate is off by one
    // least-significant bit (~2.7e-6 deg lon, ~1.3e-6 deg lat).
    lon = kLonMin + ((static_cast<double>(lonIndex) + 0.5) / limit) * (kLonMax - kLonMin);
    lat = kLatMin + ((static_cast<double>(latIndex) + 0.5) / limit) * (kLatMax - kLatMin);
    return true;
}

double distanceMeters(double lon1, double lat1, double lon2, double lat2) {
    const double toRad = M_PI / 180.0;
    const double lat1r = lat1 * toRad;
    const double lat2r = lat2 * toRad;
    const double dLat = (lat2 - lat1) * toRad;
    const double dLon = (lon2 - lon1) * toRad;
    const double a = std::sin(dLat / 2) * std::sin(dLat / 2) +
                     std::cos(lat1r) * std::cos(lat2r) * std::sin(dLon / 2) * std::sin(dLon / 2);
    return 2.0 * kEarthRadiusMeters * std::atan2(std::sqrt(a), std::sqrt(1.0 - a));
}

bool unitToMeters(const std::string& unit, double value, double& meters) {
    const std::string u = strutil::toLower(unit);
    if (u == "m") { meters = value; return true; }
    if (u == "km") { meters = value * 1000.0; return true; }
    if (u == "mi") { meters = value * kMetersPerMile; return true; }
    if (u == "ft") { meters = value * kMetersPerFeet; return true; }
    return false;
}

}  // namespace redis::geo
