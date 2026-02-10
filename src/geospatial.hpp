/**
 * =============================================================================
 *                          GEOSPATIAL OPERATIONS
 * =============================================================================
 * 
 * Paaji eh file hai geo commands di - location location location!
 * (Bro this is for geo commands - location based stuff!)
 * 
 * Redis geo commands internally sorted sets use karde ne
 * (Redis geo commands internally use sorted sets)
 * 
 * Coordinates nu geohash mein convert karke score bana dinde ne
 * (Convert coordinates to geohash and use as score)
 * 
 * Jiven Zomato nearby restaurants dikhata hai, ohi karte hain
 * (Like Zomato shows nearby restaurants, we do the same)
 * 
 * =============================================================================
 */

#pragma once

#include "common.hpp"
#include "data_store.hpp"
#include <cmath>

namespace Redis {

// ============================================================================
// GEOSPATIAL CONSTANTS
// Earth di info - geography time
// (Earth's info - geography class flashbacks)
// ============================================================================

// Earth's radius in meters
// Dharti di radius - maths ch kaam aayegi
// (Earth's radius - useful for distance calculations)
constexpr double EARTH_RADIUS_METERS = 6372797.560856;

// Latitude/Longitude limits
// Lat/Long ki limits - valid range
// (Lat/Long limits - valid coordinate range)
constexpr double MIN_LATITUDE = -85.05112878;
constexpr double MAX_LATITUDE = 85.05112878;
constexpr double MIN_LONGITUDE = -180.0;
constexpr double MAX_LONGITUDE = 180.0;

// Geohash step - bits for encoding
constexpr int GEOHASH_STEP = 26;

// ============================================================================
// GEOSPATIAL UTILITIES
// Geo related helper functions - maths intensive
// (Geo helper functions - lots of math, sorry!)
// ============================================================================

/**
 * @class GeoUtil
 * @brief Utility functions for geospatial operations
 * 
 * Maths functions for:
 * - Coordinate validation
 * - Geohash encoding/decoding
 * - Distance calculation (Haversine formula)
 * - Coordinate conversion
 */
class GeoUtil {
public:
    // ========================================================================
    // COORDINATE VALIDATION
    // Coordinates valid hain ki nahi
    // (Are coordinates valid?)
    // ========================================================================
    
    /**
     * Validate latitude
     * Latitude check karo - -90 to 90 honi chahiye (roughly)
     * (Validate latitude - should be in valid range)
     */
    static bool isValidLatitude(double lat) {
        return lat >= MIN_LATITUDE && lat <= MAX_LATITUDE;
    }
    
    /**
     * Validate longitude
     * Longitude check karo - -180 to 180 honi chahiye
     * (Validate longitude - should be in valid range)
     */
    static bool isValidLongitude(double lon) {
        return lon >= MIN_LONGITUDE && lon <= MAX_LONGITUDE;
    }
    
    /**
     * Validate coordinates
     * Dono check karo - full validation
     * (Validate both - complete check)
     */
    static bool isValidCoordinate(double lon, double lat) {
        return isValidLongitude(lon) && isValidLatitude(lat);
    }
    
    // ========================================================================
    // GEOHASH ENCODING/DECODING
    // Geohash magic - coordinates to single number
    // (Geohash magic - convert coordinates to single number)
    // ========================================================================
    
    /**
     * Encode longitude and latitude to geohash
     * Coordinates nu ek number mein badlo
     * (Convert coordinates to single number - compression magic)
     * 
     * Eh number sorted set mein score ban jaanda hai
     * (This number becomes the score in sorted set)
     */
    static uint64_t encodeGeohash(double lon, double lat) {
        // Normalize coordinates to 0-1 range
        // Coordinates nu 0-1 range ch lao
        // (Normalize coordinates to 0-1 range)
        double lonNorm = (lon - MIN_LONGITUDE) / (MAX_LONGITUDE - MIN_LONGITUDE);
        double latNorm = (lat - MIN_LATITUDE) / (MAX_LATITUDE - MIN_LATITUDE);
        
        // Convert to integer
        // Integer mein badlo
        // (Convert to integer)
        uint64_t lonBits = static_cast<uint64_t>(lonNorm * (1ULL << 32));
        uint64_t latBits = static_cast<uint64_t>(latNorm * (1ULL << 32));
        
        // Interleave bits
        // Bits nu mix karo - zigzag pattern
        // (Interleave bits - zigzag pattern)
        return interleave(lonBits, latBits);
    }
    
    /**
     * Decode geohash to longitude and latitude
     * Number se wapas coordinates nikalo
     * (Extract coordinates from number - reverse magic)
     */
    static void decodeGeohash(uint64_t hash, double& lon, double& lat) {
        // De-interleave bits
        // Bits nu wapas separate karo
        // (De-interleave bits - undo the zigzag)
        uint64_t lonBits, latBits;
        deinterleave(hash, lonBits, latBits);
        
        // Convert back to coordinates
        // Wapas coordinates mein badlo
        // (Convert back to coordinates)
        double lonNorm = static_cast<double>(lonBits) / (1ULL << 32);
        double latNorm = static_cast<double>(latBits) / (1ULL << 32);
        
        lon = lonNorm * (MAX_LONGITUDE - MIN_LONGITUDE) + MIN_LONGITUDE;
        lat = latNorm * (MAX_LATITUDE - MIN_LATITUDE) + MIN_LATITUDE;
    }
    
    /**
     * Convert score (double) to geohash (uint64)
     * Score se geohash banao
     * (Score to geohash conversion)
     */
    static uint64_t scoreToGeohash(double score) {
        // Redis stores geohash as the score
        // Score geohash hi hai basically
        // (Score is basically the geohash)
        return static_cast<uint64_t>(score);
    }
    
    /**
     * Convert geohash to score
     * Geohash se score banao
     * (Geohash to score conversion)
     */
    static double geohashToScore(uint64_t hash) {
        return static_cast<double>(hash);
    }
    
    // ========================================================================
    // DISTANCE CALCULATION
    // Distance calculate karo - kitna door hai
    // (Calculate distance - how far is it)
    // ========================================================================
    
    /**
     * Calculate distance using Haversine formula
     * Haversine formula - dharti gol hai isliye special formula
     * (Haversine formula - earth is round so special formula needed)
     * 
     * @return Distance in meters
     */
    static double haversineDistance(double lon1, double lat1, double lon2, double lat2) {
        // Convert to radians
        // Radians mein badlo - trigonometry ka kaam
        // (Convert to radians - trigonometry requires it)
        double lat1Rad = degToRad(lat1);
        double lat2Rad = degToRad(lat2);
        double deltaLat = degToRad(lat2 - lat1);
        double deltaLon = degToRad(lon2 - lon1);
        
        // Haversine formula
        // Famous formula - 10th class yaad hai?
        // (Famous formula - remember 10th class math?)
        double a = std::sin(deltaLat / 2) * std::sin(deltaLat / 2) +
                   std::cos(lat1Rad) * std::cos(lat2Rad) *
                   std::sin(deltaLon / 2) * std::sin(deltaLon / 2);
        
        double c = 2 * std::atan2(std::sqrt(a), std::sqrt(1 - a));
        
        return EARTH_RADIUS_METERS * c;
    }
    
    /**
     * Convert distance to different units
     * Distance nu different units mein badlo
     * (Convert distance to different units - flexibility)
     */
    static double convertDistance(double meters, const std::string& unit) {
        std::string u = unit;
        std::transform(u.begin(), u.end(), u.begin(), ::tolower);
        
        if (u == "m" || u.empty()) {
            return meters;
        } else if (u == "km") {
            return meters / 1000.0;
        } else if (u == "mi") {
            return meters / 1609.344;  // Miles
        } else if (u == "ft") {
            return meters / 0.3048;    // Feet
        }
        
        return meters;  // Default to meters
    }
    
    /**
     * Convert distance from unit to meters
     * Unit se meters mein badlo
     * (Convert from unit to meters)
     */
    static double toMeters(double distance, const std::string& unit) {
        std::string u = unit;
        std::transform(u.begin(), u.end(), u.begin(), ::tolower);
        
        if (u == "m" || u.empty()) {
            return distance;
        } else if (u == "km") {
            return distance * 1000.0;
        } else if (u == "mi") {
            return distance * 1609.344;
        } else if (u == "ft") {
            return distance * 0.3048;
        }
        
        return distance;
    }

private:
    // ========================================================================
    // PRIVATE HELPER FUNCTIONS
    // Internal maths - complicated stuff
    // (Internal math functions - the nerdy stuff)
    // ========================================================================
    
    /**
     * Degrees to radians
     * Degree se radian mein badlo
     * (Convert degrees to radians)
     */
    static double degToRad(double deg) {
        return deg * M_PI / 180.0;
    }
    
    /**
     * Radians to degrees
     * Radian se degree mein badlo
     * (Convert radians to degrees)
     */
    static double radToDeg(double rad) {
        return rad * 180.0 / M_PI;
    }
    
    /**
     * Interleave bits of two numbers
     * Do numbers ke bits mix karo
     * (Interleave bits of two numbers - bit manipulation magic)
     * 
     * Example: A = 1010, B = 0011 -> AB = 01001101
     */
    static uint64_t interleave(uint64_t x, uint64_t y) {
        uint64_t result = 0;
        for (int i = 0; i < 32; i++) {
            result |= ((x >> (31 - i)) & 1ULL) << (63 - 2 * i);
            result |= ((y >> (31 - i)) & 1ULL) << (62 - 2 * i);
        }
        return result;
    }
    
    /**
     * De-interleave bits
     * Mixed bits wapas separate karo
     * (De-interleave bits - undo the mixing)
     */
    static void deinterleave(uint64_t hash, uint64_t& x, uint64_t& y) {
        x = 0;
        y = 0;
        for (int i = 0; i < 32; i++) {
            x |= ((hash >> (63 - 2 * i)) & 1ULL) << (31 - i);
            y |= ((hash >> (62 - 2 * i)) & 1ULL) << (31 - i);
        }
    }
};

// ============================================================================
// GEO COMMAND HELPERS
// Geo commands lai helper functions
// (Helper functions for geo commands)
// ============================================================================

/**
 * @struct GeoPoint
 * @brief Represents a geographic point
 * 
 * Ek jagah di info - coordinates te naam
 * (Info about a place - coordinates and name)
 */
struct GeoPoint {
    std::string member;     // Name of the location
    double longitude;       // Longitude (-180 to 180)
    double latitude;        // Latitude (-85.05 to 85.05)
    double distance;        // Distance from reference (for radius search)
    uint64_t hash;          // Geohash value
    
    GeoPoint() : longitude(0), latitude(0), distance(0), hash(0) {}
    
    GeoPoint(const std::string& m, double lon, double lat)
        : member(m), longitude(lon), latitude(lat), distance(0) {
        hash = GeoUtil::encodeGeohash(lon, lat);
    }
};

/**
 * @class GeoCommands
 * @brief Implements geospatial commands
 * 
 * GEOADD, GEOPOS, GEODIST, GEORADIUS implement karna
 * (Implement GEOADD, GEOPOS, GEODIST, GEORADIUS)
 */
class GeoCommands {
private:
    DataStore& store_;

public:
    GeoCommands(DataStore& store) : store_(store) {}
    
    /**
     * GEOADD - Add geo locations
     * Locations add karo - GPS coordinates store
     * (Add locations - store GPS coordinates)
     * 
     * Internally sorted set mein store hota hai
     * (Internally stored in sorted set)
     */
    std::string geoAdd(const std::string& key, 
                       const std::vector<std::tuple<double, double, std::string>>& locations,
                       bool nx = false, bool xx = false, bool ch = false) {
        
        std::vector<std::pair<double, std::string>> scoreMembers;
        
        for (const auto& [lon, lat, member] : locations) {
            // Validate coordinates
            // Coordinates valid hain ki nahi
            // (Validate coordinates - no fake locations please!)
            if (!GeoUtil::isValidCoordinate(lon, lat)) {
                return respError("ERR invalid longitude,latitude pair " + 
                                std::to_string(lon) + "," + std::to_string(lat));
            }
            
            // Encode to geohash
            // Geohash banao - single number mein convert
            // (Create geohash - convert to single number)
            uint64_t hash = GeoUtil::encodeGeohash(lon, lat);
            double score = GeoUtil::geohashToScore(hash);
            
            scoreMembers.emplace_back(score, member);
        }
        
        int64_t added = store_.zadd(key, scoreMembers, nx, xx, false, false, ch);
        return respInteger(added);
    }
    
    /**
     * GEOPOS - Get positions
     * Positions lo - coordinates wapas chahiye
     * (Get positions - retrieve stored coordinates)
     */
    std::string geoPos(const std::string& key, const std::vector<std::string>& members) {
        std::vector<RespValue> results;
        
        for (const auto& member : members) {
            auto score = store_.zscore(key, member);
            
            if (score) {
                // Decode geohash to coordinates
                // Geohash se wapas coordinates nikalo
                // (Decode geohash to coordinates)
                uint64_t hash = GeoUtil::scoreToGeohash(*score);
                double lon, lat;
                GeoUtil::decodeGeohash(hash, lon, lat);
                
                // Format coordinates
                // Coordinates format karo - proper decimal places
                // (Format coordinates - proper decimal places)
                std::ostringstream lonSs, latSs;
                lonSs << std::setprecision(17) << lon;
                latSs << std::setprecision(17) << lat;
                
                std::vector<RespValue> coord;
                coord.push_back(RespValue::bulkString(lonSs.str()));
                coord.push_back(RespValue::bulkString(latSs.str()));
                results.push_back(RespValue::array(coord));
            } else {
                results.push_back(RespValue::null());
            }
        }
        
        return RespValue::array(results).serialize();
    }
    
    /**
     * GEODIST - Calculate distance between two members
     * Do points ke beech distance nikalo
     * (Calculate distance between two points - how far apart)
     */
    std::string geoDist(const std::string& key, 
                        const std::string& member1, 
                        const std::string& member2,
                        const std::string& unit = "m") {
        
        auto score1 = store_.zscore(key, member1);
        auto score2 = store_.zscore(key, member2);
        
        if (!score1 || !score2) {
            return respNull();
        }
        
        // Decode both coordinates
        // Dono coordinates nikalo
        // (Decode both coordinates)
        double lon1, lat1, lon2, lat2;
        GeoUtil::decodeGeohash(GeoUtil::scoreToGeohash(*score1), lon1, lat1);
        GeoUtil::decodeGeohash(GeoUtil::scoreToGeohash(*score2), lon2, lat2);
        
        // Calculate distance
        // Distance calculate karo - Haversine formula
        // (Calculate distance - using Haversine formula)
        double distanceMeters = GeoUtil::haversineDistance(lon1, lat1, lon2, lat2);
        double distance = GeoUtil::convertDistance(distanceMeters, unit);
        
        std::ostringstream ss;
        ss << std::setprecision(17) << distance;
        return respBulkString(ss.str());
    }
    
    /**
     * GEORADIUS/GEOSEARCH - Search within radius
     * Radius mein search karo - nearby khojo
     * (Search within radius - find nearby locations)
     * 
     * Jiven Swiggy nearby restaurants dikhata hai
     * (Like Swiggy shows nearby restaurants)
     */
    std::string geoRadius(const std::string& key,
                          double lon, double lat,
                          double radius, const std::string& unit,
                          bool withCoord = false,
                          bool withDist = false,
                          bool withHash = false,
                          int count = -1,
                          bool asc = true) {
        
        // Convert radius to meters
        // Radius nu meters mein badlo
        // (Convert radius to meters)
        double radiusMeters = GeoUtil::toMeters(radius, unit);
        
        // Get all members from sorted set
        // Saare members lo sorted set se
        // (Get all members from sorted set)
        auto allMembers = store_.zrange(key, 0, -1, true);
        
        // Filter by distance
        // Distance se filter karo
        // (Filter by distance - only nearby ones)
        std::vector<GeoPoint> results;
        
        for (const auto& [member, score] : allMembers) {
            double memberLon, memberLat;
            GeoUtil::decodeGeohash(GeoUtil::scoreToGeohash(score), memberLon, memberLat);
            
            double distance = GeoUtil::haversineDistance(lon, lat, memberLon, memberLat);
            
            if (distance <= radiusMeters) {
                GeoPoint point;
                point.member = member;
                point.longitude = memberLon;
                point.latitude = memberLat;
                point.distance = GeoUtil::convertDistance(distance, unit);
                point.hash = GeoUtil::scoreToGeohash(score);
                results.push_back(point);
            }
        }
        
        // Sort by distance
        // Distance se sort karo
        // (Sort by distance - nearest first or farthest first)
        if (asc) {
            std::sort(results.begin(), results.end(),
                     [](const GeoPoint& a, const GeoPoint& b) {
                         return a.distance < b.distance;
                     });
        } else {
            std::sort(results.begin(), results.end(),
                     [](const GeoPoint& a, const GeoPoint& b) {
                         return a.distance > b.distance;
                     });
        }
        
        // Apply count limit
        // Count limit lagao
        // (Apply count limit - only return N results)
        if (count > 0 && results.size() > static_cast<size_t>(count)) {
            results.resize(count);
        }
        
        // Build response
        // Response banao
        // (Build response - format results)
        std::vector<RespValue> arr;
        
        for (const auto& point : results) {
            if (withCoord || withDist || withHash) {
                // Complex response with additional info
                std::vector<RespValue> entry;
                entry.push_back(RespValue::bulkString(point.member));
                
                if (withDist) {
                    std::ostringstream ss;
                    ss << std::setprecision(17) << point.distance;
                    entry.push_back(RespValue::bulkString(ss.str()));
                }
                
                if (withHash) {
                    entry.push_back(RespValue::integer(point.hash));
                }
                
                if (withCoord) {
                    std::ostringstream lonSs, latSs;
                    lonSs << std::setprecision(17) << point.longitude;
                    latSs << std::setprecision(17) << point.latitude;
                    
                    std::vector<RespValue> coord;
                    coord.push_back(RespValue::bulkString(lonSs.str()));
                    coord.push_back(RespValue::bulkString(latSs.str()));
                    entry.push_back(RespValue::array(coord));
                }
                
                arr.push_back(RespValue::array(entry));
            } else {
                // Simple response - just member names
                arr.push_back(RespValue::bulkString(point.member));
            }
        }
        
        return RespValue::array(arr).serialize();
    }
    
    /**
     * GEOHASH - Get geohash strings
     * Geohash strings do - base32 encoded
     * (Get geohash strings - base32 encoded representation)
     */
    std::string geoHash(const std::string& key, const std::vector<std::string>& members) {
        static const char base32[] = "0123456789bcdefghjkmnpqrstuvwxyz";
        
        std::vector<RespValue> results;
        
        for (const auto& member : members) {
            auto score = store_.zscore(key, member);
            
            if (score) {
                uint64_t hash = GeoUtil::scoreToGeohash(*score);
                
                // Convert to base32 string (11 characters)
                // Base32 string mein badlo - 11 characters
                // (Convert to base32 string - 11 characters)
                std::string hashStr;
                for (int i = 0; i < 11; i++) {
                    int idx = (hash >> (59 - i * 5)) & 0x1F;
                    hashStr += base32[idx];
                }
                
                results.push_back(RespValue::bulkString(hashStr));
            } else {
                results.push_back(RespValue::null());
            }
        }
        
        return RespValue::array(results).serialize();
    }
};

} // namespace Redis
