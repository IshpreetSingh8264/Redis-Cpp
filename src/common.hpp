/**
 * =============================================================================
 *                          COMMON DEFINITIONS - REDIS CPP
 * =============================================================================
 * 
 * Oye paaji, eh file hai saari common cheezaan di - types, constants, macros!
 * (Hey bro, this file contains all common stuff - types, constants, macros!)
 * 
 * Jiven ghar da foundation hunda hai, ohi eh file hai saare project di!
 * (Just like a house needs foundation, this file is the foundation of the project!)
 * 
 * =============================================================================
 */

#pragma once

// ============================================================================
// STANDARD LIBRARY INCLUDES - Saare important headers
// (All the important headers we need to survive in this cruel world)
// ============================================================================

#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <queue>
#include <deque>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <chrono>
#include <optional>
#include <variant>
#include <functional>
#include <sstream>
#include <fstream>
#include <algorithm>
#include <cmath>
#include <cstring>
#include <cstdint>
#include <random>
#include <regex>
#include <iomanip>
#include <limits>

// ============================================================================
// NETWORKING INCLUDES - Sockets te network stuff
// (Sockets and network stuff - jiven telephone lines hundi si purane zamane ch)
// (Sockets and network stuff - like telephone lines in the old days)
// ============================================================================

#include <sys/socket.h>
#include <sys/types.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>

// ============================================================================
// NAMESPACE DEFINITIONS
// ============================================================================

namespace Redis {

// ============================================================================
// TYPE ALIASES - Chhote naam lambe types de lyi
// (Short names for long types - kyunki life already bohut complicated hai)
// (Short names for long types - because life is already too complicated)
// ============================================================================

using TimePoint = std::chrono::steady_clock::time_point;
using Duration = std::chrono::milliseconds;
using StringVector = std::vector<std::string>;
using ByteVector = std::vector<uint8_t>;

// ============================================================================
// CONSTANTS - Jo kabhi change nahi hunde (jiven meri single status)
// (Things that never change - like my single status lol)
// ============================================================================

constexpr int DEFAULT_PORT = 6379;
constexpr int MAX_CLIENTS = 10000;
constexpr int BUFFER_SIZE = 65536;
constexpr int BACKLOG = 511;

// Redis protocol constants
// RESP Protocol - Redis Serialization Protocol
// Paaji eh protocol simple hai - + for simple strings, - for errors, etc.
// (Bro this protocol is simple - + for simple strings, - for errors, etc.)
constexpr char RESP_SIMPLE_STRING = '+';
constexpr char RESP_ERROR = '-';
constexpr char RESP_INTEGER = ':';
constexpr char RESP_BULK_STRING = '$';
constexpr char RESP_ARRAY = '*';
constexpr char RESP_NULL = '_';
constexpr char RESP_BOOLEAN = '#';
constexpr char RESP_DOUBLE = ',';
constexpr char RESP_BIG_NUMBER = '(';
constexpr char RESP_BULK_ERROR = '!';
constexpr char RESP_VERBATIM_STRING = '=';
constexpr char RESP_MAP = '%';
constexpr char RESP_SET = '~';
constexpr char RESP_PUSH = '>';

// ============================================================================
// CRLF - Carriage Return Line Feed
// Jiven WhatsApp pe blue tick hundi hai, ohi CRLF hai Redis di
// (Just like blue tick on WhatsApp, CRLF is Redis's way of saying "message complete")
// ============================================================================

const std::string CRLF = "\r\n";

// ============================================================================
// ERROR MESSAGES - Jado galti ho jaave taan kya bolna hai
// (What to say when things go wrong - story of my life)
// ============================================================================

const std::string ERR_WRONG_TYPE = "WRONGTYPE Operation against a key holding the wrong kind of value";
const std::string ERR_SYNTAX = "ERR syntax error";
const std::string ERR_NO_KEY = "ERR no such key";
const std::string ERR_INVALID_INT = "ERR value is not an integer or out of range";
const std::string ERR_INVALID_FLOAT = "ERR value is not a valid float";
const std::string ERR_WRONG_ARGC = "ERR wrong number of arguments";
const std::string ERR_UNKNOWN_CMD = "ERR unknown command";
const std::string ERR_NOT_INTEGER = "ERR value is not an integer or out of range";
const std::string ERR_STREAM_ID = "ERR The ID specified in XADD is equal or smaller than the target stream top item";
const std::string ERR_STREAM_ID_ZERO = "ERR The ID specified in XADD must be greater than 0-0";

// ============================================================================
// ENUMS - Choices in life (Redis has fewer than my commitment issues)
// ============================================================================

/**
 * Data types in Redis - Jiven caste system hai India ch, ohi types hain Redis ch
 * (Just kidding! But seriously, Redis has different types for different purposes)
 */
enum class DataType {
    NONE,           // Key doesn't exist - jiven meri GF
                    // (Key doesn't exist - like my girlfriend)
    STRING,         // Simple string - sabse common
                    // (Simple string - most common type)
    LIST,           // Linked list - queue jahi cheez
                    // (Linked list - queue-like thing)
    SET,            // Unique elements - no duplicates allowed
                    // (Unique elements - like my brain cells, very few)
    ZSET,           // Sorted set - set with scores
                    // (Sorted set - set with scores, like my exam marks sorted)
    HASH,           // Hash map - key-value pairs inside a key
                    // (Hash map - yo dawg, I heard you like keys...)
    STREAM,         // Stream - append-only log
                    // (Stream - like my tears when code doesn't work)
    NONE_BUT_EXISTS // Special case for TYPE command
};

/**
 * Replication role - Master ya Slave
 * (Master or Slave - no comments, purely technical terms!)
 */
enum class ReplicationRole {
    MASTER,     // Main server - BOSS
    REPLICA     // Follower - Chamcha (just kidding, it's important too!)
};

/**
 * Client state - Ki haal hai client da
 * (What's the state of the client)
 */
enum class ClientState {
    NORMAL,         // Normal operation - sab theek
                    // (Normal operation - all good)
    SUBSCRIBING,    // In pub/sub mode - sunne lag gya
                    // (In pub/sub mode - started listening)
    TRANSACTION,    // In MULTI/EXEC block - queue kar rya
                    // (In MULTI/EXEC block - queuing commands)
    BLOCKED         // Waiting for something - zindagi ruk gayi
                    // (Waiting for something - life on pause)
};

// ============================================================================
// UTILITY FUNCTIONS DECLARATIONS
// ============================================================================

/**
 * Get current time in milliseconds since epoch
 * Time check karo - jiven alarm lagaunde ho subah uthne lai
 * (Check time - like setting alarm to wake up in morning)
 */
inline int64_t getCurrentTimeMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

/**
 * Get monotonic time for internal measurements
 * Eh time kabhi backwards nahi jaanda - unlike my career
 * (This time never goes backwards - unlike my career)
 */
inline TimePoint getSteadyTime() {
    return std::chrono::steady_clock::now();
}

/**
 * Convert string to uppercase - CAPS LOCK ON
 * Jiven WhatsApp pe MOM message karti hai
 * (Like how MOM sends messages on WhatsApp - ALL CAPS)
 */
inline std::string toUpper(const std::string& str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(), ::toupper);
    return result;
}

/**
 * Convert string to lowercase - chill mode
 * Opposite of MOM's texting style
 * (Opposite of MOM's texting style)
 */
inline std::string toLower(const std::string& str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(), ::tolower);
    return result;
}

/**
 * Check if string is a valid integer
 * Paaji number hai ya bakwaas?
 * (Bro is this a number or nonsense?)
 */
inline bool isInteger(const std::string& str) {
    if (str.empty()) return false;
    size_t start = (str[0] == '-' || str[0] == '+') ? 1 : 0;
    if (start == str.length()) return false;
    for (size_t i = start; i < str.length(); i++) {
        if (!std::isdigit(str[i])) return false;
    }
    return true;
}

/**
 * Check if string is a valid float
 * Decimal number hai ya scam?
 * (Is this a decimal number or a scam?)
 */
inline bool isFloat(const std::string& str) {
    if (str.empty()) return false;
    try {
        std::stod(str);
        return true;
    } catch (...) {
        return false;
    }
}

/**
 * Split string by delimiter
 * String nu tode jiven dil tuta mera
 * (Split string like my heart was broken)
 */
inline StringVector split(const std::string& str, char delimiter) {
    StringVector tokens;
    std::stringstream ss(str);
    std::string token;
    while (std::getline(ss, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

/**
 * Join strings with delimiter
 * Strings nu jodo - reunion jaisa
 * (Join strings - like a reunion)
 */
inline std::string join(const StringVector& parts, const std::string& delimiter) {
    std::string result;
    for (size_t i = 0; i < parts.size(); i++) {
        if (i > 0) result += delimiter;
        result += parts[i];
    }
    return result;
}

/**
 * Trim whitespace from string
 * Extra space hata do - jiven flat ch jagah nahi hundi
 * (Remove extra space - like there's never enough space in a flat)
 */
inline std::string trim(const std::string& str) {
    auto start = str.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return "";
    auto end = str.find_last_not_of(" \t\r\n");
    return str.substr(start, end - start + 1);
}

/**
 * Generate random alphanumeric string
 * Random string generate karo - jiven CAPTCHA
 * (Generate random string - like CAPTCHA that nobody can read)
 */
inline std::string generateRandomString(size_t length) {
    static const char charset[] = 
        "0123456789"
        "abcdefghijklmnopqrstuvwxyz";
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<> dist(0, sizeof(charset) - 2);
    
    std::string result;
    result.reserve(length);
    for (size_t i = 0; i < length; i++) {
        result += charset[dist(gen)];
    }
    return result;
}

/**
 * Generate replication ID - 40 character hex string
 * Unique ID jiven Aadhaar card
 * (Unique ID like Aadhaar card)
 */
inline std::string generateReplId() {
    static const char hexchars[] = "0123456789abcdef";
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<> dist(0, 15);
    
    std::string result;
    result.reserve(40);
    for (int i = 0; i < 40; i++) {
        result += hexchars[dist(gen)];
    }
    return result;
}

} // namespace Redis
