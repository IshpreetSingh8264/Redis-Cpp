/**
 * =============================================================================
 *                              REDIS MAIN
 * =============================================================================
 * 
 * Paaji eh hai main entry point - jithon sab shuru hunda hai!
 * (Bro this is the main entry point - where everything starts!)
 * 
 * CLI arguments parse karke server start karna
 * (Parse CLI arguments and start the server)
 * 
 * Jiven dukaan da darwaza - sab yahan se andar aunde ne
 * (Like the shop's entrance - everyone enters from here)
 * 
 * Usage:
 *   ./your_program.sh                     # Default port 6379
 *   ./your_program.sh --port 6380         # Custom port
 *   ./your_program.sh --replicaof <host> <port>   # Start as replica
 *   ./your_program.sh --dir /tmp --dbfilename dump.rdb  # RDB config
 *
 * =============================================================================
 */

#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>
#include <signal.h>
#include <vector>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <deque>
#include <sstream>
#include <fstream>
#include <optional>
#include <chrono>
#include <thread>
#include <mutex>
#include <shared_mutex>
#include <filesystem>
#include <algorithm>
#include <regex>
#include <iomanip>

// ============================================================================
// PINGLISH COMMENTS LEGEND:
// Paaji = Bro
// Oye = Hey
// Ki haal hai = How's it going
// Koi nahi = No worries
// Sab theek = All good
// ============================================================================

// Forward declarations
// Pehle declare karo - baad mein define karenge
std::string handleCommand(const std::vector<std::string>& args, int clientFd);

// ============================================================================
// CONFIGURATION
// Server di settings - port, directories, etc.
// (Server settings - port, directories, etc.)
// ============================================================================

struct Config {
    int port = 6379;
    std::string dir = ".";
    std::string dbfilename = "dump.rdb";
    bool isReplica = false;
    std::string masterHost;
    int masterPort = 0;
};

Config gConfig;

// ============================================================================
// DATA STORE
// Redis data rakhna - keys, values, expiry
// (Store Redis data - keys, values, expiration)
// ============================================================================

enum class DataType { NONE, STRING, LIST, SET, HASH, ZSET, STREAM };

struct StreamEntry {
    std::string id;
    uint64_t timestamp = 0;
    uint64_t sequence = 0;
    std::vector<std::pair<std::string, std::string>> fields;
};

struct RedisValue {
    DataType type = DataType::NONE;
    std::string stringValue;
    std::deque<std::string> listValue;
    std::unordered_set<std::string> setValue;
    std::unordered_map<std::string, std::string> hashValue;
    std::multimap<double, std::string> zsetByScore;
    std::unordered_map<std::string, double> zsetScores;
    std::vector<StreamEntry> streamValue;
    uint64_t streamLastTimestamp = 0;
    uint64_t streamLastSequence = 0;
    int64_t expiryMs = -1;  // -1 = no expiry
    
    bool isExpired() const {
        if (expiryMs < 0) return false;
        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
        return now >= expiryMs;
    }
};

// Global data store - thread-safe
// Global data store - multiple threads lai safe
// (Global data store - safe for multiple threads)
std::unordered_map<std::string, RedisValue> gData;
std::shared_mutex gDataMutex;

// Replication state
// Replication info - master/replica sync
// (Replication info - for master/replica synchronization)
std::string gReplId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
int64_t gReplOffset = 0;
std::vector<int> gReplicas;
std::mutex gReplicaMutex;

// ============================================================================
// UTILITY FUNCTIONS
// Helper functions - commonly used stuff
// (Helper functions - commonly used operations)
// ============================================================================

// Get current time in milliseconds
// Abhi da time milliseconds mein - very precise!
// (Current time in milliseconds - very precise!)
int64_t getCurrentTimeMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

// Convert string to uppercase
// String nu uppercase karo - CHOTE AKSHAR TO WADDE AKSHAR
// (Convert string to uppercase - small to capital letters)
std::string toUpper(const std::string& str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(), ::toupper);
    return result;
}

// Simple pattern matching for KEYS command
// Pattern match karo - * te ? support
// (Pattern matching - supports * and ? wildcards)
bool matchPattern(const std::string& pattern, const std::string& str) {
    if (pattern == "*") return true;
    
    size_t pi = 0, si = 0;
    size_t starIdx = std::string::npos;
    size_t matchIdx = 0;
    
    while (si < str.size()) {
        if (pi < pattern.size() && (pattern[pi] == '?' || pattern[pi] == str[si])) {
            pi++;
            si++;
        } else if (pi < pattern.size() && pattern[pi] == '*') {
            starIdx = pi;
            matchIdx = si;
            pi++;
        } else if (starIdx != std::string::npos) {
            pi = starIdx + 1;
            matchIdx++;
            si = matchIdx;
        } else {
            return false;
        }
    }
    
    while (pi < pattern.size() && pattern[pi] == '*') pi++;
    return pi == pattern.size();
}

// ============================================================================
// RESP PROTOCOL
// Redis protocol parser te builder
// (Redis protocol parser and response builder)
// ============================================================================

// Parse a RESP command from buffer
// Buffer se command parse karo - RESP format
// (Parse command from buffer in RESP format)
std::vector<std::string> parseRespCommand(const std::string& buffer, size_t& consumed) {
    std::vector<std::string> args;
    consumed = 0;
    
    if (buffer.empty()) return args;
    
    size_t pos = 0;
    
    // Check for inline command (telnet style)
    // Inline command check - telnet style bhi support
    // (Check for inline command - support telnet style too)
    if (buffer[0] != '*') {
        auto endPos = buffer.find("\r\n");
        if (endPos == std::string::npos) return args;
        
        std::string line = buffer.substr(0, endPos);
        std::istringstream iss(line);
        std::string word;
        while (iss >> word) {
            args.push_back(word);
        }
        consumed = endPos + 2;
        return args;
    }
    
    // Parse RESP array
    // RESP array parse karo - *N format
    // (Parse RESP array - *N format where N is count)
    auto crlfPos = buffer.find("\r\n", pos);
    if (crlfPos == std::string::npos) return args;
    
    int numArgs = std::stoi(buffer.substr(pos + 1, crlfPos - pos - 1));
    pos = crlfPos + 2;
    
    for (int i = 0; i < numArgs; i++) {
        if (pos >= buffer.size()) {
            args.clear();
            return args;
        }
        
        if (buffer[pos] != '$') {
            args.clear();
            return args;
        }
        
        crlfPos = buffer.find("\r\n", pos);
        if (crlfPos == std::string::npos) {
            args.clear();
            return args;
        }
        
        int len = std::stoi(buffer.substr(pos + 1, crlfPos - pos - 1));
        pos = crlfPos + 2;
        
        if (pos + len + 2 > buffer.size()) {
            args.clear();
            return args;
        }
        
        args.push_back(buffer.substr(pos, len));
        pos = pos + len + 2;
    }
    
    consumed = pos;
    return args;
}

// RESP response builders
// Response banao - different types lai
// (Build responses for different types)

std::string respSimpleString(const std::string& str) {
    return "+" + str + "\r\n";
}

std::string respError(const std::string& msg) {
    return "-" + msg + "\r\n";
}

std::string respInteger(int64_t val) {
    return ":" + std::to_string(val) + "\r\n";
}

std::string respBulkString(const std::string& str) {
    return "$" + std::to_string(str.length()) + "\r\n" + str + "\r\n";
}

std::string respNull() {
    return "$-1\r\n";
}

std::string respNullArray() {
    return "*-1\r\n";
}

std::string respArray(const std::vector<std::string>& items) {
    std::string result = "*" + std::to_string(items.size()) + "\r\n";
    for (const auto& item : items) {
        result += item;
    }
    return result;
}

// Encode command as RESP array
// Command nu RESP format mein encode karo
// (Encode command in RESP format for replication)
std::string encodeRespArray(const std::vector<std::string>& args) {
    std::string result = "*" + std::to_string(args.size()) + "\r\n";
    for (const auto& arg : args) {
        result += "$" + std::to_string(arg.length()) + "\r\n" + arg + "\r\n";
    }
    return result;
}

// ============================================================================
// RDB PERSISTENCE
// RDB file reading - startup pe data load
// (RDB file reading - load data on startup)
// ============================================================================

// Read RDB file and load data
// RDB file read karo te data load karo
// (Read RDB file and load data into memory)
void loadRdb() {
    std::string path = gConfig.dir + "/" + gConfig.dbfilename;
    
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
        std::cout << "RDB file not found: " << path << " (starting fresh)" << std::endl;
        return;
    }
    
    std::cout << "Loading RDB from " << path << std::endl;
    
    try {
        // Read magic "REDIS"
        char magic[5];
        file.read(magic, 5);
        if (std::string(magic, 5) != "REDIS") {
            std::cerr << "Invalid RDB magic" << std::endl;
            return;
        }
        
        // Read version
        char version[4];
        file.read(version, 4);
        
        int64_t currentExpiry = -1;
        
        while (file.good() && !file.eof()) {
            uint8_t opcode;
            file.read(reinterpret_cast<char*>(&opcode), 1);
            if (!file.good()) break;
            
            if (opcode == 0xFF) {
                // EOF
                break;
            } else if (opcode == 0xFE) {
                // SELECTDB
                uint8_t db;
                file.read(reinterpret_cast<char*>(&db), 1);
            } else if (opcode == 0xFB) {
                // RESIZEDB
                // Read two length-encoded integers
                auto readLen = [&file]() -> uint64_t {
                    uint8_t byte;
                    file.read(reinterpret_cast<char*>(&byte), 1);
                    uint8_t type = (byte & 0xC0) >> 6;
                    if (type == 0) return byte & 0x3F;
                    if (type == 1) {
                        uint8_t next;
                        file.read(reinterpret_cast<char*>(&next), 1);
                        return ((byte & 0x3F) << 8) | next;
                    }
                    if (type == 2) {
                        uint32_t len;
                        file.read(reinterpret_cast<char*>(&len), 4);
                        return len;
                    }
                    return 0;
                };
                readLen();  // hash table size
                readLen();  // expires hash table size
            } else if (opcode == 0xFA) {
                // AUX field
                auto readString = [&file]() -> std::string {
                    uint8_t byte;
                    file.read(reinterpret_cast<char*>(&byte), 1);
                    uint8_t type = (byte & 0xC0) >> 6;
                    
                    uint64_t len = 0;
                    if (type == 3) {
                        // Special encoding - integer
                        uint8_t fmt = byte & 0x3F;
                        if (fmt == 0) {
                            int8_t val;
                            file.read(reinterpret_cast<char*>(&val), 1);
                            return std::to_string(val);
                        } else if (fmt == 1) {
                            int16_t val;
                            file.read(reinterpret_cast<char*>(&val), 2);
                            return std::to_string(val);
                        } else if (fmt == 2) {
                            int32_t val;
                            file.read(reinterpret_cast<char*>(&val), 4);
                            return std::to_string(val);
                        }
                        return "";
                    } else if (type == 0) {
                        len = byte & 0x3F;
                    } else if (type == 1) {
                        uint8_t next;
                        file.read(reinterpret_cast<char*>(&next), 1);
                        len = ((byte & 0x3F) << 8) | next;
                    } else if (type == 2) {
                        uint32_t len32;
                        file.read(reinterpret_cast<char*>(&len32), 4);
                        len = len32;
                    }
                    
                    std::string result(len, '\0');
                    file.read(&result[0], len);
                    return result;
                };
                
                std::string auxKey = readString();
                std::string auxValue = readString();
                std::cout << "RDB aux: " << auxKey << " = " << auxValue << std::endl;
            } else if (opcode == 0xFD) {
                // Expiry in seconds
                uint32_t expiry;
                file.read(reinterpret_cast<char*>(&expiry), 4);
                currentExpiry = static_cast<int64_t>(expiry) * 1000;
            } else if (opcode == 0xFC) {
                // Expiry in milliseconds
                file.read(reinterpret_cast<char*>(&currentExpiry), 8);
            } else {
                // Value type
                auto readString = [&file]() -> std::string {
                    uint8_t byte;
                    file.read(reinterpret_cast<char*>(&byte), 1);
                    uint8_t type = (byte & 0xC0) >> 6;
                    
                    uint64_t len = 0;
                    if (type == 3) {
                        uint8_t fmt = byte & 0x3F;
                        if (fmt == 0) {
                            int8_t val;
                            file.read(reinterpret_cast<char*>(&val), 1);
                            return std::to_string(val);
                        } else if (fmt == 1) {
                            int16_t val;
                            file.read(reinterpret_cast<char*>(&val), 2);
                            return std::to_string(val);
                        } else if (fmt == 2) {
                            int32_t val;
                            file.read(reinterpret_cast<char*>(&val), 4);
                            return std::to_string(val);
                        }
                        return "";
                    } else if (type == 0) {
                        len = byte & 0x3F;
                    } else if (type == 1) {
                        uint8_t next;
                        file.read(reinterpret_cast<char*>(&next), 1);
                        len = ((byte & 0x3F) << 8) | next;
                    } else if (type == 2) {
                        uint32_t len32;
                        file.read(reinterpret_cast<char*>(&len32), 4);
                        len = len32;
                    }
                    
                    std::string result(len, '\0');
                    file.read(&result[0], len);
                    return result;
                };
                
                std::string key = readString();
                
                if (opcode == 0) {
                    // String type
                    std::string value = readString();
                    
                    std::unique_lock lock(gDataMutex);
                    gData[key].type = DataType::STRING;
                    gData[key].stringValue = value;
                    if (currentExpiry > 0) {
                        gData[key].expiryMs = currentExpiry;
                    }
                }
                // For now, only handle strings
                
                currentExpiry = -1;
            }
        }
        
        std::cout << "RDB loaded successfully" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "RDB parse error: " << e.what() << std::endl;
    }
}

// ============================================================================
// COMMAND HANDLERS
// Redis commands implement - sab commands idher
// (Implement Redis commands - all commands here)
// ============================================================================

// Helper to check expiry and delete if expired
// Expiry check karo te delete karo agar expire ho gaya
// (Check expiry and delete if expired)
bool checkAndDeleteExpired(const std::string& key) {
    auto it = gData.find(key);
    if (it != gData.end() && it->second.isExpired()) {
        gData.erase(it);
        return true;
    }
    return false;
}

std::string handlePing(const std::vector<std::string>& args) {
    if (args.size() > 1) {
        return respBulkString(args[1]);
    }
    return respSimpleString("PONG");
}

std::string handleEcho(const std::vector<std::string>& args) {
    if (args.size() < 2) {
        return respError("ERR wrong number of arguments for 'echo' command");
    }
    return respBulkString(args[1]);
}

std::string handleSet(const std::vector<std::string>& args) {
    if (args.size() < 3) {
        return respError("ERR wrong number of arguments for 'set' command");
    }
    
    const std::string& key = args[1];
    const std::string& value = args[2];
    int64_t expiryMs = -1;
    bool nx = false, xx = false;
    
    // Parse options
    for (size_t i = 3; i < args.size(); i++) {
        std::string opt = toUpper(args[i]);
        if (opt == "EX" && i + 1 < args.size()) {
            expiryMs = getCurrentTimeMs() + std::stoll(args[++i]) * 1000;
        } else if (opt == "PX" && i + 1 < args.size()) {
            expiryMs = getCurrentTimeMs() + std::stoll(args[++i]);
        } else if (opt == "EXAT" && i + 1 < args.size()) {
            expiryMs = std::stoll(args[++i]) * 1000;
        } else if (opt == "PXAT" && i + 1 < args.size()) {
            expiryMs = std::stoll(args[++i]);
        } else if (opt == "NX") {
            nx = true;
        } else if (opt == "XX") {
            xx = true;
        }
    }
    
    std::unique_lock lock(gDataMutex);
    checkAndDeleteExpired(key);
    
    bool exists = gData.find(key) != gData.end();
    if (nx && exists) return respNull();
    if (xx && !exists) return respNull();
    
    gData[key].type = DataType::STRING;
    gData[key].stringValue = value;
    gData[key].expiryMs = expiryMs;
    
    return respSimpleString("OK");
}

std::string handleGet(const std::vector<std::string>& args) {
    if (args.size() < 2) {
        return respError("ERR wrong number of arguments for 'get' command");
    }
    
    std::shared_lock lock(gDataMutex);
    const std::string& key = args[1];
    
    auto it = gData.find(key);
    if (it == gData.end() || it->second.isExpired()) {
        return respNull();
    }
    
    if (it->second.type != DataType::STRING) {
        return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    
    return respBulkString(it->second.stringValue);
}

std::string handleIncr(const std::vector<std::string>& args) {
    if (args.size() < 2) {
        return respError("ERR wrong number of arguments for 'incr' command");
    }
    
    std::unique_lock lock(gDataMutex);
    const std::string& key = args[1];
    checkAndDeleteExpired(key);
    
    auto it = gData.find(key);
    int64_t val = 0;
    
    if (it != gData.end()) {
        if (it->second.type != DataType::STRING) {
            return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        try {
            val = std::stoll(it->second.stringValue);
        } catch (...) {
            return respError("ERR value is not an integer or out of range");
        }
    }
    
    val++;
    gData[key].type = DataType::STRING;
    gData[key].stringValue = std::to_string(val);
    
    return respInteger(val);
}

std::string handleType(const std::vector<std::string>& args) {
    if (args.size() < 2) {
        return respError("ERR wrong number of arguments for 'type' command");
    }
    
    std::shared_lock lock(gDataMutex);
    const std::string& key = args[1];
    
    auto it = gData.find(key);
    if (it == gData.end() || it->second.isExpired()) {
        return respSimpleString("none");
    }
    
    switch (it->second.type) {
        case DataType::STRING: return respSimpleString("string");
        case DataType::LIST: return respSimpleString("list");
        case DataType::SET: return respSimpleString("set");
        case DataType::HASH: return respSimpleString("hash");
        case DataType::ZSET: return respSimpleString("zset");
        case DataType::STREAM: return respSimpleString("stream");
        default: return respSimpleString("none");
    }
}

std::string handleKeys(const std::vector<std::string>& args) {
    if (args.size() < 2) {
        return respError("ERR wrong number of arguments for 'keys' command");
    }
    
    std::shared_lock lock(gDataMutex);
    const std::string& pattern = args[1];
    
    std::vector<std::string> result;
    for (const auto& [key, value] : gData) {
        if (!value.isExpired() && matchPattern(pattern, key)) {
            result.push_back(respBulkString(key));
        }
    }
    
    return respArray(result);
}

std::string handleConfig(const std::vector<std::string>& args) {
    if (args.size() < 3) {
        return respError("ERR wrong number of arguments for 'config' command");
    }
    
    std::string subCmd = toUpper(args[1]);
    std::string param = args[2];
    
    if (subCmd == "GET") {
        std::vector<std::string> result;
        if (param == "dir" || param == "*") {
            result.push_back(respBulkString("dir"));
            result.push_back(respBulkString(gConfig.dir));
        }
        if (param == "dbfilename" || param == "*") {
            result.push_back(respBulkString("dbfilename"));
            result.push_back(respBulkString(gConfig.dbfilename));
        }
        return respArray(result);
    }
    
    return respSimpleString("OK");
}

std::string handleInfo(const std::vector<std::string>& args) {
    std::ostringstream ss;
    
    ss << "# Replication\r\n";
    ss << "role:" << (gConfig.isReplica ? "slave" : "master") << "\r\n";
    if (!gConfig.isReplica) {
        ss << "master_replid:" << gReplId << "\r\n";
        ss << "master_repl_offset:" << gReplOffset << "\r\n";
    }
    
    return respBulkString(ss.str());
}

// List commands
std::string handleLPush(const std::vector<std::string>& args) {
    if (args.size() < 3) {
        return respError("ERR wrong number of arguments for 'lpush' command");
    }
    
    std::unique_lock lock(gDataMutex);
    const std::string& key = args[1];
    checkAndDeleteExpired(key);
    
    auto& val = gData[key];
    if (val.type != DataType::NONE && val.type != DataType::LIST) {
        return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    
    val.type = DataType::LIST;
    for (size_t i = 2; i < args.size(); i++) {
        val.listValue.push_front(args[i]);
    }
    
    return respInteger(val.listValue.size());
}

std::string handleRPush(const std::vector<std::string>& args) {
    if (args.size() < 3) {
        return respError("ERR wrong number of arguments for 'rpush' command");
    }
    
    std::unique_lock lock(gDataMutex);
    const std::string& key = args[1];
    checkAndDeleteExpired(key);
    
    auto& val = gData[key];
    if (val.type != DataType::NONE && val.type != DataType::LIST) {
        return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    
    val.type = DataType::LIST;
    for (size_t i = 2; i < args.size(); i++) {
        val.listValue.push_back(args[i]);
    }
    
    return respInteger(val.listValue.size());
}

std::string handleLRange(const std::vector<std::string>& args) {
    if (args.size() < 4) {
        return respError("ERR wrong number of arguments for 'lrange' command");
    }
    
    std::shared_lock lock(gDataMutex);
    const std::string& key = args[1];
    int64_t start = std::stoll(args[2]);
    int64_t stop = std::stoll(args[3]);
    
    auto it = gData.find(key);
    if (it == gData.end() || it->second.isExpired()) {
        return respArray({});
    }
    
    if (it->second.type != DataType::LIST) {
        return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    
    int64_t len = it->second.listValue.size();
    if (start < 0) start = len + start;
    if (stop < 0) stop = len + stop;
    if (start < 0) start = 0;
    if (stop >= len) stop = len - 1;
    
    std::vector<std::string> result;
    for (int64_t i = start; i <= stop && i < len; i++) {
        result.push_back(respBulkString(it->second.listValue[i]));
    }
    
    return respArray(result);
}

std::string handleLLen(const std::vector<std::string>& args) {
    // LLEN - list di length pata karo
    // (LLEN - find out the length of the list)
    if (args.size() < 2) {
        return respError("ERR wrong number of arguments for 'llen' command");
    }
    
    std::shared_lock lock(gDataMutex);
    const std::string& key = args[1];
    
    auto it = gData.find(key);
    if (it == gData.end() || it->second.isExpired()) {
        // List nahi mili - 0 return karo
        // (List not found - return 0)
        return respInteger(0);
    }
    
    if (it->second.type != DataType::LIST) {
        return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    
    return respInteger(it->second.listValue.size());
}

std::string handleLPop(const std::vector<std::string>& args) {
    // LPOP - list de shuru ton element kaddo
    // (LPOP - remove element from the beginning of list)
    if (args.size() < 2) {
        return respError("ERR wrong number of arguments for 'lpop' command");
    }
    
    std::unique_lock lock(gDataMutex);
    const std::string& key = args[1];
    checkAndDeleteExpired(key);
    
    // Count parameter check karo - kitne elements kadhne ne
    // (Check count parameter - how many elements to remove)
    int64_t count = 1;
    bool hasCount = false;
    if (args.size() >= 3) {
        count = std::stoll(args[2]);
        hasCount = true;
        if (count < 0) {
            return respError("ERR value is out of range, must be positive");
        }
    }
    
    auto it = gData.find(key);
    if (it == gData.end()) {
        // List nahi mili - null return karo
        // (List not found - return null)
        return respNull();
    }
    
    if (it->second.type != DataType::LIST) {
        return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    
    if (it->second.listValue.empty()) {
        // List khali hai - null return karo
        // (List is empty - return null)
        return respNull();
    }
    
    // Agar count dita hai te array return karo, nahi te single element
    // (If count is given, return array, otherwise single element)
    if (!hasCount) {
        // Single element kadho
        // (Remove single element)
        std::string value = it->second.listValue.front();
        it->second.listValue.pop_front();
        
        if (it->second.listValue.empty()) {
            gData.erase(it);
        }
        
        return respBulkString(value);
    }
    
    // Multiple elements kadho
    // (Remove multiple elements)
    std::vector<std::string> result;
    int64_t actualCount = std::min(count, static_cast<int64_t>(it->second.listValue.size()));
    
    for (int64_t i = 0; i < actualCount; i++) {
        result.push_back(respBulkString(it->second.listValue.front()));
        it->second.listValue.pop_front();
    }
    
    // Agar list khali ho gayi, key hata do
    // (If list became empty, remove the key)
    if (it->second.listValue.empty()) {
        gData.erase(it);
    }
    
    return respArray(result);
}

std::string handleRPop(const std::vector<std::string>& args) {
    // RPOP - list de end ton element kaddo
    // (RPOP - remove element from the end of list)
    if (args.size() < 2) {
        return respError("ERR wrong number of arguments for 'rpop' command");
    }
    
    std::unique_lock lock(gDataMutex);
    const std::string& key = args[1];
    checkAndDeleteExpired(key);
    
    // Count parameter check karo
    // (Check count parameter)
    int64_t count = 1;
    bool hasCount = false;
    if (args.size() >= 3) {
        count = std::stoll(args[2]);
        hasCount = true;
        if (count < 0) {
            return respError("ERR value is out of range, must be positive");
        }
    }
    
    auto it = gData.find(key);
    if (it == gData.end()) {
        return respNull();
    }
    
    if (it->second.type != DataType::LIST) {
        return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    
    if (it->second.listValue.empty()) {
        return respNull();
    }
    
    if (!hasCount) {
        // Single element
        std::string value = it->second.listValue.back();
        it->second.listValue.pop_back();
        
        if (it->second.listValue.empty()) {
            gData.erase(it);
        }
        
        return respBulkString(value);
    }
    
    // Multiple elements
    std::vector<std::string> result;
    int64_t actualCount = std::min(count, static_cast<int64_t>(it->second.listValue.size()));
    
    for (int64_t i = 0; i < actualCount; i++) {
        result.push_back(respBulkString(it->second.listValue.back()));
        it->second.listValue.pop_back();
    }
    
    if (it->second.listValue.empty()) {
        gData.erase(it);
    }
    
    return respArray(result);
}

// BLPOP - Blocking LPOP
// Blocking list pop - wait karo jab tak list mein kuch na aaye
// (Blocking list pop - wait until list has something)
std::string handleBLPop(const std::vector<std::string>& args, int clientFd) {
    if (args.size() < 3) {
        return respError("ERR wrong number of arguments for 'blpop' command");
    }
    
    // Last argument is timeout
    // Akhri argument timeout hai
    double timeout = std::stod(args[args.size() - 1]);
    
    // Keys are args[1] to args[size-2]
    // Keys hain args[1] se args[size-2] tak
    std::vector<std::string> keys;
    for (size_t i = 1; i < args.size() - 1; i++) {
        keys.push_back(args[i]);
    }
    
    auto startTime = std::chrono::steady_clock::now();
    int64_t timeoutMs = (timeout == 0) ? INT64_MAX : static_cast<int64_t>(timeout * 1000);
    
    while (true) {
        {
            std::unique_lock lock(gDataMutex);
            
            for (const auto& key : keys) {
                auto it = gData.find(key);
                if (it != gData.end() && it->second.type == DataType::LIST && !it->second.listValue.empty()) {
                    // List mili with data - pop karo
                    // (Found list with data - pop it)
                    std::string value = it->second.listValue.front();
                    it->second.listValue.pop_front();
                    
                    if (it->second.listValue.empty()) {
                        gData.erase(it);
                    }
                    
                    std::vector<std::string> result;
                    result.push_back(respBulkString(key));
                    result.push_back(respBulkString(value));
                    return respArray(result);
                }
            }
        }
        
        // Check timeout
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - startTime
        ).count();
        
        if (elapsed >= timeoutMs) {
            return respNullArray();
        }
        
        // Wait thoda
        // (Wait a bit)
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

// BRPOP - same as BLPOP but from right
std::string handleBRPop(const std::vector<std::string>& args, int clientFd) {
    if (args.size() < 3) {
        return respError("ERR wrong number of arguments for 'brpop' command");
    }
    
    double timeout = std::stod(args[args.size() - 1]);
    
    std::vector<std::string> keys;
    for (size_t i = 1; i < args.size() - 1; i++) {
        keys.push_back(args[i]);
    }
    
    auto startTime = std::chrono::steady_clock::now();
    int64_t timeoutMs = (timeout == 0) ? INT64_MAX : static_cast<int64_t>(timeout * 1000);
    
    while (true) {
        {
            std::unique_lock lock(gDataMutex);
            
            for (const auto& key : keys) {
                auto it = gData.find(key);
                if (it != gData.end() && it->second.type == DataType::LIST && !it->second.listValue.empty()) {
                    std::string value = it->second.listValue.back();
                    it->second.listValue.pop_back();
                    
                    if (it->second.listValue.empty()) {
                        gData.erase(it);
                    }
                    
                    std::vector<std::string> result;
                    result.push_back(respBulkString(key));
                    result.push_back(respBulkString(value));
                    return respArray(result);
                }
            }
        }
        
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - startTime
        ).count();
        
        if (elapsed >= timeoutMs) {
            return respNullArray();
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

// ============================================================================
// TRANSACTION COMMANDS
// Transactions - MULTI/EXEC/DISCARD
// ============================================================================

// Per-client transaction state
// Har client di apni transaction state
std::map<int, bool> gClientInMulti;
std::map<int, std::vector<std::vector<std::string>>> gClientQueues;

std::string handleMulti(const std::vector<std::string>& args, int clientFd) {
    // MULTI - transaction shuru karo
    // (MULTI - start a transaction)
    if (gClientInMulti[clientFd]) {
        return respError("ERR MULTI calls can not be nested");
    }
    gClientInMulti[clientFd] = true;
    gClientQueues[clientFd].clear();
    return respSimpleString("OK");
}

std::string handleExec(const std::vector<std::string>& args, int clientFd) {
    // EXEC - transaction execute karo
    // (EXEC - execute the transaction)
    if (!gClientInMulti[clientFd]) {
        return respError("ERR EXEC without MULTI");
    }
    
    gClientInMulti[clientFd] = false;
    auto& queue = gClientQueues[clientFd];
    
    if (queue.empty()) {
        return respArray({});
    }
    
    std::vector<std::string> results;
    for (auto& cmdArgs : queue) {
        std::string result = handleCommand(cmdArgs, clientFd);
        results.push_back(result);
    }
    
    queue.clear();
    return respArray(results);
}

std::string handleDiscard(const std::vector<std::string>& args, int clientFd) {
    // DISCARD - transaction cancel karo
    // (DISCARD - cancel the transaction)
    if (!gClientInMulti[clientFd]) {
        return respError("ERR DISCARD without MULTI");
    }
    
    gClientInMulti[clientFd] = false;
    gClientQueues[clientFd].clear();
    return respSimpleString("OK");
}

// ============================================================================
// DECR COMMAND
// ============================================================================

std::string handleDecr(const std::vector<std::string>& args) {
    // DECR - value ghatao
    // (DECR - decrease value)
    if (args.size() < 2) {
        return respError("ERR wrong number of arguments for 'decr' command");
    }
    
    std::unique_lock lock(gDataMutex);
    const std::string& key = args[1];
    checkAndDeleteExpired(key);
    
    auto it = gData.find(key);
    int64_t val = 0;
    
    if (it != gData.end()) {
        if (it->second.type != DataType::STRING) {
            return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        try {
            val = std::stoll(it->second.stringValue);
        } catch (...) {
            return respError("ERR value is not an integer or out of range");
        }
    }
    
    val--;
    gData[key].type = DataType::STRING;
    gData[key].stringValue = std::to_string(val);
    
    return respInteger(val);
}

// ============================================================================
// SET COMMANDS (SADD, SMEMBERS, SISMEMBER, SREM, SCARD)
// ============================================================================

std::string handleSAdd(const std::vector<std::string>& args) {
    // SADD - set mein member add karo
    // (SADD - add member to set)
    if (args.size() < 3) {
        return respError("ERR wrong number of arguments for 'sadd' command");
    }
    
    std::unique_lock lock(gDataMutex);
    const std::string& key = args[1];
    checkAndDeleteExpired(key);
    
    auto& val = gData[key];
    if (val.type != DataType::NONE && val.type != DataType::SET) {
        return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    
    val.type = DataType::SET;
    int64_t added = 0;
    
    for (size_t i = 2; i < args.size(); i++) {
        auto result = val.setValue.insert(args[i]);
        if (result.second) added++;
    }
    
    return respInteger(added);
}

std::string handleSMembers(const std::vector<std::string>& args) {
    // SMEMBERS - set de sab members lo
    // (SMEMBERS - get all members of set)
    if (args.size() < 2) {
        return respError("ERR wrong number of arguments for 'smembers' command");
    }
    
    std::shared_lock lock(gDataMutex);
    const std::string& key = args[1];
    
    auto it = gData.find(key);
    if (it == gData.end() || it->second.isExpired()) {
        return respArray({});
    }
    
    if (it->second.type != DataType::SET) {
        return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    
    std::vector<std::string> result;
    for (const auto& member : it->second.setValue) {
        result.push_back(respBulkString(member));
    }
    
    return respArray(result);
}

std::string handleSIsMember(const std::vector<std::string>& args) {
    // SISMEMBER - check karo member set mein hai ki nahi
    // (SISMEMBER - check if member exists in set)
    if (args.size() < 3) {
        return respError("ERR wrong number of arguments for 'sismember' command");
    }
    
    std::shared_lock lock(gDataMutex);
    const std::string& key = args[1];
    const std::string& member = args[2];
    
    auto it = gData.find(key);
    if (it == gData.end() || it->second.isExpired()) {
        return respInteger(0);
    }
    
    if (it->second.type != DataType::SET) {
        return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    
    return respInteger(it->second.setValue.count(member) > 0 ? 1 : 0);
}

std::string handleSRem(const std::vector<std::string>& args) {
    // SREM - set vichon member hata do
    // (SREM - remove member from set)
    if (args.size() < 3) {
        return respError("ERR wrong number of arguments for 'srem' command");
    }
    
    std::unique_lock lock(gDataMutex);
    const std::string& key = args[1];
    
    auto it = gData.find(key);
    if (it == gData.end() || it->second.isExpired()) {
        return respInteger(0);
    }
    
    if (it->second.type != DataType::SET) {
        return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    
    int64_t removed = 0;
    for (size_t i = 2; i < args.size(); i++) {
        removed += it->second.setValue.erase(args[i]);
    }
    
    if (it->second.setValue.empty()) {
        gData.erase(it);
    }
    
    return respInteger(removed);
}

std::string handleSCard(const std::vector<std::string>& args) {
    // SCARD - set di size
    // (SCARD - size of set)
    if (args.size() < 2) {
        return respError("ERR wrong number of arguments for 'scard' command");
    }
    
    std::shared_lock lock(gDataMutex);
    const std::string& key = args[1];
    
    auto it = gData.find(key);
    if (it == gData.end() || it->second.isExpired()) {
        return respInteger(0);
    }
    
    if (it->second.type != DataType::SET) {
        return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    
    return respInteger(it->second.setValue.size());
}

// ============================================================================
// HASH COMMANDS (HSET, HGET, HGETALL, HDEL, HEXISTS, HLEN)
// ============================================================================

std::string handleHSet(const std::vector<std::string>& args) {
    // HSET - hash mein field set karo
    // (HSET - set field in hash)
    if (args.size() < 4 || (args.size() - 2) % 2 != 0) {
        return respError("ERR wrong number of arguments for 'hset' command");
    }
    
    std::unique_lock lock(gDataMutex);
    const std::string& key = args[1];
    checkAndDeleteExpired(key);
    
    auto& val = gData[key];
    if (val.type != DataType::NONE && val.type != DataType::HASH) {
        return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    
    val.type = DataType::HASH;
    int64_t added = 0;
    
    for (size_t i = 2; i + 1 < args.size(); i += 2) {
        if (val.hashValue.find(args[i]) == val.hashValue.end()) {
            added++;
        }
        val.hashValue[args[i]] = args[i + 1];
    }
    
    return respInteger(added);
}

std::string handleHGet(const std::vector<std::string>& args) {
    // HGET - hash vichon field lo
    // (HGET - get field from hash)
    if (args.size() < 3) {
        return respError("ERR wrong number of arguments for 'hget' command");
    }
    
    std::shared_lock lock(gDataMutex);
    const std::string& key = args[1];
    const std::string& field = args[2];
    
    auto it = gData.find(key);
    if (it == gData.end() || it->second.isExpired()) {
        return respNull();
    }
    
    if (it->second.type != DataType::HASH) {
        return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    
    auto fit = it->second.hashValue.find(field);
    if (fit == it->second.hashValue.end()) {
        return respNull();
    }
    
    return respBulkString(fit->second);
}

std::string handleHGetAll(const std::vector<std::string>& args) {
    // HGETALL - hash de sab fields te values
    // (HGETALL - get all fields and values from hash)
    if (args.size() < 2) {
        return respError("ERR wrong number of arguments for 'hgetall' command");
    }
    
    std::shared_lock lock(gDataMutex);
    const std::string& key = args[1];
    
    auto it = gData.find(key);
    if (it == gData.end() || it->second.isExpired()) {
        return respArray({});
    }
    
    if (it->second.type != DataType::HASH) {
        return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    
    std::vector<std::string> result;
    for (const auto& [field, value] : it->second.hashValue) {
        result.push_back(respBulkString(field));
        result.push_back(respBulkString(value));
    }
    
    return respArray(result);
}

std::string handleHDel(const std::vector<std::string>& args) {
    // HDEL - hash vichon field hata do
    // (HDEL - delete field from hash)
    if (args.size() < 3) {
        return respError("ERR wrong number of arguments for 'hdel' command");
    }
    
    std::unique_lock lock(gDataMutex);
    const std::string& key = args[1];
    
    auto it = gData.find(key);
    if (it == gData.end() || it->second.isExpired()) {
        return respInteger(0);
    }
    
    if (it->second.type != DataType::HASH) {
        return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    
    int64_t removed = 0;
    for (size_t i = 2; i < args.size(); i++) {
        removed += it->second.hashValue.erase(args[i]);
    }
    
    if (it->second.hashValue.empty()) {
        gData.erase(it);
    }
    
    return respInteger(removed);
}

std::string handleHExists(const std::vector<std::string>& args) {
    // HEXISTS - check karo field hash mein hai ki nahi
    // (HEXISTS - check if field exists in hash)
    if (args.size() < 3) {
        return respError("ERR wrong number of arguments for 'hexists' command");
    }
    
    std::shared_lock lock(gDataMutex);
    const std::string& key = args[1];
    const std::string& field = args[2];
    
    auto it = gData.find(key);
    if (it == gData.end() || it->second.isExpired()) {
        return respInteger(0);
    }
    
    if (it->second.type != DataType::HASH) {
        return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    
    return respInteger(it->second.hashValue.count(field) > 0 ? 1 : 0);
}

std::string handleHLen(const std::vector<std::string>& args) {
    // HLEN - hash di size
    // (HLEN - size of hash)
    if (args.size() < 2) {
        return respError("ERR wrong number of arguments for 'hlen' command");
    }
    
    std::shared_lock lock(gDataMutex);
    const std::string& key = args[1];
    
    auto it = gData.find(key);
    if (it == gData.end() || it->second.isExpired()) {
        return respInteger(0);
    }
    
    if (it->second.type != DataType::HASH) {
        return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    
    return respInteger(it->second.hashValue.size());
}

// ============================================================================
// SORTED SET COMMANDS (ZADD, ZSCORE, ZRANK, ZRANGE, ZCOUNT, ZCARD, ZREM)
// ============================================================================

std::string handleZAdd(const std::vector<std::string>& args) {
    // ZADD - sorted set mein member add karo with score
    // (ZADD - add member to sorted set with score)
    if (args.size() < 4 || (args.size() - 2) % 2 != 0) {
        return respError("ERR wrong number of arguments for 'zadd' command");
    }
    
    std::unique_lock lock(gDataMutex);
    const std::string& key = args[1];
    checkAndDeleteExpired(key);
    
    auto& val = gData[key];
    if (val.type != DataType::NONE && val.type != DataType::ZSET) {
        return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    
    val.type = DataType::ZSET;
    int64_t added = 0;
    
    for (size_t i = 2; i + 1 < args.size(); i += 2) {
        double score = std::stod(args[i]);
        const std::string& member = args[i + 1];
        
        // Check agar member pehle se hai
        auto it = val.zsetScores.find(member);
        if (it != val.zsetScores.end()) {
            // Remove old entry from sorted multimap
            double oldScore = it->second;
            auto range = val.zsetByScore.equal_range(oldScore);
            for (auto sit = range.first; sit != range.second; ++sit) {
                if (sit->second == member) {
                    val.zsetByScore.erase(sit);
                    break;
                }
            }
        } else {
            added++;
        }
        
        val.zsetScores[member] = score;
        val.zsetByScore.insert({score, member});
    }
    
    return respInteger(added);
}

std::string handleZScore(const std::vector<std::string>& args) {
    // ZSCORE - member da score lo
    // (ZSCORE - get score of member)
    if (args.size() < 3) {
        return respError("ERR wrong number of arguments for 'zscore' command");
    }
    
    std::shared_lock lock(gDataMutex);
    const std::string& key = args[1];
    const std::string& member = args[2];
    
    auto it = gData.find(key);
    if (it == gData.end() || it->second.isExpired()) {
        return respNull();
    }
    
    if (it->second.type != DataType::ZSET) {
        return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    
    auto sit = it->second.zsetScores.find(member);
    if (sit == it->second.zsetScores.end()) {
        return respNull();
    }
    
    // Return score as bulk string
    std::ostringstream oss;
    oss << std::setprecision(17) << sit->second;
    return respBulkString(oss.str());
}

std::string handleZRank(const std::vector<std::string>& args) {
    // ZRANK - member da rank lo (0-based)
    // (ZRANK - get rank of member, 0-based)
    if (args.size() < 3) {
        return respError("ERR wrong number of arguments for 'zrank' command");
    }
    
    std::shared_lock lock(gDataMutex);
    const std::string& key = args[1];
    const std::string& member = args[2];
    
    auto it = gData.find(key);
    if (it == gData.end() || it->second.isExpired()) {
        return respNull();
    }
    
    if (it->second.type != DataType::ZSET) {
        return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    
    auto scoreIt = it->second.zsetScores.find(member);
    if (scoreIt == it->second.zsetScores.end()) {
        return respNull();
    }
    
    // Count rank
    int64_t rank = 0;
    for (const auto& [score, mem] : it->second.zsetByScore) {
        if (mem == member) {
            return respInteger(rank);
        }
        rank++;
    }
    
    return respNull();
}

std::string handleZRange(const std::vector<std::string>& args) {
    // ZRANGE - sorted set de members by rank
    // (ZRANGE - get members by rank range)
    if (args.size() < 4) {
        return respError("ERR wrong number of arguments for 'zrange' command");
    }
    
    std::shared_lock lock(gDataMutex);
    const std::string& key = args[1];
    int64_t start = std::stoll(args[2]);
    int64_t stop = std::stoll(args[3]);
    
    bool withScores = false;
    if (args.size() >= 5 && toUpper(args[4]) == "WITHSCORES") {
        withScores = true;
    }
    
    auto it = gData.find(key);
    if (it == gData.end() || it->second.isExpired()) {
        return respArray({});
    }
    
    if (it->second.type != DataType::ZSET) {
        return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    
    int64_t len = it->second.zsetByScore.size();
    
    // Handle negative indexes
    if (start < 0) start = len + start;
    if (stop < 0) stop = len + stop;
    if (start < 0) start = 0;
    if (stop >= len) stop = len - 1;
    
    std::vector<std::string> result;
    int64_t idx = 0;
    
    for (const auto& [score, member] : it->second.zsetByScore) {
        if (idx >= start && idx <= stop) {
            result.push_back(respBulkString(member));
            if (withScores) {
                std::ostringstream oss;
                oss << std::setprecision(17) << score;
                result.push_back(respBulkString(oss.str()));
            }
        }
        idx++;
        if (idx > stop) break;
    }
    
    return respArray(result);
}

std::string handleZCount(const std::vector<std::string>& args) {
    // ZCOUNT - count members in score range
    // (ZCOUNT - score range mein kitne members)
    if (args.size() < 4) {
        return respError("ERR wrong number of arguments for 'zcount' command");
    }
    
    std::shared_lock lock(gDataMutex);
    const std::string& key = args[1];
    
    auto it = gData.find(key);
    if (it == gData.end() || it->second.isExpired()) {
        return respInteger(0);
    }
    
    if (it->second.type != DataType::ZSET) {
        return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    
    // Parse min/max (handle -inf, +inf, (exclusive)
    double minScore, maxScore;
    bool minExclusive = false, maxExclusive = false;
    
    std::string minStr = args[2];
    std::string maxStr = args[3];
    
    if (minStr[0] == '(') {
        minExclusive = true;
        minStr = minStr.substr(1);
    }
    if (maxStr[0] == '(') {
        maxExclusive = true;
        maxStr = maxStr.substr(1);
    }
    
    if (minStr == "-inf") {
        minScore = -std::numeric_limits<double>::infinity();
    } else if (minStr == "+inf") {
        minScore = std::numeric_limits<double>::infinity();
    } else {
        minScore = std::stod(minStr);
    }
    
    if (maxStr == "-inf") {
        maxScore = -std::numeric_limits<double>::infinity();
    } else if (maxStr == "+inf") {
        maxScore = std::numeric_limits<double>::infinity();
    } else {
        maxScore = std::stod(maxStr);
    }
    
    int64_t count = 0;
    for (const auto& [score, member] : it->second.zsetByScore) {
        bool inRange = true;
        if (minExclusive) {
            inRange = inRange && (score > minScore);
        } else {
            inRange = inRange && (score >= minScore);
        }
        if (maxExclusive) {
            inRange = inRange && (score < maxScore);
        } else {
            inRange = inRange && (score <= maxScore);
        }
        if (inRange) count++;
    }
    
    return respInteger(count);
}

std::string handleZCard(const std::vector<std::string>& args) {
    // ZCARD - sorted set di size
    // (ZCARD - size of sorted set)
    if (args.size() < 2) {
        return respError("ERR wrong number of arguments for 'zcard' command");
    }
    
    std::shared_lock lock(gDataMutex);
    const std::string& key = args[1];
    
    auto it = gData.find(key);
    if (it == gData.end() || it->second.isExpired()) {
        return respInteger(0);
    }
    
    if (it->second.type != DataType::ZSET) {
        return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    
    return respInteger(it->second.zsetScores.size());
}

std::string handleZRem(const std::vector<std::string>& args) {
    // ZREM - sorted set vichon member hata do
    // (ZREM - remove member from sorted set)
    if (args.size() < 3) {
        return respError("ERR wrong number of arguments for 'zrem' command");
    }
    
    std::unique_lock lock(gDataMutex);
    const std::string& key = args[1];
    
    auto it = gData.find(key);
    if (it == gData.end() || it->second.isExpired()) {
        return respInteger(0);
    }
    
    if (it->second.type != DataType::ZSET) {
        return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    
    int64_t removed = 0;
    for (size_t i = 2; i < args.size(); i++) {
        const std::string& member = args[i];
        auto scoreIt = it->second.zsetScores.find(member);
        if (scoreIt != it->second.zsetScores.end()) {
            double score = scoreIt->second;
            
            // Remove from multimap
            auto range = it->second.zsetByScore.equal_range(score);
            for (auto sit = range.first; sit != range.second; ++sit) {
                if (sit->second == member) {
                    it->second.zsetByScore.erase(sit);
                    break;
                }
            }
            
            it->second.zsetScores.erase(scoreIt);
            removed++;
        }
    }
    
    if (it->second.zsetScores.empty()) {
        gData.erase(it);
    }
    
    return respInteger(removed);
}

// ============================================================================
// DEL AND EXISTS COMMANDS
// ============================================================================

std::string handleDel(const std::vector<std::string>& args) {
    // DEL - keys hata do
    // (DEL - delete keys)
    if (args.size() < 2) {
        return respError("ERR wrong number of arguments for 'del' command");
    }
    
    std::unique_lock lock(gDataMutex);
    int64_t deleted = 0;
    
    for (size_t i = 1; i < args.size(); i++) {
        deleted += gData.erase(args[i]);
    }
    
    return respInteger(deleted);
}

std::string handleExists(const std::vector<std::string>& args) {
    // EXISTS - check karo key hai ki nahi
    // (EXISTS - check if key exists)
    if (args.size() < 2) {
        return respError("ERR wrong number of arguments for 'exists' command");
    }
    
    std::shared_lock lock(gDataMutex);
    int64_t count = 0;
    
    for (size_t i = 1; i < args.size(); i++) {
        auto it = gData.find(args[i]);
        if (it != gData.end() && !it->second.isExpired()) {
            count++;
        }
    }
    
    return respInteger(count);
}

// Stream commands
std::string handleXAdd(const std::vector<std::string>& args) {
    if (args.size() < 5) {
        return respError("ERR wrong number of arguments for 'xadd' command");
    }
    
    std::unique_lock lock(gDataMutex);
    const std::string& key = args[1];
    std::string id = args[2];
    
    auto& val = gData[key];
    if (val.type != DataType::NONE && val.type != DataType::STREAM) {
        return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    val.type = DataType::STREAM;
    
    uint64_t timestamp, sequence;
    
    if (id == "*") {
        timestamp = getCurrentTimeMs();
        if (timestamp == val.streamLastTimestamp) {
            sequence = val.streamLastSequence + 1;
        } else if (timestamp <= val.streamLastTimestamp) {
            timestamp = val.streamLastTimestamp;
            sequence = val.streamLastSequence + 1;
        } else {
            sequence = 0;
        }
    } else {
        auto dashPos = id.find('-');
        if (dashPos == std::string::npos) {
            return respError("ERR Invalid stream ID specified as stream command argument");
        }
        
        std::string tsStr = id.substr(0, dashPos);
        std::string seqStr = id.substr(dashPos + 1);
        
        if (seqStr == "*") {
            timestamp = std::stoull(tsStr);
            if (timestamp == val.streamLastTimestamp) {
                sequence = val.streamLastSequence + 1;
            } else if (timestamp > val.streamLastTimestamp) {
                sequence = 0;
            } else {
                return respError("ERR The ID specified in XADD is equal or smaller than the target stream top item");
            }
        } else {
            timestamp = std::stoull(tsStr);
            sequence = std::stoull(seqStr);
        }
        
        if (timestamp == 0 && sequence == 0) {
            return respError("ERR The ID specified in XADD must be greater than 0-0");
        }
        
        if (timestamp < val.streamLastTimestamp ||
            (timestamp == val.streamLastTimestamp && sequence <= val.streamLastSequence)) {
            return respError("ERR The ID specified in XADD is equal or smaller than the target stream top item");
        }
    }
    
    StreamEntry entry;
    entry.timestamp = timestamp;
    entry.sequence = sequence;
    entry.id = std::to_string(timestamp) + "-" + std::to_string(sequence);
    
    for (size_t i = 3; i + 1 < args.size(); i += 2) {
        entry.fields.emplace_back(args[i], args[i + 1]);
    }
    
    val.streamValue.push_back(entry);
    val.streamLastTimestamp = timestamp;
    val.streamLastSequence = sequence;
    
    return respBulkString(entry.id);
}

std::string handleXRange(const std::vector<std::string>& args) {
    if (args.size() < 4) {
        return respError("ERR wrong number of arguments for 'xrange' command");
    }
    
    std::shared_lock lock(gDataMutex);
    const std::string& key = args[1];
    std::string start = args[2];
    std::string end = args[3];
    
    auto it = gData.find(key);
    if (it == gData.end() || it->second.isExpired()) {
        return respArray({});
    }
    
    if (it->second.type != DataType::STREAM) {
        return respError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    
    if (start == "-") start = "0-0";
    if (end == "+") end = "18446744073709551615-18446744073709551615";
    
    std::vector<std::string> result;
    for (const auto& entry : it->second.streamValue) {
        bool include = true;
        
        // Simple ID comparison
        if (start != "-") {
            auto [ts1, seq1] = std::make_pair(entry.timestamp, entry.sequence);
            auto dashPos = start.find('-');
            if (dashPos != std::string::npos) {
                uint64_t startTs = std::stoull(start.substr(0, dashPos));
                uint64_t startSeq = std::stoull(start.substr(dashPos + 1));
                if (ts1 < startTs || (ts1 == startTs && seq1 < startSeq)) {
                    include = false;
                }
            }
        }
        
        if (end != "+") {
            auto [ts1, seq1] = std::make_pair(entry.timestamp, entry.sequence);
            auto dashPos = end.find('-');
            if (dashPos != std::string::npos) {
                uint64_t endTs = std::stoull(end.substr(0, dashPos));
                uint64_t endSeq = std::stoull(end.substr(dashPos + 1));
                if (ts1 > endTs || (ts1 == endTs && seq1 > endSeq)) {
                    include = false;
                }
            }
        }
        
        if (include) {
            std::vector<std::string> entryArr;
            entryArr.push_back(respBulkString(entry.id));
            
            std::vector<std::string> fieldsArr;
            for (const auto& [field, value] : entry.fields) {
                fieldsArr.push_back(respBulkString(field));
                fieldsArr.push_back(respBulkString(value));
            }
            entryArr.push_back(respArray(fieldsArr));
            
            result.push_back(respArray(entryArr));
        }
    }
    
    return respArray(result);
}

std::string handleXRead(const std::vector<std::string>& args) {
    // Parse XREAD args
    size_t streamsIdx = 0;
    int64_t blockMs = -1;
    int64_t count = -1;
    
    for (size_t i = 1; i < args.size(); i++) {
        std::string opt = toUpper(args[i]);
        if (opt == "BLOCK" && i + 1 < args.size()) {
            blockMs = std::stoll(args[++i]);
        } else if (opt == "COUNT" && i + 1 < args.size()) {
            count = std::stoll(args[++i]);
        } else if (opt == "STREAMS") {
            streamsIdx = i + 1;
            break;
        }
    }
    
    if (streamsIdx == 0 || streamsIdx >= args.size()) {
        return respError("ERR syntax error");
    }
    
    size_t numStreams = (args.size() - streamsIdx) / 2;
    std::vector<std::string> keys;
    std::vector<std::string> ids;
    
    for (size_t i = 0; i < numStreams; i++) {
        keys.push_back(args[streamsIdx + i]);
        ids.push_back(args[streamsIdx + numStreams + i]);
    }
    
    auto readStreams = [&]() -> std::string {
        std::shared_lock lock(gDataMutex);
        std::vector<std::string> result;
        
        for (size_t i = 0; i < keys.size(); i++) {
            auto it = gData.find(keys[i]);
            if (it == gData.end() || it->second.type != DataType::STREAM) {
                continue;
            }
            
            std::string startId = ids[i];
            if (startId == "$") {
                // Start from newest
                if (it->second.streamValue.empty()) {
                    startId = "0-0";
                } else {
                    startId = it->second.streamValue.back().id;
                }
            }
            
            std::vector<std::string> entries;
            int64_t cnt = 0;
            
            for (const auto& entry : it->second.streamValue) {
                // Compare IDs
                auto dashPos1 = entry.id.find('-');
                auto dashPos2 = startId.find('-');
                
                uint64_t ts1 = std::stoull(entry.id.substr(0, dashPos1));
                uint64_t seq1 = std::stoull(entry.id.substr(dashPos1 + 1));
                uint64_t ts2 = std::stoull(startId.substr(0, dashPos2));
                uint64_t seq2 = std::stoull(startId.substr(dashPos2 + 1));
                
                if (ts1 > ts2 || (ts1 == ts2 && seq1 > seq2)) {
                    std::vector<std::string> entryArr;
                    entryArr.push_back(respBulkString(entry.id));
                    
                    std::vector<std::string> fieldsArr;
                    for (const auto& [field, value] : entry.fields) {
                        fieldsArr.push_back(respBulkString(field));
                        fieldsArr.push_back(respBulkString(value));
                    }
                    entryArr.push_back(respArray(fieldsArr));
                    entries.push_back(respArray(entryArr));
                    
                    cnt++;
                    if (count > 0 && cnt >= count) break;
                }
            }
            
            if (!entries.empty()) {
                std::vector<std::string> streamArr;
                streamArr.push_back(respBulkString(keys[i]));
                streamArr.push_back(respArray(entries));
                result.push_back(respArray(streamArr));
            }
        }
        
        if (result.empty()) {
            return "";
        }
        return respArray(result);
    };
    
    std::string response = readStreams();
    if (!response.empty()) {
        return response;
    }
    
    if (blockMs >= 0) {
        // Blocking read
        auto deadline = std::chrono::steady_clock::now() + 
                        std::chrono::milliseconds(blockMs == 0 ? 100000000 : blockMs);
        
        while (std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            response = readStreams();
            if (!response.empty()) {
                return response;
            }
        }
    }
    
    return respNullArray();
}

// Replication commands
std::string handleReplConf(const std::vector<std::string>& args) {
    // Just ACK for now
    return respSimpleString("OK");
}

std::string handlePSync(const std::vector<std::string>& args, int clientFd) {
    // Send FULLRESYNC
    std::string response = "+FULLRESYNC " + gReplId + " 0\r\n";
    write(clientFd, response.c_str(), response.length());
    
    // Send empty RDB
    std::string rdb;
    rdb += "REDIS0011";  // Magic + version
    rdb += "\xfa";  // AUX
    rdb += "\x09redis-ver\x05""7.2.0";
    rdb += "\xff";  // EOF
    
    // Add 8 zero bytes for checksum
    rdb += std::string(8, '\0');
    
    std::string rdbResp = "$" + std::to_string(rdb.length()) + "\r\n" + rdb;
    write(clientFd, rdbResp.c_str(), rdbResp.length());
    
    // Add to replicas list
    {
        std::lock_guard<std::mutex> lock(gReplicaMutex);
        gReplicas.push_back(clientFd);
    }
    
    return "";  // Already sent response
}

std::string handleWait(const std::vector<std::string>& args) {
    if (args.size() < 3) {
        return respError("ERR wrong number of arguments for 'wait' command");
    }
    
    int numReplicas = std::stoi(args[1]);
    int64_t timeout = std::stoll(args[2]);
    
    // Send REPLCONF GETACK to all replicas
    std::string getack = encodeRespArray({"REPLCONF", "GETACK", "*"});
    
    {
        std::lock_guard<std::mutex> lock(gReplicaMutex);
        for (int fd : gReplicas) {
            write(fd, getack.c_str(), getack.length());
        }
    }
    
    // Wait for timeout
    std::this_thread::sleep_for(std::chrono::milliseconds(timeout));
    
    // Return number of connected replicas
    std::lock_guard<std::mutex> lock(gReplicaMutex);
    return respInteger(std::min(static_cast<int>(gReplicas.size()), numReplicas));
}

// ============================================================================
// COMMAND DISPATCHER
// Command dispatch karo - right handler nu bhejo
// (Dispatch command to the right handler)
// ============================================================================

std::string handleCommand(const std::vector<std::string>& args, int clientFd) {
    if (args.empty()) {
        return respError("ERR empty command");
    }
    
    std::string cmd = toUpper(args[0]);
    
    // Check if in MULTI mode - queue commands instead of executing
    // Agar MULTI mode mein hai te commands queue karo
    if (gClientInMulti[clientFd] && cmd != "EXEC" && cmd != "DISCARD" && cmd != "MULTI") {
        gClientQueues[clientFd].push_back(args);
        return respSimpleString("QUEUED");
    }
    
    // String commands
    if (cmd == "PING") return handlePing(args);
    if (cmd == "ECHO") return handleEcho(args);
    if (cmd == "SET") return handleSet(args);
    if (cmd == "GET") return handleGet(args);
    if (cmd == "INCR") return handleIncr(args);
    if (cmd == "DECR") return handleDecr(args);
    if (cmd == "DEL") return handleDel(args);
    if (cmd == "EXISTS") return handleExists(args);
    if (cmd == "TYPE") return handleType(args);
    if (cmd == "KEYS") return handleKeys(args);
    if (cmd == "CONFIG") return handleConfig(args);
    if (cmd == "INFO") return handleInfo(args);
    
    // List commands
    if (cmd == "LPUSH") return handleLPush(args);
    if (cmd == "RPUSH") return handleRPush(args);
    if (cmd == "LRANGE") return handleLRange(args);
    if (cmd == "LLEN") return handleLLen(args);
    if (cmd == "LPOP") return handleLPop(args);
    if (cmd == "RPOP") return handleRPop(args);
    if (cmd == "BLPOP") return handleBLPop(args, clientFd);
    if (cmd == "BRPOP") return handleBRPop(args, clientFd);
    
    // Set commands
    if (cmd == "SADD") return handleSAdd(args);
    if (cmd == "SMEMBERS") return handleSMembers(args);
    if (cmd == "SISMEMBER") return handleSIsMember(args);
    if (cmd == "SREM") return handleSRem(args);
    if (cmd == "SCARD") return handleSCard(args);
    
    // Hash commands
    if (cmd == "HSET") return handleHSet(args);
    if (cmd == "HGET") return handleHGet(args);
    if (cmd == "HGETALL") return handleHGetAll(args);
    if (cmd == "HDEL") return handleHDel(args);
    if (cmd == "HEXISTS") return handleHExists(args);
    if (cmd == "HLEN") return handleHLen(args);
    
    // Sorted set commands
    if (cmd == "ZADD") return handleZAdd(args);
    if (cmd == "ZSCORE") return handleZScore(args);
    if (cmd == "ZRANK") return handleZRank(args);
    if (cmd == "ZRANGE") return handleZRange(args);
    if (cmd == "ZCOUNT") return handleZCount(args);
    if (cmd == "ZCARD") return handleZCard(args);
    if (cmd == "ZREM") return handleZRem(args);
    
    // Stream commands
    if (cmd == "XADD") return handleXAdd(args);
    if (cmd == "XRANGE") return handleXRange(args);
    if (cmd == "XREAD") return handleXRead(args);
    
    // Transaction commands
    if (cmd == "MULTI") return handleMulti(args, clientFd);
    if (cmd == "EXEC") return handleExec(args, clientFd);
    if (cmd == "DISCARD") return handleDiscard(args, clientFd);
    
    // Replication commands
    if (cmd == "REPLCONF") return handleReplConf(args);
    if (cmd == "PSYNC") return handlePSync(args, clientFd);
    if (cmd == "WAIT") return handleWait(args);
    
    return respError("ERR unknown command '" + cmd + "'");
}

// ============================================================================
// MAIN FUNCTION
// Main function - jithon sab shuru hunda hai
// (Main function - where everything begins)
// ============================================================================

int main(int argc, char **argv) {
    // Flush after every std::cout / std::cerr
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;
    
    // Ignore SIGPIPE
    signal(SIGPIPE, SIG_IGN);
    
    // Parse command line arguments
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--port" && i + 1 < argc) {
            gConfig.port = std::stoi(argv[++i]);
        } else if (arg == "--dir" && i + 1 < argc) {
            gConfig.dir = argv[++i];
        } else if (arg == "--dbfilename" && i + 1 < argc) {
            gConfig.dbfilename = argv[++i];
        } else if (arg == "--replicaof" && i + 2 < argc) {
            gConfig.isReplica = true;
            gConfig.masterHost = argv[++i];
            gConfig.masterPort = std::stoi(argv[++i]);
        }
    }
    
    std::cout << "Starting Redis server on port " << gConfig.port << std::endl;
    
    // Load RDB
    loadRdb();
    
    // Create server socket
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create socket\n";
        return 1;
    }
    
    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    
    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(gConfig.port);
    
    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) != 0) {
        std::cerr << "Failed to bind to port " << gConfig.port << "\n";
        return 1;
    }
    
    if (listen(server_fd, SOMAXCONN) != 0) {
        std::cerr << "Listen failed\n";
        return 1;
    }
    
    // Set non-blocking
    fcntl(server_fd, F_SETFL, O_NONBLOCK);
    
    // Connect to master if replica
    int masterFd = -1;
    if (gConfig.isReplica) {
        masterFd = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in master_addr;
        master_addr.sin_family = AF_INET;
        master_addr.sin_port = htons(gConfig.masterPort);
        inet_pton(AF_INET, gConfig.masterHost.c_str(), &master_addr.sin_addr);
        
        if (connect(masterFd, (struct sockaddr *)&master_addr, sizeof(master_addr)) == 0) {
            std::cout << "Connected to master\n";
            
            // Handshake
            std::string ping = encodeRespArray({"PING"});
            write(masterFd, ping.c_str(), ping.length());
            
            char buf[1024];
            read(masterFd, buf, sizeof(buf));
            
            std::string replconf1 = encodeRespArray({"REPLCONF", "listening-port", std::to_string(gConfig.port)});
            write(masterFd, replconf1.c_str(), replconf1.length());
            read(masterFd, buf, sizeof(buf));
            
            std::string replconf2 = encodeRespArray({"REPLCONF", "capa", "psync2"});
            write(masterFd, replconf2.c_str(), replconf2.length());
            read(masterFd, buf, sizeof(buf));
            
            std::string psync = encodeRespArray({"PSYNC", "?", "-1"});
            write(masterFd, psync.c_str(), psync.length());
            
            // Read FULLRESYNC response
            read(masterFd, buf, sizeof(buf));
            
            fcntl(masterFd, F_SETFL, O_NONBLOCK);
        }
    }
    
    // Create epoll
    int epoll_fd = epoll_create1(0);
    
    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);
    
    if (masterFd >= 0) {
        ev.events = EPOLLIN;
        ev.data.fd = masterFd;
        epoll_ctl(epoll_fd, EPOLL_CTL_ADD, masterFd, &ev);
    }
    
    std::map<int, std::string> clientBuffers;
    
    struct epoll_event events[64];
    
    std::cout << "Server ready to accept connections\n";
    
    while (true) {
        int nfds = epoll_wait(epoll_fd, events, 64, 100);
        
        for (int i = 0; i < nfds; i++) {
            int fd = events[i].data.fd;
            
            if (fd == server_fd) {
                // Accept new connection
                struct sockaddr_in client_addr;
                socklen_t client_len = sizeof(client_addr);
                int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
                
                if (client_fd >= 0) {
                    fcntl(client_fd, F_SETFL, O_NONBLOCK);
                    
                    ev.events = EPOLLIN;
                    ev.data.fd = client_fd;
                    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev);
                    
                    clientBuffers[client_fd] = "";
                }
            } else {
                // Read from client
                char buffer[4096];
                ssize_t n = read(fd, buffer, sizeof(buffer));
                
                if (n <= 0) {
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
                    close(fd);
                    clientBuffers.erase(fd);
                    continue;
                }
                
                clientBuffers[fd] += std::string(buffer, n);
                
                // Process commands
                while (true) {
                    size_t consumed = 0;
                    auto args = parseRespCommand(clientBuffers[fd], consumed);
                    
                    if (args.empty()) break;
                    
                    clientBuffers[fd] = clientBuffers[fd].substr(consumed);
                    
                    // Skip processing if from master
                    if (fd == masterFd) {
                        std::string cmd = toUpper(args[0]);
                        if (cmd == "SET" || cmd == "XADD") {
                            handleCommand(args, fd);
                        }
                        continue;
                    }
                    
                    std::string response = handleCommand(args, fd);
                    
                    if (!response.empty()) {
                        write(fd, response.c_str(), response.length());
                    }
                    
                    // Propagate to replicas for write commands
                    std::string cmd = toUpper(args[0]);
                    if (!gConfig.isReplica && (cmd == "SET" || cmd == "XADD")) {
                        std::string encoded = encodeRespArray(args);
                        std::lock_guard<std::mutex> lock(gReplicaMutex);
                        for (int replicaFd : gReplicas) {
                            write(replicaFd, encoded.c_str(), encoded.length());
                        }
                    }
                }
            }
        }
    }
    
    return 0;
}
