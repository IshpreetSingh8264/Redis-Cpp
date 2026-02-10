/**
 * =============================================================================
 *                          RDB PERSISTENCE
 * =============================================================================
 * 
 * Paaji eh file hai RDB persistence di - data save/load
 * (Bro this file is for RDB persistence - save and load data)
 * 
 * Redis Dump Binary - snapshot of database at a point in time
 * (Binary format for saving Redis data to disk)
 * 
 * Jiven photo khichde ho - us moment da snapshot
 * (Like taking a photo - snapshot of that moment)
 * 
 * =============================================================================
 */

#pragma once

#include "common.hpp"
#include "data_store.hpp"
#include <fstream>
#include <filesystem>

namespace Redis {

// ============================================================================
// RDB OPCODES
// RDB file de opcodes - ki matlab hai har byte da
// (RDB file opcodes - what each byte means)
// ============================================================================

namespace RdbOpcode {
    // End of file
    constexpr uint8_t EOF_CODE = 0xFF;
    
    // Select DB
    constexpr uint8_t SELECTDB = 0xFE;
    
    // Expiry time in seconds
    constexpr uint8_t EXPIRETIME = 0xFD;
    
    // Expiry time in milliseconds
    constexpr uint8_t EXPIRETIME_MS = 0xFC;
    
    // Hash table size info
    constexpr uint8_t RESIZEDB = 0xFB;
    
    // Aux field (metadata)
    constexpr uint8_t AUX = 0xFA;
    
    // Module data
    constexpr uint8_t MODULE_AUX = 0xF7;
    
    // LRU idle time
    constexpr uint8_t IDLE = 0xF8;
    
    // LFU frequency
    constexpr uint8_t FREQ = 0xF9;
}

namespace RdbType {
    constexpr uint8_t STRING = 0;
    constexpr uint8_t LIST = 1;
    constexpr uint8_t SET = 2;
    constexpr uint8_t ZSET = 3;
    constexpr uint8_t HASH = 4;
    
    // Encoded types
    constexpr uint8_t HASH_ZIPMAP = 9;
    constexpr uint8_t LIST_ZIPLIST = 10;
    constexpr uint8_t SET_INTSET = 11;
    constexpr uint8_t ZSET_ZIPLIST = 12;
    constexpr uint8_t HASH_ZIPLIST = 13;
    constexpr uint8_t LIST_QUICKLIST = 14;
    constexpr uint8_t STREAM_LISTPACKS = 15;
}

// ============================================================================
// RDB READER
// RDB file read karo - binary parsing
// (Read RDB file - binary data parsing)
// ============================================================================

/**
 * @class RdbReader
 * @brief Reads RDB files and loads into DataStore
 * 
 * RDB file parse karke DataStore mein load karo
 * (Parse RDB file and load into DataStore)
 * 
 * Binary data hai - byte by byte read karna
 * (Binary data - read byte by byte)
 */
class RdbReader {
private:
    std::ifstream file_;
    std::string dir_;
    std::string filename_;
    
public:
    RdbReader(const std::string& dir, const std::string& filename)
        : dir_(dir), filename_(filename) {
    }
    
    /**
     * Load RDB file into DataStore
     * RDB file load karo DataStore mein
     * (Load RDB file into DataStore)
     */
    bool load(DataStore& store) {
        // Construct full path
        // Full path banao
        // (Construct full file path)
        std::filesystem::path path = std::filesystem::path(dir_) / filename_;
        
        if (!std::filesystem::exists(path)) {
            // File nahi hai - no problem
            // (File doesn't exist - that's okay)
            std::cerr << "RDB file not found: " << path << " (starting fresh)" << std::endl;
            return false;
        }
        
        file_.open(path, std::ios::binary);
        if (!file_.is_open()) {
            std::cerr << "Failed to open RDB file: " << path << std::endl;
            return false;
        }
        
        try {
            // Read and verify magic string
            // Magic string check karo - "REDIS"
            // (Verify magic string - should be "REDIS")
            char magic[5];
            file_.read(magic, 5);
            if (std::string(magic, 5) != "REDIS") {
                std::cerr << "Invalid RDB file magic string" << std::endl;
                return false;
            }
            
            // Read version
            // Version read karo
            // (Read RDB version)
            char version[4];
            file_.read(version, 4);
            int rdbVersion = std::stoi(std::string(version, 4));
            
            std::cout << "Loading RDB version " << rdbVersion << " from " << path << std::endl;
            
            // Parse RDB content
            // RDB content parse karo
            // (Parse the actual RDB content)
            return parseRdbContent(store, rdbVersion);
            
        } catch (const std::exception& e) {
            std::cerr << "Error parsing RDB file: " << e.what() << std::endl;
            return false;
        }
    }

private:
    /**
     * Parse RDB file content
     * RDB content parse karo - opcodes te values
     * (Parse RDB content - opcodes and values)
     */
    bool parseRdbContent(DataStore& store, int version) {
        int currentDb = 0;
        int64_t currentExpiry = -1;  // No expiry by default
        
        while (file_.good() && !file_.eof()) {
            uint8_t opcode;
            if (!readByte(opcode)) {
                break;
            }
            
            switch (opcode) {
                case RdbOpcode::EOF_CODE: {
                    // End of file
                    // File khatam - checksum skip karo
                    // (End of file - skip checksum for now)
                    std::cout << "RDB load complete" << std::endl;
                    return true;
                }
                
                case RdbOpcode::SELECTDB: {
                    // Select database
                    // Database select karo
                    // (Select database number)
                    currentDb = readLength();
                    break;
                }
                
                case RdbOpcode::RESIZEDB: {
                    // DB size hints
                    // DB size hints - skip karo
                    // (DB size hints - just skip them)
                    readLength();  // db size
                    readLength();  // expires size
                    break;
                }
                
                case RdbOpcode::AUX: {
                    // Aux field (metadata)
                    // Metadata field - key-value pair
                    // (Metadata field - redis-ver, ctime, etc.)
                    std::string auxKey = readString();
                    std::string auxValue = readString();
                    std::cout << "RDB aux: " << auxKey << " = " << auxValue << std::endl;
                    break;
                }
                
                case RdbOpcode::EXPIRETIME: {
                    // Expiry in seconds
                    // Expiry seconds mein - 4 bytes
                    // (Expiry in seconds - 4 bytes)
                    uint32_t expiry;
                    file_.read(reinterpret_cast<char*>(&expiry), 4);
                    currentExpiry = static_cast<int64_t>(expiry) * 1000;  // Convert to ms
                    break;
                }
                
                case RdbOpcode::EXPIRETIME_MS: {
                    // Expiry in milliseconds
                    // Expiry milliseconds mein - 8 bytes
                    // (Expiry in milliseconds - 8 bytes)
                    file_.read(reinterpret_cast<char*>(&currentExpiry), 8);
                    break;
                }
                
                default: {
                    // This should be a value type
                    // Eh value type hona chahida - key-value pair
                    // (This should be a value type - actual data)
                    
                    // Read key
                    std::string key = readString();
                    
                    // Read value based on type
                    // Value type de hisab naal read karo
                    // (Read value based on type)
                    if (!readValue(store, opcode, key, currentExpiry)) {
                        std::cerr << "Failed to read value for key: " << key << std::endl;
                        return false;
                    }
                    
                    // Reset expiry for next key
                    currentExpiry = -1;
                    break;
                }
            }
        }
        
        return true;
    }
    
    /**
     * Read value based on type and store it
     * Value read karke store karo
     * (Read value and store in DataStore)
     */
    bool readValue(DataStore& store, uint8_t type, const std::string& key, int64_t expiry) {
        switch (type) {
            case RdbType::STRING: {
                // Simple string
                // Simple string - seedhi baat
                // (Simple string - straightforward)
                std::string value = readString();
                store.set(key, value);
                
                // Set expiry if specified
                // Expiry set karo agar di gayi hai
                // (Set expiry if specified)
                if (expiry > 0) {
                    int64_t now = getCurrentTimeMs();
                    if (expiry > now) {
                        int64_t ttl = expiry - now;
                        store.pexpire(key, ttl);
                    } else {
                        // Already expired
                        // Pehle hi expire ho chuki hai
                        // (Already expired - delete it)
                        store.del({key});
                    }
                }
                return true;
            }
            
            case RdbType::LIST:
            case RdbType::LIST_QUICKLIST: {
                // List
                // List data - multiple elements
                // (List data - multiple elements)
                uint64_t len = readLength();
                std::vector<std::string> values;
                for (uint64_t i = 0; i < len; i++) {
                    values.push_back(readString());
                }
                
                // Use rpush to add elements
                // rpush use karke elements add karo
                // (Use rpush to add elements)
                store.rpush(key, values);
                
                if (expiry > 0) {
                    int64_t now = getCurrentTimeMs();
                    if (expiry > now) {
                        store.pexpire(key, expiry - now);
                    }
                }
                return true;
            }
            
            case RdbType::SET:
            case RdbType::SET_INTSET: {
                // Set
                // Set data - unique elements
                // (Set data - unique elements)
                uint64_t len = readLength();
                std::vector<std::string> members;
                for (uint64_t i = 0; i < len; i++) {
                    members.push_back(readString());
                }
                
                store.sadd(key, members);
                
                if (expiry > 0) {
                    int64_t now = getCurrentTimeMs();
                    if (expiry > now) {
                        store.pexpire(key, expiry - now);
                    }
                }
                return true;
            }
            
            case RdbType::HASH:
            case RdbType::HASH_ZIPLIST: {
                // Hash
                // Hash data - field-value pairs
                // (Hash data - field-value pairs)
                uint64_t len = readLength();
                std::vector<std::pair<std::string, std::string>> entries;
                for (uint64_t i = 0; i < len; i++) {
                    std::string field = readString();
                    std::string value = readString();
                    entries.emplace_back(field, value);
                }
                
                store.hset(key, entries);
                
                if (expiry > 0) {
                    int64_t now = getCurrentTimeMs();
                    if (expiry > now) {
                        store.pexpire(key, expiry - now);
                    }
                }
                return true;
            }
            
            case RdbType::ZSET:
            case RdbType::ZSET_ZIPLIST: {
                // Sorted Set
                // Sorted set - score-member pairs
                // (Sorted set - score-member pairs)
                uint64_t len = readLength();
                std::vector<std::pair<double, std::string>> scoreMembers;
                for (uint64_t i = 0; i < len; i++) {
                    std::string member = readString();
                    double score = readDouble();
                    scoreMembers.emplace_back(score, member);
                }
                
                store.zadd(key, scoreMembers, false, false, false, false, false);
                
                if (expiry > 0) {
                    int64_t now = getCurrentTimeMs();
                    if (expiry > now) {
                        store.pexpire(key, expiry - now);
                    }
                }
                return true;
            }
            
            default: {
                std::cerr << "Unknown RDB type: " << static_cast<int>(type) << std::endl;
                return false;
            }
        }
    }
    
    /**
     * Read a single byte
     * Ek byte read karo
     * (Read single byte)
     */
    bool readByte(uint8_t& byte) {
        if (file_.eof()) return false;
        file_.read(reinterpret_cast<char*>(&byte), 1);
        return file_.good();
    }
    
    /**
     * Read length-encoded integer
     * Length-encoded integer read karo
     * (Read length-encoded integer - Redis special format)
     */
    uint64_t readLength() {
        uint8_t byte;
        if (!readByte(byte)) return 0;
        
        // Check encoding type
        // Encoding type check karo - first 2 bits
        // (Check encoding type - first 2 bits determine format)
        uint8_t type = (byte & 0xC0) >> 6;
        
        switch (type) {
            case 0: {
                // 6-bit length
                // 6-bit length - 0 to 63
                return byte & 0x3F;
            }
            case 1: {
                // 14-bit length
                // 14-bit length - next byte bhi include
                // (14-bit length - include next byte)
                uint8_t nextByte;
                readByte(nextByte);
                return ((byte & 0x3F) << 8) | nextByte;
            }
            case 2: {
                // 32-bit length
                // 32-bit length - 4 more bytes
                uint32_t len;
                file_.read(reinterpret_cast<char*>(&len), 4);
                return len;
            }
            case 3: {
                // Special encoding
                // Special encoding - depends on lower 6 bits
                // (Special encoding - integer stored as string)
                uint8_t format = byte & 0x3F;
                
                if (format == 0) {
                    // 8-bit integer
                    int8_t val;
                    file_.read(reinterpret_cast<char*>(&val), 1);
                    return static_cast<uint64_t>(val);
                } else if (format == 1) {
                    // 16-bit integer
                    int16_t val;
                    file_.read(reinterpret_cast<char*>(&val), 2);
                    return static_cast<uint64_t>(val);
                } else if (format == 2) {
                    // 32-bit integer
                    int32_t val;
                    file_.read(reinterpret_cast<char*>(&val), 4);
                    return static_cast<uint64_t>(val);
                }
                
                return 0;
            }
        }
        
        return 0;
    }
    
    /**
     * Read string (length-prefixed)
     * String read karo - length first, then data
     * (Read string - length prefix followed by data)
     */
    std::string readString() {
        uint8_t byte;
        if (!readByte(byte)) return "";
        
        uint8_t type = (byte & 0xC0) >> 6;
        
        if (type == 3) {
            // Special encoding - integer as string
            // Integer stored as string
            // (Integer encoded as string)
            uint8_t format = byte & 0x3F;
            
            if (format == 0) {
                int8_t val;
                file_.read(reinterpret_cast<char*>(&val), 1);
                return std::to_string(val);
            } else if (format == 1) {
                int16_t val;
                file_.read(reinterpret_cast<char*>(&val), 2);
                return std::to_string(val);
            } else if (format == 2) {
                int32_t val;
                file_.read(reinterpret_cast<char*>(&val), 4);
                return std::to_string(val);
            } else if (format == 3) {
                // LZF compressed
                // Compressed data - skip for now
                // (LZF compressed - skipping for simplicity)
                uint64_t compLen = readLength();
                uint64_t origLen = readLength();
                
                // Skip compressed data
                std::vector<char> data(compLen);
                file_.read(data.data(), compLen);
                
                // For now, return empty (should decompress)
                return std::string(origLen, '?');
            }
            
            return "";
        }
        
        // Normal string - get length
        // Normal string - length read karke data lo
        // (Normal string - read length then data)
        uint64_t len;
        if (type == 0) {
            len = byte & 0x3F;
        } else if (type == 1) {
            uint8_t nextByte;
            readByte(nextByte);
            len = ((byte & 0x3F) << 8) | nextByte;
        } else if (type == 2) {
            uint32_t len32;
            file_.read(reinterpret_cast<char*>(&len32), 4);
            len = len32;
        } else {
            return "";
        }
        
        // Read the actual string
        // String data read karo
        // (Read string data)
        std::string result(len, '\0');
        file_.read(&result[0], len);
        return result;
    }
    
    /**
     * Read double (8 bytes)
     * Double read karo - 8 bytes
     * (Read double - 8 bytes, little endian)
     */
    double readDouble() {
        // Redis uses a specific encoding for doubles
        // Redis special encoding use karda - check first byte
        // (Redis uses special encoding - check first byte)
        uint8_t len;
        if (!readByte(len)) return 0.0;
        
        if (len == 255) {
            return std::numeric_limits<double>::quiet_NaN();
        } else if (len == 254) {
            return std::numeric_limits<double>::infinity();
        } else if (len == 253) {
            return -std::numeric_limits<double>::infinity();
        } else {
            // Read as string and convert
            std::string str(len, '\0');
            file_.read(&str[0], len);
            return std::stod(str);
        }
    }
};

// ============================================================================
// RDB WRITER
// RDB file write karo - database save
// (Write RDB file - save database to disk)
// ============================================================================

/**
 * @class RdbWriter
 * @brief Writes DataStore to RDB file
 * 
 * DataStore nu RDB file mein save karo
 * (Save DataStore to RDB file)
 * 
 * Snapshot le lo - database da photo
 * (Take a snapshot - photo of database)
 */
class RdbWriter {
private:
    std::ofstream file_;
    std::string dir_;
    std::string filename_;
    
public:
    RdbWriter(const std::string& dir, const std::string& filename)
        : dir_(dir), filename_(filename) {
    }
    
    /**
     * Save DataStore to RDB file
     * DataStore nu RDB file mein save karo
     * (Save DataStore to RDB file)
     */
    bool save(const DataStore& store) {
        // Construct full path
        std::filesystem::path path = std::filesystem::path(dir_) / filename_;
        
        // Create directory if doesn't exist
        // Directory banao agar nahi hai
        // (Create directory if it doesn't exist)
        std::filesystem::create_directories(dir_);
        
        // Write to temp file first
        // Pehle temp file mein likho
        // (Write to temp file first - for atomicity)
        std::string tempPath = path.string() + ".tmp";
        
        file_.open(tempPath, std::ios::binary);
        if (!file_.is_open()) {
            std::cerr << "Failed to create RDB file: " << tempPath << std::endl;
            return false;
        }
        
        try {
            // Write magic string and version
            // Magic string te version likho
            // (Write magic string and version)
            file_.write("REDIS", 5);
            file_.write("0011", 4);  // Version 11
            
            // Write aux fields (metadata)
            // Metadata likho
            // (Write metadata)
            writeAux("redis-ver", "7.0.0");
            writeAux("redis-bits", "64");
            writeAux("ctime", std::to_string(std::time(nullptr)));
            writeAux("used-mem", "0");
            
            // Select DB 0
            // DB 0 select karo
            // (Select database 0)
            writeByte(RdbOpcode::SELECTDB);
            writeLength(0);
            
            // Write resize info
            // Resize info likho - approximate sizes
            // (Write resize info - approximate sizes)
            writeByte(RdbOpcode::RESIZEDB);
            
            // Get all keys for size estimation
            auto allKeys = store.keys("*");
            writeLength(allKeys.size());
            
            // Count expiring keys
            int expiringKeys = 0;
            // We'd need to track this separately
            writeLength(expiringKeys);
            
            // Write each key-value
            // Har key-value pair likho
            // (Write each key-value pair)
            for (const auto& key : allKeys) {
                auto type = store.type(key);
                
                // Check and write expiry if exists
                // Expiry check karke likho
                // (Check and write expiry if exists)
                auto pttlVal = store.pttl(key);
                if (pttlVal > 0) {
                    int64_t expireTime = getCurrentTimeMs() + pttlVal;
                    writeByte(RdbOpcode::EXPIRETIME_MS);
                    file_.write(reinterpret_cast<const char*>(&expireTime), 8);
                }
                
                // Write value based on type
                // Value type de hisab naal likho
                // (Write value based on its type)
                writeKeyValue(store, key, type);
            }
            
            // Write EOF
            // End of file marker
            // (Write end of file marker)
            writeByte(RdbOpcode::EOF_CODE);
            
            // Write checksum (simplified - just 8 zero bytes)
            // Checksum likho - abhi lai zeros
            // (Write checksum - just zeros for now)
            uint64_t checksum = 0;
            file_.write(reinterpret_cast<const char*>(&checksum), 8);
            
            file_.close();
            
            // Rename temp file to final
            // Temp file nu rename karo
            // (Rename temp file to final - atomic operation)
            std::filesystem::rename(tempPath, path);
            
            std::cout << "RDB saved to " << path << std::endl;
            return true;
            
        } catch (const std::exception& e) {
            std::cerr << "Error writing RDB file: " << e.what() << std::endl;
            file_.close();
            std::filesystem::remove(tempPath);
            return false;
        }
    }

private:
    /**
     * Write aux field
     * Aux field likho - metadata
     * (Write aux/metadata field)
     */
    void writeAux(const std::string& key, const std::string& value) {
        writeByte(RdbOpcode::AUX);
        writeString(key);
        writeString(value);
    }
    
    /**
     * Write key-value based on type
     * Key-value likho type de hisab naal
     * (Write key-value based on data type)
     */
    void writeKeyValue(const DataStore& store, const std::string& key, const std::string& type) {
        if (type == "string") {
            auto value = store.get(key);
            if (value) {
                writeByte(RdbType::STRING);
                writeString(key);
                writeString(*value);
            }
        }
        else if (type == "list") {
            auto list = store.lrange(key, 0, -1);
            writeByte(RdbType::LIST);
            writeString(key);
            writeLength(list.size());
            for (const auto& elem : list) {
                writeString(elem);
            }
        }
        else if (type == "set") {
            auto members = store.smembers(key);
            writeByte(RdbType::SET);
            writeString(key);
            writeLength(members.size());
            for (const auto& member : members) {
                writeString(member);
            }
        }
        else if (type == "hash") {
            auto hash = store.hgetall(key);
            writeByte(RdbType::HASH);
            writeString(key);
            writeLength(hash.size() / 2);  // field-value pairs
            for (size_t i = 0; i < hash.size(); i += 2) {
                writeString(hash[i]);
                writeString(hash[i + 1]);
            }
        }
        else if (type == "zset") {
            auto members = store.zrange(key, 0, -1, true);
            writeByte(RdbType::ZSET);
            writeString(key);
            writeLength(members.size());
            for (const auto& [member, score] : members) {
                writeString(member);
                writeDouble(score);
            }
        }
        // Streams would need more complex handling
        // Streams lai complex handling chahiye
        // (Streams need more complex handling - skipping for now)
    }
    
    /**
     * Write single byte
     * Ek byte likho
     * (Write single byte)
     */
    void writeByte(uint8_t byte) {
        file_.write(reinterpret_cast<const char*>(&byte), 1);
    }
    
    /**
     * Write length-encoded integer
     * Length encoded integer likho
     * (Write length-encoded integer)
     */
    void writeLength(uint64_t len) {
        if (len < 64) {
            // 6-bit length
            writeByte(static_cast<uint8_t>(len));
        } else if (len < 16384) {
            // 14-bit length
            writeByte(static_cast<uint8_t>(0x40 | (len >> 8)));
            writeByte(static_cast<uint8_t>(len & 0xFF));
        } else {
            // 32-bit length
            writeByte(0x80);
            uint32_t len32 = static_cast<uint32_t>(len);
            file_.write(reinterpret_cast<const char*>(&len32), 4);
        }
    }
    
    /**
     * Write string (length-prefixed)
     * String likho - length first, then data
     * (Write string with length prefix)
     */
    void writeString(const std::string& str) {
        writeLength(str.size());
        file_.write(str.data(), str.size());
    }
    
    /**
     * Write double
     * Double likho
     * (Write double value)
     */
    void writeDouble(double value) {
        // Write as string for simplicity
        // String mein likho - simple approach
        // (Write as string - simpler approach)
        std::string str = std::to_string(value);
        writeByte(static_cast<uint8_t>(str.size()));
        file_.write(str.data(), str.size());
    }
};

// ============================================================================
// RDB MANAGER
// RDB operations manage karo
// (Manage RDB operations)
// ============================================================================

/**
 * @class RdbManager
 * @brief Manages RDB persistence operations
 * 
 * RDB save/load operations manage karo
 * (Manage RDB save and load operations)
 */
class RdbManager {
private:
    std::string dir_;
    std::string filename_;
    
public:
    RdbManager(const std::string& dir = ".", const std::string& filename = "dump.rdb")
        : dir_(dir), filename_(filename) {
    }
    
    /**
     * Set directory for RDB file
     * Directory set karo
     * (Set RDB file directory)
     */
    void setDir(const std::string& dir) {
        dir_ = dir;
    }
    
    /**
     * Set filename for RDB file
     * Filename set karo
     * (Set RDB filename)
     */
    void setFilename(const std::string& filename) {
        filename_ = filename;
    }
    
    /**
     * Get current directory
     * Current directory lo
     * (Get current directory)
     */
    const std::string& getDir() const {
        return dir_;
    }
    
    /**
     * Get current filename
     * Current filename lo
     * (Get current filename)
     */
    const std::string& getFilename() const {
        return filename_;
    }
    
    /**
     * Load RDB file
     * RDB file load karo
     * (Load RDB file into DataStore)
     */
    bool load(DataStore& store) {
        RdbReader reader(dir_, filename_);
        return reader.load(store);
    }
    
    /**
     * Save to RDB file
     * RDB file mein save karo
     * (Save DataStore to RDB file)
     */
    bool save(const DataStore& store) {
        RdbWriter writer(dir_, filename_);
        return writer.save(store);
    }
    
    /**
     * Background save
     * Background mein save karo - fork() use karke
     * (Save in background using fork)
     */
    bool bgSave(const DataStore& store) {
        // For now, just do a regular save
        // Abhi lai normal save - fork later
        // (Just do normal save for now - implement fork later)
        std::cout << "Background saving started" << std::endl;
        return save(store);
    }
};

} // namespace Redis
