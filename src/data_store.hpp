/**
 * =============================================================================
 *                          REDIS DATA STORE
 * =============================================================================
 * 
 * Paaji eh hai asli godown jahan saara data rakha jaanda hai!
 * (Bro this is the real warehouse where all data is stored!)
 * 
 * Jiven Amazon da warehouse hunda hai, ohi eh hai Redis da!
 * (Just like Amazon has warehouses, this is Redis's warehouse!)
 * 
 * Features:
 * - Thread-safe operations (multiple log samaan handle kar sakde)
 *   (Thread-safe - can handle multiple people accessing same stuff)
 * - Key expiration (samaan expire ho janda hai jiven doodh)
 *   (Key expiration - data expires like milk)
 * - Multiple data types (strings, lists, sets, sorted sets, streams, hashes)
 * 
 * =============================================================================
 */

#pragma once

#include "common.hpp"
#include "resp_parser.hpp"
#include <shared_mutex>

namespace Redis {

// ============================================================================
// FORWARD DECLARATIONS
// ============================================================================

class DataStore;

// ============================================================================
// STREAM ENTRY STRUCTURE
// Stream ch har entry - jiven diary ch ek page
// (Each entry in stream - like one page in diary)
// ============================================================================

/**
 * @struct StreamEntry
 * @brief Represents a single entry in a Redis Stream
 * 
 * Har entry ch hunda hai:
 * - ID (timestamp-sequence format)
 * - Key-value pairs (field-value jodiyaan)
 * 
 * Jiven Instagram post hundi hai - timestamp + content
 * (Like an Instagram post - timestamp + content)
 */
struct StreamEntry {
    std::string id;                             // Entry ID: "timestamp-sequence"
    std::vector<std::pair<std::string, std::string>> fields;  // Key-value pairs
    
    // Timestamps for ID parsing
    // ID de parts - jiven Aadhaar number hunda hai
    // (Parts of ID - like Aadhaar number has parts)
    uint64_t timestamp = 0;
    uint64_t sequence = 0;
    
    /**
     * Parse ID into timestamp and sequence
     * ID nu tode do parts ch
     * (Split ID into two parts)
     */
    void parseId() {
        auto dashPos = id.find('-');
        if (dashPos != std::string::npos) {
            timestamp = std::stoull(id.substr(0, dashPos));
            sequence = std::stoull(id.substr(dashPos + 1));
        }
    }
    
    /**
     * Compare two entry IDs
     * Do IDs compare karo - kaun pehle aaya
     * (Compare two IDs - who came first)
     */
    static int compare(const std::string& id1, const std::string& id2) {
        auto parseId = [](const std::string& id) -> std::pair<uint64_t, uint64_t> {
            auto dash = id.find('-');
            if (dash == std::string::npos) {
                return {std::stoull(id), 0};
            }
            return {std::stoull(id.substr(0, dash)), std::stoull(id.substr(dash + 1))};
        };
        
        auto [ts1, seq1] = parseId(id1);
        auto [ts2, seq2] = parseId(id2);
        
        if (ts1 != ts2) return ts1 < ts2 ? -1 : 1;
        if (seq1 != seq2) return seq1 < seq2 ? -1 : 1;
        return 0;
    }
};

// ============================================================================
// XADD RESULT - Result type for XADD command
// XADD result - success ya error
// (XADD result - success with ID or error message)
// ============================================================================

/**
 * @struct XaddResult
 * @brief Result of XADD operation
 * 
 * XADD ka result - ya toh ID ya error
 * (Result of XADD - either ID or error message)
 */
struct XaddResult {
    bool success = false;
    std::string value;  // ID if success, error message if not
    
    static XaddResult ok(const std::string& id) {
        return {true, id};
    }
    
    static XaddResult error(const std::string& msg) {
        return {false, msg};
    }
};

// ============================================================================
// REDIS VALUE STRUCTURE
// Ek key di value - multiple types ho sakdi hai
// (Value of a key - can be multiple types)
// ============================================================================

/**
 * @struct RedisValue
 * @brief Represents a value stored in Redis
 * 
 * Polymorphic value - ek container jo sab kuch rakh sakda
 * (Polymorphic value - one container that holds everything)
 * 
 * Jiven dabba jo sab kuch store kare - kapde, books, snacks
 * (Like a box that stores everything - clothes, books, snacks)
 */
struct RedisValue {
    DataType type = DataType::NONE;     // Type of value - konsi variety hai
                                         // (Type of value - which variety)
    
    // String value - simple string ya number as string
    // (Simple string or number stored as string)
    std::string stringValue;
    
    // List value - ordered collection
    // Ordered list - jiven waiting line
    // (Ordered list - like a waiting line)
    std::deque<std::string> listValue;
    
    // Set value - unique elements, no order
    // Unique elements - duplicate allowed nahi
    // (Unique elements - no duplicates allowed, like VIP list)
    std::unordered_set<std::string> setValue;
    
    // Sorted Set - elements with scores
    // Score wali set - sorted by score
    // (Set with scores - sorted by score, like leaderboard)
    // Member -> Score mapping
    std::unordered_map<std::string, double> zsetScores;
    // Score -> Members (for range queries)  
    std::multimap<double, std::string> zsetByScore;
    
    // Hash value - field -> value mapping
    // Hash table - jiven dictionary
    // (Hash table - like a dictionary, key se value milti hai)
    std::unordered_map<std::string, std::string> hashValue;
    
    // Stream value - append-only log
    // Stream - jiven news feed, naye messages add hunde rahende
    // (Stream - like news feed, new messages keep adding)
    std::vector<StreamEntry> streamValue;
    uint64_t streamLastTimestamp = 0;    // Last used timestamp
    uint64_t streamLastSequence = 0;      // Last used sequence for same timestamp
    
    // Expiration
    // Expire hone da time - jiven doodh expire hunda hai
    // (Expiration time - like milk expiring)
    std::optional<int64_t> expiresAt;    // Unix timestamp in milliseconds
    
    /**
     * Check if key has expired
     * Check karo ki expire ho gya ki nahi
     * (Check if expired or not - like checking milk date)
     */
    bool isExpired() const {
        if (!expiresAt) return false;
        return getCurrentTimeMs() > *expiresAt;
    }
    
    /**
     * Get TTL (time to live) in milliseconds
     * Kitna time bacha hai expire hone ch
     * (How much time left before expiry - countdown to doom)
     */
    int64_t getTTL() const {
        if (!expiresAt) return -1;  // No expiry
        int64_t ttl = *expiresAt - getCurrentTimeMs();
        return ttl > 0 ? ttl : -2;  // -2 means already expired
    }
    
    /**
     * Constructor - default
     */
    RedisValue() = default;
    
    /**
     * Constructor for string type
     * String value set karo
     * (Set string value)
     */
    explicit RedisValue(const std::string& str) 
        : type(DataType::STRING), stringValue(str) {}
};

// ============================================================================
// DATA STORE CLASS
// Main storage class - godown ka manager
// (Main storage class - warehouse manager)
// ============================================================================

/**
 * @class DataStore
 * @brief Thread-safe key-value store
 * 
 * Eh class hai asli Redis data store!
 * (This class is the real Redis data store!)
 * 
 * Features:
 * - Multiple data types support
 * - Key expiration
 * - Thread-safe with read-write locks
 * - Efficient lookups
 * 
 * Jiven bank da vault hunda hai - safe te secure
 * (Like a bank vault - safe and secure)
 */
class DataStore {
private:
    // Main data storage - O(1) lookup jiven Google search
    // (O(1) lookup - fast like Google search, but actually works offline)
    std::unordered_map<std::string, RedisValue> data_;
    
    // Read-write lock - multiple readers, single writer
    // Lock mechanism - bohut log padh sakde, ek hi likh sakda
    // (Lock mechanism - many can read, only one can write)
    mutable std::shared_mutex mutex_;
    
    // Pub/Sub channels
    // Channel -> Set of subscriber client IDs
    // Jiven WhatsApp groups - ek channel, multiple subscribers
    // (Like WhatsApp groups - one channel, multiple subscribers)
    std::unordered_map<std::string, std::unordered_set<int>> channels_;
    mutable std::mutex pubsubMutex_;
    
    // Blocked clients waiting for list operations
    // BLPOP/BRPOP lai waiting list
    // (Waiting list for BLPOP/BRPOP - like waiting for food at restaurant)
    struct BlockedClient {
        int clientId;
        std::vector<std::string> keys;
        bool isLeft;  // true for BLPOP, false for BRPOP
        std::chrono::steady_clock::time_point deadline;
        std::function<void(const std::string&, const std::string&)> callback;
    };
    std::list<BlockedClient> blockedClients_;
    mutable std::mutex blockedMutex_;
    
    // Stream blocked readers
    // XREAD BLOCK lai waiting clients
    // (Clients waiting for XREAD BLOCK - stream watchers)
    struct StreamBlockedClient {
        int clientId;
        std::vector<std::pair<std::string, std::string>> streams; // stream -> lastId
        std::chrono::steady_clock::time_point deadline;
        bool hasDeadline;
        std::function<void(const std::vector<std::pair<std::string, std::vector<StreamEntry>>>&)> callback;
    };
    std::list<StreamBlockedClient> streamBlockedClients_;
    mutable std::mutex streamBlockedMutex_;

public:
    /**
     * Constructor - empty store banao
     * (Create empty store - fresh start)
     */
    DataStore() = default;
    
    // ========================================================================
    // BASIC KEY OPERATIONS - Seedhe saadhe key operations
    // (Basic key operations - simple and straightforward)
    // ========================================================================
    
    /**
     * Check if key exists (and not expired)
     * Key hai ki nahi check karo
     * (Check if key exists - like checking if your crush knows you exist)
     */
    bool exists(const std::string& key) {
        std::shared_lock lock(mutex_);
        return existsInternal(key);
    }
    
    /**
     * Delete a key
     * Key delete karo - goodbye!
     * (Delete key - like unfriending on Facebook)
     * 
     * @return true if key was deleted, false if didn't exist
     */
    bool del(const std::string& key) {
        std::unique_lock lock(mutex_);
        auto it = data_.find(key);
        if (it == data_.end()) return false;
        if (it->second.isExpired()) {
            data_.erase(it);
            return false;
        }
        data_.erase(it);
        return true;
    }
    
    /**
     * Delete multiple keys
     * Multiple keys delete karo - bulk unfriend
     * (Delete multiple keys - mass unfriending)
     * 
     * @return Number of keys deleted
     */
    int64_t del(const std::vector<std::string>& keys) {
        std::unique_lock lock(mutex_);
        int64_t count = 0;
        for (const auto& key : keys) {
            auto it = data_.find(key);
            if (it != data_.end() && !it->second.isExpired()) {
                data_.erase(it);
                count++;
            }
        }
        return count;
    }
    
    /**
     * Get type of key
     * Key da type kya hai?
     * (What's the type of key - like asking "What's your sign?")
     */
    DataType type(const std::string& key) {
        std::shared_lock lock(mutex_);
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            return DataType::NONE;
        }
        return it->second.type;
    }
    
    /**
     * Get type as string for TYPE command
     * Type command lai string return karo
     * (Return type as string for TYPE command)
     */
    std::string typeString(const std::string& key) {
        DataType t = type(key);
        switch (t) {
            case DataType::STRING: return "string";
            case DataType::LIST: return "list";
            case DataType::SET: return "set";
            case DataType::ZSET: return "zset";
            case DataType::HASH: return "hash";
            case DataType::STREAM: return "stream";
            default: return "none";
        }
    }
    
    /**
     * Get all keys matching pattern
     * Pattern match karne wale saare keys
     * (All keys matching pattern - like Tinder but for keys)
     */
    std::vector<std::string> keys(const std::string& pattern) {
        std::shared_lock lock(mutex_);
        std::vector<std::string> result;
        
        // Simple pattern matching - * matches everything
        // TODO: Full glob pattern support
        // Abhi sirf * support hai - baaki baad mein
        // (Only * supported now - rest later, like my life plans)
        
        if (pattern == "*") {
            for (const auto& [key, value] : data_) {
                if (!value.isExpired()) {
                    result.push_back(key);
                }
            }
        } else {
            // Basic prefix matching if pattern ends with *
            // Prefix matching - agar pattern * pe end hove
            // (Prefix matching - if pattern ends with *)
            if (!pattern.empty() && pattern.back() == '*') {
                std::string prefix = pattern.substr(0, pattern.length() - 1);
                for (const auto& [key, value] : data_) {
                    if (!value.isExpired() && key.substr(0, prefix.length()) == prefix) {
                        result.push_back(key);
                    }
                }
            } else {
                // Exact match
                if (existsInternal(pattern)) {
                    result.push_back(pattern);
                }
            }
        }
        
        return result;
    }
    
    /**
     * Set expiration on key (in milliseconds)
     * Expire time set karo - countdown shuru
     * (Set expire time - countdown begins, like New Year countdown)
     */
    bool pexpire(const std::string& key, int64_t milliseconds) {
        std::unique_lock lock(mutex_);
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) return false;
        
        it->second.expiresAt = getCurrentTimeMs() + milliseconds;
        return true;
    }
    
    /**
     * Set expiration on key (in seconds)
     * Seconds mein expire time set karo
     * (Set expire time in seconds)
     */
    bool expire(const std::string& key, int64_t seconds) {
        return pexpire(key, seconds * 1000);
    }
    
    /**
     * Set expiration at Unix timestamp (milliseconds)
     * Specific time pe expire karo
     * (Expire at specific time - like Cinderella at midnight)
     */
    bool pexpireat(const std::string& key, int64_t unixTimeMs) {
        std::unique_lock lock(mutex_);
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) return false;
        
        it->second.expiresAt = unixTimeMs;
        return true;
    }
    
    /**
     * Get TTL in milliseconds
     * Kitna time bacha hai (ms mein)
     * (How much time left in milliseconds - tick tock)
     */
    int64_t pttl(const std::string& key) {
        std::shared_lock lock(mutex_);
        auto it = data_.find(key);
        if (it == data_.end()) return -2;  // Key doesn't exist
        if (it->second.isExpired()) {
            return -2;
        }
        return it->second.getTTL();
    }
    
    /**
     * Get TTL in seconds
     * Kitna time bacha hai (seconds mein)
     * (How much time left in seconds)
     */
    int64_t ttl(const std::string& key) {
        int64_t ms = pttl(key);
        if (ms < 0) return ms;
        return ms / 1000;
    }
    
    /**
     * Remove expiration from key
     * Expire hatao - immortal bana do
     * (Remove expiration - make it immortal, like Voldemort tried to be)
     */
    bool persist(const std::string& key) {
        std::unique_lock lock(mutex_);
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) return false;
        if (!it->second.expiresAt) return false;
        
        it->second.expiresAt = std::nullopt;
        return true;
    }

    // ========================================================================
    // STRING OPERATIONS - String commands
    // Simple strings - Redis da bread and butter
    // (Simple strings - Redis's bread and butter)
    // ========================================================================
    
    /**
     * SET command - set a string value
     * Value set karo - sabse basic operation
     * (Set value - most basic operation, like "Hello World")
     */
    void set(const std::string& key, const std::string& value, 
             std::optional<int64_t> px = std::nullopt,
             std::optional<int64_t> ex = std::nullopt,
             bool nx = false, bool xx = false) {
        std::unique_lock lock(mutex_);
        
        bool keyExists = existsInternal(key);
        
        // NX - only set if not exists
        // NX - sirf tab set karo jab key nahi hai
        // (NX - only set when key doesn't exist - playing hard to get)
        if (nx && keyExists) return;
        
        // XX - only set if exists  
        // XX - sirf tab set karo jab key hai
        // (XX - only set when key exists - committed relationship only)
        if (xx && !keyExists) return;
        
        RedisValue val(value);
        
        // Set expiration if provided
        // Expire time set karo agar diya hai
        // (Set expire time if provided)
        if (px) {
            val.expiresAt = getCurrentTimeMs() + *px;
        } else if (ex) {
            val.expiresAt = getCurrentTimeMs() + (*ex * 1000);
        }
        
        data_[key] = std::move(val);
    }
    
    /**
     * GET command - get string value
     * Value lo - simple retrieval
     * (Get value - simple retrieval, like getting coffee)
     */
    std::optional<std::string> get(const std::string& key) {
        std::shared_lock lock(mutex_);
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            return std::nullopt;
        }
        if (it->second.type != DataType::STRING) {
            return std::nullopt;  // Wrong type - galat type hai bhai
                                  // (Wrong type - it's not a string!)
        }
        return it->second.stringValue;
    }
    
    /**
     * GETEX command - get with expiration options
     * Get karo te expire bhi set karo
     * (Get and also set expiration - multitasking)
     */
    std::optional<std::string> getex(const std::string& key,
                                      std::optional<int64_t> ex = std::nullopt,
                                      std::optional<int64_t> px = std::nullopt,
                                      bool persist = false) {
        std::unique_lock lock(mutex_);
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            return std::nullopt;
        }
        if (it->second.type != DataType::STRING) {
            return std::nullopt;
        }
        
        // Modify expiration
        if (persist) {
            it->second.expiresAt = std::nullopt;
        } else if (px) {
            it->second.expiresAt = getCurrentTimeMs() + *px;
        } else if (ex) {
            it->second.expiresAt = getCurrentTimeMs() + (*ex * 1000);
        }
        
        return it->second.stringValue;
    }
    
    /**
     * INCR command - increment integer
     * Number vadha do - ++
     * (Increment number - like your age every year, sadly)
     */
    std::variant<int64_t, std::string> incr(const std::string& key) {
        return incrby(key, 1);
    }
    
    /**
     * INCRBY command - increment by value
     * Number vadha do specific amount se
     * (Increment by specific amount - customized increment)
     */
    std::variant<int64_t, std::string> incrby(const std::string& key, int64_t increment) {
        std::unique_lock lock(mutex_);
        
        auto it = data_.find(key);
        int64_t currentValue = 0;
        
        if (it != data_.end() && !it->second.isExpired()) {
            if (it->second.type != DataType::STRING) {
                return ERR_WRONG_TYPE;
            }
            
            // Try to parse as integer
            // Number parse karne ki koshish
            // (Trying to parse as number - fingers crossed)
            try {
                currentValue = std::stoll(it->second.stringValue);
            } catch (...) {
                return ERR_NOT_INTEGER;
            }
        }
        
        // Check for overflow
        // Overflow check - number too big ho gaya
        // (Overflow check - number got too big, like my stress levels)
        if ((increment > 0 && currentValue > std::numeric_limits<int64_t>::max() - increment) ||
            (increment < 0 && currentValue < std::numeric_limits<int64_t>::min() - increment)) {
            return std::string("ERR increment or decrement would overflow");
        }
        
        int64_t newValue = currentValue + increment;
        
        if (it == data_.end() || it->second.isExpired()) {
            data_[key] = RedisValue(std::to_string(newValue));
        } else {
            it->second.stringValue = std::to_string(newValue);
        }
        
        return newValue;
    }
    
    /**
     * DECR command - decrement integer
     * Number ghata do - --
     * (Decrement number - like my bank balance after online shopping)
     */
    std::variant<int64_t, std::string> decr(const std::string& key) {
        return incrby(key, -1);
    }
    
    /**
     * DECRBY command - decrement by value
     * Specific amount se ghata do
     * (Decrement by specific amount)
     */
    std::variant<int64_t, std::string> decrby(const std::string& key, int64_t decrement) {
        return incrby(key, -decrement);
    }
    
    /**
     * APPEND command - append to string
     * String ke end mein add karo
     * (Append to string - like adding to your food order)
     */
    int64_t append(const std::string& key, const std::string& value) {
        std::unique_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            data_[key] = RedisValue(value);
            return value.length();
        }
        
        if (it->second.type != DataType::STRING) {
            return -1;  // Error case
        }
        
        it->second.stringValue += value;
        return it->second.stringValue.length();
    }
    
    /**
     * STRLEN command - get string length
     * String ki length - kitni lambi hai
     * (String length - how long is it)
     */
    int64_t strlen(const std::string& key) {
        std::shared_lock lock(mutex_);
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            return 0;
        }
        if (it->second.type != DataType::STRING) {
            return -1;
        }
        return it->second.stringValue.length();
    }
    
    /**
     * MSET command - set multiple keys
     * Multiple keys ek saath set karo
     * (Set multiple keys at once - bulk operation, like wholesale shopping)
     */
    void mset(const std::vector<std::pair<std::string, std::string>>& kvs) {
        std::unique_lock lock(mutex_);
        for (const auto& [key, value] : kvs) {
            data_[key] = RedisValue(value);
        }
    }
    
    /**
     * MGET command - get multiple keys
     * Multiple keys ek saath get karo
     * (Get multiple keys at once - bulk retrieval)
     */
    std::vector<std::optional<std::string>> mget(const std::vector<std::string>& keys) {
        std::shared_lock lock(mutex_);
        std::vector<std::optional<std::string>> result;
        result.reserve(keys.size());
        
        for (const auto& key : keys) {
            auto it = data_.find(key);
            if (it == data_.end() || it->second.isExpired() || 
                it->second.type != DataType::STRING) {
                result.push_back(std::nullopt);
            } else {
                result.push_back(it->second.stringValue);
            }
        }
        
        return result;
    }

    // ========================================================================
    // LIST OPERATIONS - List commands
    // Lists - jiven queue hundi hai
    // (Lists - like queues, FIFO/LIFO operations)
    // ========================================================================
    
    /**
     * LPUSH command - push to left (head)
     * Left side se daalo - queue ke shuru mein
     * (Push from left - beginning of queue, cutting the line basically)
     */
    int64_t lpush(const std::string& key, const std::vector<std::string>& values) {
        std::unique_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it != data_.end() && !it->second.isExpired()) {
            if (it->second.type != DataType::LIST) {
                return -1;  // Wrong type
            }
        } else {
            data_[key] = RedisValue();
            data_[key].type = DataType::LIST;
            it = data_.find(key);
        }
        
        // Push all values to front (in order, so last value ends up at front)
        // Ulta order mein daalo - jiven stack hoti hai
        // (Insert in reverse order - like a stack)
        for (auto rit = values.rbegin(); rit != values.rend(); ++rit) {
            it->second.listValue.push_front(*rit);
        }
        
        // Check for blocked clients waiting on this key
        // Blocked clients check karo - koi wait kar raha tha?
        // (Check blocked clients - was someone waiting?)
        lock.unlock();
        notifyBlockedListClients(key, true);
        
        return it->second.listValue.size();
    }
    
    /**
     * RPUSH command - push to right (tail)
     * Right side se daalo - queue ke end mein
     * (Push from right - end of queue, proper queue behavior)
     */
    int64_t rpush(const std::string& key, const std::vector<std::string>& values) {
        std::unique_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it != data_.end() && !it->second.isExpired()) {
            if (it->second.type != DataType::LIST) {
                return -1;
            }
        } else {
            data_[key] = RedisValue();
            data_[key].type = DataType::LIST;
            it = data_.find(key);
        }
        
        for (const auto& val : values) {
            it->second.listValue.push_back(val);
        }
        
        lock.unlock();
        notifyBlockedListClients(key, false);
        
        return it->second.listValue.size();
    }
    
    /**
     * LPOP command - pop from left
     * Left se nikalo - queue se pehla aadmi
     * (Pop from left - first person in queue, FIFO style)
     */
    std::optional<std::string> lpop(const std::string& key) {
        std::unique_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            return std::nullopt;
        }
        if (it->second.type != DataType::LIST) {
            return std::nullopt;
        }
        if (it->second.listValue.empty()) {
            return std::nullopt;
        }
        
        std::string value = it->second.listValue.front();
        it->second.listValue.pop_front();
        
        // Remove key if list is empty
        // Khali ho gayi list - key bhi hatao
        // (List empty - remove key too)
        if (it->second.listValue.empty()) {
            data_.erase(it);
        }
        
        return value;
    }
    
    /**
     * RPOP command - pop from right
     * Right se nikalo - queue se aakhri aadmi
     * (Pop from right - last person in queue)
     */
    std::optional<std::string> rpop(const std::string& key) {
        std::unique_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            return std::nullopt;
        }
        if (it->second.type != DataType::LIST) {
            return std::nullopt;
        }
        if (it->second.listValue.empty()) {
            return std::nullopt;
        }
        
        std::string value = it->second.listValue.back();
        it->second.listValue.pop_back();
        
        if (it->second.listValue.empty()) {
            data_.erase(it);
        }
        
        return value;
    }
    
    /**
     * LLEN command - get list length
     * List ki length - kitne log queue mein
     * (List length - how many people in queue)
     */
    int64_t llen(const std::string& key) {
        std::shared_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            return 0;
        }
        if (it->second.type != DataType::LIST) {
            return -1;  // Wrong type
        }
        
        return it->second.listValue.size();
    }
    
    /**
     * LRANGE command - get range of elements
     * List ka range do - start se stop tak
     * (Give range of list - from start to stop, like Netflix's "Continue Watching")
     */
    std::vector<std::string> lrange(const std::string& key, int64_t start, int64_t stop) {
        std::shared_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            return {};
        }
        if (it->second.type != DataType::LIST) {
            return {};  // Could return error indicator
        }
        
        const auto& list = it->second.listValue;
        int64_t len = list.size();
        
        // Handle negative indices
        // Negative indices - ulta count, Python style
        // (Negative indices - count from end, Python style, -1 is last)
        if (start < 0) start = std::max(len + start, (int64_t)0);
        if (stop < 0) stop = len + stop;
        
        // Bounds check
        if (start >= len || start > stop) {
            return {};
        }
        
        stop = std::min(stop, len - 1);
        
        std::vector<std::string> result;
        for (int64_t i = start; i <= stop; i++) {
            result.push_back(list[i]);
        }
        
        return result;
    }
    
    /**
     * LINDEX command - get element at index
     * Specific index pe element do
     * (Give element at specific index - like array access)
     */
    std::optional<std::string> lindex(const std::string& key, int64_t index) {
        std::shared_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            return std::nullopt;
        }
        if (it->second.type != DataType::LIST) {
            return std::nullopt;
        }
        
        const auto& list = it->second.listValue;
        int64_t len = list.size();
        
        // Handle negative index
        if (index < 0) index = len + index;
        
        if (index < 0 || index >= len) {
            return std::nullopt;
        }
        
        return list[index];
    }
    
    /**
     * LSET command - set element at index
     * Index pe element set karo
     * (Set element at index - overwriting existing value)
     */
    bool lset(const std::string& key, int64_t index, const std::string& value) {
        std::unique_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            return false;
        }
        if (it->second.type != DataType::LIST) {
            return false;
        }
        
        auto& list = it->second.listValue;
        int64_t len = list.size();
        
        if (index < 0) index = len + index;
        
        if (index < 0 || index >= len) {
            return false;
        }
        
        list[index] = value;
        return true;
    }
    
    /**
     * LREM command - remove elements
     * Elements hatao - count specify karo
     * (Remove elements - specify count, like unfollowing people)
     * count > 0: Remove first N matching elements
     * count < 0: Remove last N matching elements  
     * count = 0: Remove all matching elements
     */
    int64_t lrem(const std::string& key, int64_t count, const std::string& value) {
        std::unique_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            return 0;
        }
        if (it->second.type != DataType::LIST) {
            return -1;
        }
        
        auto& list = it->second.listValue;
        int64_t removed = 0;
        
        if (count > 0) {
            // Remove first N from head
            // Pehle N elements hatao
            // (Remove first N elements)
            for (auto iter = list.begin(); iter != list.end() && removed < count;) {
                if (*iter == value) {
                    iter = list.erase(iter);
                    removed++;
                } else {
                    ++iter;
                }
            }
        } else if (count < 0) {
            // Remove last N from tail
            // Peeche se N elements hatao
            // (Remove last N elements from tail)
            count = -count;
            for (auto iter = list.rbegin(); iter != list.rend() && removed < count;) {
                if (*iter == value) {
                    iter = std::deque<std::string>::reverse_iterator(
                        list.erase(std::next(iter).base()));
                    removed++;
                } else {
                    ++iter;
                }
            }
        } else {
            // Remove all
            // Saare matching hatao
            // (Remove all matching - clean sweep)
            for (auto iter = list.begin(); iter != list.end();) {
                if (*iter == value) {
                    iter = list.erase(iter);
                    removed++;
                } else {
                    ++iter;
                }
            }
        }
        
        // Remove key if empty
        if (list.empty()) {
            data_.erase(it);
        }
        
        return removed;
    }
    
    /**
     * Register a blocked client for BLPOP/BRPOP
     * Client nu block karo - wait karega jab tak value nahi aati
     * (Block client - will wait until value arrives, like waiting for crush's reply)
     */
    void registerBlockedListClient(int clientId, const std::vector<std::string>& keys,
                                    bool isLeft, int64_t timeoutMs,
                                    std::function<void(const std::string&, const std::string&)> callback) {
        std::lock_guard lock(blockedMutex_);
        
        BlockedClient bc;
        bc.clientId = clientId;
        bc.keys = keys;
        bc.isLeft = isLeft;
        bc.callback = callback;
        
        if (timeoutMs > 0) {
            bc.deadline = std::chrono::steady_clock::now() + 
                          std::chrono::milliseconds(timeoutMs);
        } else {
            bc.deadline = std::chrono::steady_clock::time_point::max();
        }
        
        blockedClients_.push_back(std::move(bc));
    }
    
    /**
     * Unregister blocked client
     * Client nu unblock karo - ab wait nahi karega
     * (Unblock client - stop waiting, give up hope)
     */
    void unregisterBlockedClient(int clientId) {
        std::lock_guard lock(blockedMutex_);
        blockedClients_.remove_if([clientId](const BlockedClient& bc) {
            return bc.clientId == clientId;
        });
    }
    
    /**
     * Check and handle timed out blocked clients
     * Timeout ho gaye clients handle karo
     * (Handle timed out clients - their patience ran out)
     */
    void checkBlockedTimeouts() {
        std::lock_guard lock(blockedMutex_);
        auto now = std::chrono::steady_clock::now();
        
        blockedClients_.remove_if([&now](BlockedClient& bc) {
            if (bc.deadline <= now) {
                // Timeout ho gaya - null return karo
                // (Timed out - return null, like my love life)
                bc.callback("", "");
                return true;
            }
            return false;
        });
    }

    // ========================================================================
    // HASH OPERATIONS - Hash commands (key -> field -> value)
    // Hash - dictionary ke andar dictionary
    // (Hash - dictionary inside dictionary, inception level data)
    // ========================================================================
    
    /**
     * HSET command - set hash field
     * Hash mein field set karo
     * (Set field in hash - nested storage)
     */
    int64_t hset(const std::string& key, const std::vector<std::pair<std::string, std::string>>& fieldValues) {
        std::unique_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it != data_.end() && !it->second.isExpired()) {
            if (it->second.type != DataType::HASH) {
                return -1;
            }
        } else {
            data_[key] = RedisValue();
            data_[key].type = DataType::HASH;
            it = data_.find(key);
        }
        
        int64_t newFields = 0;
        for (const auto& [field, value] : fieldValues) {
            if (it->second.hashValue.find(field) == it->second.hashValue.end()) {
                newFields++;
            }
            it->second.hashValue[field] = value;
        }
        
        return newFields;
    }
    
    /**
     * HGET command - get hash field
     * Hash se field ki value lo
     * (Get field value from hash)
     */
    std::optional<std::string> hget(const std::string& key, const std::string& field) {
        std::shared_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            return std::nullopt;
        }
        if (it->second.type != DataType::HASH) {
            return std::nullopt;
        }
        
        auto fieldIt = it->second.hashValue.find(field);
        if (fieldIt == it->second.hashValue.end()) {
            return std::nullopt;
        }
        
        return fieldIt->second;
    }
    
    /**
     * HGETALL command - get all fields and values
     * Saare fields te values do
     * (Get all fields and values - full disclosure)
     */
    std::vector<std::pair<std::string, std::string>> hgetall(const std::string& key) {
        std::shared_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            return {};
        }
        if (it->second.type != DataType::HASH) {
            return {};
        }
        
        std::vector<std::pair<std::string, std::string>> result;
        for (const auto& [field, value] : it->second.hashValue) {
            result.emplace_back(field, value);
        }
        
        return result;
    }
    
    /**
     * HDEL command - delete hash fields
     * Hash se fields hatao
     * (Delete fields from hash)
     */
    int64_t hdel(const std::string& key, const std::vector<std::string>& fields) {
        std::unique_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            return 0;
        }
        if (it->second.type != DataType::HASH) {
            return -1;
        }
        
        int64_t deleted = 0;
        for (const auto& field : fields) {
            deleted += it->second.hashValue.erase(field);
        }
        
        if (it->second.hashValue.empty()) {
            data_.erase(it);
        }
        
        return deleted;
    }
    
    /**
     * HEXISTS command - check if field exists
     * Field hai ki nahi check karo
     * (Check if field exists)
     */
    bool hexists(const std::string& key, const std::string& field) {
        std::shared_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            return false;
        }
        if (it->second.type != DataType::HASH) {
            return false;
        }
        
        return it->second.hashValue.find(field) != it->second.hashValue.end();
    }
    
    /**
     * HLEN command - get number of fields
     * Kitne fields hain hash mein
     * (How many fields in hash)
     */
    int64_t hlen(const std::string& key) {
        std::shared_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            return 0;
        }
        if (it->second.type != DataType::HASH) {
            return -1;
        }
        
        return it->second.hashValue.size();
    }

    // ========================================================================
    // SET OPERATIONS - Set commands (unique elements)
    // Set - unique elements, no duplicates
    // (Set - like a VIP list, no duplicates allowed)
    // ========================================================================
    
    /**
     * SADD command - add members to set
     * Set mein members add karo
     * (Add members to set - join the exclusive club)
     */
    int64_t sadd(const std::string& key, const std::vector<std::string>& members) {
        std::unique_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it != data_.end() && !it->second.isExpired()) {
            if (it->second.type != DataType::SET) {
                return -1;
            }
        } else {
            data_[key] = RedisValue();
            data_[key].type = DataType::SET;
            it = data_.find(key);
        }
        
        int64_t added = 0;
        for (const auto& member : members) {
            if (it->second.setValue.insert(member).second) {
                added++;
            }
        }
        
        return added;
    }
    
    /**
     * SMEMBERS command - get all members
     * Saare members do
     * (Get all members - full guest list)
     */
    std::vector<std::string> smembers(const std::string& key) {
        std::shared_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            return {};
        }
        if (it->second.type != DataType::SET) {
            return {};
        }
        
        return std::vector<std::string>(it->second.setValue.begin(), 
                                         it->second.setValue.end());
    }
    
    /**
     * SISMEMBER command - check if member exists
     * Member hai ki nahi
     * (Is member in set - are you on the list?)
     */
    bool sismember(const std::string& key, const std::string& member) {
        std::shared_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            return false;
        }
        if (it->second.type != DataType::SET) {
            return false;
        }
        
        return it->second.setValue.count(member) > 0;
    }
    
    /**
     * SREM command - remove members
     * Members hatao
     * (Remove members - kicked out of the club)
     */
    int64_t srem(const std::string& key, const std::vector<std::string>& members) {
        std::unique_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            return 0;
        }
        if (it->second.type != DataType::SET) {
            return -1;
        }
        
        int64_t removed = 0;
        for (const auto& member : members) {
            removed += it->second.setValue.erase(member);
        }
        
        if (it->second.setValue.empty()) {
            data_.erase(it);
        }
        
        return removed;
    }
    
    /**
     * SCARD command - get set cardinality
     * Set mein kitne members hain
     * (How many members in set - headcount)
     */
    int64_t scard(const std::string& key) {
        std::shared_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            return 0;
        }
        if (it->second.type != DataType::SET) {
            return -1;
        }
        
        return it->second.setValue.size();
    }

    // ========================================================================
    // SORTED SET OPERATIONS - Sorted set commands
    // Sorted Set - set with scores, sorted by score
    // (Sorted Set - leaderboard style, everyone has a score)
    // ========================================================================
    
    /**
     * ZADD command - add members with scores
     * Score ke saath members add karo
     * (Add members with scores - like adding students with marks)
     */
    int64_t zadd(const std::string& key, 
                 const std::vector<std::pair<double, std::string>>& scoreMembers,
                 bool nx = false, bool xx = false, bool gt = false, bool lt = false,
                 bool ch = false) {
        std::unique_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it != data_.end() && !it->second.isExpired()) {
            if (it->second.type != DataType::ZSET) {
                return -1;
            }
        } else {
            data_[key] = RedisValue();
            data_[key].type = DataType::ZSET;
            it = data_.find(key);
        }
        
        int64_t added = 0;
        int64_t changed = 0;
        
        for (const auto& [score, member] : scoreMembers) {
            auto memberIt = it->second.zsetScores.find(member);
            bool exists = (memberIt != it->second.zsetScores.end());
            
            // NX - only add new elements
            if (nx && exists) continue;
            // XX - only update existing elements
            if (xx && !exists) continue;
            
            if (exists) {
                double oldScore = memberIt->second;
                
                // GT - only update if new score > old score
                if (gt && score <= oldScore) continue;
                // LT - only update if new score < old score
                if (lt && score >= oldScore) continue;
                
                if (score != oldScore) {
                    // Remove old entry from score index
                    auto range = it->second.zsetByScore.equal_range(oldScore);
                    for (auto scoreIt = range.first; scoreIt != range.second; ++scoreIt) {
                        if (scoreIt->second == member) {
                            it->second.zsetByScore.erase(scoreIt);
                            break;
                        }
                    }
                    
                    // Update score
                    memberIt->second = score;
                    it->second.zsetByScore.emplace(score, member);
                    changed++;
                }
            } else {
                // New member
                it->second.zsetScores[member] = score;
                it->second.zsetByScore.emplace(score, member);
                added++;
            }
        }
        
        return ch ? (added + changed) : added;
    }
    
    /**
     * ZSCORE command - get member's score
     * Member ka score do
     * (Get member's score - check your marks)
     */
    std::optional<double> zscore(const std::string& key, const std::string& member) {
        std::shared_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            return std::nullopt;
        }
        if (it->second.type != DataType::ZSET) {
            return std::nullopt;
        }
        
        auto memberIt = it->second.zsetScores.find(member);
        if (memberIt == it->second.zsetScores.end()) {
            return std::nullopt;
        }
        
        return memberIt->second;
    }
    
    /**
     * ZRANK command - get member's rank (0-based)
     * Member ka rank do - position in leaderboard
     * (Get member's rank - your position in leaderboard, hopefully not last)
     */
    std::optional<int64_t> zrank(const std::string& key, const std::string& member) {
        std::shared_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            return std::nullopt;
        }
        if (it->second.type != DataType::ZSET) {
            return std::nullopt;
        }
        
        auto memberIt = it->second.zsetScores.find(member);
        if (memberIt == it->second.zsetScores.end()) {
            return std::nullopt;
        }
        
        // Count members with lower score
        // Kitne log tujhse peeche hain
        // (How many people are behind you - competition check)
        int64_t rank = 0;
        double targetScore = memberIt->second;
        
        for (const auto& [score, m] : it->second.zsetByScore) {
            if (score < targetScore) {
                rank++;
            } else if (score == targetScore && m < member) {
                rank++;
            } else if (score == targetScore && m == member) {
                break;
            } else {
                break;
            }
        }
        
        return rank;
    }
    
    /**
     * ZRANGE command - get range of members by index
     * Index range mein members do
     * (Get members in index range - like top 10 leaderboard)
     */
    std::vector<std::pair<std::string, double>> zrange(const std::string& key, 
                                                         int64_t start, int64_t stop,
                                                         bool withScores = false,
                                                         bool rev = false) {
        std::shared_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            return {};
        }
        if (it->second.type != DataType::ZSET) {
            return {};
        }
        
        int64_t len = it->second.zsetByScore.size();
        
        // Handle negative indices
        if (start < 0) start = std::max(len + start, (int64_t)0);
        if (stop < 0) stop = len + stop;
        
        if (start >= len || start > stop) {
            return {};
        }
        
        stop = std::min(stop, len - 1);
        
        std::vector<std::pair<std::string, double>> result;
        
        if (rev) {
            // Reverse order - top scores first
            // Ulta order - toppers pehle
            // (Reverse order - toppers first, like exam results)
            int64_t idx = len - 1;
            for (auto rit = it->second.zsetByScore.rbegin(); 
                 rit != it->second.zsetByScore.rend() && idx >= start; 
                 ++rit, --idx) {
                if (idx <= stop && idx >= start) {
                    result.emplace_back(rit->second, rit->first);
                }
            }
        } else {
            int64_t idx = 0;
            for (const auto& [score, member] : it->second.zsetByScore) {
                if (idx > stop) break;
                if (idx >= start) {
                    result.emplace_back(member, score);
                }
                idx++;
            }
        }
        
        return result;
    }
    
    /**
     * ZCOUNT command - count members in score range
     * Score range mein kitne members hain
     * (Count members in score range - how many passed the exam)
     */
    int64_t zcount(const std::string& key, double min, double max) {
        std::shared_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            return 0;
        }
        if (it->second.type != DataType::ZSET) {
            return -1;
        }
        
        int64_t count = 0;
        for (const auto& [score, member] : it->second.zsetByScore) {
            if (score >= min && score <= max) {
                count++;
            }
        }
        
        return count;
    }
    
    /**
     * ZCARD command - get sorted set cardinality
     * Sorted set mein kitne members hain
     * (How many members in sorted set)
     */
    int64_t zcard(const std::string& key) {
        std::shared_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            return 0;
        }
        if (it->second.type != DataType::ZSET) {
            return -1;
        }
        
        return it->second.zsetScores.size();
    }
    
    /**
     * ZREM command - remove members
     * Members hatao
     * (Remove members - expelled from leaderboard)
     */
    int64_t zrem(const std::string& key, const std::vector<std::string>& members) {
        std::unique_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            return 0;
        }
        if (it->second.type != DataType::ZSET) {
            return -1;
        }
        
        int64_t removed = 0;
        for (const auto& member : members) {
            auto memberIt = it->second.zsetScores.find(member);
            if (memberIt != it->second.zsetScores.end()) {
                double score = memberIt->second;
                
                // Remove from score index
                auto range = it->second.zsetByScore.equal_range(score);
                for (auto scoreIt = range.first; scoreIt != range.second; ++scoreIt) {
                    if (scoreIt->second == member) {
                        it->second.zsetByScore.erase(scoreIt);
                        break;
                    }
                }
                
                it->second.zsetScores.erase(memberIt);
                removed++;
            }
        }
        
        if (it->second.zsetScores.empty()) {
            data_.erase(it);
        }
        
        return removed;
    }

    // ========================================================================
    // STREAM OPERATIONS - Stream commands
    // Streams - append-only log jiven Kafka
    // (Streams - append-only log like Kafka, but cooler)
    // ========================================================================
    
    /**
     * XADD command - add entry to stream
     * Stream mein entry add karo
     * (Add entry to stream - like posting on timeline)
     */
    XaddResult xadd(const std::string& key, 
                    const std::string& idInput,
                    const std::vector<std::pair<std::string, std::string>>& fields) {
        std::unique_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it != data_.end() && !it->second.isExpired()) {
            if (it->second.type != DataType::STREAM) {
                return XaddResult::error(ERR_WRONG_TYPE);
            }
        } else {
            data_[key] = RedisValue();
            data_[key].type = DataType::STREAM;
            it = data_.find(key);
        }
        
        uint64_t timestamp, sequence;
        
        // Parse/generate ID
        // ID parse ya generate karo - identity banana hai
        // (Parse or generate ID - gotta have an identity)
        if (idInput == "*") {
            // Fully auto-generated ID
            // Pura automatic - AI style
            // (Fully automatic - AI style, we do everything)
            timestamp = getCurrentTimeMs();
            if (timestamp == it->second.streamLastTimestamp) {
                sequence = it->second.streamLastSequence + 1;
            } else if (timestamp < it->second.streamLastTimestamp) {
                // Time went backwards (shouldn't happen but just in case)
                // Time ulta gaya - koi nahi, adjust kar lete hain
                // (Time went backwards - no worries, we adjust)
                timestamp = it->second.streamLastTimestamp;
                sequence = it->second.streamLastSequence + 1;
            } else {
                sequence = 0;
            }
        } else {
            // Parse provided ID
            auto dashPos = idInput.find('-');
            if (dashPos == std::string::npos) {
                return XaddResult::error("ERR Invalid stream ID specified as stream command argument");
            }
            
            std::string tsStr = idInput.substr(0, dashPos);
            std::string seqStr = idInput.substr(dashPos + 1);
            
            if (tsStr == "*" || seqStr == "*") {
                // Partial auto-generation
                // Partial automatic - thoda help chahiye
                // (Partial automatic - need some help)
                if (tsStr == "*") {
                    timestamp = getCurrentTimeMs();
                } else {
                    try {
                        timestamp = std::stoull(tsStr);
                    } catch (...) {
                        return XaddResult::error("ERR Invalid stream ID specified as stream command argument");
                    }
                }
                
                if (seqStr == "*") {
                    if (timestamp == it->second.streamLastTimestamp) {
                        sequence = it->second.streamLastSequence + 1;
                    } else if (timestamp > it->second.streamLastTimestamp) {
                        sequence = 0;
                    } else {
                        // timestamp < lastTimestamp
                        return XaddResult::error(ERR_STREAM_ID);
                    }
                } else {
                    try {
                        sequence = std::stoull(seqStr);
                    } catch (...) {
                        return XaddResult::error("ERR Invalid stream ID specified as stream command argument");
                    }
                }
            } else {
                // Fully specified ID
                // Pura specified - user ne sab decide kiya
                // (Fully specified - user decided everything)
                try {
                    timestamp = std::stoull(tsStr);
                    sequence = std::stoull(seqStr);
                } catch (...) {
                    return XaddResult::error("ERR Invalid stream ID specified as stream command argument");
                }
            }
        }
        
        // Validate ID is greater than last
        // Check karo ki ID pichle se bada hai
        // (Check that ID is greater than last - no going back!)
        if (timestamp < it->second.streamLastTimestamp ||
            (timestamp == it->second.streamLastTimestamp && 
             sequence <= it->second.streamLastSequence)) {
            if (timestamp == 0 && sequence == 0) {
                return XaddResult::error(ERR_STREAM_ID_ZERO);
            }
            return XaddResult::error(ERR_STREAM_ID);
        }
        
        // Create entry
        StreamEntry entry;
        entry.timestamp = timestamp;
        entry.sequence = sequence;
        entry.id = std::to_string(timestamp) + "-" + std::to_string(sequence);
        entry.fields = fields;
        
        it->second.streamValue.push_back(std::move(entry));
        it->second.streamLastTimestamp = timestamp;
        it->second.streamLastSequence = sequence;
        
        std::string resultId = std::to_string(timestamp) + "-" + std::to_string(sequence);
        
        // Notify blocked clients
        // Blocked clients ko batao - naya data aaya
        // (Tell blocked clients - new data arrived, wake up!)
        lock.unlock();
        notifyStreamBlockedClients(key);
        
        return XaddResult::ok(resultId);
    }
    
    /**
     * XRANGE command - get range of entries
     * Entries ka range do
     * (Get range of entries - timeline scroll)
     */
    std::vector<StreamEntry> xrange(const std::string& key, 
                                     const std::string& start, 
                                     const std::string& end,
                                     std::optional<int64_t> count = std::nullopt) {
        std::shared_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            return {};
        }
        if (it->second.type != DataType::STREAM) {
            return {};
        }
        
        std::vector<StreamEntry> result;
        
        for (const auto& entry : it->second.streamValue) {
            bool include = true;
            
            // Check start
            if (start != "-") {
                if (StreamEntry::compare(entry.id, start) < 0) {
                    include = false;
                }
            }
            
            // Check end
            if (end != "+") {
                if (StreamEntry::compare(entry.id, end) > 0) {
                    include = false;
                }
            }
            
            if (include) {
                result.push_back(entry);
                if (count && result.size() >= static_cast<size_t>(*count)) {
                    break;
                }
            }
        }
        
        return result;
    }
    
    /**
     * XREAD command - read from streams
     * Streams se padho - multiple streams ek saath
     * (Read from streams - multiple streams at once, multitasking)
     */
    std::vector<std::pair<std::string, std::vector<StreamEntry>>> 
    xread(const std::vector<std::pair<std::string, std::string>>& streams,
          std::optional<int64_t> count = std::nullopt) {
        std::shared_lock lock(mutex_);
        
        std::vector<std::pair<std::string, std::vector<StreamEntry>>> result;
        
        for (const auto& [streamKey, lastId] : streams) {
            auto it = data_.find(streamKey);
            if (it == data_.end() || it->second.isExpired()) {
                continue;
            }
            if (it->second.type != DataType::STREAM) {
                continue;
            }
            
            std::vector<StreamEntry> entries;
            
            for (const auto& entry : it->second.streamValue) {
                // Only include entries after lastId
                if (lastId == "$") {
                    // $ means only new entries - skip all existing
                    continue;
                }
                
                if (StreamEntry::compare(entry.id, lastId) > 0) {
                    entries.push_back(entry);
                    if (count && entries.size() >= static_cast<size_t>(*count)) {
                        break;
                    }
                }
            }
            
            if (!entries.empty()) {
                result.emplace_back(streamKey, std::move(entries));
            }
        }
        
        return result;
    }
    
    /**
     * Get last stream entry ID
     * Stream di last entry ID do
     * (Get last entry ID - for $ reference)
     */
    std::string getLastStreamId(const std::string& key) {
        std::shared_lock lock(mutex_);
        
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired() || 
            it->second.type != DataType::STREAM ||
            it->second.streamValue.empty()) {
            return "0-0";
        }
        
        return it->second.streamValue.back().id;
    }
    
    /**
     * Register blocked stream reader
     * Stream reader nu block karo
     * (Block stream reader - will wait for new data)
     */
    void registerStreamBlockedClient(int clientId,
                                      const std::vector<std::pair<std::string, std::string>>& streams,
                                      int64_t timeoutMs,
                                      std::function<void(const std::vector<std::pair<std::string, std::vector<StreamEntry>>>&)> callback) {
        std::lock_guard lock(streamBlockedMutex_);
        
        StreamBlockedClient sbc;
        sbc.clientId = clientId;
        sbc.streams = streams;
        sbc.callback = callback;
        sbc.hasDeadline = (timeoutMs > 0);
        
        if (timeoutMs > 0) {
            sbc.deadline = std::chrono::steady_clock::now() + 
                           std::chrono::milliseconds(timeoutMs);
        } else {
            sbc.deadline = std::chrono::steady_clock::time_point::max();
        }
        
        streamBlockedClients_.push_back(std::move(sbc));
    }
    
    /**
     * Unregister blocked stream client
     * Stream blocked client hatao
     * (Remove blocked stream client)
     */
    void unregisterStreamBlockedClient(int clientId) {
        std::lock_guard lock(streamBlockedMutex_);
        streamBlockedClients_.remove_if([clientId](const StreamBlockedClient& sbc) {
            return sbc.clientId == clientId;
        });
    }
    
    /**
     * Check stream blocked timeouts
     * Stream blocked timeouts check karo
     * (Check stream blocked timeouts)
     */
    void checkStreamBlockedTimeouts() {
        std::lock_guard lock(streamBlockedMutex_);
        auto now = std::chrono::steady_clock::now();
        
        streamBlockedClients_.remove_if([&now](StreamBlockedClient& sbc) {
            if (sbc.hasDeadline && sbc.deadline <= now) {
                // Timed out
                sbc.callback({});
                return true;
            }
            return false;
        });
    }

    // ========================================================================
    // PUB/SUB OPERATIONS - Publish/Subscribe
    // Pub/Sub - jiven WhatsApp broadcast
    // (Pub/Sub - like WhatsApp broadcast, one message many receivers)
    // ========================================================================
    
    /**
     * Subscribe to channel
     * Channel subscribe karo
     * (Subscribe to channel - like YouTube subscribe, but free!)
     */
    void subscribe(int clientId, const std::string& channel) {
        std::lock_guard lock(pubsubMutex_);
        channels_[channel].insert(clientId);
    }
    
    /**
     * Unsubscribe from channel
     * Channel unsubscribe karo
     * (Unsubscribe - like when you're done with drama)
     */
    void unsubscribe(int clientId, const std::string& channel) {
        std::lock_guard lock(pubsubMutex_);
        auto it = channels_.find(channel);
        if (it != channels_.end()) {
            it->second.erase(clientId);
            if (it->second.empty()) {
                channels_.erase(it);
            }
        }
    }
    
    /**
     * Unsubscribe from all channels
     * Saare channels se unsubscribe karo
     * (Unsubscribe from everything - digital detox)
     */
    void unsubscribeAll(int clientId) {
        std::lock_guard lock(pubsubMutex_);
        for (auto& [channel, subscribers] : channels_) {
            subscribers.erase(clientId);
        }
        // Clean up empty channels
        for (auto it = channels_.begin(); it != channels_.end();) {
            if (it->second.empty()) {
                it = channels_.erase(it);
            } else {
                ++it;
            }
        }
    }
    
    /**
     * Get subscribers of a channel
     * Channel ke subscribers do
     * (Get channel subscribers - who's listening?)
     */
    std::unordered_set<int> getSubscribers(const std::string& channel) {
        std::lock_guard lock(pubsubMutex_);
        auto it = channels_.find(channel);
        if (it != channels_.end()) {
            return it->second;
        }
        return {};
    }
    
    /**
     * Get subscription count for client
     * Client kitne channels pe hai
     * (How many channels is client subscribed to)
     */
    int64_t getSubscriptionCount(int clientId) {
        std::lock_guard lock(pubsubMutex_);
        int64_t count = 0;
        for (const auto& [channel, subscribers] : channels_) {
            if (subscribers.count(clientId)) {
                count++;
            }
        }
        return count;
    }
    
    /**
     * Get all subscribed channels for client
     * Client ke saare subscribed channels
     * (All channels client is subscribed to)
     */
    std::vector<std::string> getSubscribedChannels(int clientId) {
        std::lock_guard lock(pubsubMutex_);
        std::vector<std::string> result;
        for (const auto& [channel, subscribers] : channels_) {
            if (subscribers.count(clientId)) {
                result.push_back(channel);
            }
        }
        return result;
    }

    // ========================================================================
    // RDB LOADING - Load from RDB file
    // RDB file se load karo - persistence
    // (Load from RDB file - bring back the dead data)
    // ========================================================================
    
    /**
     * Load data from RDB file
     * RDB file se data load karo
     * (Load data from RDB file - resurrection time)
     */
    bool loadRDB(const std::string& filename) {
        std::ifstream file(filename, std::ios::binary);
        if (!file.is_open()) {
            return false;
        }
        
        std::unique_lock lock(mutex_);
        
        // Read magic string "REDIS"
        // Magic string padho - jiven "Open Sesame"
        // (Read magic string - like "Open Sesame" for the cave)
        char magic[5];
        file.read(magic, 5);
        if (std::string(magic, 5) != "REDIS") {
            return false;
        }
        
        // Read version (4 bytes)
        char version[4];
        file.read(version, 4);
        
        // Parse RDB content
        // RDB content parse karo - treasure hunt
        // (Parse RDB content - treasure hunt for data)
        int64_t currentExpireMs = -1;
        
        while (file.good()) {
            uint8_t type;
            file.read(reinterpret_cast<char*>(&type), 1);
            
            if (!file.good()) break;
            
            switch (type) {
                case 0xFD: {
                    // Expire time in seconds
                    uint32_t expireSec;
                    file.read(reinterpret_cast<char*>(&expireSec), 4);
                    currentExpireMs = static_cast<int64_t>(expireSec) * 1000;
                    break;
                }
                case 0xFC: {
                    // Expire time in milliseconds
                    uint64_t expireMs;
                    file.read(reinterpret_cast<char*>(&expireMs), 8);
                    currentExpireMs = static_cast<int64_t>(expireMs);
                    break;
                }
                case 0xFE: {
                    // Database selector
                    readRDBLength(file);  // DB number
                    break;
                }
                case 0xFB: {
                    // Resize DB
                    readRDBLength(file);  // DB size
                    readRDBLength(file);  // Expires size
                    break;
                }
                case 0xFF: {
                    // End of file
                    return true;
                }
                case 0x00: {
                    // String type
                    std::string key = readRDBString(file);
                    std::string value = readRDBString(file);
                    
                    RedisValue val(value);
                    if (currentExpireMs > 0) {
                        // Check if already expired
                        if (currentExpireMs > getCurrentTimeMs()) {
                            val.expiresAt = currentExpireMs;
                        } else {
                            // Already expired - skip
                            currentExpireMs = -1;
                            continue;
                        }
                    }
                    data_[key] = std::move(val);
                    currentExpireMs = -1;
                    break;
                }
                case 0xFA: {
                    // Auxiliary field
                    readRDBString(file);  // Key
                    readRDBString(file);  // Value
                    break;
                }
                default: {
                    // Unknown type - try to recover
                    // Pata nahi kya hai - skip karo
                    // (Don't know what this is - skip it)
                    break;
                }
            }
        }
        
        return true;
    }
    
    /**
     * Get value directly for RDB config purposes
     * Direct value lo - config lai
     * (Get value directly - for config purposes)
     */
    std::optional<std::string> getDirectValue(const std::string& key) {
        std::shared_lock lock(mutex_);
        auto it = data_.find(key);
        if (it == data_.end()) return std::nullopt;
        if (it->second.type == DataType::STRING) {
            return it->second.stringValue;
        }
        return std::nullopt;
    }
    
    /**
     * Set value directly
     * Direct value set karo
     * (Set value directly - bypass normal flow)
     */
    void setDirect(const std::string& key, RedisValue&& value) {
        std::unique_lock lock(mutex_);
        data_[key] = std::move(value);
    }
    
    /**
     * Get all keys (for replication)
     * Saare keys do - replication lai
     * (Get all keys - for replication purposes)
     */
    std::vector<std::string> getAllKeys() {
        std::shared_lock lock(mutex_);
        std::vector<std::string> result;
        for (const auto& [key, value] : data_) {
            if (!value.isExpired()) {
                result.push_back(key);
            }
        }
        return result;
    }
    
    /**
     * Get value with type (for replication)
     * Type ke saath value do
     * (Get value with type - for replication)
     */
    std::optional<RedisValue> getValue(const std::string& key) {
        std::shared_lock lock(mutex_);
        auto it = data_.find(key);
        if (it == data_.end() || it->second.isExpired()) {
            return std::nullopt;
        }
        return it->second;
    }

private:
    // ========================================================================
    // PRIVATE HELPER METHODS
    // Internal helper functions - kitchen mein kya ho raha hai
    // (Internal helpers - what's cooking in the kitchen)
    // ========================================================================
    
    /**
     * Internal exists check (without lock)
     * Internal existence check - lock nahi lagana
     * (Internal existence check - caller already has lock)
     */
    bool existsInternal(const std::string& key) const {
        auto it = data_.find(key);
        if (it == data_.end()) return false;
        if (it->second.isExpired()) return false;
        return true;
    }
    
    /**
     * Notify blocked list clients
     * Blocked list clients nu jagao
     * (Wake up blocked list clients - their data arrived!)
     */
    void notifyBlockedListClients(const std::string& key, bool fromLeft) {
        std::lock_guard bLock(blockedMutex_);
        
        for (auto it = blockedClients_.begin(); it != blockedClients_.end();) {
            bool found = false;
            for (const auto& k : it->keys) {
                if (k == key) {
                    found = true;
                    break;
                }
            }
            
            if (found) {
                // Try to pop value
                std::unique_lock lock(mutex_);
                auto dataIt = data_.find(key);
                if (dataIt != data_.end() && !dataIt->second.isExpired() &&
                    dataIt->second.type == DataType::LIST &&
                    !dataIt->second.listValue.empty()) {
                    
                    std::string value;
                    if (it->isLeft) {
                        value = dataIt->second.listValue.front();
                        dataIt->second.listValue.pop_front();
                    } else {
                        value = dataIt->second.listValue.back();
                        dataIt->second.listValue.pop_back();
                    }
                    
                    if (dataIt->second.listValue.empty()) {
                        data_.erase(dataIt);
                    }
                    
                    lock.unlock();
                    it->callback(key, value);
                    it = blockedClients_.erase(it);
                    continue;
                }
            }
            ++it;
        }
    }
    
    /**
     * Notify blocked stream clients
     * Blocked stream clients nu jagao
     * (Wake up blocked stream clients - new stream data!)
     */
    void notifyStreamBlockedClients(const std::string& streamKey) {
        std::lock_guard sLock(streamBlockedMutex_);
        
        for (auto it = streamBlockedClients_.begin(); it != streamBlockedClients_.end();) {
            bool interested = false;
            std::string lastId;
            
            for (const auto& [key, lid] : it->streams) {
                if (key == streamKey) {
                    interested = true;
                    lastId = lid;
                    break;
                }
            }
            
            if (interested) {
                // Get new entries
                std::shared_lock lock(mutex_);
                auto result = xread(it->streams);
                lock.unlock();
                
                if (!result.empty()) {
                    it->callback(result);
                    it = streamBlockedClients_.erase(it);
                    continue;
                }
            }
            ++it;
        }
    }
    
    /**
     * Read RDB length-encoded value
     * RDB length-encoded value padho
     * (Read RDB length-encoded value - compression magic)
     */
    uint64_t readRDBLength(std::ifstream& file) {
        uint8_t byte;
        file.read(reinterpret_cast<char*>(&byte), 1);
        
        uint8_t type = (byte & 0xC0) >> 6;
        
        switch (type) {
            case 0: {
                // 6-bit length
                return byte & 0x3F;
            }
            case 1: {
                // 14-bit length
                uint8_t next;
                file.read(reinterpret_cast<char*>(&next), 1);
                return ((byte & 0x3F) << 8) | next;
            }
            case 2: {
                // 32-bit length
                uint32_t len;
                file.read(reinterpret_cast<char*>(&len), 4);
                return len;
            }
            case 3: {
                // Special encoding
                return byte & 0x3F;
            }
            default:
                return 0;
        }
    }
    
    /**
     * Read RDB string
     * RDB string padho
     * (Read RDB string - extract the text)
     */
    std::string readRDBString(std::ifstream& file) {
        uint8_t byte;
        file.read(reinterpret_cast<char*>(&byte), 1);
        
        uint8_t type = (byte & 0xC0) >> 6;
        
        if (type == 3) {
            // Special encoding
            uint8_t encoding = byte & 0x3F;
            
            switch (encoding) {
                case 0: {
                    // 8-bit integer
                    int8_t val;
                    file.read(reinterpret_cast<char*>(&val), 1);
                    return std::to_string(val);
                }
                case 1: {
                    // 16-bit integer
                    int16_t val;
                    file.read(reinterpret_cast<char*>(&val), 2);
                    return std::to_string(val);
                }
                case 2: {
                    // 32-bit integer
                    int32_t val;
                    file.read(reinterpret_cast<char*>(&val), 4);
                    return std::to_string(val);
                }
                default:
                    return "";
            }
        }
        
        // Regular string
        file.seekg(-1, std::ios::cur);  // Go back
        uint64_t len = readRDBLength(file);
        
        std::string str(len, '\0');
        file.read(&str[0], len);
        
        return str;
    }
};

} // namespace Redis
