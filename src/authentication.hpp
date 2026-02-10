/**
 * =============================================================================
 *                          AUTHENTICATION SYSTEM
 * =============================================================================
 * 
 * Paaji eh file hai authentication di - security first!
 * (Bro this file is for authentication - security is priority!)
 * 
 * AUTH command te ACL (Access Control List) handle karna
 * (Handle AUTH command and ACL - Access Control List)
 * 
 * Jiven bank mein security guard - koi bhi andar nahi aane dete
 * (Like a bank security guard - doesn't let anyone in without verification)
 * 
 * =============================================================================
 */

#pragma once

#include "common.hpp"
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <vector>

namespace Redis {

// ============================================================================
// ACL PERMISSION FLAGS
// Permissions - ki ki kar sakde ne
// (Permissions - what all can be done)
// ============================================================================

/**
 * @enum AclCategory
 * @brief Command categories for ACL
 * 
 * Commands ki categories - grouping for permissions
 * (Command categories - grouping for easier permission management)
 */
enum class AclCategory : uint32_t {
    NONE            = 0,
    READ            = 1 << 0,    // Read commands (GET, LRANGE, etc.)
    WRITE           = 1 << 1,    // Write commands (SET, LPUSH, etc.)
    SET             = 1 << 2,    // Set commands
    SORTEDSET       = 1 << 3,    // Sorted set commands
    LIST            = 1 << 4,    // List commands
    HASH            = 1 << 5,    // Hash commands
    STRING          = 1 << 6,    // String commands
    BITMAP          = 1 << 7,    // Bitmap commands
    HYPERLOGLOG     = 1 << 8,    // HyperLogLog commands
    GEO             = 1 << 9,    // Geo commands
    STREAM          = 1 << 10,   // Stream commands
    PUBSUB          = 1 << 11,   // Pub/Sub commands
    ADMIN           = 1 << 12,   // Admin commands
    DANGEROUS       = 1 << 13,   // Dangerous commands
    CONNECTION      = 1 << 14,   // Connection commands
    TRANSACTION     = 1 << 15,   // Transaction commands
    FAST            = 1 << 16,   // Fast commands (O(1) or O(log N))
    SLOW            = 1 << 17,   // Slow commands (O(N) or worse)
    ALL             = 0xFFFFFFFF // All categories
};

// Enable bitwise operations for AclCategory
// Bitwise operations enable karo - flags combine karne lai
// (Enable bitwise operations - for combining flags)
inline AclCategory operator|(AclCategory a, AclCategory b) {
    return static_cast<AclCategory>(static_cast<uint32_t>(a) | static_cast<uint32_t>(b));
}

inline AclCategory operator&(AclCategory a, AclCategory b) {
    return static_cast<AclCategory>(static_cast<uint32_t>(a) & static_cast<uint32_t>(b));
}

inline AclCategory& operator|=(AclCategory& a, AclCategory b) {
    return a = a | b;
}

// ============================================================================
// USER DEFINITION
// User ki definition - permissions te password
// (User definition - permissions and password)
// ============================================================================

/**
 * @struct AclUser
 * @brief Represents an ACL user
 * 
 * User di puri info - password, permissions, keys access
 * (Complete user info - password, permissions, key patterns)
 */
struct AclUser {
    std::string username;                              // Username
    std::string passwordHash;                          // Hashed password (simple hash for now)
    bool enabled;                                       // Is user active?
    bool nopass;                                        // No password required?
    
    // Command permissions
    // Command permissions - ki allow hai ki nahi
    // (Command permissions - what's allowed what's not)
    AclCategory allowedCategories;                     // Allowed command categories
    AclCategory deniedCategories;                      // Denied command categories
    std::unordered_set<std::string> allowedCommands;   // Specific allowed commands
    std::unordered_set<std::string> deniedCommands;    // Specific denied commands
    
    // Key patterns
    // Key patterns - ki keys access kar sakde ne
    // (Key patterns - which keys can be accessed)
    std::vector<std::string> keyPatterns;              // Allowed key patterns
    bool allKeys;                                       // Access to all keys?
    
    // Channel patterns for Pub/Sub
    // Channel patterns - Pub/Sub lai
    // (Channel patterns - for Pub/Sub access)
    std::vector<std::string> channelPatterns;
    bool allChannels;
    
    /**
     * Constructor - default user settings
     * Default settings - normal user
     * (Default settings - for regular users)
     */
    AclUser(const std::string& name = "default") 
        : username(name)
        , enabled(true)
        , nopass(true)
        , allowedCategories(AclCategory::ALL)
        , deniedCategories(AclCategory::NONE)
        , allKeys(true)
        , allChannels(true) {
    }
    
    /**
     * Check if user can execute a command
     * User eh command kar sakda hai ki nahi
     * (Can this user execute this command?)
     */
    bool canExecuteCommand(const std::string& cmd, AclCategory category) const {
        if (!enabled) {
            return false;
        }
        
        // Check denied first
        // Pehle denied check karo
        // (Check denied first - security priority)
        if (deniedCommands.count(cmd) > 0) {
            return false;
        }
        
        if ((deniedCategories & category) != AclCategory::NONE) {
            return false;
        }
        
        // Check allowed
        // Allowed check karo
        // (Check if allowed)
        if (allowedCommands.count(cmd) > 0) {
            return true;
        }
        
        return (allowedCategories & category) != AclCategory::NONE;
    }
    
    /**
     * Check if user can access a key
     * User eh key access kar sakda hai ki nahi
     * (Can this user access this key?)
     */
    bool canAccessKey(const std::string& key) const {
        if (!enabled) {
            return false;
        }
        
        if (allKeys) {
            return true;
        }
        
        // Check key patterns
        // Key patterns match karo
        // (Check if key matches any pattern)
        for (const auto& pattern : keyPatterns) {
            if (matchPattern(pattern, key)) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * Check if user can access a channel
     * User eh channel access kar sakda hai ki nahi
     * (Can this user access this channel?)
     */
    bool canAccessChannel(const std::string& channel) const {
        if (!enabled) {
            return false;
        }
        
        if (allChannels) {
            return true;
        }
        
        // Check channel patterns
        for (const auto& pattern : channelPatterns) {
            if (matchPattern(pattern, channel)) {
                return true;
            }
        }
        
        return false;
    }

private:
    /**
     * Simple pattern matching
     * Pattern match karo - * wildcard support
     * (Pattern matching - supports * wildcard)
     */
    static bool matchPattern(const std::string& pattern, const std::string& str) {
        if (pattern == "*") {
            return true;
        }
        
        // Simple prefix match with *
        // Simple prefix match - * at end
        // (Simple prefix match - if pattern ends with *)
        if (!pattern.empty() && pattern.back() == '*') {
            std::string prefix = pattern.substr(0, pattern.length() - 1);
            return str.rfind(prefix, 0) == 0;
        }
        
        // Exact match
        // Exact match - pura match hona chahiye
        // (Exact match required)
        return pattern == str;
    }
};

// ============================================================================
// AUTH MANAGER
// Authentication manager - users te authentication handle
// (Authentication manager - handle users and authentication)
// ============================================================================

/**
 * @class AuthManager
 * @brief Manages user authentication and ACL
 * 
 * Pura authentication system - users, passwords, permissions
 * (Complete authentication system - users, passwords, permissions)
 * 
 * Jiven school mein register - kaun hai, ki kar sakda hai
 * (Like a school register - who's who, who can do what)
 */
class AuthManager {
private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, AclUser> users_;
    std::string legacyPassword_;                  // For simple AUTH password
    bool requireAuth_;                            // Is authentication required?
    
public:
    /**
     * Constructor
     * Default user create karo - "default" with full permissions
     * (Create default user with full permissions)
     */
    AuthManager() : requireAuth_(false) {
        // Create default user
        // Default user banao - full access
        // (Create default user with full access)
        AclUser defaultUser("default");
        defaultUser.enabled = true;
        defaultUser.nopass = true;
        defaultUser.allowedCategories = AclCategory::ALL;
        defaultUser.allKeys = true;
        defaultUser.allChannels = true;
        users_["default"] = defaultUser;
    }
    
    // ========================================================================
    // AUTHENTICATION METHODS
    // Authentication methods - login/logout
    // (Authentication methods - verify users)
    // ========================================================================
    
    /**
     * Set legacy password (simple AUTH)
     * Simple password set karo - purana style
     * (Set simple password - old style AUTH)
     */
    void setLegacyPassword(const std::string& password) {
        std::lock_guard<std::mutex> lock(mutex_);
        legacyPassword_ = password;
        requireAuth_ = !password.empty();
        
        // Update default user password
        // Default user ka password update karo
        // (Update default user's password)
        if (!password.empty()) {
            users_["default"].nopass = false;
            users_["default"].passwordHash = simpleHash(password);
        } else {
            users_["default"].nopass = true;
            users_["default"].passwordHash = "";
        }
    }
    
    /**
     * Check if authentication is required
     * Auth chahiye ki nahi
     * (Is authentication required?)
     */
    bool isAuthRequired() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return requireAuth_;
    }
    
    /**
     * Authenticate with legacy password
     * Simple AUTH - just password
     * (Simple AUTH - just password, old style)
     */
    bool authenticateLegacy(const std::string& password) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        if (legacyPassword_.empty()) {
            return true;  // No password set, allow
        }
        
        return password == legacyPassword_;
    }
    
    /**
     * Authenticate with username and password
     * ACL style AUTH - username + password
     * (ACL style AUTH - with username)
     */
    bool authenticate(const std::string& username, const std::string& password) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        auto it = users_.find(username);
        if (it == users_.end()) {
            return false;
        }
        
        const AclUser& user = it->second;
        
        if (!user.enabled) {
            return false;
        }
        
        if (user.nopass) {
            return true;
        }
        
        return user.passwordHash == simpleHash(password);
    }
    
    /**
     * Get user for authenticated client
     * Authenticated user di info lo
     * (Get info for authenticated user)
     */
    const AclUser* getUser(const std::string& username) const {
        std::lock_guard<std::mutex> lock(mutex_);
        
        auto it = users_.find(username);
        if (it != users_.end()) {
            return &it->second;
        }
        
        return nullptr;
    }
    
    // ========================================================================
    // ACL MANAGEMENT
    // ACL management - users add/remove/modify
    // (ACL management - user CRUD operations)
    // ========================================================================
    
    /**
     * Create or update user
     * User banao ya update karo
     * (Create or update user)
     */
    void setUser(const AclUser& user) {
        std::lock_guard<std::mutex> lock(mutex_);
        users_[user.username] = user;
    }
    
    /**
     * Delete user
     * User delete karo - bye bye
     * (Delete user - remove from system)
     */
    bool deleteUser(const std::string& username) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        // Can't delete default user
        // Default user delete nahi kar sakte
        // (Cannot delete default user)
        if (username == "default") {
            return false;
        }
        
        return users_.erase(username) > 0;
    }
    
    /**
     * List all users
     * Saare users ki list
     * (List all users)
     */
    std::vector<std::string> listUsers() const {
        std::lock_guard<std::mutex> lock(mutex_);
        
        std::vector<std::string> usernames;
        for (const auto& [name, user] : users_) {
            usernames.push_back(name);
        }
        
        return usernames;
    }
    
    /**
     * Get user info for ACL LIST
     * User di puri info - ACL LIST lai
     * (Get complete user info for ACL LIST command)
     */
    std::string getUserInfo(const std::string& username) const {
        std::lock_guard<std::mutex> lock(mutex_);
        
        auto it = users_.find(username);
        if (it == users_.end()) {
            return "";
        }
        
        const AclUser& user = it->second;
        std::ostringstream ss;
        
        ss << "user " << username << " ";
        
        // Status
        ss << (user.enabled ? "on" : "off") << " ";
        
        // Password
        if (user.nopass) {
            ss << "nopass ";
        } else {
            ss << "#" << user.passwordHash.substr(0, 8) << "... ";
        }
        
        // Keys
        if (user.allKeys) {
            ss << "~* ";
        } else {
            for (const auto& pattern : user.keyPatterns) {
                ss << "~" << pattern << " ";
            }
        }
        
        // Channels
        if (user.allChannels) {
            ss << "&* ";
        }
        
        // Commands
        if (user.allowedCategories == AclCategory::ALL) {
            ss << "+@all";
        } else {
            ss << "-@all";
        }
        
        return ss.str();
    }
    
    /**
     * Get WHOAMI response
     * Kaun hai tu - current user
     * (Who are you - return current username)
     */
    std::string whoami(const std::string& username) const {
        return respBulkString(username);
    }
    
    // ========================================================================
    // ACL PARSING
    // ACL rules parse karo
    // (Parse ACL rules from string)
    // ========================================================================
    
    /**
     * Parse ACL rules and apply to user
     * ACL rules parse karke user update karo
     * (Parse ACL rules and apply to user)
     * 
     * Syntax: on/off, nopass, >password, ~keys, +cmd, -cmd, +@category
     */
    std::string parseAndApplyRules(const std::string& username, 
                                   const std::vector<std::string>& rules) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        // Get or create user
        // User lo ya banao
        // (Get existing user or create new)
        if (users_.find(username) == users_.end()) {
            users_[username] = AclUser(username);
        }
        
        AclUser& user = users_[username];
        
        for (const auto& rule : rules) {
            if (rule == "on") {
                user.enabled = true;
            }
            else if (rule == "off") {
                user.enabled = false;
            }
            else if (rule == "nopass") {
                user.nopass = true;
            }
            else if (rule == "resetpass") {
                user.nopass = true;
                user.passwordHash = "";
            }
            else if (rule == "allkeys" || rule == "~*") {
                user.allKeys = true;
            }
            else if (rule == "resetkeys") {
                user.allKeys = false;
                user.keyPatterns.clear();
            }
            else if (rule == "allchannels" || rule == "&*") {
                user.allChannels = true;
            }
            else if (rule == "resetchannels") {
                user.allChannels = false;
                user.channelPatterns.clear();
            }
            else if (rule == "allcommands" || rule == "+@all") {
                user.allowedCategories = AclCategory::ALL;
            }
            else if (rule == "nocommands" || rule == "-@all") {
                user.allowedCategories = AclCategory::NONE;
            }
            else if (rule[0] == '>') {
                // Set password
                // Password set karo
                // (Set password)
                user.nopass = false;
                user.passwordHash = simpleHash(rule.substr(1));
            }
            else if (rule[0] == '~') {
                // Add key pattern
                // Key pattern add karo
                // (Add key pattern)
                user.allKeys = false;
                user.keyPatterns.push_back(rule.substr(1));
            }
            else if (rule[0] == '&') {
                // Add channel pattern
                // Channel pattern add karo
                // (Add channel pattern)
                user.allChannels = false;
                user.channelPatterns.push_back(rule.substr(1));
            }
            else if (rule[0] == '+') {
                // Allow command/category
                // Command/category allow karo
                // (Allow command or category)
                if (rule[1] == '@') {
                    // Category
                    user.allowedCategories |= parseCategory(rule.substr(2));
                } else {
                    // Command
                    user.allowedCommands.insert(toUpper(rule.substr(1)));
                }
            }
            else if (rule[0] == '-') {
                // Deny command/category
                // Command/category deny karo
                // (Deny command or category)
                if (rule[1] == '@') {
                    user.deniedCategories |= parseCategory(rule.substr(2));
                } else {
                    user.deniedCommands.insert(toUpper(rule.substr(1)));
                }
            }
            else {
                return respError("ERR Unknown ACL rule: " + rule);
            }
        }
        
        return respOK();
    }
    
    /**
     * Check user permission for command
     * User ki permission check karo
     * (Check if user has permission for command)
     */
    bool checkPermission(const std::string& username, 
                        const std::string& command,
                        const std::string& key = "") const {
        std::lock_guard<std::mutex> lock(mutex_);
        
        auto it = users_.find(username);
        if (it == users_.end()) {
            return false;
        }
        
        const AclUser& user = it->second;
        
        // Check command permission
        // Command permission check karo
        // (Check if command is allowed)
        AclCategory category = getCommandCategory(command);
        if (!user.canExecuteCommand(command, category)) {
            return false;
        }
        
        // Check key permission if key is provided
        // Key permission check karo agar key di hai
        // (Check key permission if key is provided)
        if (!key.empty() && !user.canAccessKey(key)) {
            return false;
        }
        
        return true;
    }

private:
    /**
     * Simple hash function for passwords
     * Simple hash - production mein proper hash use karo
     * (Simple hash - use proper hash in production!)
     */
    static std::string simpleHash(const std::string& input) {
        // Simple hash for demo - NOT for production!
        // Eh production lai nahi hai - just demo
        // (This is NOT for production - just a demo!)
        std::hash<std::string> hasher;
        size_t hash = hasher(input);
        std::ostringstream ss;
        ss << std::hex << hash;
        return ss.str();
    }
    
    /**
     * Parse category name to enum
     * Category name se enum banao
     * (Convert category name to enum)
     */
    static AclCategory parseCategory(const std::string& name) {
        std::string lower = name;
        std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
        
        if (lower == "read") return AclCategory::READ;
        if (lower == "write") return AclCategory::WRITE;
        if (lower == "set") return AclCategory::SET;
        if (lower == "sortedset") return AclCategory::SORTEDSET;
        if (lower == "list") return AclCategory::LIST;
        if (lower == "hash") return AclCategory::HASH;
        if (lower == "string") return AclCategory::STRING;
        if (lower == "geo") return AclCategory::GEO;
        if (lower == "stream") return AclCategory::STREAM;
        if (lower == "pubsub") return AclCategory::PUBSUB;
        if (lower == "admin") return AclCategory::ADMIN;
        if (lower == "dangerous") return AclCategory::DANGEROUS;
        if (lower == "connection") return AclCategory::CONNECTION;
        if (lower == "transaction") return AclCategory::TRANSACTION;
        if (lower == "fast") return AclCategory::FAST;
        if (lower == "slow") return AclCategory::SLOW;
        if (lower == "all") return AclCategory::ALL;
        
        return AclCategory::NONE;
    }
    
    /**
     * Get command category
     * Command ki category return karo
     * (Return the category for a command)
     */
    static AclCategory getCommandCategory(const std::string& command) {
        std::string cmd = command;
        std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
        
        // Read commands
        if (cmd == "GET" || cmd == "MGET" || cmd == "LRANGE" || cmd == "SMEMBERS" ||
            cmd == "HGET" || cmd == "HGETALL" || cmd == "ZRANGE" || cmd == "KEYS" ||
            cmd == "EXISTS" || cmd == "TYPE" || cmd == "TTL" || cmd == "PTTL" ||
            cmd == "STRLEN" || cmd == "LLEN" || cmd == "SCARD" || cmd == "ZCARD" ||
            cmd == "XRANGE" || cmd == "XREAD" || cmd == "GEOPOS" || cmd == "GEODIST") {
            return AclCategory::READ;
        }
        
        // Write commands
        if (cmd == "SET" || cmd == "MSET" || cmd == "DEL" || cmd == "LPUSH" || cmd == "RPUSH" ||
            cmd == "LPOP" || cmd == "RPOP" || cmd == "SADD" || cmd == "SREM" ||
            cmd == "HSET" || cmd == "HDEL" || cmd == "ZADD" || cmd == "ZREM" ||
            cmd == "XADD" || cmd == "GEOADD" || cmd == "EXPIRE" || cmd == "EXPIREAT" ||
            cmd == "INCR" || cmd == "DECR" || cmd == "INCRBY" || cmd == "DECRBY") {
            return AclCategory::WRITE;
        }
        
        // Admin commands
        if (cmd == "CONFIG" || cmd == "DEBUG" || cmd == "FLUSHDB" || cmd == "FLUSHALL" ||
            cmd == "BGSAVE" || cmd == "BGREWRITEAOF" || cmd == "SHUTDOWN" ||
            cmd == "SLAVEOF" || cmd == "REPLICAOF" || cmd == "ACL") {
            return AclCategory::ADMIN | AclCategory::DANGEROUS;
        }
        
        // Pub/Sub commands
        if (cmd == "SUBSCRIBE" || cmd == "UNSUBSCRIBE" || cmd == "PUBLISH" ||
            cmd == "PSUBSCRIBE" || cmd == "PUNSUBSCRIBE") {
            return AclCategory::PUBSUB;
        }
        
        // Transaction commands
        if (cmd == "MULTI" || cmd == "EXEC" || cmd == "DISCARD" || cmd == "WATCH") {
            return AclCategory::TRANSACTION;
        }
        
        // Connection commands
        if (cmd == "PING" || cmd == "ECHO" || cmd == "AUTH" || cmd == "QUIT" ||
            cmd == "SELECT" || cmd == "CLIENT") {
            return AclCategory::CONNECTION | AclCategory::FAST;
        }
        
        // Default to slow read
        return AclCategory::READ | AclCategory::SLOW;
    }
};

} // namespace Redis
