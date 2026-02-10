/**
 * =============================================================================
 *                          REDIS COMMAND HANDLER
 * =============================================================================
 * 
 * Paaji eh file hai jahan saare commands handle hunde ne!
 * (Bro this is where all commands are handled!)
 * 
 * Jiven restaurant mein waiter order leta hai, ohi eh karta hai commands lai
 * (Just like a waiter takes orders in a restaurant, this handles commands)
 * 
 * Har command da apna function hai - organized like Indian Railways (just kidding)
 * (Each command has its own function - organized unlike Indian Railways lol)
 * 
 * =============================================================================
 */

#pragma once

#include "common.hpp"
#include "resp_parser.hpp"
#include "data_store.hpp"
#include "client.hpp"

namespace Redis {

// Forward declaration
class RedisServer;

// ============================================================================
// COMMAND HANDLER CLASS
// Commands process karne wala - Brain of Redis
// (Command processor - the brain of Redis)
// ============================================================================

/**
 * @class CommandHandler
 * @brief Handles all Redis commands
 * 
 * Sab commands yahan process hunde ne:
 * (All commands are processed here:)
 * - String commands (GET, SET, INCR, etc.)
 * - List commands (LPUSH, RPUSH, LRANGE, etc.)
 * - Set commands (SADD, SMEMBERS, etc.)
 * - Sorted Set commands (ZADD, ZRANGE, etc.)
 * - Stream commands (XADD, XREAD, etc.)
 * - Pub/Sub commands (SUBSCRIBE, PUBLISH, etc.)
 * - Transaction commands (MULTI, EXEC, etc.)
 * - Server commands (PING, INFO, CONFIG, etc.)
 * 
 * Jiven call center sab calls handle karta hai
 * (Like a call center handles all calls)
 */
class CommandHandler {
private:
    DataStore& store_;                  // Data store reference - godown ka address
                                        // (Data store reference - warehouse address)
    RedisServer* server_;               // Server reference - boss ka address
                                        // (Server reference - boss's address)

public:
    /**
     * Constructor - handler banao
     * (Create handler - initialize)
     */
    CommandHandler(DataStore& store, RedisServer* server = nullptr) 
        : store_(store), server_(server) {}
    
    /**
     * Set server reference
     * Server reference set karo - late initialization
     * (Set server reference - late binding)
     */
    void setServer(RedisServer* server) {
        server_ = server;
    }

    /**
     * Execute a command
     * Command execute karo - main entry point
     * (Execute command - main entry point)
     * 
     * @param client The client connection
     * @param args Command and arguments
     * @return Response to send to client
     */
    std::string execute(ClientConnection* client, const StringVector& args);

private:
    // ========================================================================
    // STRING COMMANDS - Strings ke saath khelna
    // (Playing with strings - basic stuff)
    // ========================================================================
    
    /**
     * PING command - server alive hai ki nahi
     * (PING - is server alive? PONG means yes!)
     */
    std::string cmdPing(ClientConnection* client, const StringVector& args) {
        // Agar argument diya hai, toh wohi return karo
        // (If argument given, return that instead)
        if (args.size() > 1) {
            return respBulkString(args[1]);
        }
        return respSimpleString("PONG");
    }
    
    /**
     * ECHO command - jo bolo wohi wapas
     * (ECHO - whatever you say, I say back - parrot mode)
     */
    std::string cmdEcho(ClientConnection* client, const StringVector& args) {
        if (args.size() < 2) {
            return respError("ERR wrong number of arguments for 'echo' command");
        }
        return respBulkString(args[1]);
    }
    
    /**
     * SET command - value set karo
     * (SET - store a value, most used command probably)
     * 
     * SET key value [EX seconds] [PX milliseconds] [NX|XX]
     */
    std::string cmdSet(ClientConnection* client, const StringVector& args) {
        if (args.size() < 3) {
            return respError("ERR wrong number of arguments for 'set' command");
        }
        
        const std::string& key = args[1];
        const std::string& value = args[2];
        
        std::optional<int64_t> ex, px;
        bool nx = false, xx = false;
        
        // Parse optional arguments
        // Optional arguments parse karo - EX, PX, NX, XX
        // (Parse optional arguments - variety of options)
        for (size_t i = 3; i < args.size(); i++) {
            std::string opt = toUpper(args[i]);
            
            if (opt == "EX" && i + 1 < args.size()) {
                // Expire in seconds - seconds mein expire
                // (Expire in seconds)
                try {
                    ex = std::stoll(args[++i]);
                } catch (...) {
                    return respError(ERR_INVALID_INT);
                }
            } else if (opt == "PX" && i + 1 < args.size()) {
                // Expire in milliseconds - milliseconds mein expire
                // (Expire in milliseconds - more precise)
                try {
                    px = std::stoll(args[++i]);
                } catch (...) {
                    return respError(ERR_INVALID_INT);
                }
            } else if (opt == "NX") {
                // Only set if not exists - sirf tab set karo jab key nahi hai
                // (Only set if key doesn't exist - playing hard to get)
                nx = true;
            } else if (opt == "XX") {
                // Only set if exists - sirf tab set karo jab key hai
                // (Only set if key exists - committed relationship)
                xx = true;
            } else if (opt == "KEEPTTL") {
                // Keep existing TTL - purana expire time rakhna hai
                // (Keep existing TTL - don't touch the timer)
                // TODO: Implement KEEPTTL
            } else if (opt == "GET") {
                // Return old value - purani value return karo
                // (Return old value - nostalgia)
                // This is GETSET behavior
                auto oldVal = store_.get(key);
                store_.set(key, value, px, ex, nx, xx);
                if (oldVal) {
                    return respBulkString(*oldVal);
                }
                return respNull();
            }
        }
        
        // NX and XX are mutually exclusive
        // NX te XX ek saath nahi ho sakde - contradiction hai
        // (NX and XX can't be together - logical contradiction)
        if (nx && xx) {
            return respError("ERR XX and NX options at the same time are not compatible");
        }
        
        // Check NX/XX conditions before setting
        if (nx && store_.exists(key)) {
            return respNull();
        }
        if (xx && !store_.exists(key)) {
            return respNull();
        }
        
        store_.set(key, value, px, ex, nx, xx);
        return respOK();
    }
    
    /**
     * GET command - value le lo
     * (GET - retrieve a value, bread and butter)
     */
    std::string cmdGet(ClientConnection* client, const StringVector& args) {
        if (args.size() < 2) {
            return respError("ERR wrong number of arguments for 'get' command");
        }
        
        // Pehle type check karo - galat type pe error
        // (First check type - error on wrong type)
        DataType type = store_.type(args[1]);
        if (type != DataType::NONE && type != DataType::STRING) {
            return respError(ERR_WRONG_TYPE);
        }
        
        auto value = store_.get(args[1]);
        if (value) {
            return respBulkString(*value);
        }
        return respNull();
    }
    
    /**
     * INCR command - 1 vadha do
     * (INCR - add 1, counting up)
     */
    std::string cmdIncr(ClientConnection* client, const StringVector& args) {
        if (args.size() < 2) {
            return respError("ERR wrong number of arguments for 'incr' command");
        }
        
        auto result = store_.incr(args[1]);
        if (std::holds_alternative<int64_t>(result)) {
            return respInteger(std::get<int64_t>(result));
        }
        return respError(std::get<std::string>(result));
    }
    
    /**
     * INCRBY command - specific amount vadha do
     * (INCRBY - add specific amount)
     */
    std::string cmdIncrBy(ClientConnection* client, const StringVector& args) {
        if (args.size() < 3) {
            return respError("ERR wrong number of arguments for 'incrby' command");
        }
        
        int64_t increment;
        try {
            increment = std::stoll(args[2]);
        } catch (...) {
            return respError(ERR_INVALID_INT);
        }
        
        auto result = store_.incrby(args[1], increment);
        if (std::holds_alternative<int64_t>(result)) {
            return respInteger(std::get<int64_t>(result));
        }
        return respError(std::get<std::string>(result));
    }
    
    /**
     * DECR command - 1 ghata do
     * (DECR - subtract 1, counting down)
     */
    std::string cmdDecr(ClientConnection* client, const StringVector& args) {
        if (args.size() < 2) {
            return respError("ERR wrong number of arguments for 'decr' command");
        }
        
        auto result = store_.decr(args[1]);
        if (std::holds_alternative<int64_t>(result)) {
            return respInteger(std::get<int64_t>(result));
        }
        return respError(std::get<std::string>(result));
    }
    
    /**
     * MGET command - multiple keys get karo
     * (MGET - get multiple keys at once - bulk retrieval)
     */
    std::string cmdMGet(ClientConnection* client, const StringVector& args) {
        if (args.size() < 2) {
            return respError("ERR wrong number of arguments for 'mget' command");
        }
        
        std::vector<std::string> keys(args.begin() + 1, args.end());
        auto values = store_.mget(keys);
        
        std::vector<RespValue> arr;
        for (const auto& val : values) {
            if (val) {
                arr.push_back(RespValue::bulkString(*val));
            } else {
                arr.push_back(RespValue::null());
            }
        }
        
        return RespValue::array(arr).serialize();
    }
    
    /**
     * MSET command - multiple keys set karo
     * (MSET - set multiple keys at once - bulk operation)
     */
    std::string cmdMSet(ClientConnection* client, const StringVector& args) {
        if (args.size() < 3 || (args.size() - 1) % 2 != 0) {
            return respError("ERR wrong number of arguments for 'mset' command");
        }
        
        std::vector<std::pair<std::string, std::string>> kvs;
        for (size_t i = 1; i < args.size(); i += 2) {
            kvs.emplace_back(args[i], args[i + 1]);
        }
        
        store_.mset(kvs);
        return respOK();
    }

    // ========================================================================
    // KEY COMMANDS - Keys ke saath khelna
    // (Playing with keys - managing keys)
    // ========================================================================
    
    /**
     * DEL command - key delete karo
     * (DEL - delete keys, say goodbye)
     */
    std::string cmdDel(ClientConnection* client, const StringVector& args) {
        if (args.size() < 2) {
            return respError("ERR wrong number of arguments for 'del' command");
        }
        
        std::vector<std::string> keys(args.begin() + 1, args.end());
        int64_t deleted = store_.del(keys);
        return respInteger(deleted);
    }
    
    /**
     * EXISTS command - key hai ki nahi
     * (EXISTS - check if key exists, existential crisis resolver)
     */
    std::string cmdExists(ClientConnection* client, const StringVector& args) {
        if (args.size() < 2) {
            return respError("ERR wrong number of arguments for 'exists' command");
        }
        
        int64_t count = 0;
        for (size_t i = 1; i < args.size(); i++) {
            if (store_.exists(args[i])) {
                count++;
            }
        }
        return respInteger(count);
    }
    
    /**
     * TYPE command - key da type kya hai
     * (TYPE - what type is this key, identity check)
     */
    std::string cmdType(ClientConnection* client, const StringVector& args) {
        if (args.size() < 2) {
            return respError("ERR wrong number of arguments for 'type' command");
        }
        
        return respSimpleString(store_.typeString(args[1]));
    }
    
    /**
     * KEYS command - pattern match karne wale keys
     * (KEYS - find keys matching pattern, like Tinder for keys)
     */
    std::string cmdKeys(ClientConnection* client, const StringVector& args) {
        if (args.size() < 2) {
            return respError("ERR wrong number of arguments for 'keys' command");
        }
        
        auto keys = store_.keys(args[1]);
        return respStringArray(keys);
    }
    
    /**
     * EXPIRE command - key nu seconds mein expire karo
     * (EXPIRE - set expiration in seconds, countdown begins)
     */
    std::string cmdExpire(ClientConnection* client, const StringVector& args) {
        if (args.size() < 3) {
            return respError("ERR wrong number of arguments for 'expire' command");
        }
        
        int64_t seconds;
        try {
            seconds = std::stoll(args[2]);
        } catch (...) {
            return respError(ERR_INVALID_INT);
        }
        
        return respInteger(store_.expire(args[1], seconds) ? 1 : 0);
    }
    
    /**
     * PEXPIRE command - milliseconds mein expire
     * (PEXPIRE - set expiration in milliseconds, more precise)
     */
    std::string cmdPExpire(ClientConnection* client, const StringVector& args) {
        if (args.size() < 3) {
            return respError("ERR wrong number of arguments for 'pexpire' command");
        }
        
        int64_t ms;
        try {
            ms = std::stoll(args[2]);
        } catch (...) {
            return respError(ERR_INVALID_INT);
        }
        
        return respInteger(store_.pexpire(args[1], ms) ? 1 : 0);
    }
    
    /**
     * TTL command - kitna time bacha hai (seconds)
     * (TTL - time to live in seconds, countdown check)
     */
    std::string cmdTTL(ClientConnection* client, const StringVector& args) {
        if (args.size() < 2) {
            return respError("ERR wrong number of arguments for 'ttl' command");
        }
        
        return respInteger(store_.ttl(args[1]));
    }
    
    /**
     * PTTL command - kitna time bacha hai (milliseconds)
     * (PTTL - time to live in milliseconds, precise countdown)
     */
    std::string cmdPTTL(ClientConnection* client, const StringVector& args) {
        if (args.size() < 2) {
            return respError("ERR wrong number of arguments for 'pttl' command");
        }
        
        return respInteger(store_.pttl(args[1]));
    }

    // ========================================================================
    // LIST COMMANDS - Lists ke saath khelna
    // (Playing with lists - queue/stack operations)
    // ========================================================================
    
    /**
     * LPUSH command - left se daalo
     * (LPUSH - push to left/head, cutting the line)
     */
    std::string cmdLPush(ClientConnection* client, const StringVector& args) {
        if (args.size() < 3) {
            return respError("ERR wrong number of arguments for 'lpush' command");
        }
        
        // Type check karo - galat type pe error
        // (Check type - error on wrong type)
        DataType type = store_.type(args[1]);
        if (type != DataType::NONE && type != DataType::LIST) {
            return respError(ERR_WRONG_TYPE);
        }
        
        std::vector<std::string> values(args.begin() + 2, args.end());
        int64_t len = store_.lpush(args[1], values);
        
        if (len < 0) {
            return respError(ERR_WRONG_TYPE);
        }
        
        return respInteger(len);
    }
    
    /**
     * RPUSH command - right se daalo
     * (RPUSH - push to right/tail, proper queue behavior)
     */
    std::string cmdRPush(ClientConnection* client, const StringVector& args) {
        if (args.size() < 3) {
            return respError("ERR wrong number of arguments for 'rpush' command");
        }
        
        DataType type = store_.type(args[1]);
        if (type != DataType::NONE && type != DataType::LIST) {
            return respError(ERR_WRONG_TYPE);
        }
        
        std::vector<std::string> values(args.begin() + 2, args.end());
        int64_t len = store_.rpush(args[1], values);
        
        if (len < 0) {
            return respError(ERR_WRONG_TYPE);
        }
        
        return respInteger(len);
    }
    
    /**
     * LPOP command - left se nikalo
     * (LPOP - pop from left/head, FIFO dequeue)
     */
    std::string cmdLPop(ClientConnection* client, const StringVector& args) {
        if (args.size() < 2) {
            return respError("ERR wrong number of arguments for 'lpop' command");
        }
        
        DataType type = store_.type(args[1]);
        if (type != DataType::NONE && type != DataType::LIST) {
            return respError(ERR_WRONG_TYPE);
        }
        
        auto value = store_.lpop(args[1]);
        if (value) {
            return respBulkString(*value);
        }
        return respNull();
    }
    
    /**
     * RPOP command - right se nikalo
     * (RPOP - pop from right/tail, stack pop)
     */
    std::string cmdRPop(ClientConnection* client, const StringVector& args) {
        if (args.size() < 2) {
            return respError("ERR wrong number of arguments for 'rpop' command");
        }
        
        DataType type = store_.type(args[1]);
        if (type != DataType::NONE && type != DataType::LIST) {
            return respError(ERR_WRONG_TYPE);
        }
        
        auto value = store_.rpop(args[1]);
        if (value) {
            return respBulkString(*value);
        }
        return respNull();
    }
    
    /**
     * LLEN command - list ki length
     * (LLEN - list length, headcount)
     */
    std::string cmdLLen(ClientConnection* client, const StringVector& args) {
        if (args.size() < 2) {
            return respError("ERR wrong number of arguments for 'llen' command");
        }
        
        DataType type = store_.type(args[1]);
        if (type != DataType::NONE && type != DataType::LIST) {
            return respError(ERR_WRONG_TYPE);
        }
        
        return respInteger(store_.llen(args[1]));
    }
    
    /**
     * LRANGE command - list ka range
     * (LRANGE - get list range, Netflix continue watching)
     */
    std::string cmdLRange(ClientConnection* client, const StringVector& args) {
        if (args.size() < 4) {
            return respError("ERR wrong number of arguments for 'lrange' command");
        }
        
        DataType type = store_.type(args[1]);
        if (type != DataType::NONE && type != DataType::LIST) {
            return respError(ERR_WRONG_TYPE);
        }
        
        int64_t start, stop;
        try {
            start = std::stoll(args[2]);
            stop = std::stoll(args[3]);
        } catch (...) {
            return respError(ERR_INVALID_INT);
        }
        
        auto values = store_.lrange(args[1], start, stop);
        return respStringArray(values);
    }
    
    /**
     * LINDEX command - specific index pe element
     * (LINDEX - element at index, array access)
     */
    std::string cmdLIndex(ClientConnection* client, const StringVector& args) {
        if (args.size() < 3) {
            return respError("ERR wrong number of arguments for 'lindex' command");
        }
        
        DataType type = store_.type(args[1]);
        if (type != DataType::NONE && type != DataType::LIST) {
            return respError(ERR_WRONG_TYPE);
        }
        
        int64_t index;
        try {
            index = std::stoll(args[2]);
        } catch (...) {
            return respError(ERR_INVALID_INT);
        }
        
        auto value = store_.lindex(args[1], index);
        if (value) {
            return respBulkString(*value);
        }
        return respNull();
    }
    
    /**
     * LREM command - elements hatao
     * (LREM - remove elements, mass unfollowing)
     */
    std::string cmdLRem(ClientConnection* client, const StringVector& args) {
        if (args.size() < 4) {
            return respError("ERR wrong number of arguments for 'lrem' command");
        }
        
        DataType type = store_.type(args[1]);
        if (type != DataType::NONE && type != DataType::LIST) {
            return respError(ERR_WRONG_TYPE);
        }
        
        int64_t count;
        try {
            count = std::stoll(args[2]);
        } catch (...) {
            return respError(ERR_INVALID_INT);
        }
        
        int64_t removed = store_.lrem(args[1], count, args[3]);
        if (removed < 0) {
            return respError(ERR_WRONG_TYPE);
        }
        
        return respInteger(removed);
    }
    
    /**
     * BLPOP command - blocking left pop
     * (BLPOP - blocking pop from left, patient waiting)
     */
    std::string cmdBLPop(ClientConnection* client, const StringVector& args);
    
    /**
     * BRPOP command - blocking right pop
     * (BRPOP - blocking pop from right, patient waiting v2)
     */
    std::string cmdBRPop(ClientConnection* client, const StringVector& args);

    // ========================================================================
    // SET COMMANDS - Sets ke saath khelna
    // (Playing with sets - unique elements club)
    // ========================================================================
    
    /**
     * SADD command - set mein add karo
     * (SADD - add to set, join the club)
     */
    std::string cmdSAdd(ClientConnection* client, const StringVector& args) {
        if (args.size() < 3) {
            return respError("ERR wrong number of arguments for 'sadd' command");
        }
        
        DataType type = store_.type(args[1]);
        if (type != DataType::NONE && type != DataType::SET) {
            return respError(ERR_WRONG_TYPE);
        }
        
        std::vector<std::string> members(args.begin() + 2, args.end());
        int64_t added = store_.sadd(args[1], members);
        
        if (added < 0) {
            return respError(ERR_WRONG_TYPE);
        }
        
        return respInteger(added);
    }
    
    /**
     * SMEMBERS command - saare members do
     * (SMEMBERS - get all members, full guest list)
     */
    std::string cmdSMembers(ClientConnection* client, const StringVector& args) {
        if (args.size() < 2) {
            return respError("ERR wrong number of arguments for 'smembers' command");
        }
        
        DataType type = store_.type(args[1]);
        if (type != DataType::NONE && type != DataType::SET) {
            return respError(ERR_WRONG_TYPE);
        }
        
        auto members = store_.smembers(args[1]);
        return respStringArray(members);
    }
    
    /**
     * SISMEMBER command - member hai ki nahi
     * (SISMEMBER - is member in set, bouncer check)
     */
    std::string cmdSIsMember(ClientConnection* client, const StringVector& args) {
        if (args.size() < 3) {
            return respError("ERR wrong number of arguments for 'sismember' command");
        }
        
        DataType type = store_.type(args[1]);
        if (type != DataType::NONE && type != DataType::SET) {
            return respError(ERR_WRONG_TYPE);
        }
        
        return respInteger(store_.sismember(args[1], args[2]) ? 1 : 0);
    }
    
    /**
     * SREM command - members hatao
     * (SREM - remove members, kicked out of club)
     */
    std::string cmdSRem(ClientConnection* client, const StringVector& args) {
        if (args.size() < 3) {
            return respError("ERR wrong number of arguments for 'srem' command");
        }
        
        DataType type = store_.type(args[1]);
        if (type != DataType::NONE && type != DataType::SET) {
            return respError(ERR_WRONG_TYPE);
        }
        
        std::vector<std::string> members(args.begin() + 2, args.end());
        int64_t removed = store_.srem(args[1], members);
        
        if (removed < 0) {
            return respError(ERR_WRONG_TYPE);
        }
        
        return respInteger(removed);
    }
    
    /**
     * SCARD command - set cardinality
     * (SCARD - set size, headcount)
     */
    std::string cmdSCard(ClientConnection* client, const StringVector& args) {
        if (args.size() < 2) {
            return respError("ERR wrong number of arguments for 'scard' command");
        }
        
        DataType type = store_.type(args[1]);
        if (type != DataType::NONE && type != DataType::SET) {
            return respError(ERR_WRONG_TYPE);
        }
        
        return respInteger(store_.scard(args[1]));
    }

    // ========================================================================
    // SORTED SET COMMANDS - Sorted sets ke saath khelna
    // (Playing with sorted sets - leaderboard vibes)
    // ========================================================================
    
    /**
     * ZADD command - sorted set mein add karo
     * (ZADD - add with score, leaderboard entry)
     */
    std::string cmdZAdd(ClientConnection* client, const StringVector& args) {
        if (args.size() < 4) {
            return respError("ERR wrong number of arguments for 'zadd' command");
        }
        
        DataType type = store_.type(args[1]);
        if (type != DataType::NONE && type != DataType::ZSET) {
            return respError(ERR_WRONG_TYPE);
        }
        
        // Parse options
        bool nx = false, xx = false, gt = false, lt = false, ch = false;
        size_t i = 2;
        
        while (i < args.size()) {
            std::string opt = toUpper(args[i]);
            if (opt == "NX") { nx = true; i++; }
            else if (opt == "XX") { xx = true; i++; }
            else if (opt == "GT") { gt = true; i++; }
            else if (opt == "LT") { lt = true; i++; }
            else if (opt == "CH") { ch = true; i++; }
            else break;
        }
        
        // Parse score-member pairs
        // Score-member pairs parse karo - marks aur naam
        // (Parse score-member pairs - marks and names)
        std::vector<std::pair<double, std::string>> scoreMembers;
        
        while (i + 1 < args.size()) {
            double score;
            try {
                score = std::stod(args[i]);
            } catch (...) {
                return respError(ERR_INVALID_FLOAT);
            }
            scoreMembers.emplace_back(score, args[i + 1]);
            i += 2;
        }
        
        if (scoreMembers.empty()) {
            return respError("ERR wrong number of arguments for 'zadd' command");
        }
        
        int64_t result = store_.zadd(args[1], scoreMembers, nx, xx, gt, lt, ch);
        if (result < 0) {
            return respError(ERR_WRONG_TYPE);
        }
        
        return respInteger(result);
    }
    
    /**
     * ZSCORE command - member ka score
     * (ZSCORE - get member's score, check your marks)
     */
    std::string cmdZScore(ClientConnection* client, const StringVector& args) {
        if (args.size() < 3) {
            return respError("ERR wrong number of arguments for 'zscore' command");
        }
        
        DataType type = store_.type(args[1]);
        if (type != DataType::NONE && type != DataType::ZSET) {
            return respError(ERR_WRONG_TYPE);
        }
        
        auto score = store_.zscore(args[1], args[2]);
        if (score) {
            // Format score as string
            std::ostringstream oss;
            oss << std::setprecision(17) << *score;
            return respBulkString(oss.str());
        }
        return respNull();
    }
    
    /**
     * ZRANK command - member ka rank
     * (ZRANK - get member's rank, your position in leaderboard)
     */
    std::string cmdZRank(ClientConnection* client, const StringVector& args) {
        if (args.size() < 3) {
            return respError("ERR wrong number of arguments for 'zrank' command");
        }
        
        DataType type = store_.type(args[1]);
        if (type != DataType::NONE && type != DataType::ZSET) {
            return respError(ERR_WRONG_TYPE);
        }
        
        auto rank = store_.zrank(args[1], args[2]);
        if (rank) {
            return respInteger(*rank);
        }
        return respNull();
    }
    
    /**
     * ZRANGE command - range of members
     * (ZRANGE - get range of members, top N leaderboard)
     */
    std::string cmdZRange(ClientConnection* client, const StringVector& args) {
        if (args.size() < 4) {
            return respError("ERR wrong number of arguments for 'zrange' command");
        }
        
        DataType type = store_.type(args[1]);
        if (type != DataType::NONE && type != DataType::ZSET) {
            return respError(ERR_WRONG_TYPE);
        }
        
        int64_t start, stop;
        try {
            start = std::stoll(args[2]);
            stop = std::stoll(args[3]);
        } catch (...) {
            return respError(ERR_INVALID_INT);
        }
        
        bool withScores = false;
        bool rev = false;
        
        for (size_t i = 4; i < args.size(); i++) {
            std::string opt = toUpper(args[i]);
            if (opt == "WITHSCORES") withScores = true;
            else if (opt == "REV") rev = true;
        }
        
        auto results = store_.zrange(args[1], start, stop, withScores, rev);
        
        if (withScores) {
            std::vector<RespValue> arr;
            for (const auto& [member, score] : results) {
                arr.push_back(RespValue::bulkString(member));
                std::ostringstream oss;
                oss << std::setprecision(17) << score;
                arr.push_back(RespValue::bulkString(oss.str()));
            }
            return RespValue::array(arr).serialize();
        } else {
            std::vector<std::string> members;
            for (const auto& [member, score] : results) {
                members.push_back(member);
            }
            return respStringArray(members);
        }
    }
    
    /**
     * ZCOUNT command - score range mein kitne
     * (ZCOUNT - count in score range, how many passed)
     */
    std::string cmdZCount(ClientConnection* client, const StringVector& args) {
        if (args.size() < 4) {
            return respError("ERR wrong number of arguments for 'zcount' command");
        }
        
        DataType type = store_.type(args[1]);
        if (type != DataType::NONE && type != DataType::ZSET) {
            return respError(ERR_WRONG_TYPE);
        }
        
        double min, max;
        try {
            // Handle -inf and +inf
            std::string minStr = args[2];
            std::string maxStr = args[3];
            
            if (minStr == "-inf") min = -std::numeric_limits<double>::infinity();
            else if (minStr == "+inf") min = std::numeric_limits<double>::infinity();
            else min = std::stod(minStr);
            
            if (maxStr == "-inf") max = -std::numeric_limits<double>::infinity();
            else if (maxStr == "+inf") max = std::numeric_limits<double>::infinity();
            else max = std::stod(maxStr);
        } catch (...) {
            return respError("ERR min or max is not a float");
        }
        
        int64_t count = store_.zcount(args[1], min, max);
        if (count < 0) {
            return respError(ERR_WRONG_TYPE);
        }
        
        return respInteger(count);
    }
    
    /**
     * ZCARD command - sorted set size
     * (ZCARD - sorted set cardinality, total players)
     */
    std::string cmdZCard(ClientConnection* client, const StringVector& args) {
        if (args.size() < 2) {
            return respError("ERR wrong number of arguments for 'zcard' command");
        }
        
        DataType type = store_.type(args[1]);
        if (type != DataType::NONE && type != DataType::ZSET) {
            return respError(ERR_WRONG_TYPE);
        }
        
        return respInteger(store_.zcard(args[1]));
    }
    
    /**
     * ZREM command - members hatao
     * (ZREM - remove members, expelled from leaderboard)
     */
    std::string cmdZRem(ClientConnection* client, const StringVector& args) {
        if (args.size() < 3) {
            return respError("ERR wrong number of arguments for 'zrem' command");
        }
        
        DataType type = store_.type(args[1]);
        if (type != DataType::NONE && type != DataType::ZSET) {
            return respError(ERR_WRONG_TYPE);
        }
        
        std::vector<std::string> members(args.begin() + 2, args.end());
        int64_t removed = store_.zrem(args[1], members);
        
        if (removed < 0) {
            return respError(ERR_WRONG_TYPE);
        }
        
        return respInteger(removed);
    }

    // ========================================================================
    // HASH COMMANDS - Hashes ke saath khelna
    // (Playing with hashes - nested key-value)
    // ========================================================================
    
    /**
     * HSET command - hash field set karo
     * (HSET - set hash field, nested storage)
     */
    std::string cmdHSet(ClientConnection* client, const StringVector& args) {
        if (args.size() < 4 || (args.size() - 2) % 2 != 0) {
            return respError("ERR wrong number of arguments for 'hset' command");
        }
        
        DataType type = store_.type(args[1]);
        if (type != DataType::NONE && type != DataType::HASH) {
            return respError(ERR_WRONG_TYPE);
        }
        
        std::vector<std::pair<std::string, std::string>> fieldValues;
        for (size_t i = 2; i < args.size(); i += 2) {
            fieldValues.emplace_back(args[i], args[i + 1]);
        }
        
        int64_t added = store_.hset(args[1], fieldValues);
        if (added < 0) {
            return respError(ERR_WRONG_TYPE);
        }
        
        return respInteger(added);
    }
    
    /**
     * HGET command - hash field get karo
     * (HGET - get hash field value)
     */
    std::string cmdHGet(ClientConnection* client, const StringVector& args) {
        if (args.size() < 3) {
            return respError("ERR wrong number of arguments for 'hget' command");
        }
        
        DataType type = store_.type(args[1]);
        if (type != DataType::NONE && type != DataType::HASH) {
            return respError(ERR_WRONG_TYPE);
        }
        
        auto value = store_.hget(args[1], args[2]);
        if (value) {
            return respBulkString(*value);
        }
        return respNull();
    }
    
    /**
     * HGETALL command - saare fields aur values
     * (HGETALL - get all fields and values, full disclosure)
     */
    std::string cmdHGetAll(ClientConnection* client, const StringVector& args) {
        if (args.size() < 2) {
            return respError("ERR wrong number of arguments for 'hgetall' command");
        }
        
        DataType type = store_.type(args[1]);
        if (type != DataType::NONE && type != DataType::HASH) {
            return respError(ERR_WRONG_TYPE);
        }
        
        auto pairs = store_.hgetall(args[1]);
        
        std::vector<RespValue> arr;
        for (const auto& [field, value] : pairs) {
            arr.push_back(RespValue::bulkString(field));
            arr.push_back(RespValue::bulkString(value));
        }
        
        return RespValue::array(arr).serialize();
    }

    // ========================================================================
    // STREAM COMMANDS - Streams ke saath khelna
    // (Playing with streams - timeline vibes)
    // ========================================================================
    
    /**
     * XADD command - stream mein add karo
     * (XADD - add to stream, post on timeline)
     */
    std::string cmdXAdd(ClientConnection* client, const StringVector& args) {
        if (args.size() < 4) {
            return respError("ERR wrong number of arguments for 'xadd' command");
        }
        
        const std::string& key = args[1];
        
        DataType type = store_.type(key);
        if (type != DataType::NONE && type != DataType::STREAM) {
            return respError(ERR_WRONG_TYPE);
        }
        
        // Parse ID and field-value pairs
        size_t idIndex = 2;
        
        // Check for NOMKSTREAM, MAXLEN, etc.
        // Abhi ke liye skip karte hain
        // (Skip for now - keep it simple)
        while (idIndex < args.size()) {
            std::string opt = toUpper(args[idIndex]);
            if (opt == "NOMKSTREAM" || opt == "MAXLEN" || opt == "MINID") {
                idIndex++;
                if (idIndex < args.size() && (args[idIndex] == "~" || args[idIndex] == "=")) {
                    idIndex++;
                }
                if (idIndex < args.size()) idIndex++; // skip value
            } else {
                break;
            }
        }
        
        if (idIndex >= args.size()) {
            return respError("ERR wrong number of arguments for 'xadd' command");
        }
        
        std::string id = args[idIndex];
        
        // Parse field-value pairs
        std::vector<std::pair<std::string, std::string>> fields;
        for (size_t i = idIndex + 1; i + 1 < args.size(); i += 2) {
            fields.emplace_back(args[i], args[i + 1]);
        }
        
        if (fields.empty()) {
            return respError("ERR wrong number of arguments for 'xadd' command");
        }
        
        auto result = store_.xadd(key, id, fields);
        if (!result.success) {
            return respError(result.value);
        }
        return respBulkString(result.value);
    }
    
    /**
     * XRANGE command - stream range
     * (XRANGE - get stream range, scroll timeline)
     */
    std::string cmdXRange(ClientConnection* client, const StringVector& args) {
        if (args.size() < 4) {
            return respError("ERR wrong number of arguments for 'xrange' command");
        }
        
        const std::string& key = args[1];
        
        DataType type = store_.type(key);
        if (type != DataType::NONE && type != DataType::STREAM) {
            return respError(ERR_WRONG_TYPE);
        }
        
        std::string start = args[2];
        std::string end = args[3];
        
        std::optional<int64_t> count;
        if (args.size() > 5 && toUpper(args[4]) == "COUNT") {
            try {
                count = std::stoll(args[5]);
            } catch (...) {
                return respError(ERR_INVALID_INT);
            }
        }
        
        auto entries = store_.xrange(key, start, end, count);
        
        return serializeStreamEntries(entries);
    }
    
    /**
     * XREAD command - read from streams
     * (XREAD - read from streams, multi-timeline scroll)
     */
    std::string cmdXRead(ClientConnection* client, const StringVector& args);
    
    /**
     * XLEN command - stream length
     * (XLEN - stream length, how many posts)
     */
    std::string cmdXLen(ClientConnection* client, const StringVector& args) {
        if (args.size() < 2) {
            return respError("ERR wrong number of arguments for 'xlen' command");
        }
        
        DataType type = store_.type(args[1]);
        if (type != DataType::NONE && type != DataType::STREAM) {
            return respError(ERR_WRONG_TYPE);
        }
        
        auto entries = store_.xrange(args[1], "-", "+");
        return respInteger(entries.size());
    }

    // ========================================================================
    // PUB/SUB COMMANDS - Pub/Sub
    // (Publish/Subscribe - broadcast system)
    // ========================================================================
    
    /**
     * SUBSCRIBE command - channel subscribe karo
     * (SUBSCRIBE - subscribe to channels, join the group)
     */
    std::string cmdSubscribe(ClientConnection* client, const StringVector& args);
    
    /**
     * UNSUBSCRIBE command - unsubscribe karo
     * (UNSUBSCRIBE - unsubscribe from channels, leave the group)
     */
    std::string cmdUnsubscribe(ClientConnection* client, const StringVector& args);
    
    /**
     * PUBLISH command - message publish karo
     * (PUBLISH - publish message, broadcast to everyone)
     */
    std::string cmdPublish(ClientConnection* client, const StringVector& args);

    // ========================================================================
    // TRANSACTION COMMANDS - Transactions
    // (Transactions - all or nothing)
    // ========================================================================
    
    /**
     * MULTI command - transaction shuru karo
     * (MULTI - start transaction, queue mode ON)
     */
    std::string cmdMulti(ClientConnection* client, const StringVector& args) {
        if (client->inTransaction) {
            return respError("ERR MULTI calls can not be nested");
        }
        
        client->startTransaction();
        return respOK();
    }
    
    /**
     * EXEC command - transaction execute karo
     * (EXEC - execute transaction, fire all commands)
     */
    std::string cmdExec(ClientConnection* client, const StringVector& args);
    
    /**
     * DISCARD command - transaction cancel karo
     * (DISCARD - discard transaction, abort mission)
     */
    std::string cmdDiscard(ClientConnection* client, const StringVector& args) {
        if (!client->inTransaction) {
            return respError("ERR DISCARD without MULTI");
        }
        
        client->endTransaction();
        return respOK();
    }

    // ========================================================================
    // SERVER COMMANDS - Server related
    // (Server management - admin stuff)
    // ========================================================================
    
    /**
     * INFO command - server info
     * (INFO - server information, stats and status)
     */
    std::string cmdInfo(ClientConnection* client, const StringVector& args);
    
    /**
     * CONFIG command - configuration
     * (CONFIG - server configuration, settings)
     */
    std::string cmdConfig(ClientConnection* client, const StringVector& args);
    
    /**
     * COMMAND command - command info
     * (COMMAND - list all commands, documentation)
     */
    std::string cmdCommand(ClientConnection* client, const StringVector& args) {
        // Return empty array for now - basic implementation
        // Abhi ke liye empty array - baad mein proper implement
        // (Empty array for now - proper implementation later)
        if (args.size() > 1) {
            std::string subCmd = toUpper(args[1]);
            if (subCmd == "DOCS") {
                return RespValue::array({}).serialize();
            }
            if (subCmd == "COUNT") {
                return respInteger(100); // Approximate
            }
        }
        return RespValue::array({}).serialize();
    }
    
    /**
     * CLIENT command - client management
     * (CLIENT - client management, who's connected)
     */
    std::string cmdClient(ClientConnection* client, const StringVector& args) {
        if (args.size() < 2) {
            return respError("ERR wrong number of arguments for 'client' command");
        }
        
        std::string subCmd = toUpper(args[1]);
        
        if (subCmd == "ID") {
            return respInteger(client->id);
        } else if (subCmd == "SETNAME" && args.size() > 2) {
            // Set client name (we're not storing it but acknowledge)
            return respOK();
        } else if (subCmd == "GETNAME") {
            return respNull();  // No name set
        } else if (subCmd == "LIST") {
            return respBulkString(client->getInfoString());
        }
        
        return respOK();
    }
    
    /**
     * DBSIZE command - keys count
     * (DBSIZE - total keys in database)
     */
    std::string cmdDbSize(ClientConnection* client, const StringVector& args) {
        auto keys = store_.keys("*");
        return respInteger(keys.size());
    }
    
    /**
     * FLUSHDB/FLUSHALL command - sab delete karo
     * (FLUSHDB - delete everything, clean slate)
     */
    std::string cmdFlushDb(ClientConnection* client, const StringVector& args) {
        auto keys = store_.keys("*");
        store_.del(keys);
        return respOK();
    }
    
    /**
     * DEBUG command - debugging
     * (DEBUG - for debugging, developer stuff)
     */
    std::string cmdDebug(ClientConnection* client, const StringVector& args) {
        if (args.size() < 2) {
            return respError("ERR wrong number of arguments for 'debug' command");
        }
        
        std::string subCmd = toUpper(args[1]);
        
        if (subCmd == "SLEEP" && args.size() > 2) {
            double seconds = std::stod(args[2]);
            std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int>(seconds * 1000)));
            return respOK();
        }
        
        return respOK();
    }

    // ========================================================================
    // REPLICATION COMMANDS - Replication
    // (Replication - master/slave sync)
    // ========================================================================
    
    /**
     * REPLCONF command - replication config
     * (REPLCONF - replication configuration)
     */
    std::string cmdReplConf(ClientConnection* client, const StringVector& args);
    
    /**
     * PSYNC command - partial sync
     * (PSYNC - partial synchronization, catch up)
     */
    std::string cmdPSync(ClientConnection* client, const StringVector& args);
    
    /**
     * WAIT command - wait for replicas
     * (WAIT - wait for replica acknowledgments)
     */
    std::string cmdWait(ClientConnection* client, const StringVector& args);

    // ========================================================================
    // GEOSPATIAL COMMANDS - Geo stuff
    // (Geospatial - location based operations)
    // ========================================================================
    
    /**
     * GEOADD command - add geo location
     * (GEOADD - add geographic location, GPS coordinates)
     */
    std::string cmdGeoAdd(ClientConnection* client, const StringVector& args);
    
    /**
     * GEOPOS command - get position
     * (GEOPOS - get geographic position)
     */
    std::string cmdGeoPos(ClientConnection* client, const StringVector& args);
    
    /**
     * GEODIST command - distance between points
     * (GEODIST - distance between two locations)
     */
    std::string cmdGeoDist(ClientConnection* client, const StringVector& args);
    
    /**
     * GEORADIUS/GEOSEARCH command - search in radius
     * (GEORADIUS - search within radius, nearby search)
     */
    std::string cmdGeoRadius(ClientConnection* client, const StringVector& args);

    // ========================================================================
    // AUTH COMMANDS - Authentication
    // (Authentication - security stuff)
    // ========================================================================
    
    /**
     * AUTH command - authenticate
     * (AUTH - authenticate user, login)
     */
    std::string cmdAuth(ClientConnection* client, const StringVector& args);
    
    /**
     * ACL command - access control
     * (ACL - access control list, permissions)
     */
    std::string cmdAcl(ClientConnection* client, const StringVector& args);

    // ========================================================================
    // HELPER METHODS - Internal helpers
    // (Internal helper functions)
    // ========================================================================
    
    /**
     * Serialize stream entries to RESP format
     * Stream entries nu RESP mein convert karo
     * (Convert stream entries to RESP format)
     */
    std::string serializeStreamEntries(const std::vector<StreamEntry>& entries) {
        std::vector<RespValue> arr;
        
        for (const auto& entry : entries) {
            std::vector<RespValue> entryArr;
            entryArr.push_back(RespValue::bulkString(entry.id));
            
            std::vector<RespValue> fieldsArr;
            for (const auto& [field, value] : entry.fields) {
                fieldsArr.push_back(RespValue::bulkString(field));
                fieldsArr.push_back(RespValue::bulkString(value));
            }
            entryArr.push_back(RespValue::array(fieldsArr));
            
            arr.push_back(RespValue::array(entryArr));
        }
        
        return RespValue::array(arr).serialize();
    }
};

} // namespace Redis
