/**
 * =============================================================================
 *                          RESP PROTOCOL PARSER
 * =============================================================================
 * 
 * RESP = Redis Serialization Protocol
 * 
 * Paaji eh parser hai jo Redis commands nu samajhda hai!
 * (Bro this is the parser that understands Redis commands!)
 * 
 * RESP2 format:
 * - Simple Strings: +OK\r\n
 * - Errors: -ERR message\r\n  
 * - Integers: :1000\r\n
 * - Bulk Strings: $6\r\nfoobar\r\n
 * - Arrays: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
 * 
 * Jiven translator hunda hai 2 languages de vich, ohi eh hai Redis te client de vich
 * (Just like a translator between 2 languages, this is between Redis and client)
 * 
 * =============================================================================
 */

#pragma once

#include "common.hpp"

namespace Redis {

// ============================================================================
// RESP VALUE CLASS
// Eh class store karti hai parsed value
// (This class stores the parsed value - simple hai na?)
// ============================================================================

/**
 * @class RespValue
 * @brief Represents a parsed RESP value
 * 
 * Eh class hai jiven Swiss Army knife - sab kuch store kar sakdi hai!
 * (This class is like a Swiss Army knife - can store everything!)
 * 
 * Can hold: strings, integers, arrays, nulls, errors
 */
class RespValue {
public:
    // Types of RESP values - varieties jiven Maggi ki
    // (Types like Maggi flavors - many options!)
    enum class Type {
        SIMPLE_STRING,      // +OK
        ERROR,              // -ERR something
        INTEGER,            // :42
        BULK_STRING,        // $6\r\nfoobar
        ARRAY,              // *2\r\n...
        NULL_VALUE          // $-1 (nil)
    };

private:
    Type type_;
    std::string stringValue_;          // For strings and errors
    int64_t intValue_ = 0;             // For integers
    std::vector<RespValue> arrayValue_; // For arrays
    bool isNull_ = false;              // Null check - jiven bank balance month end pe
                                       // (Null check - like bank balance at month end)

public:
    // ========================================================================
    // CONSTRUCTORS - Object banane ke tarike
    // (Ways to create objects - like recipes)
    // ========================================================================
    
    /**
     * Default constructor - empty value
     * Khali dabba - kuch nahi andar
     * (Empty box - nothing inside)
     */
    RespValue() : type_(Type::NULL_VALUE), isNull_(true) {}
    
    /**
     * String constructor
     * String value set karo
     * (Set string value)
     */
    RespValue(Type t, const std::string& s) : type_(t), stringValue_(s), isNull_(false) {}
    
    /**
     * Integer constructor  
     * Number set karo
     * (Set number value)
     */
    RespValue(int64_t i) : type_(Type::INTEGER), intValue_(i), isNull_(false) {}
    
    /**
     * Array constructor
     * List of values set karo
     * (Set list of values)
     */
    RespValue(const std::vector<RespValue>& arr) 
        : type_(Type::ARRAY), arrayValue_(arr), isNull_(false) {}

    // ========================================================================
    // STATIC FACTORY METHODS - Object banane ke shortcuts
    // (Shortcuts to create objects - jiven instant noodles)
    // ========================================================================
    
    /**
     * Create simple string response
     * Seedha saadha jawab - +OK jaise
     * (Simple straightforward answer - like +OK)
     */
    static RespValue simpleString(const std::string& s) {
        return RespValue(Type::SIMPLE_STRING, s);
    }
    
    /**
     * Create error response
     * Galti ka jawab - jaise teacher daant rahi ho
     * (Error response - like teacher scolding)
     */
    static RespValue error(const std::string& s) {
        return RespValue(Type::ERROR, s);
    }
    
    /**
     * Create bulk string response
     * Bada string - binary safe
     * (Big string - binary safe, handles any data)
     */
    static RespValue bulkString(const std::string& s) {
        return RespValue(Type::BULK_STRING, s);
    }
    
    /**
     * Create integer response
     * Number return karo - maths ka result
     * (Return number - math result)
     */
    static RespValue integer(int64_t i) {
        return RespValue(i);
    }
    
    /**
     * Create null response
     * Kuch nahi hai - void jaise
     * (Nothing here - like void, like my social life)
     */
    static RespValue null() {
        RespValue v;
        v.type_ = Type::BULK_STRING;
        v.isNull_ = true;
        return v;
    }
    
    /**
     * Create null array response
     * Khali array - empty playlist
     * (Empty array - like empty playlist on Monday morning)
     */
    static RespValue nullArray() {
        RespValue v;
        v.type_ = Type::ARRAY;
        v.isNull_ = true;
        return v;
    }
    
    /**
     * Create array response
     * List of items - shopping list jaise
     * (List of items - like shopping list that keeps growing)
     */
    static RespValue array(const std::vector<RespValue>& arr) {
        return RespValue(arr);
    }

    // ========================================================================
    // GETTERS - Values nikalne ke functions
    // (Functions to get values - like ATM withdrawal)
    // ========================================================================
    
    Type getType() const { return type_; }
    const std::string& getString() const { return stringValue_; }
    int64_t getInteger() const { return intValue_; }
    const std::vector<RespValue>& getArray() const { return arrayValue_; }
    std::vector<RespValue>& getArray() { return arrayValue_; }
    bool isNull() const { return isNull_; }
    bool isError() const { return type_ == Type::ERROR; }
    
    /**
     * Get value as string (works for bulk and simple strings)
     * String format ch value do - type koi bhi ho
     * (Give value in string format - whatever the type)
     */
    std::string asString() const {
        if (type_ == Type::INTEGER) {
            return std::to_string(intValue_);
        }
        return stringValue_;
    }

    // ========================================================================
    // SERIALIZATION - Wire format ch convert karo
    // (Convert to wire format - packaging for delivery)
    // ========================================================================
    
    /**
     * Serialize to RESP format
     * 
     * Eh function value nu string ch convert karda hai jo network pe bhej sakde ho
     * (This function converts value to string that can be sent over network)
     * 
     * Jiven Zomato order pack karda hai, ohi eh karta hai data lai
     * (Just like Zomato packs orders, this packs data for delivery)
     */
    std::string serialize() const {
        std::ostringstream oss;
        
        switch (type_) {
            case Type::SIMPLE_STRING:
                // +OK\r\n - simple te sweet
                // (+OK\r\n - simple and sweet)
                oss << RESP_SIMPLE_STRING << stringValue_ << CRLF;
                break;
                
            case Type::ERROR:
                // -ERR message\r\n - galti bata do
                // (-ERR message\r\n - tell the mistake)
                oss << RESP_ERROR << stringValue_ << CRLF;
                break;
                
            case Type::INTEGER:
                // :42\r\n - number bata do
                // (:42\r\n - tell the number)
                oss << RESP_INTEGER << intValue_ << CRLF;
                break;
                
            case Type::BULK_STRING:
                if (isNull_) {
                    // $-1\r\n - null bulk string
                    // (Null hai bhai - nothing here)
                    oss << RESP_BULK_STRING << "-1" << CRLF;
                } else {
                    // $6\r\nfoobar\r\n - length then data
                    // (Length phir data - organized delivery)
                    oss << RESP_BULK_STRING << stringValue_.length() << CRLF 
                        << stringValue_ << CRLF;
                }
                break;
                
            case Type::ARRAY:
                if (isNull_) {
                    // *-1\r\n - null array
                    oss << RESP_ARRAY << "-1" << CRLF;
                } else {
                    // *2\r\n... - array with elements
                    // (Array hai - items ki list)
                    oss << RESP_ARRAY << arrayValue_.size() << CRLF;
                    for (const auto& elem : arrayValue_) {
                        oss << elem.serialize();
                    }
                }
                break;
                
            case Type::NULL_VALUE:
                // $-1\r\n - the void, nothingness
                // (Shunyata - emptiness, like my fridge)
                oss << RESP_BULK_STRING << "-1" << CRLF;
                break;
        }
        
        return oss.str();
    }
};

// ============================================================================
// RESP PARSER CLASS
// Main parser - jo commands samjhe
// (Main parser - the one who understands commands)
// ============================================================================

/**
 * @class RespParser
 * @brief Parses RESP protocol data
 * 
 * Eh class raw bytes nu samajh ke RespValue banandi hai
 * (This class understands raw bytes and creates RespValue objects)
 * 
 * Jiven Google Translate karta hai languages lai, eh karta hai Redis protocol lai
 * (Just like Google Translate for languages, this is for Redis protocol)
 */
class RespParser {
private:
    std::string buffer_;      // Data jo parse karna hai
                              // (Data that needs to be parsed)
    size_t pos_ = 0;          // Current position - kahaan tak padh liya
                              // (Current position - how far we've read)

public:
    /**
     * Constructor - buffer set karo
     * (Set the buffer to parse)
     */
    RespParser(const std::string& data) : buffer_(data), pos_(0) {}
    
    /**
     * Reset parser with new data
     * Naya data do, fresh start
     * (New data, fresh start - like Monday motivation)
     */
    void reset(const std::string& data) {
        buffer_ = data;
        pos_ = 0;
    }
    
    /**
     * Add more data to buffer
     * Aur data add karo - buffering jaise YouTube pe
     * (Add more data - buffering like on YouTube when internet is slow)
     */
    void append(const std::string& data) {
        buffer_ += data;
    }
    
    /**
     * Check if parsing is complete
     * Sab parse ho gaya ki nahi?
     * (Is everything parsed or not?)
     */
    bool isComplete() const {
        return pos_ >= buffer_.length();
    }
    
    /**
     * Get remaining unparsed data
     * Jo bacha hai wo do
     * (Give what's left - leftovers)
     */
    std::string remaining() const {
        return buffer_.substr(pos_);
    }
    
    /**
     * Get current position
     * Kahaan tak pahunche?
     * (How far have we reached?)
     */
    size_t getPosition() const {
        return pos_;
    }
    
    /**
     * Parse next RESP value
     * 
     * Agla value parse karo - line by line padho jaise exam mein
     * (Parse next value - read line by line like in exam)
     * 
     * @return Optional RespValue - value if parsed, nullopt if incomplete
     * 
     * Returns nullopt jab data incomplete hai
     * (Returns nullopt when data is incomplete - patience rakh paaji)
     */
    std::optional<RespValue> parse() {
        if (pos_ >= buffer_.length()) {
            // Buffer khatam ho gaya
            // (Buffer exhausted - nothing left to parse)
            return std::nullopt;
        }
        
        char type = buffer_[pos_];
        
        switch (type) {
            case RESP_SIMPLE_STRING:
                return parseSimpleString();
            case RESP_ERROR:
                return parseError();
            case RESP_INTEGER:
                return parseInteger();
            case RESP_BULK_STRING:
                return parseBulkString();
            case RESP_ARRAY:
                return parseArray();
            default:
                // Inline command support - purana format
                // (Old format support - backwards compatibility)
                return parseInlineCommand();
        }
    }
    
    /**
     * Parse command from client
     * Client ka command parse karo - kya chahta hai?
     * (Parse client's command - what does he want?)
     * 
     * @return Vector of strings - command and arguments
     */
    std::optional<StringVector> parseCommand() {
        auto value = parse();
        if (!value) return std::nullopt;
        
        StringVector result;
        
        if (value->getType() == RespValue::Type::ARRAY) {
            // Normal RESP array command
            // Proper format hai - sahi tarika
            // (Proper format - the right way)
            for (const auto& elem : value->getArray()) {
                result.push_back(elem.asString());
            }
        } else {
            // Single value or inline
            // Ek hi value hai ya inline command
            // (Single value or inline command)
            result.push_back(value->asString());
        }
        
        return result;
    }

private:
    // ========================================================================
    // PRIVATE HELPER METHODS - Internal kaam
    // (Internal work - kitchen mein kya ho raha hai)
    // ========================================================================
    
    /**
     * Read line until CRLF
     * CRLF tak padho - paragraph end
     * (Read until CRLF - like reading until paragraph end)
     */
    std::optional<std::string> readLine() {
        auto crlfPos = buffer_.find(CRLF, pos_);
        if (crlfPos == std::string::npos) {
            return std::nullopt;
        }
        
        std::string line = buffer_.substr(pos_, crlfPos - pos_);
        pos_ = crlfPos + 2; // Skip CRLF
        return line;
    }
    
    /**
     * Read exact number of bytes
     * Itne bytes padho - exact count
     * (Read this many bytes - exact count, no more no less)
     */
    std::optional<std::string> readBytes(size_t count) {
        if (pos_ + count > buffer_.length()) {
            return std::nullopt;
        }
        
        std::string data = buffer_.substr(pos_, count);
        pos_ += count;
        return data;
    }
    
    /**
     * Parse simple string (+OK\r\n)
     * Simple string parse karo - easy peasy
     * (Parse simple string - easy peasy lemon squeezy)
     */
    std::optional<RespValue> parseSimpleString() {
        pos_++; // Skip '+'
        auto line = readLine();
        if (!line) {
            pos_--; // Restore position
            return std::nullopt;
        }
        return RespValue::simpleString(*line);
    }
    
    /**
     * Parse error (-ERR message\r\n)
     * Error parse karo - galti samjho
     * (Parse error - understand the mistake)
     */
    std::optional<RespValue> parseError() {
        pos_++; // Skip '-'
        auto line = readLine();
        if (!line) {
            pos_--;
            return std::nullopt;
        }
        return RespValue::error(*line);
    }
    
    /**
     * Parse integer (:42\r\n)
     * Integer parse karo - maths time
     * (Parse integer - math time, don't be scared)
     */
    std::optional<RespValue> parseInteger() {
        pos_++; // Skip ':'
        auto line = readLine();
        if (!line) {
            pos_--;
            return std::nullopt;
        }
        
        try {
            int64_t value = std::stoll(*line);
            return RespValue::integer(value);
        } catch (...) {
            return RespValue::error("ERR invalid integer");
        }
    }
    
    /**
     * Parse bulk string ($6\r\nfoobar\r\n)
     * Bulk string parse karo - bada data
     * (Parse bulk string - big data, handles binary too)
     */
    std::optional<RespValue> parseBulkString() {
        size_t startPos = pos_;
        pos_++; // Skip '$'
        
        auto lenLine = readLine();
        if (!lenLine) {
            pos_ = startPos;
            return std::nullopt;
        }
        
        int len;
        try {
            len = std::stoi(*lenLine);
        } catch (...) {
            return RespValue::error("ERR invalid bulk length");
        }
        
        // Null bulk string check
        if (len < 0) {
            return RespValue::null();
        }
        
        // Read exact bytes + CRLF
        if (pos_ + len + 2 > buffer_.length()) {
            pos_ = startPos;
            return std::nullopt;
        }
        
        std::string data = buffer_.substr(pos_, len);
        pos_ += len;
        
        // Verify and skip CRLF
        if (buffer_.substr(pos_, 2) != CRLF) {
            return RespValue::error("ERR protocol error");
        }
        pos_ += 2;
        
        return RespValue::bulkString(data);
    }
    
    /**
     * Parse array (*2\r\n...)
     * Array parse karo - list of items
     * (Parse array - list of items, like shopping list)
     */
    std::optional<RespValue> parseArray() {
        size_t startPos = pos_;
        pos_++; // Skip '*'
        
        auto countLine = readLine();
        if (!countLine) {
            pos_ = startPos;
            return std::nullopt;
        }
        
        int count;
        try {
            count = std::stoi(*countLine);
        } catch (...) {
            return RespValue::error("ERR invalid array count");
        }
        
        // Null array
        if (count < 0) {
            return RespValue::nullArray();
        }
        
        // Parse each element
        // Har element parse karo - one by one
        // (Parse each element - one by one, slow and steady)
        std::vector<RespValue> elements;
        elements.reserve(count);
        
        for (int i = 0; i < count; i++) {
            auto elem = parse();
            if (!elem) {
                pos_ = startPos;
                return std::nullopt;
            }
            elements.push_back(std::move(*elem));
        }
        
        return RespValue::array(elements);
    }
    
    /**
     * Parse inline command (legacy format)
     * Purana format - PING\r\n style
     * (Old format - PING\r\n style, for backwards compatibility)
     * 
     * Jiven purane zamane mein letters likhte the, ab WhatsApp hai
     * (Like writing letters in old days, now we have WhatsApp)
     */
    std::optional<RespValue> parseInlineCommand() {
        auto line = readLine();
        if (!line) {
            return std::nullopt;
        }
        
        // Split by spaces
        std::vector<RespValue> args;
        std::istringstream iss(*line);
        std::string word;
        
        while (iss >> word) {
            args.push_back(RespValue::bulkString(word));
        }
        
        if (args.empty()) {
            return std::nullopt;
        }
        
        return RespValue::array(args);
    }
};

// ============================================================================
// HELPER FUNCTIONS FOR BUILDING RESPONSES
// Response banane ke shortcuts - copy paste friendly
// (Shortcuts for building responses - developer's best friend)
// ============================================================================

/**
 * Build OK response
 * Sab theek hai response
 * (All good response - everything went well)
 */
inline std::string respOK() {
    return RespValue::simpleString("OK").serialize();
}

/**
 * Build PONG response
 * PING ka jawab PONG - table tennis jaise
 * (Answer to PING is PONG - like table tennis)
 */
inline std::string respPong() {
    return RespValue::simpleString("PONG").serialize();
}

/**
 * Build error response
 * Error response - jab galti ho
 * (Error response - when things go wrong, story of our lives)
 */
inline std::string respError(const std::string& msg) {
    return RespValue::error(msg).serialize();
}

/**
 * Build null response
 * Null response - kuch nahi mila
 * (Null response - nothing found, like my search for happiness)
 */
inline std::string respNull() {
    return RespValue::null().serialize();
}

/**
 * Build null array response
 * Null array - khali list
 * (Null array - empty list)
 */
inline std::string respNullArray() {
    return RespValue::nullArray().serialize();
}

/**
 * Build integer response
 * Number response
 * (Number response - math ki wapsi)
 * (Number response - math makes a comeback)
 */
inline std::string respInteger(int64_t value) {
    return RespValue::integer(value).serialize();
}

/**
 * Build bulk string response
 * Bulk string response - bada data
 * (Bulk string response - big data delivery)
 */
inline std::string respBulkString(const std::string& str) {
    return RespValue::bulkString(str).serialize();
}

/**
 * Build simple string response
 * Simple string response - chhota data
 * (Simple string response - small data)
 */
inline std::string respSimpleString(const std::string& str) {
    return RespValue::simpleString(str).serialize();
}

/**
 * Build array response from strings
 * String array response - list of strings
 * (String array response - list of strings banao)
 */
inline std::string respStringArray(const StringVector& strings) {
    std::vector<RespValue> arr;
    for (const auto& s : strings) {
        arr.push_back(RespValue::bulkString(s));
    }
    return RespValue::array(arr).serialize();
}

} // namespace Redis
