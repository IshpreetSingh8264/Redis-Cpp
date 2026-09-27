/**
 * resp.hpp -- the wire format, in both directions.
 *
 * Reading returns the argv of one command. Writing returns a raw byte string
 * ready for write(2). Nothing here knows what a command means.
 */
#ifndef REDIS_PROTOCOL_RESP_HPP
#define REDIS_PROTOCOL_RESP_HPP

#include <cstdint>
#include <string>
#include <vector>

namespace redis::resp {

/// Outcome of trying to pull one command off the front of a byte buffer.
struct ParseResult {
    bool ok = false;        // a complete command was decoded
    bool fatal = false;     // the bytes cannot be a command; drop the connection
    size_t consumed = 0;    // bytes of the command, only meaningful when ok
    std::vector<std::string> args;
};

/// Pull one RESP array-of-bulk-strings command off the front of `buffer`.
/// Returns ok=false (consumed=0) when the buffer holds only part of a command,
/// which is the normal case for a socket read. Also accepts inline commands
/// ("PING\r\n") because the CodeCrafters harness and telnet both send those.
ParseResult parseCommand(const std::string& buffer);

/// Number of bytes a RESP-encoded command of these args occupies. The
/// replication layer needs this to advance its offset without re-encoding.
size_t encodedLength(const std::vector<std::string>& args);

/// Encode `args` as a RESP array of bulk strings. This is the unit that
/// propagates to replicas and that the AOF stores.
std::string encodeCommand(const std::vector<std::string>& args);

// --- writers -------------------------------------------------------------
std::string simpleString(const std::string& s);
std::string error(const std::string& s);
std::string integer(int64_t v);
std::string bulkString(const std::string& s);
std::string nullBulk();
std::string nullArray();
std::string emptyArray();
std::string boolean(bool b);
std::string doubleValue(double v);
/// Wrap already-encoded elements in an array header.
std::string array(const std::vector<std::string>& encodedElements);
/// Convenience: encode a list of raw strings as an array of bulk strings.
std::string arrayOfBulkStrings(const std::vector<std::string>& values);

}  // namespace redis::resp

#endif  // REDIS_PROTOCOL_RESP_HPP
