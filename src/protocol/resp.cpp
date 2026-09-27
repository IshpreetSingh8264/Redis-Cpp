#include "protocol/resp.hpp"

#include <cctype>
#include <cmath>
#include <cstdlib>

#include "utils/strutil.hpp"

namespace redis::resp {

namespace {

/// Read one CRLF-terminated line starting at `pos`. Returns false if the
/// terminator is not there yet.
bool readLine(const std::string& b, size_t pos, std::string& line, size_t& next) {
    size_t crlf = b.find("\r\n", pos);
    if (crlf == std::string::npos) return false;
    line = b.substr(pos, crlf - pos);
    next = crlf + 2;
    return true;
}

}  // namespace

ParseResult parseCommand(const std::string& buffer) {
    ParseResult r;
    if (buffer.empty()) return r;

    if (buffer[0] != '*') {
        // Inline command (telnet style). Only complete when we see the CRLF.
        std::string line;
        size_t next = 0;
        if (!readLine(buffer, 0, line, next)) return r;
        r.args = strutil::splitWhitespace(line);
        if (r.args.empty()) {
            // A bare CRLF is noise, not a command. Consume it and try again.
            r.ok = true;
            r.args.clear();
            r.consumed = next;
        } else {
            r.ok = true;
            r.consumed = next;
        }
        return r;
    }

    std::string line;
    size_t pos = 0;
    if (!readLine(buffer, pos, line, pos)) return r;

    int64_t count = 0;
    if (!strutil::parseInt64(line.substr(1), count) || count < 0) {
        r.fatal = true;
        return r;
    }

    for (int64_t i = 0; i < count; i++) {
        if (pos >= buffer.size()) return r;  // incomplete
        if (buffer[pos] != '$') {
            r.fatal = true;
            return r;
        }
        if (!readLine(buffer, pos, line, pos)) return r;
        int64_t len = 0;
        if (!strutil::parseInt64(line.substr(1), len) || len < -1) {
            r.fatal = true;
            return r;
        }
        if (len == -1) {
            r.fatal = true;  // null bulk inside a command array: not a command
            return r;
        }
        if (buffer.size() < pos + static_cast<size_t>(len) + 2) return r;  // incomplete
        r.args.push_back(buffer.substr(pos, static_cast<size_t>(len)));
        pos += static_cast<size_t>(len) + 2;
    }

    r.ok = true;
    r.consumed = pos;
    return r;
}

size_t encodedLength(const std::vector<std::string>& args) {
    size_t n = 1 + std::to_string(args.size()).size() + 2;
    for (const auto& a : args) n += 1 + std::to_string(a.size()).size() + 2 + a.size() + 2;
    return n;
}

std::string encodeCommand(const std::vector<std::string>& args) {
    std::string out = "*" + std::to_string(args.size()) + "\r\n";
    for (const auto& a : args) {
        out += "$" + std::to_string(a.size()) + "\r\n" + a + "\r\n";
    }
    return out;
}

std::string simpleString(const std::string& s) { return "+" + s + "\r\n"; }
std::string error(const std::string& s) { return "-" + s + "\r\n"; }
std::string integer(int64_t v) { return ":" + std::to_string(v) + "\r\n"; }
std::string bulkString(const std::string& s) {
    return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
}
std::string nullBulk() { return "$-1\r\n"; }
std::string nullArray() { return "*-1\r\n"; }
std::string emptyArray() { return "*0\r\n"; }
std::string boolean(bool b) { return std::string(1, b ? '#' : '-') + (b ? "t\r\n" : "f\r\n"); }

std::string doubleValue(double v) {
    if (std::isinf(v)) return v > 0 ? "inf\r\n" : "-inf\r\n";
    if (std::isnan(v)) return "nan\r\n";
    return "," + strutil::formatScore(v) + "\r\n";
}

std::string array(const std::vector<std::string>& encodedElements) {
    std::string out = "*" + std::to_string(encodedElements.size()) + "\r\n";
    for (const auto& e : encodedElements) out += e;
    return out;
}

std::string arrayOfBulkStrings(const std::vector<std::string>& values) {
    std::string out = "*" + std::to_string(values.size()) + "\r\n";
    for (const auto& v : values) out += bulkString(v);
    return out;
}

}  // namespace redis::resp
