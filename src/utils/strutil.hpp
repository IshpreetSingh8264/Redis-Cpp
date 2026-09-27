/**
 * strutil.hpp -- leaf string helpers.
 *
 * No domain knowledge. Nothing in here knows what a key or a command is.
 */
#ifndef REDIS_UTILS_STRUTIL_HPP
#define REDIS_UTILS_STRUTIL_HPP

#include <cstdint>
#include <string>
#include <vector>

namespace redis::strutil {

std::string toUpper(std::string s);
std::string toLower(std::string s);

/// Glob match with Redis's KEYS semantics: '*' any run, '?' exactly one,
/// '[]' character classes. Backtracking, so it is linear in practice.
bool matchPattern(const std::string& pattern, const std::string& str);

/// Strict integer parse. Unlike std::stoll these never throw and never accept
/// trailing garbage -- every call site wants the same failure behaviour and
/// none of them want a try/catch.
bool parseInt64(const std::string& s, int64_t& out);
bool parseDouble(const std::string& s, double& out);

/// Parse "<ms>-<seq>". Returns valid=false for anything else, including
/// "<ms>-" and "" -- callers must check rather than assume.
struct StreamIdParsed {
    uint64_t ms = 0;
    uint64_t seq = 0;
    bool valid = false;
};
StreamIdParsed parseStreamId(const std::string& s);

/// Render a double the way Redis does: shortest round-trippable form, no
/// trailing zeros, no exponent for ordinary magnitudes.
std::string formatScore(double score);

/// Split on whitespace, dropping empties. Used for inline (telnet) commands.
std::vector<std::string> splitWhitespace(const std::string& s);

}  // namespace redis::strutil

#endif  // REDIS_UTILS_STRUTIL_HPP
