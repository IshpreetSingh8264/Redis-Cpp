#include "utils/strutil.hpp"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <sstream>

namespace redis::strutil {

std::string toUpper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    return s;
}

std::string toLower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return s;
}

namespace {

// Recursive glob matcher. Kept separate so matchPattern() stays a one-liner.
bool matchHere(const char* p, const char* pe, const char* s, const char* se) {
    while (p < pe) {
        if (*p == '*') {
            // Collapse runs of '*' so "a**b" does not blow the stack.
            while (p < pe && *p == '*') p++;
            if (p == pe) return true;
            for (const char* q = s; q <= se; q++) {
                if (matchHere(p, pe, q, se)) return true;
            }
            return false;
        }
        if (s >= se) return false;

        if (*p == '?') {
            p++;
            s++;
            continue;
        }
        if (*p == '[') {
            const char* q = p + 1;
            bool negate = false;
            if (q < pe && (*q == '^' || *q == '!')) {
                negate = true;
                q++;
            }
            bool matched = false;
            bool closed = false;
            while (q < pe) {
                if (*q == ']' && q > p + 1) {
                    closed = true;
                    break;
                }
                if (q + 2 < pe && q[1] == '-' && q[2] != ']') {
                    if (*s >= *q && *s <= q[2]) matched = true;
                    q += 3;
                } else {
                    if (*s == *q) matched = true;
                    q++;
                }
            }
            if (!closed) {
                // Unterminated class: treat '[' literally, as Redis does.
                if (*s != '[') return false;
                p++;
                s++;
                continue;
            }
            if (matched == negate) return false;
            p = q + 1;
            s++;
            continue;
        }
        if (*p == '\\' && p + 1 < pe) {
            p++;
            if (*s != *p) return false;
            p++;
            s++;
            continue;
        }
        if (*p != *s) return false;
        p++;
        s++;
    }
    return s == se;
}

}  // namespace

bool matchPattern(const std::string& pattern, const std::string& str) {
    return matchHere(pattern.data(), pattern.data() + pattern.size(), str.data(),
                     str.data() + str.size());
}

bool parseInt64(const std::string& s, int64_t& out) {
    if (s.empty()) return false;
    errno = 0;
    char* end = nullptr;
    long long v = std::strtoll(s.c_str(), &end, 10);
    if (errno != 0 || end == s.c_str() || *end != '\0') return false;
    out = v;
    return true;
}

bool parseDouble(const std::string& s, double& out) {
    if (s.empty()) return false;
    // Redis accepts inf/+inf/-inf and rejects nan in score positions.
    std::string lower = toLower(s);
    if (lower == "inf" || lower == "+inf" || lower == "infinity" || lower == "+infinity") {
        out = HUGE_VAL;
        return true;
    }
    if (lower == "-inf" || lower == "-infinity") {
        out = -HUGE_VAL;
        return true;
    }
    errno = 0;
    char* end = nullptr;
    double v = std::strtod(s.c_str(), &end);
    if (errno != 0 || end == s.c_str() || *end != '\0' || std::isnan(v)) return false;
    out = v;
    return true;
}

StreamIdParsed parseStreamId(const std::string& s) {
    StreamIdParsed out;
    size_t dash = s.find('-');
    if (dash == std::string::npos || dash == 0 || dash + 1 == s.size()) return out;
    uint64_t ms = 0, seq = 0;
    for (size_t i = 0; i < dash; i++) {
        if (!std::isdigit(static_cast<unsigned char>(s[i]))) return out;
        ms = ms * 10 + static_cast<uint64_t>(s[i] - '0');
    }
    for (size_t i = dash + 1; i < s.size(); i++) {
        if (!std::isdigit(static_cast<unsigned char>(s[i]))) return out;
        seq = seq * 10 + static_cast<uint64_t>(s[i] - '0');
    }
    out.ms = ms;
    out.seq = seq;
    out.valid = true;
    return out;
}

std::string formatScore(double score) {
    if (score == static_cast<double>(static_cast<int64_t>(score)) && std::fabs(score) < 1e17) {
        return std::to_string(static_cast<int64_t>(score));
    }
    // 17 significant digits round-trips an IEEE double exactly, which is what
    // Redis emits; trim to the shortest form that still round-trips.
    for (int precision = 1; precision <= 17; precision++) {
        std::ostringstream oss;
        oss.precision(precision);
        oss << score;
        if (std::strtod(oss.str().c_str(), nullptr) == score) return oss.str();
    }
    std::ostringstream oss;
    oss.precision(17);
    oss << score;
    return oss.str();
}

std::vector<std::string> splitWhitespace(const std::string& s) {
    std::vector<std::string> out;
    std::istringstream iss(s);
    std::string word;
    while (iss >> word) out.push_back(word);
    return out;
}

}  // namespace redis::strutil
