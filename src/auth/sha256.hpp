/**
 * sha256.hpp -- SHA-256, hex encoded.
 *
 * ACL GETUSER and ACL LIST report a password as the hex SHA-256 of the
 * password, never in clear, and that is the only form a client is expected to
 * be able to compare against. There is no libc call for it and no dependency
 * in this project to borrow one from, so it lives here.
 *
 * Leaf helper: it knows about bytes and about nothing else in this server.
 */
#ifndef REDIS_AUTH_SHA256_HPP
#define REDIS_AUTH_SHA256_HPP

#include <string>

namespace redis::auth {

/// Lowercase hex SHA-256 of `data`, which is how Redis spells a password hash.
std::string sha256Hex(const std::string& data);

}  // namespace redis::auth

#endif  // REDIS_AUTH_SHA256_HPP
