/**
 * io.hpp -- raw socket writes.
 *
 * Leaf helper with no domain knowledge. write(2) can return short, and every
 * caller here is pushing a RESP frame at a peer that will hang up on a
 * truncated one, so partial writes are looped over and a dead peer is
 * reported rather than ignored.
 */
#ifndef REDIS_UTILS_IO_HPP
#define REDIS_UTILS_IO_HPP

#include <string>

namespace redis::io {

/// Write every byte of `data` to `fd`, looping over short writes.
/// Returns false if the peer is gone or the write failed.
bool sendAll(int fd, const std::string& data);

}  // namespace redis::io

#endif  // REDIS_UTILS_IO_HPP
