/**
 * time.hpp -- the one clock the whole server reads.
 *
 * Two clocks, because two different questions get asked: "what time is it"
 * (expiry, IDs) uses the wall clock, "how long have I been waiting" (blocking
 * timeouts) uses the steady clock so that an NTP step cannot unblock or hang
 * a client.
 */
#ifndef REDIS_UTILS_TIME_HPP
#define REDIS_UTILS_TIME_HPP

#include <cstdint>

namespace redis::timeutil {

/// Milliseconds since the Unix epoch. Used for key expiry and stream IDs.
int64_t nowMs();

/// Monotonic milliseconds from an arbitrary origin. Used for timeouts.
int64_t steadyMs();

}  // namespace redis::timeutil

#endif  // REDIS_UTILS_TIME_HPP
