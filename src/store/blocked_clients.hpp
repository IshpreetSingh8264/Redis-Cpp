/**
 * blocked_clients.hpp -- who is parked waiting for data, and how they get it.
 *
 * BLPOP/BRPOP and `XREAD ... BLOCK` do not answer when they are called. They
 * park a ClientSession here and the command that later creates the data is
 * what completes their reply. That makes this the one module that both the
 * list commands, the stream commands and the event loop have to agree on, so
 * it lives in store/ rather than in either command group.
 */
#ifndef REDIS_STORE_BLOCKED_CLIENTS_HPP
#define REDIS_STORE_BLOCKED_CLIENTS_HPP

#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

#include "types/client_session.hpp"
#include "types/values.hpp"

namespace redis {

class DataStore;

/// A client waiting for an element to appear on any of `keys`.
struct ListBlockedClient {
    ClientSession* client = nullptr;
    std::vector<std::string> keys;
    bool fromLeft = true;         // true for BLPOP, false for BRPOP
    int64_t timeoutMs = -1;       // -1 = wait forever
    int64_t startedAtMs = 0;      // steady clock
};

/// A client waiting for stream entries newer than `lastIds`.
struct StreamBlockedClient {
    ClientSession* client = nullptr;
    std::vector<std::string> keys;
    std::vector<StreamId> lastIds;
    int64_t count = -1;           // -1 = unlimited
    int64_t timeoutMs = -1;
    int64_t startedAtMs = 0;
};

class BlockedClients {
public:
    // --- registration, called from the BLPOP / XREAD handlers ------------
    void blockOnList(ClientSession* client, std::vector<std::string> keys, bool fromLeft,
                     int64_t timeoutMs);
    void blockOnStreams(ClientSession* client, std::vector<std::string> keys,
                        std::vector<StreamId> lastIds, int64_t count, int64_t timeoutMs);

    // --- notification, called by LPUSH/RPUSH/XADD once the write is done --
    /// Hand elements to every waiting client that `key` can satisfy. Returns
    /// how many clients were completed.
    ///
    /// Every waiter is served, not just the first. The previous implementation
    /// returned after one, so a single LPUSH with four parked BLPOPs left
    /// three clients hanging until their timeout.
    int notifyList(const std::string& key, DataStore& store);

    /// Deliver newly appended entries to every waiting reader of `key`.
    /// Stream entries are not consumed by reading, so all matching readers
    /// get the same entries.
    int notifyStreams(const std::string& key, DataStore& store);

    // --- housekeeping, called once per event-loop tick -------------------
    /// Send the null reply to everyone whose deadline has passed.
    void expireTimedOut();

    /// Forget everything about a connection that has gone away.
    void removeClient(ClientSession* client);

    bool empty() const;

private:
    mutable std::mutex mutex_;
    std::vector<ListBlockedClient> listWaiters_;
    std::vector<StreamBlockedClient> streamWaiters_;
};

}  // namespace redis

#endif  // REDIS_STORE_BLOCKED_CLIENTS_HPP
