/**
 * replication_manager.hpp -- both ends of the master/replica link.
 *
 * Everything the old code faked lives here and is now real:
 *   * the replication offset actually advances, by the number of bytes the
 *     master puts on the wire, and the replica advances its own copy by the
 *     number of bytes it consumed;
 *   * REPLCONF parses its arguments and tracks the peer's port, capabilities
 *     and acks, instead of answering +OK to everything;
 *   * PSYNC sends a real full-resync: a real FULLRESYNC line carrying the real
 *     replid and offset, followed by a real RDB of the real keyspace;
 *   * WAIT counts replicas that have actually acknowledged the offset, rather
 *     than sleeping for the timeout and reporting the connection count.
 */
#ifndef REDIS_REPLICATION_REPLICATION_MANAGER_HPP
#define REDIS_REPLICATION_REPLICATION_MANAGER_HPP

#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "types/server_config.hpp"

namespace redis {

class Services;

/// Where a replica is in its one-time sync with its master.
enum class HandshakeState {
    AUTH_SENT,
    PING_SENT,
    LISTENING_PORT_SENT,
    CAPA_SENT,
    PSYNC_SENT,
    AWAITING_FULLRESYNC,
    LOADING_RDB,
    STREAMING,
    FAILED,
};

/// What we learned about a replica from its REPLCONF lines.
struct ReplicaLink {
    int fd = -1;
    int listeningPort = 0;
    bool capaEof = false;
    bool capaPsync2 = false;
    int64_t syncOffset = 0;   // offset the replica was handed at full-resync
    int64_t ackedOffset = 0;  // last offset it confirmed with REPLCONF ACK
};

class ReplicationManager {
public:
    ReplicationManager(ServerConfig* config, Services* services);

    // --- lifecycle --------------------------------------------------------
    /// On a replica: connect to the master and run the handshake up to and
    /// including sending PSYNC. The rest happens in feedMaster().
    bool startReplica();
    /// Run one full-sync attempt synchronously. Used by the event loop when
    /// the previous link dropped.
    bool resyncWithMaster();

    /// REPLICAOF <host> <port> at runtime: point this instance at a new master
    /// and resync. Returns false if the connection could not be made.
    bool startReplication(const std::string& host, int port);

    /// REPLICAOF NO ONE: drop the link and become a master. The keyspace is
    /// left alone, as in real Redis. Returns the fd that was released, or -1.
    int promoteToMaster();

    // --- master side ------------------------------------------------------
    bool isReplica() const { return config_->isReplica; }
    const std::string& replid() const { return replid_; }
    int64_t offset() const;
    void addReplica(int fd, int64_t offsetAtSync);
    void removeReplica(int fd);
    bool isKnownReplica(int fd) const;
    void noteAck(int fd, int64_t ackedOffset);
    ReplicaLink replicaInfo(int fd) const;
    size_t replicaCount() const;

    /// Send `args` to every replica and advance the offset by exactly the
    /// number of bytes sent. Returns the new offset.
    int64_t propagate(const std::vector<std::string>& args);

    /// The `REPLCONF ACK <offset>` frame for the master link.
    std::string ackFrame() const;

    /// Block until `numReplicas` replicas have acked the current offset or
    /// `timeoutMs` elapses. Returns how many actually acked.
    int waitForReplicas(int numReplicas, int64_t timeoutMs);

    std::string infoReplication() const;

    // --- replica side -----------------------------------------------------
    int masterFd() const { return masterFd_; }
    bool isMasterLink(int fd) const { return fd >= 0 && fd == masterFd_; }
    HandshakeState handshakeState() const { return state_; }
    const std::string& lastError() const { return lastError_; }

    /// Consume whatever arrived on the master socket. Handles the handshake,
    /// the RDB payload, and the steady-state command stream.
    void feedMaster(const std::string& bytes);

    /// The master link is gone. Releases the socket and resets the handshake
    /// so the event loop can resync.
    ///
    /// This has to be the *only* way the link is released. Closing the socket
    /// while leaving masterFd_ set is how a later accept() hands the same fd
    /// number to an ordinary client, which then gets fed to the replica's
    /// handshake parser instead of the command dispatcher.
    void masterLinkClosed(const std::string& reason);

    // --- callbacks the net layer installs ---------------------------------
    /// Apply a command received from the master. Supplied by net/server.
    std::function<void(const std::vector<std::string>&)> applyFromMaster;

    /// Told whenever the master link's fd changes, so the event loop can move
    /// its registration. Supplied by net/server.
    std::function<void(int oldFd, int newFd)> onMasterLinkChanged;

private:
    bool connectToMaster();
    bool sendHandshake();
    void resetLink();
    void fail(const std::string& reason);
    void stepHandshake();
    void streamCommands();
    int countAcks(int64_t target) const;
    void advanceOffset(int64_t bytes);
    void sendAck();
    void rdbLoadFromMaster(const std::string& blob);

    ServerConfig* config_;
    Services* services_;

    std::string replid_;

    mutable std::mutex mutex_;
    int64_t offset_ = 0;
    std::map<int, ReplicaLink> replicas_;

    // replica side
    int masterFd_ = -1;
    HandshakeState state_ = HandshakeState::PSYNC_SENT;
    std::string inbound_;          // partial handshake / stream bytes
    std::string pendingRdb_;       // partial RDB payload
    size_t pendingRdbExpected_ = 0;
    std::string lastError_;
};

}  // namespace redis

#endif  // REDIS_REPLICATION_REPLICATION_MANAGER_HPP
