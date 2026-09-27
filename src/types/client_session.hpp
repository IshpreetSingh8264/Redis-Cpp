/**
 * client_session.hpp -- all per-connection state.
 *
 * Deliberately pure data: the command layer reads and writes it, the net layer
 * owns its lifetime. Keeping it out of net/ is what lets commands/ never
 * include net/, which is what keeps the dependency graph acyclic.
 */
#ifndef REDIS_TYPES_CLIENT_SESSION_HPP
#define REDIS_TYPES_CLIENT_SESSION_HPP

#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "types/enums.hpp"

namespace redis {

class ClientSession {
public:
    explicit ClientSession(int fd) : fd_(fd) {}

    int fd() const { return fd_; }
    ClientState state() const { return state_; }
    bool inMulti() const { return state_ == ClientState::MULTI; }
    bool subscribed() const { return !channels_.empty(); }

    // --- read side (net/ owns these) -------------------------------------
    std::string& inputBuffer() { return inputBuffer_; }

    // --- transaction side (transaction_commands/ owns these) -------------
    void beginMulti() { state_ = ClientState::MULTI; }
    void endMulti() { state_ = ClientState::NORMAL; }
    void queue(const std::vector<std::string>& args) { queue_.push_back(args); }
    std::vector<std::vector<std::string>>& queue() { return queue_; }
    void clearQueue() { queue_.clear(); }

    /// Keys this connection is watching, together with the value fingerprint
    /// each key had at the moment of WATCH. A write to any of them aborts
    /// EXEC; so does a change to any of these fingerprints.
    void watch(const std::string& key, uint64_t fingerprint) {
        watched_[key] = fingerprint;
    }
    void clearWatched() { watched_.clear(); }
    const std::unordered_map<std::string, uint64_t>& watched() const { return watched_; }
    bool watchesAnything() const { return !watched_.empty(); }

    // --- pub/sub side (pubsub_commands/ owns these) ----------------------
    void subscribe(const std::string& channel) { channels_.insert(channel); }
    void unsubscribe(const std::string& channel) { channels_.erase(channel); }
    const std::unordered_set<std::string>& channels() const { return channels_; }
    void setSubscribedMode() { state_ = ClientState::SUBSCRIBED; }
    void refreshSubscribedMode() {
        if (channels_.empty() && state_ == ClientState::SUBSCRIBED) state_ = ClientState::NORMAL;
    }

    // --- auth side (auth_commands/ owns these) --------------------------
    void setAuthenticated() { authenticated_ = true; }
    bool authenticated() const { return authenticated_; }
    void setUser(const std::string& user) { user_ = user; }
    const std::string& user() const { return user_; }
    void setName(const std::string& name) { name_ = name; }
    const std::string& name() const { return name_; }

    // --- replication side (replication/ owns these) ---------------------
    /// This connection is our upstream master (we are the replica).
    void markMasterLink() { masterLink_ = true; }
    bool isMasterLink() const { return masterLink_; }
    /// This connection is a downstream replica (we are the master). Marked
    /// once PSYNC has succeeded, which is also the point after which nothing
    /// on this socket may be answered: a reply is not replication traffic, and
    /// a replica that counted one would drift out of step with the master's
    /// offset.
    void markReplicaLink() { replicaLink_ = true; }
    bool isReplicaLink() const { return replicaLink_; }
    bool isReplicationLink() const { return masterLink_ || replicaLink_; }
    /// Offset this replica has acknowledged to its master, used by WAIT.
    void setAckedOffset(int64_t off) { ackedOffset_ = off; }
    int64_t ackedOffset() const { return ackedOffset_; }

private:
    int fd_;
    ClientState state_ = ClientState::NORMAL;
    std::string inputBuffer_;
    std::vector<std::vector<std::string>> queue_;
    std::unordered_map<std::string, uint64_t> watched_;
    std::unordered_set<std::string> channels_;
    bool authenticated_ = false;
    std::string user_ = "default";
    std::string name_;
    bool masterLink_ = false;
    bool replicaLink_ = false;
    int64_t ackedOffset_ = 0;
};

}  // namespace redis

#endif  // REDIS_TYPES_CLIENT_SESSION_HPP
