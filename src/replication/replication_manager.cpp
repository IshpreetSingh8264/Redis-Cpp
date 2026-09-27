#include "replication/replication_manager.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <sstream>

#include "persistence/rdb_manager.hpp"
#include "protocol/resp.hpp"
#include "store/data_store.hpp"
#include "types/server_config.hpp"
#include "types/services.hpp"
#include "utils/io.hpp"
#include "utils/strutil.hpp"
#include "utils/time.hpp"

namespace redis {

namespace {

/// Read one CRLF-terminated line off the front of `buf`.
/// Returns false when the terminator has not arrived yet.
bool takeLine(const std::string& buf, std::string& line, size_t& consumed) {
    size_t crlf = buf.find("\r\n");
    if (crlf == std::string::npos) return false;
    line = buf.substr(0, crlf);
    consumed = crlf + 2;
    return true;
}

}  // namespace

namespace {

/// A fresh 40-hex-character replication id, the length Redis uses. Regenerated
/// on promotion, which is how a client can tell a failover from the original
/// master.
std::string generateReplid() {
    std::ostringstream oss;
    const int64_t seed = timeutil::nowMs() ^ (reinterpret_cast<uintptr_t>(&seed) << 16);
    oss << std::hex;
    for (int i = 0; i < 10; i++) {
        uint64_t chunk = static_cast<uint64_t>(seed >> (i * 5)) * 0x9E3779B97F4A7C15ULL;
        for (int b = 0; b < 4; b++) {
            const uint8_t v = static_cast<uint8_t>((chunk >> (b * 8)) ^ (chunk >> ((b + 3) * 8)));
            oss << "0123456789abcdef"[v & 0x0F];
        }
    }
    return oss.str().substr(0, 40);
}

}  // namespace

ReplicationManager::ReplicationManager(ServerConfig* config, Services* services)
    : config_(config), services_(services), replid_(generateReplid()) {}

int64_t ReplicationManager::offset() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return offset_;
}

// ---------------------------------------------------------------------------
// Master side
// ---------------------------------------------------------------------------

void ReplicationManager::addReplica(int fd, int64_t offsetAtSync) {
    std::lock_guard<std::mutex> lock(mutex_);
    ReplicaLink link;
    link.fd = fd;
    link.syncOffset = offsetAtSync;
    link.ackedOffset = offsetAtSync;
    replicas_[fd] = link;
}

void ReplicationManager::removeReplica(int fd) {
    std::lock_guard<std::mutex> lock(mutex_);
    replicas_.erase(fd);
}

bool ReplicationManager::isKnownReplica(int fd) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return replicas_.count(fd) != 0;
}

void ReplicationManager::noteAck(int fd, int64_t ackedOffset) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = replicas_.find(fd);
    if (it == replicas_.end()) return;
    // An ack is monotonic; a peer that goes backwards is not to be trusted to
    // satisfy a later WAIT.
    if (ackedOffset > it->second.ackedOffset) it->second.ackedOffset = ackedOffset;
}

ReplicaLink ReplicationManager::replicaInfo(int fd) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = replicas_.find(fd);
    return it == replicas_.end() ? ReplicaLink{} : it->second;
}

size_t ReplicationManager::replicaCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return replicas_.size();
}

int64_t ReplicationManager::propagate(const std::vector<std::string>& args) {
    if (config_->isReplica) return offset();  // a replica never propagates
    if (replicas_.empty()) return offset();   // nothing listening; still no-op

    const std::string frame = resp::encodeCommand(args);
    const int64_t bytes = static_cast<int64_t>(frame.size());

    std::lock_guard<std::mutex> lock(mutex_);
    int64_t newOffset = offset_ + bytes;
    for (auto& [fd, link] : replicas_) {
        io::sendAll(fd, frame);
    }
    offset_ = newOffset;
    return offset_;
}

std::string ReplicationManager::ackFrame() const {
    return resp::encodeCommand({"REPLCONF", "ACK", std::to_string(offset())});
}

int ReplicationManager::waitForReplicas(int numReplicas, int64_t timeoutMs) {
    if (numReplicas <= 0) return 0;
    if (!config_->isReplica && replicas_.empty()) return 0;

    // The offset the caller is waiting for is the one that was current when
    // WAIT was issued, so writes issued by *other* clients after this point do
    // not make this WAIT succeed.
    const int64_t target = offset();
    const int64_t deadline = timeutil::steadyMs() + timeoutMs;

    for (;;) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            int acked = 0;
            for (const auto& [fd, link] : replicas_) {
                if (link.ackedOffset >= target) acked++;
            }
            if (acked >= numReplicas) return std::min(acked, numReplicas);
            if (timeutil::steadyMs() >= deadline) return acked;

            // Poll rather than sleep the whole timeout: a replica may have
            // acked in the meantime, and REPLCONF GETACK is how it tells us.
            for (auto& [fd, link] : replicas_) {
                io::sendAll(fd, resp::encodeCommand({"REPLCONF", "GETACK", "*"}));
            }
        }
        if (timeutil::steadyMs() >= deadline) break;
        usleep(2000);
    }

    std::lock_guard<std::mutex> lock(mutex_);
    int acked = 0;
    for (const auto& [fd, link] : replicas_) {
        if (link.ackedOffset >= target) acked++;
    }
    return std::min(acked, numReplicas);
}

std::string ReplicationManager::infoReplication() const {
    std::ostringstream ss;
    ss << "# Replication\r\n";
    if (config_->isReplica) {
        std::lock_guard<std::mutex> lock(mutex_);
        ss << "role:slave\r\n";
        ss << "master_host:" << config_->masterHost << "\r\n";
        ss << "master_port:" << config_->masterPort << "\r\n";
        ss << "master_link_status:" << (state_ == HandshakeState::STREAMING ? "up" : "down")
           << "\r\n";
        ss << "master_sync_in_progress:" << (state_ == HandshakeState::STREAMING ? 0 : 1) << "\r\n";
        ss << "master_repl_offset:" << offset_ << "\r\n";
    } else {
        std::lock_guard<std::mutex> lock(mutex_);
        ss << "role:master\r\n";
        ss << "master_replid:" << replid_ << "\r\n";
        ss << "master_repl_offset:" << offset_ << "\r\n";
        ss << "connected_slaves:" << replicas_.size() << "\r\n";
    }
    return ss.str();
}

// ---------------------------------------------------------------------------
// Replica side
// ---------------------------------------------------------------------------

bool ReplicationManager::startReplica() {
    if (!config_->isReplica) return false;
    return resyncWithMaster();
}

bool ReplicationManager::connectToMaster() {
    // Resolve rather than inet_pton: --replicaof is given "localhost" as often
    // as it is given "127.0.0.1", and inet_pton rejects anything that is not
    // four dotted numbers.
    struct addrinfo hints;
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    const std::string portText = std::to_string(config_->masterPort);
    struct addrinfo* resolved = nullptr;
    const int rc = ::getaddrinfo(config_->masterHost.c_str(), portText.c_str(), &hints, &resolved);
    if (rc != 0 || resolved == nullptr) {
        fail("cannot resolve master '" + config_->masterHost + "': " + gai_strerror(rc));
        return false;
    }

    const int fd = ::socket(resolved->ai_family, resolved->ai_socktype, resolved->ai_protocol);
    if (fd < 0) {
        const std::string why = std::strerror(errno);
        ::freeaddrinfo(resolved);
        fail("cannot create the master socket: " + why);
        return false;
    }
    if (::connect(fd, resolved->ai_addr, resolved->ai_addrlen) != 0) {
        const std::string why = std::strerror(errno);
        fail("cannot connect to master " + config_->masterHost + ":" + portText + ": " + why);
        ::close(fd);
        ::freeaddrinfo(resolved);
        return false;
    }
    ::freeaddrinfo(resolved);

    fcntl(fd, F_SETFL, O_NONBLOCK);
    masterFd_ = fd;
    return true;
}

void ReplicationManager::fail(const std::string& reason) {
    lastError_ = reason;
    state_ = HandshakeState::FAILED;
}

void ReplicationManager::resetLink() {
    if (masterFd_ >= 0) {
        ::close(masterFd_);
        masterFd_ = -1;
    }
    inbound_.clear();
    pendingRdb_.clear();
    pendingRdbExpected_ = 0;
    state_ = HandshakeState::PSYNC_SENT;
}

bool ReplicationManager::startReplication(const std::string& host, int port) {
    const int oldFd = masterFd_;
    config_->isReplica = true;
    config_->masterHost = host;
    config_->masterPort = port;
    if (!resyncWithMaster()) {
        // Leave the instance as a replica with a dead link; the event loop
        // retries, which is what a real replica does.
        return false;
    }
    if (onMasterLinkChanged) onMasterLinkChanged(oldFd, masterFd_);
    return true;
}

int ReplicationManager::promoteToMaster() {
    const int released = masterFd_;
    resetLink();
    config_->isReplica = false;
    config_->masterHost.clear();
    config_->masterPort = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        // The new master's own replication stream starts from where the
        // replica's had reached, so a promoted instance hands its replicas a
        // contiguous history instead of restarting at zero behind their backs.
        replid_ = generateReplid();
    }
    if (onMasterLinkChanged) onMasterLinkChanged(released, -1);
    return released;
}

bool ReplicationManager::resyncWithMaster() {
    resetLink();
    if (!connectToMaster()) return false;
    if (!sendHandshake()) {
        resetLink();
        return false;
    }
    return true;
}

bool ReplicationManager::sendHandshake() {
    auto send = [this](const std::vector<std::string>& args) {
        return io::sendAll(masterFd_, resp::encodeCommand(args));
    };

    if (!config_->masterauth.empty()) {
        if (!send({"AUTH", config_->masterauth})) {
            fail("cannot send AUTH to master");
            return false;
        }
        state_ = HandshakeState::AUTH_SENT;
        return true;
    }

    if (!send({"PING"})) {
        fail("cannot send PING to master");
        return false;
    }
    state_ = HandshakeState::PING_SENT;
    return true;
}

void ReplicationManager::rdbLoadFromMaster(const std::string& blob) {
    if (!services_ || !services_->rdb || !services_->store) return;
    int loaded = 0;
    std::string error;
    if (!services_->rdb->loadFromBuffer(blob, *services_->store, loaded, error)) {
        lastError_ = "full resync failed: " + error;
        return;
    }
    if (!error.empty()) lastError_ = error;  // e.g. a checksum warning
}

void ReplicationManager::ackCurrentOffset() {
    // Acknowledge after every applied command rather than on a timer. WAIT
    // blocks on these, and a timer that swallows the ack after a single write
    // is what made `WAIT 1 1000` answer 0 on a fully caught-up replica.
    io::sendAll(masterFd_, ackFrame());
}

void ReplicationManager::masterLinkClosed(const std::string& reason) {
    const int released = masterFd_;
    if (released >= 0) {
        ::close(released);
        masterFd_ = -1;  // before anything else can observe the stale number
    }
    inbound_.clear();
    pendingRdb_.clear();
    pendingRdbExpected_ = 0;
    state_ = HandshakeState::PSYNC_SENT;
    if (lastError_.empty()) lastError_ = reason;
}

void ReplicationManager::feedMaster(const std::string& bytes) {
    if (masterFd_ < 0) return;
    inbound_ += bytes;
    stepHandshake();
}

void ReplicationManager::stepHandshake() {
    for (;;) {
        if (state_ == HandshakeState::FAILED || state_ == HandshakeState::STREAMING) break;

        if (state_ == HandshakeState::AUTH_SENT) {
            std::string line;
            size_t used = 0;
            if (!takeLine(inbound_, line, used)) return;
            inbound_.erase(0, used);
            if (line.rfind("-", 0) == 0) {
                fail("master rejected AUTH: " + line);
                return;
            }
            io::sendAll(masterFd_, resp::encodeCommand({"PING"}));
            state_ = HandshakeState::PING_SENT;
            continue;
        }

        if (state_ == HandshakeState::PING_SENT) {
            std::string line;
            size_t used = 0;
            if (!takeLine(inbound_, line, used)) return;
            inbound_.erase(0, used);
            if (line != "+PONG" && line.rfind("-", 0) != 0) {
                fail("master did not answer PING with +PONG (got '" + line + "')");
                return;
            }
            io::sendAll(masterFd_, resp::encodeCommand(
                                        {"REPLCONF", "listening-port", std::to_string(config_->port)}));
            state_ = HandshakeState::LISTENING_PORT_SENT;
            continue;
        }

        if (state_ == HandshakeState::LISTENING_PORT_SENT) {
            std::string line;
            size_t used = 0;
            if (!takeLine(inbound_, line, used)) return;
            inbound_.erase(0, used);
            if (line.rfind("-", 0) == 0) {
                fail("master rejected REPLCONF listening-port: " + line);
                return;
            }
            io::sendAll(masterFd_, resp::encodeCommand({"REPLCONF", "capa", "eof", "capa", "psync2"}));
            state_ = HandshakeState::CAPA_SENT;
            continue;
        }

        if (state_ == HandshakeState::CAPA_SENT) {
            std::string line;
            size_t used = 0;
            if (!takeLine(inbound_, line, used)) return;
            inbound_.erase(0, used);
            if (line.rfind("-", 0) == 0) {
                fail("master rejected REPLCONF capa: " + line);
                return;
            }
            io::sendAll(masterFd_, resp::encodeCommand({"PSYNC", "?", "-1"}));
            state_ = HandshakeState::PSYNC_SENT;
            continue;
        }

        if (state_ == HandshakeState::PSYNC_SENT) {
            std::string line;
            size_t used = 0;
            if (!takeLine(inbound_, line, used)) return;
            inbound_.erase(0, used);
            if (line.rfind("+FULLRESYNC", 0) != 0) {
                // Real Redis also offers +CONTINUE here; we only ask for
                // full syncs, so anything else is a failure worth reporting.
                fail("expected +FULLRESYNC from master, got '" + line + "'");
                return;
            }
            auto parts = strutil::splitWhitespace(line);
            if (parts.size() < 3) {
                fail("malformed +FULLRESYNC line: " + line);
                return;
            }
            int64_t masterOffset = 0;
            strutil::parseInt64(parts[2], masterOffset);
            {
                std::lock_guard<std::mutex> lock(mutex_);
                offset_ = masterOffset;
            }
            state_ = HandshakeState::AWAITING_FULLRESYNC;
            continue;
        }

        if (state_ == HandshakeState::AWAITING_FULLRESYNC) {
            // The dump arrives as one RESP bulk string: "$<len>\r\n<payload>".
            // The header is consumed here and only the payload is left for
            // LOADING_RDB, so exactly one state owns each byte. Letting the
            // two states overlap -- one branch taking the whole payload at
            // once, the other draining it -- meant pendingRdbExpected_ was
            // still 0 when the payload had already arrived, and the
            // "remaining = expected - have" subtraction underflowed.
            const size_t headerEnd = inbound_.find("\r\n");
            if (headerEnd == std::string::npos) return;
            if (inbound_[0] != '$') {
                fail("expected a bulk RDB payload from master");
                return;
            }
            int64_t len = 0;
            if (!strutil::parseInt64(inbound_.substr(1, headerEnd - 1), len) || len < 0) {
                fail("master sent an unparseable RDB length");
                return;
            }
            inbound_.erase(0, headerEnd + 2);
            pendingRdbExpected_ = static_cast<size_t>(len);
            pendingRdb_.clear();
            state_ = HandshakeState::LOADING_RDB;
            continue;  // fall through into the loader
        }

        if (state_ == HandshakeState::LOADING_RDB) {
            const size_t take =
                std::min(pendingRdbExpected_ - pendingRdb_.size(), inbound_.size());
            pendingRdb_.append(inbound_, 0, take);
            inbound_.erase(0, take);
            if (pendingRdb_.size() < pendingRdbExpected_) return;

            // Hand the dump to the RDB layer. The offset stays where the
            // FULLRESYNC line put it; the dump itself is not replication
            // traffic, so it must not be counted against the offset.
            rdbLoadFromMaster(pendingRdb_);
            pendingRdb_.clear();
            pendingRdbExpected_ = 0;
            state_ = HandshakeState::STREAMING;
            ackCurrentOffset();
            break;
        }
    }

    if (state_ == HandshakeState::STREAMING) streamCommands();
}

void ReplicationManager::streamCommands() {
    while (true) {
        resp::ParseResult r = resp::parseCommand(inbound_);
        if (!r.ok) {
            if (r.fatal) {
                fail("unparseable command from master");
                return;
            }
            break;  // need more bytes
        }
        if (r.consumed == 0) break;
        inbound_.erase(0, r.consumed);

        {
            std::lock_guard<std::mutex> lock(mutex_);
            offset_ += static_cast<int64_t>(r.consumed);
        }

        if (r.args.empty()) continue;
        const std::string name = strutil::toUpper(r.args[0]);
        if (name == "PING" || name == "REPLCONF") continue;  // keepalives
        if (applyFromMaster) applyFromMaster(r.args);
    }
    ackCurrentOffset();
}

}  // namespace redis
