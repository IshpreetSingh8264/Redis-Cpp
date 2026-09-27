#include "net/server.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <iostream>
#include <unordered_set>

#include "commands/auth_manager.hpp"
#include "protocol/resp.hpp"
#include "utils/io.hpp"
#include "utils/strutil.hpp"
#include "utils/time.hpp"

namespace redis {

namespace {

/// Commands that are legal inside a MULTI block without being queued.
const std::unordered_set<std::string>& nonQueuableCommands() {
    static const std::unordered_set<std::string> set = {
        "MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH", "RESET", "QUIT"};
    return set;
}

/// Commands a connection in subscribed mode may send. The RESP spec fixes this
/// list; everything else gets a refusal, and the refusal is an array because the
/// client is in array mode.
bool allowedWhileSubscribed(const std::string& upper) {
    return upper == "PING" || upper == "QUIT" || upper == "RESET" || upper == "SUBSCRIBE" ||
           upper == "UNSUBSCRIBE" || upper == "PSUBSCRIBE" || upper == "PUNSUBSCRIBE";
}

/// Commands whose effect outlives a transaction, so they may not be queued.
bool allowedInTransaction(const std::string& upper) {
    return upper != "PSYNC" && upper != "SYNC" && upper != "REPLCONF" && upper != "SHUTDOWN" &&
           upper != "SUBSCRIBE" && upper != "UNSUBSCRIBE" && upper != "PSUBSCRIBE" &&
           upper != "PUNSUBSCRIBE" && upper != "MULTI" && upper != "EXEC" && upper != "DISCARD";
}

}  // namespace

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

RedisServer::RedisServer(ServerConfig config)
    : config_(std::move(config)),
      rdb_(&config_),
      replication_(&config_, &services_),
      registry_(buildRegistry()) {
    aof_ = std::make_unique<AofManager>(&config_, &services_);
    auth_ = std::make_unique<AuthManager>(&config_);

    services_.config = &config_;
    services_.store = &store_;
    services_.blocked = &blocked_;
    services_.replication = &replication_;
    services_.rdb = &rdb_;
    services_.aof = aof_.get();
    services_.auth = auth_.get();
    services_.pubsub = &pubsub_;
    services_.execute = [this](const std::vector<std::string>& args, ClientSession* session) {
        return execute(args, *session);
    };

    auth_->bootstrap();

    // Replaying the AOF means running the commands in it, exactly as if they
    // had just arrived -- so it goes back through execute(), and a replayed
    // write is itself appended, which is what keeps the two consistent.
    aof_->applyCommand = [this](const std::vector<std::string>& args) {
        if (args.empty()) return;
        static ClientSession replaySession(-1);
        replaySession.setAuthenticated();
        execute(args, replaySession);
    };

    // The replication link's fd can change under us (REPLICAOF, a dropped
    // master, a resync), and the epoll registration has to follow it.
    replication_.onMasterLinkChanged = [this](int oldFd, int newFd) {
        if (oldFd >= 0) {
            epoll_ctl(epollFd_, EPOLL_CTL_DEL, oldFd, nullptr);
            if (oldFd != replication_.masterFd()) ::close(oldFd);
        }
        masterLinkRegistered_ = false;
        if (newFd >= 0) {
            addToPoll(newFd);
            ensureMasterSession();
            masterLinkRegistered_ = true;
        } else if (masterSession_) {
            masterSession_.reset();
        }
    };

    // Commands arriving from the master are applied, never queued, never
    // propagated back, and never answered to the master.
    replication_.applyFromMaster = [this](const std::vector<std::string>& args) {
        if (args.empty()) return;
        const std::string name = strutil::toUpper(args[0]);
        if (name == "PING" || name == "REPLCONF" || name == "SELECT") return;
        if (name == "SHUTDOWN") return;  // a master cannot shut a replica down
        execute(args, *masterSession_);
    };
}

RedisServer::~RedisServer() {
    if (aof_) aof_->close();
}

void RedisServer::ensureMasterSession() {
    if (masterSession_ && masterSession_->fd() == replication_.masterFd()) return;
    masterSession_ = std::make_unique<ClientSession>(replication_.masterFd());
    masterSession_->markMasterLink();
    masterSession_->setAuthenticated();
}

// ---------------------------------------------------------------------------
// Argument parsing
// ---------------------------------------------------------------------------

bool RedisServer::parseArguments(int argc, char** argv, ServerConfig& config) {
    for (int i = 1; i < argc; i++) {
        const std::string arg = argv[i];
        std::string value;
        int64_t number = 0;
        auto next = [&]() {
            if (i + 1 >= argc) return false;
            value = argv[++i];
            return true;
        };

        if (arg == "--port") {
            if (!next() || !strutil::parseInt64(value, number) || number < 0 || number > 65535) {
                std::cerr << "Bad port: " << value << std::endl;
                return false;
            }
            config.port = static_cast<int>(number);
        } else if (arg == "--dir") {
            if (!next()) return false;
            config.dir = value;
        } else if (arg == "--dbfilename") {
            if (!next()) return false;
            config.dbfilename = value;
        } else if (arg == "--appendonly") {
            if (!next()) return false;
            const std::string on = strutil::toLower(value);
            if (on != "yes" && on != "no") {
                std::cerr << "Invalid argument for --appendonly: " << value << std::endl;
                return false;
            }
            config.appendonly = (on == "yes");
        } else if (arg == "--appendfilename") {
            if (!next()) return false;
            config.appendfilename = value;
        } else if (arg == "--appenddirname") {
            if (!next()) return false;
            config.appenddirname = value;
        } else if (arg == "--requirepass") {
            if (!next()) return false;
            config.requirepass = value;
        } else if (arg == "--masterauth") {
            if (!next()) return false;
            config.masterauth = value;
        } else if (arg == "--user") {
            if (!next()) return false;
            config.user = value;
        } else if (arg == "--replicaof" || arg == "--slaveof") {
            // Two spellings are in the wild: Redis's own `--replicaof host port`
            // and the quoted `--replicaof "host port"` the CodeCrafters harness
            // uses. Requiring the two-argument form and rejecting the other one
            // made the process exit with a usage error, so the replica never
            // came up at all.
            std::string host, portText;
            if (i + 1 >= argc) return false;
            const std::string first = argv[i + 1];
            const size_t space = first.find_first_of(" \t");
            if (space != std::string::npos) {
                host = first.substr(0, space);
                portText = first.substr(space + 1);
                i += 1;
            } else {
                if (i + 2 >= argc) return false;
                host = first;
                portText = argv[i + 2];
                i += 2;
            }
            if (!strutil::parseInt64(portText, number) || number <= 0 || number > 65535) {
                std::cerr << "Bad replicaof port: " << portText << std::endl;
                return false;
            }
            config.isReplica = true;
            config.masterHost = host;
            config.masterPort = static_cast<int>(number);
        } else {
            std::cerr << "Bad argument: " << arg << std::endl;
            return false;
        }
    }
    return true;
}

// ---------------------------------------------------------------------------
// Startup
// ---------------------------------------------------------------------------

void RedisServer::run() {
    std::cout << "Starting Redis server on port " << config_.port << std::endl;

    int loaded = 0;
    std::string error;
    if (rdb_.load(store_, loaded, error)) {
        std::cout << "Loading RDB from " << rdb_.path() << std::endl;
        std::cout << "RDB loaded successfully (" << loaded << " keys)" << std::endl;
        if (!error.empty()) std::cerr << "Warning: " << error << std::endl;
    }

    if (!aof_->open(store_, error)) {
        std::cerr << "Warning: could not open the AOF: " << error << std::endl;
    } else if (config_.appendonly) {
        std::cout << "Reading append only file from " << aof_->dirPath() << std::endl;
    }

    // A replica still serves its own clients while it catches up, so the link
    // is started before the listening socket but never blocks startup.
    if (config_.isReplica && replication_.startReplica()) {
        ensureMasterSession();
        std::cout << "Connected to master " << config_.masterHost << ":" << config_.masterPort
                  << std::endl;
    } else if (config_.isReplica) {
        std::cerr << "Warning: replication did not start: " << replication_.lastError()
                  << std::endl;
    }

    listen();
    std::cout << "Server ready to accept connections" << std::endl;

    struct epoll_event events[64];
    while (!stopping_) {
        const int n = epoll_wait(epollFd_, events, 64, 100);
        tick();
        for (int i = 0; i < n && !stopping_; i++) handleReadable(events[i].data.fd);
    }

    if (aof_) aof_->close();
}

void RedisServer::listen() {
    listenFd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd_ < 0) {
        std::cerr << "Failed to create socket" << std::endl;
        std::exit(1);
    }

    int reuse = 1;
    setsockopt(listenFd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in address;
    std::memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(static_cast<uint16_t>(config_.port));

    if (::bind(listenFd_, reinterpret_cast<struct sockaddr*>(&address), sizeof(address)) != 0) {
        std::cerr << "Failed to bind to port " << config_.port << std::endl;
        std::exit(1);
    }
    if (::listen(listenFd_, SOMAXCONN) != 0) {
        std::cerr << "Listen failed" << std::endl;
        std::exit(1);
    }
    fcntl(listenFd_, F_SETFL, O_NONBLOCK);

    epollFd_ = epoll_create1(0);
    if (epollFd_ < 0) {
        std::cerr << "epoll_create1 failed" << std::endl;
        std::exit(1);
    }

    addToPoll(listenFd_);

    const int masterFd = replication_.masterFd();
    if (masterFd >= 0) {
        addToPoll(masterFd);
        masterLinkRegistered_ = true;
    }
}

void RedisServer::addToPoll(int fd) {
    struct epoll_event event;
    event.events = EPOLLIN;
    event.data.fd = fd;
    epoll_ctl(epollFd_, EPOLL_CTL_ADD, fd, &event);
}

void RedisServer::acceptConnection() {
    struct sockaddr_in peer;
    socklen_t length = sizeof(peer);
    const int fd = ::accept(listenFd_, reinterpret_cast<struct sockaddr*>(&peer), &length);
    if (fd < 0) return;

    fcntl(fd, F_SETFL, O_NONBLOCK);
    int nodelay = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

    auto session = std::make_unique<ClientSession>(fd);
    // With no requirepass and no user passwords there is nothing to prove, so
    // the connection starts out authenticated.
    if (!auth_->enforcementEnabled()) session->setAuthenticated();

    struct epoll_event event;
    event.events = EPOLLIN;
    event.data.fd = fd;
    if (epoll_ctl(epollFd_, EPOLL_CTL_ADD, fd, &event) != 0) {
        ::close(fd);
        return;
    }
    clients_[fd] = std::move(session);
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

void RedisServer::handleReadable(int fd) {
    if (fd == listenFd_) {
        acceptConnection();
        return;
    }

    // The replication socket carries a command stream in one direction only:
    // the replica reads its master's full-resync and command stream from it,
    // while the master reads its replicas' PING / REPLCONF / PSYNC from it.
    // Routing both ends through the replica-side parser meant a master never
    // answered a replica's PSYNC, so no replica never finished its handshake.
    //
    // This is decided before any read, so that the link's death is handled
    // here and cannot fall through to the generic client path.
    if (config_.isReplica && replication_.isMasterLink(fd)) {
        char buffer[16384];
        const ssize_t n = ::read(fd, buffer, sizeof(buffer));
        if (n > 0) {
            replication_.feedMaster(std::string(buffer, static_cast<size_t>(n)));
            return;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) return;
        epoll_ctl(epollFd_, EPOLL_CTL_DEL, fd, nullptr);
        replication_.masterLinkClosed("the master closed the connection");
        masterLinkRegistered_ = false;
        if (masterSession_) masterSession_.reset();
        std::cerr << "Warning: lost the master link, will resync" << std::endl;
        return;
    }

    char buffer[16384];
    const ssize_t n = ::read(fd, buffer, sizeof(buffer));
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) return;
        closeConnection(fd);
        return;
    }
    if (n == 0) {
        closeConnection(fd);
        return;
    }

    auto it = clients_.find(fd);
    if (it == clients_.end()) return;
    it->second->inputBuffer() += std::string(buffer, static_cast<size_t>(n));
    processInput(*it->second);
}

void RedisServer::processInput(ClientSession& session) {
    while (!stopping_) {
        std::string& buffer = session.inputBuffer();
        if (buffer.empty()) return;

        resp::ParseResult parsed = resp::parseCommand(buffer);
        if (parsed.fatal) {
            // Nothing sensible can follow a malformed frame, and guessing where
            // the next one starts would be worse than hanging up.
            io::sendAll(session.fd(), resp::error("ERR Protocol error"));
            closeConnection(session.fd());
            return;
        }
        if (!parsed.ok) return;  // an incomplete command; wait for more bytes
        if (parsed.args.empty()) {
            buffer.erase(0, parsed.consumed);
            continue;
        }

        const std::vector<std::string> args = std::move(parsed.args);
        buffer.erase(0, parsed.consumed);
        const std::string name = strutil::toUpper(args[0]);

        const std::string reply = execute(args, session);
        if (!reply.empty()) io::sendAll(session.fd(), reply);

        if (name == "QUIT") {
            closeConnection(session.fd());
            return;
        }
        if (name == "SHUTDOWN") {
            if (args.size() == 1 || strutil::toUpper(args[1]) != "NOSAVE") {
                std::string saveError;
                rdb_.save(store_, saveError);
                if (!saveError.empty()) std::cerr << "Warning: " << saveError << std::endl;
            }
            if (aof_) aof_->close();
            stopping_ = true;
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

std::string RedisServer::execute(const std::vector<std::string>& args, ClientSession& session) {
    if (args.empty()) return resp::error("ERR empty command");

    const std::string name = strutil::toUpper(args[0]);
    const auto it = registry_.find(name);
    if (it == registry_.end()) {
        return resp::error("ERR unknown command '" + name + "'");
    }

    // "fromMaster" means the command arrived over the replication link in
    // either direction, so the same exemptions apply to both ends.
    const bool fromMaster = session.isReplicationLink();

    // Auth, except for the handful of commands a client must be able to send
    // before it can authenticate.
    if (!fromMaster && !session.authenticated() && auth_->enforcementEnabled() && name != "AUTH" &&
        name != "QUIT" && name != "HELLO" && name != "RESET") {
        return resp::error("NOAUTH Authentication required.");
    }

    if (session.subscribed() && !fromMaster && !allowedWhileSubscribed(name)) {
        return resp::array({resp::error("ERR Can't execute '" + strutil::toLower(name) +
                                        "': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / "
                                        "QUIT / RESET are allowed in this context")});
    }

    if (session.inMulti() && !fromMaster && nonQueuableCommands().count(name) == 0) {
        if (!allowedInTransaction(name)) {
            return resp::error("ERR " + name + " is not allowed in transactions");
        }
        session.queue(args);
        return resp::simpleString("QUEUED");
    }

    // A replica is read-only, as in real Redis. Applied commands from the
    // master skip this.
    if (config_.isReplica && !fromMaster && isWriteCommand(name)) {
        return resp::error("READONLY You can't write against a read only replica.");
    }

    CommandContext ctx;
    ctx.services = &services_;
    ctx.client = &session;
    ctx.args = args;

    std::string reply = it->second(ctx);

    // A handler that deferred its answer (BLPOP, XREAD BLOCK, PSYNC) owns the
    // reply slot. Writing an empty frame here would truncate the client's
    // stream, so stop here.
    if (reply.empty()) return reply;

    // Never answer a replica. Only the master speaks on that socket, and a
    // "+OK" in the middle of a replication stream is a frame the replica's
    // offset would then count, so the two offsets would drift apart by
    // whatever the reply happened to be.
    if (session.isReplicaLink()) return "";

    if (!fromMaster && isWriteCommand(name)) {
        if (aof_) aof_->append(args);
        if (!config_.isReplica) replication_.propagate(args);
    }

    return reply;
}

// ---------------------------------------------------------------------------
// Teardown and housekeeping
// ---------------------------------------------------------------------------

void RedisServer::closeConnection(int fd) {
    auto it = clients_.find(fd);
    if (it != clients_.end()) {
        blocked_.removeClient(it->second.get());
        pubsub_.removeClient(it->second.get());
        if (replication_.isKnownReplica(fd)) {
            replication_.removeReplica(fd);
            std::cerr << "Replica " << fd << " disconnected" << std::endl;
        }
        clients_.erase(it);
    }
    epoll_ctl(epollFd_, EPOLL_CTL_DEL, fd, nullptr);
    ::close(fd);
}

void RedisServer::tick() {
    blocked_.expireTimedOut();

    // A handshake that failed mid-flight reports itself once, rather than
    // leaving `master_link_status:down` with nothing to say why.
    if (config_.isReplica && !linkErrorReported_ && !replication_.lastError().empty()) {
        std::cerr << "Warning: replication link: " << replication_.lastError() << std::endl;
        linkErrorReported_ = true;
    }

    const std::vector<std::string> expired = store_.collectExpired();
    if (!expired.empty()) store_.removeKeys(expired);

    if (!config_.isReplica || masterLinkRegistered_) return;

    // A dropped master link is retried, but not in a tight loop: connect() on a
    // dead port fails instantly and would otherwise spin the CPU.
    if (timeutil::steadyMs() - lastResyncAttemptMs_ < 2000) return;
    lastResyncAttemptMs_ = timeutil::steadyMs();

    if (replication_.resyncWithMaster()) {
        ensureMasterSession();
        addToPoll(replication_.masterFd());
        masterLinkRegistered_ = true;
        std::cout << "Reconnected to master " << config_.masterHost << ":" << config_.masterPort
                  << std::endl;
    }
}

}  // namespace redis
