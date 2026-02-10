/**
 * =============================================================================
 *                              REDIS SERVER
 * =============================================================================
 * 
 * Paaji eh file hai main server di - boss of all!
 * (Bro this is the main server file - the big boss!)
 * 
 * Server class jo sab kuch manage kardi hai:
 * - Client connections
 * - Command execution
 * - Replication
 * - Persistence
 * 
 * (Server class that manages everything)
 * 
 * Jiven dhaba da malik - sab kuch supervise karda
 * (Like a restaurant owner - supervises everything)
 * 
 * =============================================================================
 */

#pragma once

#include "common.hpp"
#include "resp_parser.hpp"
#include "data_store.hpp"
#include "client.hpp"
#include "commands.hpp"
#include "replication.hpp"
#include "authentication.hpp"
#include "rdb_persistence.hpp"
#include "geospatial.hpp"

#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>

namespace Redis {

// ============================================================================
// SERVER CONFIGURATION
// Server di settings - port, replication, etc.
// (Server settings - port, replication config, etc.)
// ============================================================================

/**
 * @struct ServerConfig
 * @brief Server configuration options
 * 
 * Server ki sari settings - ek jagah
 * (All server settings in one place)
 */
struct ServerConfig {
    int port = 6379;                      // Port to listen on
    std::string bindAddress = "0.0.0.0";  // Address to bind to
    
    // Replication settings
    // Replication settings - master/replica
    // (Replication settings)
    bool isReplica = false;
    std::string masterHost;
    int masterPort = 0;
    
    // RDB settings
    // RDB settings - persistence
    // (RDB persistence settings)
    std::string rdbDir = ".";
    std::string rdbFilename = "dump.rdb";
    
    // Auth settings
    // Auth settings - password
    // (Authentication settings)
    std::string requirepass;
    
    // Timeout settings
    // Timeout settings - client idle time
    // (Client timeout settings)
    int clientTimeout = 0;  // 0 = no timeout
    
    // Max clients
    int maxClients = 10000;
};

// ============================================================================
// REDIS SERVER
// Main server class - sab kuch handle
// (Main server class - handles everything)
// ============================================================================

/**
 * @class RedisServer
 * @brief Main Redis server implementation
 * 
 * Pura Redis server - networking, commands, replication sab
 * (Complete Redis server - networking, commands, replication, everything)
 * 
 * Thread-safe hai - multiple clients handle kar sakda
 * (Thread-safe - can handle multiple clients)
 */
class RedisServer {
private:
    ServerConfig config_;
    
    // Core components
    // Main components - server di jaan
    // (Main components - heart of the server)
    DataStore store_;
    ClientManager clientManager_;
    AuthManager authManager_;
    ReplicationManager replManager_;
    RdbManager rdbManager_;
    GeoCommands geoCommands_;
    
    // Command handler
    // Command handler - commands execute karta
    // (Command handler - executes commands)
    std::unique_ptr<CommandHandler> cmdHandler_;
    
    // Networking
    // Networking stuff - sockets te epoll
    // (Networking - sockets and epoll)
    int serverFd_ = -1;
    int epollFd_ = -1;
    bool running_ = false;
    
    // Blocking commands state
    // Blocking commands lai state
    // (State for blocking commands like BLPOP, BRPOP)
    std::mutex blockingMutex_;
    std::map<int, std::chrono::steady_clock::time_point> blockingTimeouts_;
    
public:
    /**
     * Constructor
     * Server initialize karo
     * (Initialize server with config)
     */
    RedisServer(const ServerConfig& config = ServerConfig())
        : config_(config)
        , replManager_(config.isReplica ? ReplicationRole::REPLICA : ReplicationRole::MASTER)
        , rdbManager_(config.rdbDir, config.rdbFilename)
        , geoCommands_(store_) {
        
        // Set auth password if configured
        // Password set karo agar configure kita hai
        // (Set password if configured)
        if (!config_.requirepass.empty()) {
            authManager_.setLegacyPassword(config_.requirepass);
        }
        
        // Create command handler
        // Command handler banao
        // (Create command handler)
        cmdHandler_ = std::make_unique<CommandHandler>(store_, replManager_, rdbManager_, authManager_);
    }
    
    /**
     * Destructor
     * Cleanup karo - sockets band karo
     * (Cleanup - close sockets)
     */
    ~RedisServer() {
        stop();
    }
    
    // ========================================================================
    // SERVER LIFECYCLE
    // Server start/stop
    // (Server lifecycle management)
    // ========================================================================
    
    /**
     * Initialize and start the server
     * Server initialize te start karo
     * (Initialize and start the server)
     */
    bool start() {
        // Load RDB if exists
        // RDB load karo agar hai
        // (Load RDB file if exists)
        std::cout << "Loading RDB file..." << std::endl;
        rdbManager_.load(store_);
        
        // Create server socket
        // Server socket banao
        // (Create server socket)
        if (!createServerSocket()) {
            std::cerr << "Oho! Socket nahi bana paaya - ki masla hai?"
                      << std::endl
                      << "(Oh no! Couldn't create socket - what's the problem?)"
                      << std::endl;
            return false;
        }
        
        // Create epoll instance
        // Epoll instance banao - efficient event handling
        // (Create epoll instance for efficient event handling)
        if (!createEpoll()) {
            std::cerr << "Epoll bhi nahi bana - Linux system hai ki nahi?"
                      << std::endl
                      << "(Epoll creation failed too - is this even Linux?)"
                      << std::endl;
            return false;
        }
        
        // If replica, connect to master
        // Agar replica hai, master naal connect karo
        // (If replica, connect to master)
        if (config_.isReplica) {
            std::cout << "Connecting to master at " << config_.masterHost 
                      << ":" << config_.masterPort << std::endl;
            replManager_.connectToMaster(config_.masterHost, config_.masterPort);
        }
        
        running_ = true;
        
        std::cout << "=====================================" << std::endl;
        std::cout << "  REDIS SERVER CHALU HO GAYA!!" << std::endl;
        std::cout << "  (Redis server is now running!)" << std::endl;
        std::cout << "=====================================" << std::endl;
        std::cout << "Port: " << config_.port << std::endl;
        std::cout << "Role: " << (config_.isReplica ? "REPLICA" : "MASTER") << std::endl;
        std::cout << "=====================================" << std::endl;
        
        // Run event loop
        // Event loop chalaao
        // (Run the event loop)
        eventLoop();
        
        return true;
    }
    
    /**
     * Stop the server
     * Server band karo
     * (Stop the server)
     */
    void stop() {
        running_ = false;
        
        if (epollFd_ >= 0) {
            close(epollFd_);
            epollFd_ = -1;
        }
        
        if (serverFd_ >= 0) {
            close(serverFd_);
            serverFd_ = -1;
        }
        
        std::cout << "Server band ho gaya - bye bye!"
                  << std::endl
                  << "(Server stopped - goodbye!)"
                  << std::endl;
    }

private:
    // ========================================================================
    // SOCKET MANAGEMENT
    // Socket setup - networking stuff
    // (Socket management for networking)
    // ========================================================================
    
    /**
     * Create and bind server socket
     * Server socket banao te bind karo
     * (Create and bind server socket)
     */
    bool createServerSocket() {
        // Create socket
        // Socket banao - AF_INET for IPv4
        // (Create socket - AF_INET for IPv4)
        serverFd_ = socket(AF_INET, SOCK_STREAM, 0);
        if (serverFd_ < 0) {
            perror("socket");
            return false;
        }
        
        // Allow address reuse
        // Address reuse allow karo - SO_REUSEADDR
        // (Allow address reuse - helpful for quick restarts)
        int opt = 1;
        if (setsockopt(serverFd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
            perror("setsockopt");
            return false;
        }
        
        // Make socket non-blocking
        // Socket non-blocking banao
        // (Make socket non-blocking for epoll)
        if (!setNonBlocking(serverFd_)) {
            return false;
        }
        
        // Bind to address
        // Address te bind karo
        // (Bind to address and port)
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(config_.port);
        
        if (bind(serverFd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            perror("bind");
            std::cerr << "Port " << config_.port << " te bind nahi ho raha - shayad koi hor use kar raha?"
                      << std::endl
                      << "(Cannot bind to port " << config_.port << " - maybe someone else is using it?)"
                      << std::endl;
            return false;
        }
        
        // Start listening
        // Listen shuru karo
        // (Start listening for connections)
        if (listen(serverFd_, SOMAXCONN) < 0) {
            perror("listen");
            return false;
        }
        
        return true;
    }
    
    /**
     * Create epoll instance
     * Epoll instance banao
     * (Create epoll instance for event-driven I/O)
     */
    bool createEpoll() {
        epollFd_ = epoll_create1(0);
        if (epollFd_ < 0) {
            perror("epoll_create1");
            return false;
        }
        
        // Add server socket to epoll
        // Server socket epoll mein add karo
        // (Add server socket to epoll for accepting connections)
        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = serverFd_;
        
        if (epoll_ctl(epollFd_, EPOLL_CTL_ADD, serverFd_, &ev) < 0) {
            perror("epoll_ctl");
            return false;
        }
        
        return true;
    }
    
    /**
     * Set socket to non-blocking mode
     * Socket non-blocking banao
     * (Set socket to non-blocking mode)
     */
    bool setNonBlocking(int fd) {
        int flags = fcntl(fd, F_GETFL, 0);
        if (flags < 0) {
            perror("fcntl F_GETFL");
            return false;
        }
        
        if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
            perror("fcntl F_SETFL");
            return false;
        }
        
        return true;
    }
    
    // ========================================================================
    // EVENT LOOP
    // Main event loop - sab events handle
    // (Main event loop - handle all events)
    // ========================================================================
    
    /**
     * Main event loop
     * Main event loop - blocking operations, client I/O
     * (Main event loop - handles all I/O events)
     */
    void eventLoop() {
        const int MAX_EVENTS = 64;
        epoll_event events[MAX_EVENTS];
        
        while (running_) {
            // Wait for events with timeout
            // Events da wait karo - 100ms timeout for blocking commands
            // (Wait for events with 100ms timeout for blocking commands)
            int nfds = epoll_wait(epollFd_, events, MAX_EVENTS, 100);
            
            if (nfds < 0) {
                if (errno == EINTR) {
                    continue;  // Interrupted by signal
                }
                perror("epoll_wait");
                break;
            }
            
            // Process events
            // Events process karo
            // (Process each event)
            for (int i = 0; i < nfds; i++) {
                if (events[i].data.fd == serverFd_) {
                    // New connection
                    // Naya client aa gaya
                    // (New client connection)
                    acceptClient();
                } else {
                    // Client data
                    // Client da data aa gaya
                    // (Client sent data)
                    handleClient(events[i].data.fd, events[i].events);
                }
            }
            
            // Check blocking command timeouts
            // Blocking commands da timeout check karo
            // (Check if any blocking commands have timed out)
            checkBlockingTimeouts();
        }
    }
    
    /**
     * Accept new client connection
     * Naye client nu accept karo
     * (Accept new client connection)
     */
    void acceptClient() {
        sockaddr_in clientAddr{};
        socklen_t clientLen = sizeof(clientAddr);
        
        while (true) {
            int clientFd = accept(serverFd_, 
                                  reinterpret_cast<sockaddr*>(&clientAddr), 
                                  &clientLen);
            
            if (clientFd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    // No more clients to accept
                    break;
                }
                perror("accept");
                break;
            }
            
            // Check max clients
            // Max clients check karo
            // (Check if we've hit max clients limit)
            if (clientManager_.getClientCount() >= config_.maxClients) {
                std::string err = respError("ERR max number of clients reached");
                write(clientFd, err.c_str(), err.length());
                close(clientFd);
                continue;
            }
            
            // Make client socket non-blocking
            // Client socket non-blocking banao
            // (Make client socket non-blocking)
            if (!setNonBlocking(clientFd)) {
                close(clientFd);
                continue;
            }
            
            // Add to epoll
            // Epoll mein add karo
            // (Add client to epoll)
            epoll_event ev{};
            ev.events = EPOLLIN | EPOLLET;  // Edge-triggered
            ev.data.fd = clientFd;
            
            if (epoll_ctl(epollFd_, EPOLL_CTL_ADD, clientFd, &ev) < 0) {
                perror("epoll_ctl client");
                close(clientFd);
                continue;
            }
            
            // Create client connection object
            // Client object banao
            // (Create client connection object)
            std::string clientIp = inet_ntoa(clientAddr.sin_addr);
            int clientPort = ntohs(clientAddr.sin_port);
            
            auto client = clientManager_.createClient(clientFd);
            client->setAddress(clientIp, clientPort);
            
            // If auth not required, mark as authenticated with default user
            // Agar auth nahi chahiye, default user set karo
            // (If auth not required, set as authenticated)
            if (!authManager_.isAuthRequired()) {
                client->setAuthenticated(true, "default");
            }
            
            std::cout << "Naya client aa gaya! FD: " << clientFd 
                      << " IP: " << clientIp << ":" << clientPort
                      << std::endl
                      << "(New client connected!)"
                      << std::endl;
        }
    }
    
    /**
     * Handle client data
     * Client da data handle karo
     * (Handle client data - read, parse, execute)
     */
    void handleClient(int fd, uint32_t events) {
        auto client = clientManager_.getClient(fd);
        if (!client) {
            // Client not found, remove from epoll
            epoll_ctl(epollFd_, EPOLL_CTL_DEL, fd, nullptr);
            close(fd);
            return;
        }
        
        // Check for errors or hangup
        // Error ya disconnect check karo
        // (Check for errors or client disconnect)
        if (events & (EPOLLERR | EPOLLHUP)) {
            disconnectClient(fd);
            return;
        }
        
        // Read available data
        // Data read karo
        // (Read available data from client)
        if (events & EPOLLIN) {
            if (!readClientData(client)) {
                disconnectClient(fd);
                return;
            }
            
            // Process commands
            // Commands process karo
            // (Process any complete commands)
            processClientCommands(client);
        }
    }
    
    /**
     * Read data from client
     * Client se data read karo
     * (Read data from client socket)
     */
    bool readClientData(std::shared_ptr<ClientConnection> client) {
        char buffer[4096];
        
        while (true) {
            ssize_t n = read(client->getFd(), buffer, sizeof(buffer));
            
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    // No more data available
                    break;
                }
                perror("read");
                return false;
            }
            
            if (n == 0) {
                // Client disconnected
                // Client chala gaya
                // (Client disconnected)
                return false;
            }
            
            // Append to client buffer
            // Client buffer mein add karo
            // (Append to client's input buffer)
            client->appendInput(std::string(buffer, n));
        }
        
        return true;
    }
    
    /**
     * Process client commands
     * Client de commands process karo
     * (Process commands from client's input buffer)
     */
    void processClientCommands(std::shared_ptr<ClientConnection> client) {
        RespParser parser;
        
        while (true) {
            // Try to parse a command from buffer
            // Buffer se command parse karo
            // (Try to parse a command from buffer)
            size_t consumed = 0;
            auto cmd = parser.parseCommand(client->getInputBuffer(), consumed);
            
            if (cmd.empty()) {
                // Not enough data for complete command
                // Pura command nahi aaya abhi
                // (Not enough data for complete command yet)
                break;
            }
            
            // Remove consumed data from buffer
            // Consumed data buffer se hatao
            // (Remove consumed data from buffer)
            client->consumeInput(consumed);
            
            // Execute command
            // Command execute karo
            // (Execute the command)
            std::string response = executeCommand(client, cmd);
            
            // Send response (if not a blocking command waiting)
            // Response bhejo (agar blocking nahi hai)
            // (Send response if not a blocking command)
            if (!response.empty()) {
                sendResponse(client->getFd(), response);
            }
        }
    }
    
    /**
     * Execute a command
     * Command execute karo
     * (Execute a Redis command)
     */
    std::string executeCommand(std::shared_ptr<ClientConnection> client, 
                               const std::vector<std::string>& args) {
        if (args.empty()) {
            return respError("ERR empty command");
        }
        
        std::string cmd = toUpper(args[0]);
        
        // Check authentication
        // Authentication check karo
        // (Check if client is authenticated)
        if (authManager_.isAuthRequired() && !client->isAuthenticated()) {
            // Only AUTH and QUIT allowed without auth
            if (cmd != "AUTH" && cmd != "QUIT") {
                return respError("NOAUTH Authentication required");
            }
        }
        
        // Handle transaction commands specially
        // Transaction commands special handling
        // (Handle MULTI/EXEC/DISCARD specially)
        if (client->isInTransaction() && cmd != "EXEC" && cmd != "DISCARD" && 
            cmd != "MULTI" && cmd != "WATCH") {
            // Queue command
            // Command queue karo
            // (Queue command for later execution)
            client->queueCommand(args);
            return "+QUEUED\r\n";
        }
        
        // Execute command
        // Command execute karo
        // (Actually execute the command)
        std::string response = cmdHandler_->execute(client, args);
        
        // Propagate write commands to replicas if master
        // Master hai toh replicas nu bhejo
        // (If master, propagate write commands to replicas)
        if (!config_.isReplica && isWriteCommand(cmd)) {
            propagateToReplicas(args);
        }
        
        return response;
    }
    
    /**
     * Send response to client
     * Client nu response bhejo
     * (Send response to client)
     */
    void sendResponse(int fd, const std::string& response) {
        size_t totalSent = 0;
        size_t remaining = response.size();
        
        while (remaining > 0) {
            ssize_t sent = write(fd, response.c_str() + totalSent, remaining);
            
            if (sent < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    // Would block, try again
                    // Busy hai, dubara try karo
                    // (Socket busy, try again)
                    continue;
                }
                perror("write");
                return;
            }
            
            totalSent += sent;
            remaining -= sent;
        }
    }
    
    /**
     * Disconnect client
     * Client nu disconnect karo
     * (Disconnect and cleanup client)
     */
    void disconnectClient(int fd) {
        auto client = clientManager_.getClient(fd);
        if (client) {
            std::cout << "Client disconnect ho gaya: FD " << fd
                      << std::endl
                      << "(Client disconnected)"
                      << std::endl;
            
            // Unsubscribe from all channels
            // Saare channels se unsubscribe karo
            // (Unsubscribe from all pub/sub channels)
            for (const auto& channel : client->getSubscriptions()) {
                store_.unsubscribe(channel, fd);
            }
        }
        
        // Remove from epoll and close
        // Epoll te client manager se hatao
        // (Remove from epoll and close socket)
        epoll_ctl(epollFd_, EPOLL_CTL_DEL, fd, nullptr);
        close(fd);
        clientManager_.removeClient(fd);
    }
    
    // ========================================================================
    // BLOCKING COMMANDS
    // Blocking commands handle - BLPOP, BRPOP, BLMOVE, XREAD
    // (Handle blocking commands)
    // ========================================================================
    
    /**
     * Check blocking command timeouts
     * Blocking commands da timeout check karo
     * (Check and handle blocking command timeouts)
     */
    void checkBlockingTimeouts() {
        std::lock_guard<std::mutex> lock(blockingMutex_);
        
        auto now = std::chrono::steady_clock::now();
        std::vector<int> timedOut;
        
        for (auto& [fd, timeout] : blockingTimeouts_) {
            if (now >= timeout) {
                timedOut.push_back(fd);
            }
        }
        
        for (int fd : timedOut) {
            blockingTimeouts_.erase(fd);
            
            auto client = clientManager_.getClient(fd);
            if (client && client->isBlocking()) {
                // Send null response for timeout
                // Timeout ho gaya - null bhejo
                // (Timeout - send null response)
                client->setBlocking(false);
                sendResponse(fd, respNull());
            }
        }
    }
    
    /**
     * Register blocking command
     * Blocking command register karo timeout lai
     * (Register blocking command with timeout)
     */
    void registerBlockingCommand(int fd, double timeoutSeconds) {
        std::lock_guard<std::mutex> lock(blockingMutex_);
        
        auto timeout = std::chrono::steady_clock::now() + 
                       std::chrono::milliseconds(static_cast<int64_t>(timeoutSeconds * 1000));
        blockingTimeouts_[fd] = timeout;
    }
    
    // ========================================================================
    // REPLICATION
    // Replication handle - master/replica sync
    // (Handle replication)
    // ========================================================================
    
    /**
     * Check if command is a write command
     * Command write hai ki nahi
     * (Is this a write command that needs replication?)
     */
    bool isWriteCommand(const std::string& cmd) {
        static const std::unordered_set<std::string> writeCommands = {
            "SET", "MSET", "DEL", "INCR", "INCRBY", "DECR", "DECRBY",
            "LPUSH", "RPUSH", "LPOP", "RPOP", "LSET", "LREM",
            "SADD", "SREM",
            "ZADD", "ZREM", "ZINCRBY",
            "HSET", "HDEL", "HINCRBY",
            "XADD",
            "EXPIRE", "EXPIREAT", "PEXPIRE", "PEXPIREAT",
            "FLUSHDB", "FLUSHALL",
            "GEOADD"
        };
        
        return writeCommands.count(cmd) > 0;
    }
    
    /**
     * Propagate command to replicas
     * Replicas nu command bhejo
     * (Propagate command to all replicas)
     */
    void propagateToReplicas(const std::vector<std::string>& args) {
        std::string respCmd = RespParser::encodeArray(args);
        replManager_.propagateCommand(respCmd);
    }

public:
    // ========================================================================
    // PUBLIC ACCESSORS
    // Public methods - stats, info, etc.
    // (Public methods for getting server info)
    // ========================================================================
    
    /**
     * Get server info
     * Server di info lo
     * (Get server information)
     */
    std::string getInfo() const {
        std::ostringstream ss;
        
        // Server section
        ss << "# Server\r\n";
        ss << "redis_version:7.0.0\r\n";
        ss << "redis_mode:standalone\r\n";
        ss << "tcp_port:" << config_.port << "\r\n";
        
        // Replication section
        ss << "\r\n# Replication\r\n";
        ss << "role:" << (config_.isReplica ? "slave" : "master") << "\r\n";
        ss << "master_replid:" << replManager_.getState().replId << "\r\n";
        ss << "master_repl_offset:" << replManager_.getState().replOffset << "\r\n";
        
        // Clients section
        ss << "\r\n# Clients\r\n";
        ss << "connected_clients:" << clientManager_.getClientCount() << "\r\n";
        
        // Stats section
        ss << "\r\n# Stats\r\n";
        // Would track these in production
        ss << "total_connections_received:0\r\n";
        ss << "total_commands_processed:0\r\n";
        
        // Keyspace section
        ss << "\r\n# Keyspace\r\n";
        ss << "db0:keys=" << store_.keys("*").size() << ",expires=0\r\n";
        
        return ss.str();
    }
    
    /**
     * Get replication manager
     * Replication manager lo
     * (Get replication manager reference)
     */
    ReplicationManager& getReplicationManager() {
        return replManager_;
    }
    
    /**
     * Get data store
     * Data store lo
     * (Get data store reference)
     */
    DataStore& getDataStore() {
        return store_;
    }
    
    /**
     * Get server config
     * Config lo
     * (Get server configuration)
     */
    const ServerConfig& getConfig() const {
        return config_;
    }
};

} // namespace Redis
