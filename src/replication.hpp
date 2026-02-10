/**
 * =============================================================================
 *                          REDIS REPLICATION
 * =============================================================================
 * 
 * Paaji eh file hai replication logic di - master te replica sync!
 * (Bro this is the replication logic - master and replica sync!)
 * 
 * Jiven Instagram pe story share hundi hai sabko, ohi replication hai
 * (Just like Instagram story is shared to everyone, that's replication)
 * 
 * Features:
 * - Master/Replica configuration
 * - Handshake protocol
 * - RDB transfer
 * - Command propagation
 * - WAIT command support
 * 
 * =============================================================================
 */

#pragma once

#include "common.hpp"
#include "resp_parser.hpp"
#include "data_store.hpp"

namespace Redis {

// Forward declaration
class RedisServer;

// ============================================================================
// REPLICATION STATE
// Replication di state - kya ho raha hai sync mein
// (Replication state - what's happening in sync)
// ============================================================================

/**
 * @struct ReplicationState
 * @brief Holds replication-related state for the server
 * 
 * Master ya replica - role te state
 * (Master or replica - role and state)
 */
struct ReplicationState {
    // Role - master ya replica
    // (Role - am I the boss or a follower)
    ReplicationRole role = ReplicationRole::MASTER;
    
    // Master info (when this server is a replica)
    // Jab ye replica hai toh master ka info
    // (When this is a replica, master's info)
    std::string masterHost;
    int masterPort = 0;
    int masterFd = -1;                  // Socket to master
    bool masterConnected = false;
    
    // Replication ID and offset
    // Unique ID te progress - kahaan tak sync hua
    // (Unique ID and progress - how far synced)
    std::string replId;                 // 40 char hex string
    std::string replId2 = "0000000000000000000000000000000000000000";  // Secondary ID
    int64_t replOffset = 0;             // Our current offset
    int64_t masterReplOffset = 0;       // Master's offset (when we're master)
    
    // Handshake state (when this server is a replica)
    // Handshake ki stage - connection setup
    // (Handshake stage - connection setup progress)
    enum class HandshakeState {
        NONE,
        PING_SENT,
        REPLCONF_PORT_SENT,
        REPLCONF_CAPA_SENT,
        PSYNC_SENT,
        CONNECTED
    };
    HandshakeState handshakeState = HandshakeState::NONE;
    
    // Replica list (when this server is master)
    // Replicas ki list - kitne followers hain
    // (List of replicas - how many followers)
    std::vector<int> replicaClientIds;
    
    // Propagation buffer
    // Commands jo replicas ko bhejna hai
    // (Commands to send to replicas)
    std::string propagationBuffer;
    
    // ACK tracking
    // ACK tracking - confirmation tracking
    // (Tracking confirmations from replicas)
    std::atomic<int64_t> ackedReplicas{0};
    
    /**
     * Constructor - initialize replication state
     * Shuru ch setup karo
     * (Initial setup)
     */
    ReplicationState() {
        // Generate random replication ID
        // Random replication ID banao - unique identifier
        // (Generate random replication ID - unique identifier)
        replId = generateReplId();
    }
    
    /**
     * Check if this server is a master
     * Master hai ki nahi
     * (Am I the boss?)
     */
    bool isMaster() const {
        return role == ReplicationRole::MASTER;
    }
    
    /**
     * Check if this server is a replica
     * Replica hai ki nahi
     * (Am I a follower?)
     */
    bool isReplica() const {
        return role == ReplicationRole::REPLICA;
    }
    
    /**
     * Get INFO replication section
     * INFO command lai replication section
     * (Replication section for INFO command)
     */
    std::string getInfoString() const {
        std::ostringstream ss;
        
        ss << "# Replication\r\n";
        ss << "role:" << (isMaster() ? "master" : "slave") << "\r\n";
        
        if (isMaster()) {
            ss << "connected_slaves:" << replicaClientIds.size() << "\r\n";
            ss << "master_replid:" << replId << "\r\n";
            ss << "master_repl_offset:" << masterReplOffset << "\r\n";
        } else {
            ss << "master_host:" << masterHost << "\r\n";
            ss << "master_port:" << masterPort << "\r\n";
            ss << "master_link_status:" << (masterConnected ? "up" : "down") << "\r\n";
            ss << "master_replid:" << replId << "\r\n";
            ss << "master_repl_offset:" << replOffset << "\r\n";
        }
        
        ss << "second_repl_offset:-1\r\n";
        ss << "repl_backlog_active:0\r\n";
        ss << "repl_backlog_size:1048576\r\n";
        ss << "repl_backlog_first_byte_offset:0\r\n";
        ss << "repl_backlog_histlen:0\r\n";
        
        return ss.str();
    }
};

// ============================================================================
// REPLICATION MANAGER
// Replication manage karna - sync ka kaam
// (Managing replication - sync work)
// ============================================================================

/**
 * @class ReplicationManager
 * @brief Manages replication between master and replicas
 * 
 * Eh class replication handle karti hai:
 * (This class handles replication:)
 * - Connecting to master (as replica)
 * - Accepting replica connections (as master)
 * - Handshake protocol
 * - Command propagation
 * - ACK handling
 */
class ReplicationManager {
private:
    ReplicationState& state_;
    RedisServer* server_;
    DataStore& store_;
    
    // Input buffer for master connection (when replica)
    std::string masterInputBuffer_;

public:
    /**
     * Constructor
     */
    ReplicationManager(ReplicationState& state, DataStore& store, RedisServer* server = nullptr)
        : state_(state), store_(store), server_(server) {}
    
    /**
     * Set server reference
     */
    void setServer(RedisServer* server) {
        server_ = server;
    }
    
    // ========================================================================
    // REPLICA OPERATIONS - Jab ye server replica hai
    // (When this server is a replica)
    // ========================================================================
    
    /**
     * Configure as replica of master
     * Replica mode ch jao - follow the leader
     * (Configure as replica - follow the leader)
     */
    bool configureAsReplica(const std::string& masterHost, int masterPort) {
        state_.role = ReplicationRole::REPLICA;
        state_.masterHost = masterHost;
        state_.masterPort = masterPort;
        state_.masterConnected = false;
        state_.handshakeState = ReplicationState::HandshakeState::NONE;
        
        return true;
    }
    
    /**
     * Connect to master
     * Master se connect karo - relationship shuru
     * (Connect to master - start the relationship)
     */
    bool connectToMaster() {
        if (!state_.isReplica()) return false;
        
        // Create socket
        // Socket banao - phone call ki tarah
        // (Create socket - like making a phone call)
        int sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) {
            std::cerr << "Error creating socket for master connection\n";
            return false;
        }
        
        // Resolve master address
        // Master ka address resolve karo
        // (Resolve master's address - find the leader)
        struct sockaddr_in masterAddr;
        masterAddr.sin_family = AF_INET;
        masterAddr.sin_port = htons(state_.masterPort);
        
        if (inet_pton(AF_INET, state_.masterHost.c_str(), &masterAddr.sin_addr) <= 0) {
            // Try hostname resolution
            struct hostent* he = gethostbyname(state_.masterHost.c_str());
            if (he == nullptr) {
                close(sockfd);
                return false;
            }
            memcpy(&masterAddr.sin_addr, he->h_addr_list[0], he->h_length);
        }
        
        // Connect
        // Connect karo - ring ring
        // (Connect - ring ring, hello master!)
        if (connect(sockfd, (struct sockaddr*)&masterAddr, sizeof(masterAddr)) < 0) {
            std::cerr << "Error connecting to master at " << state_.masterHost 
                      << ":" << state_.masterPort << "\n";
            close(sockfd);
            return false;
        }
        
        state_.masterFd = sockfd;
        state_.masterConnected = true;
        
        return true;
    }
    
    /**
     * Start handshake with master
     * Handshake shuru karo - introduction time
     * (Start handshake - time to introduce ourselves)
     */
    bool startHandshake(int listeningPort) {
        if (!state_.masterConnected || state_.masterFd < 0) {
            return false;
        }
        
        // Step 1: Send PING
        // Pehle PING bhejo - hello bolna hai
        // (First send PING - say hello)
        std::string ping = "*1\r\n$4\r\nPING\r\n";
        if (send(state_.masterFd, ping.c_str(), ping.length(), 0) < 0) {
            return false;
        }
        
        state_.handshakeState = ReplicationState::HandshakeState::PING_SENT;
        
        // Wait for PONG
        // PONG ka wait karo - reply aana chahiye
        // (Wait for PONG - should get a reply)
        char buffer[1024];
        ssize_t n = recv(state_.masterFd, buffer, sizeof(buffer) - 1, 0);
        if (n <= 0) return false;
        buffer[n] = '\0';
        
        // Step 2: Send REPLCONF listening-port
        // REPLCONF bhejo - apna port bata do
        // (Send REPLCONF - tell them our port)
        std::string replconf1 = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$"
                               + std::to_string(std::to_string(listeningPort).length()) + "\r\n"
                               + std::to_string(listeningPort) + "\r\n";
        if (send(state_.masterFd, replconf1.c_str(), replconf1.length(), 0) < 0) {
            return false;
        }
        
        state_.handshakeState = ReplicationState::HandshakeState::REPLCONF_PORT_SENT;
        
        // Wait for OK
        n = recv(state_.masterFd, buffer, sizeof(buffer) - 1, 0);
        if (n <= 0) return false;
        
        // Step 3: Send REPLCONF capa psync2
        // Capabilities bata do - kya kya kar sakte ho
        // (Tell capabilities - what can we do)
        std::string replconf2 = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
        if (send(state_.masterFd, replconf2.c_str(), replconf2.length(), 0) < 0) {
            return false;
        }
        
        state_.handshakeState = ReplicationState::HandshakeState::REPLCONF_CAPA_SENT;
        
        // Wait for OK
        n = recv(state_.masterFd, buffer, sizeof(buffer) - 1, 0);
        if (n <= 0) return false;
        
        // Step 4: Send PSYNC
        // PSYNC bhejo - data mangna hai
        // (Send PSYNC - request data sync)
        std::string psync = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
        if (send(state_.masterFd, psync.c_str(), psync.length(), 0) < 0) {
            return false;
        }
        
        state_.handshakeState = ReplicationState::HandshakeState::PSYNC_SENT;
        
        // Wait for FULLRESYNC response
        // FULLRESYNC ka wait karo - poora data aayega
        // (Wait for FULLRESYNC - full data coming)
        n = recv(state_.masterFd, buffer, sizeof(buffer) - 1, 0);
        if (n <= 0) return false;
        buffer[n] = '\0';
        
        std::string response(buffer, n);
        
        // Parse FULLRESYNC <replid> <offset>
        if (response.find("+FULLRESYNC") == 0) {
            auto parts = split(response.substr(1), ' ');
            if (parts.size() >= 3) {
                state_.replId = parts[1];
                // Remove any trailing \r\n from offset
                std::string offsetStr = parts[2];
                offsetStr.erase(std::remove(offsetStr.begin(), offsetStr.end(), '\r'), offsetStr.end());
                offsetStr.erase(std::remove(offsetStr.begin(), offsetStr.end(), '\n'), offsetStr.end());
                state_.replOffset = std::stoll(offsetStr);
            }
        }
        
        // Receive RDB file
        // RDB file receive karo - database ka backup
        // (Receive RDB file - database backup)
        if (!receiveRDB()) {
            return false;
        }
        
        state_.handshakeState = ReplicationState::HandshakeState::CONNECTED;
        
        return true;
    }
    
    /**
     * Receive RDB file from master
     * Master se RDB file lo - database ka snapshot
     * (Receive RDB file from master - database snapshot)
     */
    bool receiveRDB() {
        // Read $<length>\r\n prefix
        // Length padho - kitna data aana hai
        // (Read length - how much data is coming)
        char c;
        std::string lengthStr;
        
        // Skip any remaining response data and find $
        while (recv(state_.masterFd, &c, 1, 0) == 1) {
            if (c == '$') break;
        }
        
        // Read length
        while (recv(state_.masterFd, &c, 1, 0) == 1 && c != '\r') {
            lengthStr += c;
        }
        recv(state_.masterFd, &c, 1, 0);  // Skip \n
        
        int rdbLength = std::stoi(lengthStr);
        
        // Read RDB content
        // RDB content padho - actual data
        // (Read RDB content - actual data)
        std::string rdbContent;
        rdbContent.resize(rdbLength);
        
        size_t received = 0;
        while (received < static_cast<size_t>(rdbLength)) {
            ssize_t n = recv(state_.masterFd, &rdbContent[received], rdbLength - received, 0);
            if (n <= 0) break;
            received += n;
        }
        
        // For now, we accept empty RDB (codecrafters sends empty RDB)
        // Abhi ke liye empty RDB accept karo
        // (Accept empty RDB for now - codecrafters sends empty)
        
        return true;
    }
    
    /**
     * Process data from master
     * Master se data process karo - commands execute
     * (Process data from master - execute replicated commands)
     */
    void processFromMaster();
    
    /**
     * Send ACK to master
     * Master ko ACK bhejo - confirmation
     * (Send ACK to master - confirmation sent)
     */
    void sendAckToMaster() {
        if (!state_.isReplica() || state_.masterFd < 0) return;
        
        std::string ack = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$"
                         + std::to_string(std::to_string(state_.replOffset).length()) + "\r\n"
                         + std::to_string(state_.replOffset) + "\r\n";
        
        send(state_.masterFd, ack.c_str(), ack.length(), 0);
    }
    
    // ========================================================================
    // MASTER OPERATIONS - Jab ye server master hai
    // (When this server is master)
    // ========================================================================
    
    /**
     * Register a replica
     * Naya replica register karo - follower add
     * (Register new replica - add a follower)
     */
    void registerReplica(int clientId) {
        state_.replicaClientIds.push_back(clientId);
    }
    
    /**
     * Unregister a replica
     * Replica hatao - follower gaya
     * (Unregister replica - follower left)
     */
    void unregisterReplica(int clientId) {
        auto it = std::find(state_.replicaClientIds.begin(), 
                           state_.replicaClientIds.end(), clientId);
        if (it != state_.replicaClientIds.end()) {
            state_.replicaClientIds.erase(it);
        }
    }
    
    /**
     * Propagate command to all replicas
     * Command saare replicas ko bhejo - broadcast
     * (Propagate command to all replicas - broadcast time)
     */
    void propagateCommand(const StringVector& args);
    
    /**
     * Build RESP array from command
     * Command se RESP array banao
     * (Build RESP array from command)
     */
    std::string buildRespArray(const StringVector& args) {
        std::ostringstream ss;
        ss << "*" << args.size() << "\r\n";
        for (const auto& arg : args) {
            ss << "$" << arg.length() << "\r\n" << arg << "\r\n";
        }
        return ss.str();
    }
    
    /**
     * Handle REPLCONF command from replica
     * Replica se REPLCONF handle karo
     * (Handle REPLCONF from replica - configuration update)
     */
    std::string handleReplConf(const StringVector& args, int clientId);
    
    /**
     * Handle PSYNC command from replica
     * Replica se PSYNC handle karo - sync request
     * (Handle PSYNC from replica - sync request)
     */
    std::string handlePSync(const StringVector& args, int clientId);
    
    /**
     * Generate empty RDB for transfer
     * Empty RDB banao - minimal database
     * (Generate empty RDB - minimal database for sync)
     */
    std::string generateEmptyRDB() {
        // Minimal empty RDB file
        // Sabse chhota valid RDB
        // (Smallest valid RDB - just the skeleton)
        std::string rdb;
        
        // Magic + version (REDIS0011)
        rdb = "REDIS0011";
        
        // FA - auxiliary field (redis-ver)
        rdb += '\xFA';
        rdb += '\x09';  // 9 bytes
        rdb += "redis-ver";
        rdb += '\x05';  // 5 bytes
        rdb += "7.2.0";
        
        // FA - auxiliary field (redis-bits)
        rdb += '\xFA';
        rdb += '\x0A';  // 10 bytes
        rdb += "redis-bits";
        rdb += '\xC0';  // Special encoding: 64 as integer
        rdb += '\x40';  // 64
        
        // FE - database selector (db 0)
        rdb += '\xFE';
        rdb += '\x00';  // DB 0
        
        // FB - resize db
        rdb += '\xFB';
        rdb += '\x00';  // DB size = 0
        rdb += '\x00';  // Expires size = 0
        
        // FF - EOF
        rdb += '\xFF';
        
        // CRC64 checksum (8 bytes, we use 0s for simplicity)
        rdb += std::string(8, '\x00');
        
        return rdb;
    }
    
    /**
     * Handle WAIT command
     * WAIT command handle karo - replicas ka wait
     * (Handle WAIT - wait for replica acknowledgments)
     */
    std::string handleWait(int numReplicas, int64_t timeout);
    
    /**
     * Request ACK from all replicas
     * Saare replicas se ACK maango
     * (Request ACK from all replicas - roll call)
     */
    void requestAckFromReplicas();
    
    /**
     * Update replica ACK
     * Replica ki ACK update karo
     * (Update replica ACK - mark attendance)
     */
    void updateReplicaAck(int clientId, int64_t offset);
    
    /**
     * Get number of synced replicas
     * Kitne replicas sync hain
     * (How many replicas are synced)
     */
    int getSyncedReplicaCount(int64_t offset);
    
    /**
     * Get master file descriptor (for replica)
     * Master ka FD do
     * (Get master FD - for event loop)
     */
    int getMasterFd() const {
        return state_.masterFd;
    }
    
    /**
     * Get replication offset
     */
    int64_t getOffset() const {
        return state_.replOffset;
    }
    
    /**
     * Add to replication offset
     */
    void addToOffset(int64_t bytes) {
        state_.replOffset += bytes;
    }
    
    /**
     * Get master replication offset
     */
    int64_t getMasterOffset() const {
        return state_.masterReplOffset;
    }
    
    /**
     * Add to master offset
     */
    void addToMasterOffset(int64_t bytes) {
        state_.masterReplOffset += bytes;
    }
};

} // namespace Redis
