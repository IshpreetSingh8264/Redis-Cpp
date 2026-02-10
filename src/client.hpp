/**
 * =============================================================================
 *                          REDIS CLIENT CONNECTION
 * =============================================================================
 * 
 * Paaji eh class hai jo ek client connection handle karti hai!
 * (Bro this class handles a single client connection!)
 * 
 * Har client di apni state hundi hai - like apna apna room hai
 * (Each client has its own state - like everyone has their own room)
 * 
 * Features:
 * - Socket management (connection handle karna)
 * - Transaction support (MULTI/EXEC)
 * - Pub/Sub subscription state
 * - Blocking command support
 * 
 * =============================================================================
 */

#pragma once

#include "common.hpp"
#include "resp_parser.hpp"
#include "data_store.hpp"

namespace Redis {

// Forward declarations
class RedisServer;

// ============================================================================
// CLIENT CONNECTION CLASS
// Ek client di poori zindagi handle karna
// (Handle entire lifecycle of one client - birth to death)
// ============================================================================

/**
 * @class ClientConnection
 * @brief Represents a single client connection to Redis server
 * 
 * Eh class ek client di saari info rakhdi hai:
 * (This class keeps all info about one client:)
 * - Socket file descriptor
 * - Input/Output buffers
 * - Transaction state
 * - Subscription state
 * - Blocked state
 * 
 * Jiven hotel ch har guest da record hunda hai
 * (Like a hotel keeps record of each guest)
 */
class ClientConnection {
public:
    // ========================================================================
    // CLIENT IDENTIFIERS
    // Client ki pehchaan - Aadhaar jaise
    // (Client identification - like Aadhaar card)
    // ========================================================================
    
    int id;                         // Unique client ID - har client unique
                                    // (Unique client ID - every client is special)
    int fd;                         // Socket file descriptor - connection ka handle
                                    // (Socket FD - the connection handle)
    std::string address;            // Client IP address - kahaan se aaya
                                    // (Client IP - where did they come from)
    int port;                       // Client port - konsa darwaza
                                    // (Client port - which door)
    
    // ========================================================================
    // BUFFERS
    // Data store karne ki jagah - godown
    // (Storage space for data - warehouse buffers)
    // ========================================================================
    
    std::string inputBuffer;        // Data jo client ne bheja hai
                                    // (Data client has sent - incoming messages)
    std::string outputBuffer;       // Data jo client ko bhejna hai
                                    // (Data to send to client - outgoing messages)
    
    // ========================================================================
    // STATE FLAGS
    // Client ki haalat - kaisa hai
    // (Client's state - how are they doing)
    // ========================================================================
    
    ClientState state = ClientState::NORMAL;    // Current state - abhi kya ho raha hai
                                                 // (Current state - what's happening now)
    
    // ========================================================================
    // TRANSACTION STATE
    // Transaction ki info - MULTI/EXEC support
    // (Transaction info - MULTI/EXEC support)
    // ========================================================================
    
    bool inTransaction = false;     // MULTI command ke baad true
                                    // (True after MULTI command - in transaction mode)
    std::vector<StringVector> queuedCommands;  // Queue mein commands
                                                // (Queued commands - waiting for EXEC)
    bool transactionAborted = false; // Transaction mein error aayi
                                     // (Error in transaction - will fail on EXEC)
    
    // ========================================================================
    // PUB/SUB STATE
    // Pub/Sub ki info - subscription mode
    // (Pub/Sub info - subscription mode)
    // ========================================================================
    
    std::unordered_set<std::string> subscribedChannels;     // Channel subscriptions
                                                            // (Channel subscriptions - jaise YouTube)
    std::unordered_set<std::string> subscribedPatterns;     // Pattern subscriptions
                                                            // (Pattern subscriptions - wildcard)
    
    // ========================================================================
    // BLOCKING STATE
    // Blocking commands ki info - wait kar raha hai
    // (Blocking commands info - waiting for data)
    // ========================================================================
    
    bool isBlocked = false;         // Client blocked hai - wait kar raha
                                    // (Client is blocked - waiting for something)
    std::chrono::steady_clock::time_point blockDeadline;    // Kab tak wait karega
                                                            // (Until when to wait)
    std::vector<std::string> blockingKeys;  // Kin keys pe wait kar raha
                                            // (Which keys waiting on)
    
    // ========================================================================
    // REPLICATION STATE
    // Replication ki info - replica hai ya master
    // (Replication info - is this a replica or master connection)
    // ========================================================================
    
    bool isReplica = false;         // Ye replica hai - slave connection
                                    // (This is a replica - slave connection)
    int64_t replOffset = 0;         // Replication offset - kahan tak sync hai
                                    // (Replication offset - how far synced)
    bool replAckPending = false;    // ACK wait kar rahe hain
                                    // (Waiting for ACK - confirmation pending)
    std::chrono::steady_clock::time_point lastReplAck;  // Last ACK time
                                                         // (When was last ACK received)
    
    // ========================================================================
    // AUTHENTICATION STATE
    // Authentication ki info - login status
    // (Authentication info - login status)
    // ========================================================================
    
    bool authenticated = true;      // Default true (if no password set)
                                    // (Default true - if server has no password)
    std::string username = "default";   // Current user - kaun hai
                                        // (Current user - who is this)
    
    // ========================================================================
    // TIMING INFO
    // Time related info - kab aaya, kab baat ki
    // (Timing info - when connected, when last spoke)
    // ========================================================================
    
    std::chrono::steady_clock::time_point createdAt;    // Connection time - kab aaya
                                                        // (When client connected - first impression)
    std::chrono::steady_clock::time_point lastActivity; // Last activity - aakhri baar kab
                                                        // (Last activity - when we last heard from them)
    
    // ========================================================================
    // CONSTRUCTORS
    // Object banane ke tarike - client born here
    // (Ways to create object - client is born here)
    // ========================================================================
    
    /**
     * Constructor - naya client aaya
     * (New client arrived - welcome!)
     */
    ClientConnection(int clientId, int socketFd) 
        : id(clientId), fd(socketFd) {
        createdAt = std::chrono::steady_clock::now();
        lastActivity = createdAt;
    }
    
    /**
     * Destructor - client ja raha hai, goodbye
     * (Client leaving - farewell!)
     */
    ~ClientConnection() {
        if (fd >= 0) {
            close(fd);
        }
    }
    
    // Disable copy - ek hi client ho sakda hai
    // (Disable copy - only one client can exist)
    ClientConnection(const ClientConnection&) = delete;
    ClientConnection& operator=(const ClientConnection&) = delete;
    
    // Enable move - ownership transfer
    // (Enable move - can transfer ownership)
    ClientConnection(ClientConnection&& other) noexcept
        : id(other.id), fd(other.fd), address(std::move(other.address)),
          port(other.port), inputBuffer(std::move(other.inputBuffer)),
          outputBuffer(std::move(other.outputBuffer)), state(other.state),
          inTransaction(other.inTransaction), 
          queuedCommands(std::move(other.queuedCommands)),
          transactionAborted(other.transactionAborted),
          subscribedChannels(std::move(other.subscribedChannels)),
          subscribedPatterns(std::move(other.subscribedPatterns)),
          isBlocked(other.isBlocked), blockDeadline(other.blockDeadline),
          blockingKeys(std::move(other.blockingKeys)),
          isReplica(other.isReplica), replOffset(other.replOffset),
          replAckPending(other.replAckPending), lastReplAck(other.lastReplAck),
          authenticated(other.authenticated), username(std::move(other.username)),
          createdAt(other.createdAt), lastActivity(other.lastActivity) {
        other.fd = -1;  // Prevent double close
    }
    
    ClientConnection& operator=(ClientConnection&& other) noexcept {
        if (this != &other) {
            if (fd >= 0) close(fd);
            
            id = other.id;
            fd = other.fd;
            address = std::move(other.address);
            port = other.port;
            inputBuffer = std::move(other.inputBuffer);
            outputBuffer = std::move(other.outputBuffer);
            state = other.state;
            inTransaction = other.inTransaction;
            queuedCommands = std::move(other.queuedCommands);
            transactionAborted = other.transactionAborted;
            subscribedChannels = std::move(other.subscribedChannels);
            subscribedPatterns = std::move(other.subscribedPatterns);
            isBlocked = other.isBlocked;
            blockDeadline = other.blockDeadline;
            blockingKeys = std::move(other.blockingKeys);
            isReplica = other.isReplica;
            replOffset = other.replOffset;
            replAckPending = other.replAckPending;
            lastReplAck = other.lastReplAck;
            authenticated = other.authenticated;
            username = std::move(other.username);
            createdAt = other.createdAt;
            lastActivity = other.lastActivity;
            
            other.fd = -1;
        }
        return *this;
    }
    
    // ========================================================================
    // BUFFER OPERATIONS
    // Buffer de naal kaam - data add/remove
    // (Buffer operations - add/remove data)
    // ========================================================================
    
    /**
     * Add data to output buffer
     * Output buffer mein data add karo
     * (Add data to output buffer - queue for sending)
     */
    void appendOutput(const std::string& data) {
        outputBuffer += data;
    }
    
    /**
     * Add data to output buffer (RespValue)
     * RespValue nu serialize karke add karo
     * (Add RespValue by serializing it)
     */
    void appendOutput(const RespValue& value) {
        outputBuffer += value.serialize();
    }
    
    /**
     * Check if has data to send
     * Kuch bhejna hai ki nahi
     * (Is there anything to send?)
     */
    bool hasOutput() const {
        return !outputBuffer.empty();
    }
    
    /**
     * Clear output buffer after successful send
     * Bhej diya, ab buffer saaf karo
     * (Sent successfully, clear buffer now)
     */
    void clearOutput(size_t bytes) {
        if (bytes >= outputBuffer.length()) {
            outputBuffer.clear();
        } else {
            outputBuffer = outputBuffer.substr(bytes);
        }
    }
    
    /**
     * Add data to input buffer
     * Client ne jo bheja wo input buffer mein
     * (Add data client sent to input buffer)
     */
    void appendInput(const char* data, size_t len) {
        inputBuffer.append(data, len);
        lastActivity = std::chrono::steady_clock::now();
    }
    
    /**
     * Clear input buffer after processing
     * Process ho gaya, input buffer saaf karo
     * (Processed, clear input buffer)
     */
    void clearInput(size_t bytes) {
        if (bytes >= inputBuffer.length()) {
            inputBuffer.clear();
        } else {
            inputBuffer = inputBuffer.substr(bytes);
        }
    }
    
    // ========================================================================
    // STATE OPERATIONS
    // State change karna - mode switch
    // (State change operations - mode switching)
    // ========================================================================
    
    /**
     * Start transaction (MULTI)
     * Transaction shuru karo - queue mode ON
     * (Start transaction - queue mode ON)
     */
    void startTransaction() {
        inTransaction = true;
        transactionAborted = false;
        queuedCommands.clear();
        state = ClientState::TRANSACTION;
    }
    
    /**
     * Queue command in transaction
     * Command queue mein daalo - EXEC ka wait
     * (Queue command - waiting for EXEC)
     */
    void queueCommand(const StringVector& cmd) {
        queuedCommands.push_back(cmd);
    }
    
    /**
     * End transaction (EXEC or DISCARD)
     * Transaction khatam - either execute or cancel
     * (End transaction - either execute or cancel)
     */
    void endTransaction() {
        inTransaction = false;
        transactionAborted = false;
        queuedCommands.clear();
        if (subscribedChannels.empty() && subscribedPatterns.empty()) {
            state = ClientState::NORMAL;
        }
    }
    
    /**
     * Mark transaction as aborted (error in queuing)
     * Transaction mein galti ho gayi - ab fail hoga
     * (Error in transaction - will fail on EXEC)
     */
    void abortTransaction() {
        transactionAborted = true;
    }
    
    /**
     * Enter subscription mode
     * Subscription mode mein aao - pub/sub
     * (Enter subscription mode - pub/sub)
     */
    void enterSubscriptionMode() {
        state = ClientState::SUBSCRIBING;
    }
    
    /**
     * Check if in subscription mode
     * Pub/Sub mode mein hai ki nahi
     * (Is in pub/sub mode?)
     */
    bool isInSubscriptionMode() const {
        return state == ClientState::SUBSCRIBING ||
               !subscribedChannels.empty() ||
               !subscribedPatterns.empty();
    }
    
    /**
     * Get total subscription count
     * Kitni subscriptions hain
     * (Total subscriptions count)
     */
    size_t subscriptionCount() const {
        return subscribedChannels.size() + subscribedPatterns.size();
    }
    
    /**
     * Subscribe to channel
     * Channel subscribe karo
     * (Subscribe to channel - join the party)
     */
    void subscribeToChannel(const std::string& channel) {
        subscribedChannels.insert(channel);
        enterSubscriptionMode();
    }
    
    /**
     * Unsubscribe from channel
     * Channel se unsubscribe karo
     * (Unsubscribe from channel - leave the party)
     */
    bool unsubscribeFromChannel(const std::string& channel) {
        bool removed = subscribedChannels.erase(channel) > 0;
        if (subscribedChannels.empty() && subscribedPatterns.empty()) {
            state = ClientState::NORMAL;
        }
        return removed;
    }
    
    /**
     * Unsubscribe from all channels
     * Saare channels se unsubscribe
     * (Unsubscribe from all - total disconnect)
     */
    std::vector<std::string> unsubscribeFromAllChannels() {
        std::vector<std::string> channels(subscribedChannels.begin(), 
                                           subscribedChannels.end());
        subscribedChannels.clear();
        if (subscribedPatterns.empty()) {
            state = ClientState::NORMAL;
        }
        return channels;
    }
    
    /**
     * Block client for list operation
     * Client nu block karo - BLPOP/BRPOP
     * (Block client - waiting for list data)
     */
    void blockOnKeys(const std::vector<std::string>& keys, int64_t timeoutMs) {
        isBlocked = true;
        blockingKeys = keys;
        state = ClientState::BLOCKED;
        
        if (timeoutMs > 0) {
            blockDeadline = std::chrono::steady_clock::now() + 
                            std::chrono::milliseconds(timeoutMs);
        } else {
            blockDeadline = std::chrono::steady_clock::time_point::max();
        }
    }
    
    /**
     * Unblock client
     * Client nu unblock karo - wait khatam
     * (Unblock client - waiting is over)
     */
    void unblock() {
        isBlocked = false;
        blockingKeys.clear();
        state = ClientState::NORMAL;
    }
    
    /**
     * Check if block has timed out
     * Timeout ho gaya ki nahi
     * (Has blocking timed out?)
     */
    bool hasBlockTimedOut() const {
        if (!isBlocked) return false;
        return std::chrono::steady_clock::now() >= blockDeadline;
    }
    
    // ========================================================================
    // REPLICATION OPERATIONS
    // Replication related - master/replica communication
    // (Replication operations - master/replica sync)
    // ========================================================================
    
    /**
     * Mark as replica connection
     * Replica connection mark karo
     * (Mark as replica - this is a follower)
     */
    void markAsReplica() {
        isReplica = true;
        lastReplAck = std::chrono::steady_clock::now();
    }
    
    /**
     * Update replication offset
     * Replication offset update karo - sync progress
     * (Update replication offset - sync progress)
     */
    void updateReplOffset(int64_t offset) {
        replOffset = offset;
        lastReplAck = std::chrono::steady_clock::now();
        replAckPending = false;
    }
    
    /**
     * Request ACK from replica
     * Replica se ACK maango
     * (Request ACK from replica - confirmation chahiye)
     */
    void requestReplAck() {
        replAckPending = true;
    }
    
    // ========================================================================
    // UTILITY METHODS
    // Helper functions - chhote kaam
    // (Utility methods - small tasks)
    // ========================================================================
    
    /**
     * Get client info string (for CLIENT LIST)
     * Client di info string - list lai
     * (Client info string - for CLIENT LIST command)
     */
    std::string getInfoString() const {
        std::ostringstream ss;
        ss << "id=" << id;
        ss << " addr=" << address << ":" << port;
        ss << " fd=" << fd;
        ss << " name="; // Empty name for now
        
        // Flags
        ss << " flags=";
        if (isReplica) ss << "S";
        if (isInSubscriptionMode()) ss << "P";
        if (inTransaction) ss << "x";
        if (isBlocked) ss << "b";
        
        // Age and idle
        auto now = std::chrono::steady_clock::now();
        auto age = std::chrono::duration_cast<std::chrono::seconds>(now - createdAt).count();
        auto idle = std::chrono::duration_cast<std::chrono::seconds>(now - lastActivity).count();
        ss << " age=" << age;
        ss << " idle=" << idle;
        
        // Other info
        ss << " db=0";  // We only support DB 0
        ss << " sub=" << subscribedChannels.size();
        ss << " psub=" << subscribedPatterns.size();
        
        if (inTransaction) {
            ss << " multi=" << queuedCommands.size();
        }
        
        return ss.str();
    }
    
    /**
     * Check if client is healthy (not timed out)
     * Client theek hai ki nahi - alive check
     * (Is client healthy? - alive check)
     */
    bool isHealthy(int timeoutSeconds = 300) const {
        auto now = std::chrono::steady_clock::now();
        auto idle = std::chrono::duration_cast<std::chrono::seconds>(now - lastActivity).count();
        return idle < timeoutSeconds;
    }
};

// ============================================================================
// CLIENT MANAGER
// Saare clients manage karna - HR department jaise
// (Manage all clients - like HR department)
// ============================================================================

/**
 * @class ClientManager
 * @brief Manages all client connections
 * 
 * Saare clients ka record rakhna - kaun aaya, kaun gaya
 * (Keep record of all clients - who came, who left)
 */
class ClientManager {
private:
    std::unordered_map<int, std::unique_ptr<ClientConnection>> clients_;
    std::unordered_map<int, int> fdToClient_;  // fd -> client id mapping
    mutable std::shared_mutex mutex_;
    std::atomic<int> nextClientId_{1};

public:
    /**
     * Create new client connection
     * Naya client banao - welcome to the party
     * (Create new client - welcome to the party!)
     */
    ClientConnection* createClient(int fd, const std::string& address, int port) {
        std::unique_lock lock(mutex_);
        
        int clientId = nextClientId_++;
        auto client = std::make_unique<ClientConnection>(clientId, fd);
        client->address = address;
        client->port = port;
        
        ClientConnection* ptr = client.get();
        clients_[clientId] = std::move(client);
        fdToClient_[fd] = clientId;
        
        return ptr;
    }
    
    /**
     * Get client by ID
     * ID se client lo
     * (Get client by ID)
     */
    ClientConnection* getClient(int clientId) {
        std::shared_lock lock(mutex_);
        auto it = clients_.find(clientId);
        if (it != clients_.end()) {
            return it->second.get();
        }
        return nullptr;
    }
    
    /**
     * Get client by file descriptor
     * FD se client lo
     * (Get client by FD)
     */
    ClientConnection* getClientByFd(int fd) {
        std::shared_lock lock(mutex_);
        auto it = fdToClient_.find(fd);
        if (it != fdToClient_.end()) {
            return getClientInternal(it->second);
        }
        return nullptr;
    }
    
    /**
     * Remove client
     * Client hatao - goodbye
     * (Remove client - farewell!)
     */
    void removeClient(int clientId) {
        std::unique_lock lock(mutex_);
        auto it = clients_.find(clientId);
        if (it != clients_.end()) {
            fdToClient_.erase(it->second->fd);
            clients_.erase(it);
        }
    }
    
    /**
     * Remove client by FD
     * FD se client hatao
     * (Remove client by FD)
     */
    void removeClientByFd(int fd) {
        std::unique_lock lock(mutex_);
        auto it = fdToClient_.find(fd);
        if (it != fdToClient_.end()) {
            int clientId = it->second;
            fdToClient_.erase(it);
            clients_.erase(clientId);
        }
    }
    
    /**
     * Get all client IDs
     * Saare client IDs do
     * (Get all client IDs)
     */
    std::vector<int> getAllClientIds() {
        std::shared_lock lock(mutex_);
        std::vector<int> ids;
        for (const auto& [id, client] : clients_) {
            ids.push_back(id);
        }
        return ids;
    }
    
    /**
     * Get all replica client IDs
     * Saare replica IDs do
     * (Get all replica client IDs)
     */
    std::vector<int> getReplicaIds() {
        std::shared_lock lock(mutex_);
        std::vector<int> ids;
        for (const auto& [id, client] : clients_) {
            if (client->isReplica) {
                ids.push_back(id);
            }
        }
        return ids;
    }
    
    /**
     * Get client count
     * Kitne clients hain
     * (How many clients)
     */
    size_t count() const {
        std::shared_lock lock(mutex_);
        return clients_.size();
    }
    
    /**
     * Get replica count
     * Kitne replicas hain
     * (How many replicas)
     */
    size_t replicaCount() const {
        std::shared_lock lock(mutex_);
        size_t count = 0;
        for (const auto& [id, client] : clients_) {
            if (client->isReplica) count++;
        }
        return count;
    }
    
    /**
     * Iterate over all clients
     * Saare clients pe loop
     * (Iterate over all clients)
     */
    template<typename Func>
    void forEach(Func&& func) {
        std::shared_lock lock(mutex_);
        for (auto& [id, client] : clients_) {
            func(client.get());
        }
    }

private:
    ClientConnection* getClientInternal(int clientId) {
        auto it = clients_.find(clientId);
        if (it != clients_.end()) {
            return it->second.get();
        }
        return nullptr;
    }
};

} // namespace Redis
