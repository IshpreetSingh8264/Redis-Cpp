/**
 * pubsub_directory.hpp -- which connection is listening to which channel.
 *
 * Owned by the pub/sub command group because that is the only thing that
 * reads or writes it, but it lives in its own file because it is state, not a
 * command, and the net layer has to unsubscribe departing clients from it.
 */
#ifndef REDIS_COMMANDS_PUBSUB_DIRECTORY_HPP
#define REDIS_COMMANDS_PUBSUB_DIRECTORY_HPP

#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace redis {

class ClientSession;

class PubSubDirectory {
public:
    void subscribe(ClientSession* client, const std::string& channel);
    /// Returns true if the client had actually been subscribed to it.
    bool unsubscribe(ClientSession* client, const std::string& channel);
    void removeClient(ClientSession* client);

    /// Number of clients that will receive a message on `channel`.
    size_t subscriberCount(const std::string& channel) const;
    size_t totalChannels() const;
    size_t totalSubscriptions() const;

    /// Names of every channel with at least one subscriber, filtered by glob.
    std::vector<std::string> channelNames(const std::string& pattern) const;

    /// Every client subscribed to `channel`, for PUBLISH to fan out to.
    std::vector<ClientSession*> subscribers(const std::string& channel) const;

    /// Pattern subscriptions, used by PSUBSCRIBE. Kept separate from channel
    /// subscriptions so that a plain PUBLISH never reaches them.
    void psubscribe(ClientSession* client, const std::string& pattern);
    bool punsubscribe(ClientSession* client, const std::string& pattern);
    std::vector<std::pair<ClientSession*, std::string>> patternSubscribers(
        const std::string& channel) const;
    bool watchesAnything() const;

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::unordered_set<ClientSession*>> channels_;
    std::unordered_map<ClientSession*, std::unordered_set<std::string>> byClient_;
    std::unordered_map<ClientSession*, std::unordered_set<std::string>> patterns_;
};

}  // namespace redis

#endif  // REDIS_COMMANDS_PUBSUB_DIRECTORY_HPP
