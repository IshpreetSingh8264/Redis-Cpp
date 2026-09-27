#include "commands/pubsub_directory.hpp"

#include <algorithm>

#include "types/client_session.hpp"
#include "utils/strutil.hpp"

namespace redis {

namespace {
/// A p-subscription matches with the same glob syntax KEYS uses.
bool matchesPattern(const std::string& pattern, const std::string& channel) {
    return strutil::matchPattern(pattern, channel);
}
}  // namespace

void PubSubDirectory::subscribe(ClientSession* client, const std::string& channel) {
    std::lock_guard<std::mutex> lock(mutex_);
    channels_[channel].insert(client);
    byClient_[client].insert(channel);
}

bool PubSubDirectory::unsubscribe(ClientSession* client, const std::string& channel) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto mine = byClient_.find(client);
    if (mine == byClient_.end() || mine->second.erase(channel) == 0) return false;
    auto set = channels_.find(channel);
    if (set != channels_.end()) {
        set->second.erase(client);
        if (set->second.empty()) channels_.erase(set);
    }
    if (mine->second.empty()) byClient_.erase(mine);
    return true;
}

void PubSubDirectory::psubscribe(ClientSession* client, const std::string& pattern) {
    std::lock_guard<std::mutex> lock(mutex_);
    patterns_[client].insert(pattern);
}

bool PubSubDirectory::punsubscribe(ClientSession* client, const std::string& pattern) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = patterns_.find(client);
    if (it == patterns_.end() || it->second.erase(pattern) == 0) return false;
    if (it->second.empty()) patterns_.erase(it);
    return true;
}

void PubSubDirectory::removeClient(ClientSession* client) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto mine = byClient_.find(client);
    if (mine != byClient_.end()) {
        for (const auto& channel : mine->second) {
            auto set = channels_.find(channel);
            if (set != channels_.end()) {
                set->second.erase(client);
                if (set->second.empty()) channels_.erase(set);
            }
        }
        byClient_.erase(mine);
    }
    patterns_.erase(client);
}

size_t PubSubDirectory::subscriberCount(const std::string& channel) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = channels_.find(channel);
    return it == channels_.end() ? 0 : it->second.size();
}

size_t PubSubDirectory::totalChannels() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return channels_.size();
}

size_t PubSubDirectory::totalSubscriptions() const {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t n = 0;
    for (const auto& [channel, clients] : channels_) n += clients.size();
    return n;
}

bool PubSubDirectory::watchesAnything() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return !byClient_.empty() || !patterns_.empty();
}

std::vector<std::string> PubSubDirectory::channelNames(const std::string& pattern) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> out;
    for (const auto& [channel, clients] : channels_) {
        if (clients.empty()) continue;
        if (!pattern.empty() && pattern != "*" && !matchesPattern(pattern, channel)) continue;
        out.push_back(channel);
    }
    std::sort(out.begin(), out.end());
    return out;
}

std::vector<ClientSession*> PubSubDirectory::subscribers(const std::string& channel) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = channels_.find(channel);
    if (it == channels_.end()) return {};
    return {it->second.begin(), it->second.end()};
}

std::vector<std::pair<ClientSession*, std::string>> PubSubDirectory::patternSubscribers(
    const std::string& channel) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::pair<ClientSession*, std::string>> out;
    for (const auto& [client, patterns] : patterns_) {
        for (const auto& pattern : patterns) {
            if (matchesPattern(pattern, channel)) out.emplace_back(client, pattern);
        }
    }
    return out;
}

}  // namespace redis
