#include "store/blocked_clients.hpp"

#include <algorithm>
#include <utility>

#include "protocol/resp.hpp"
#include "store/data_store.hpp"
#include "utils/io.hpp"
#include "utils/time.hpp"

namespace redis {

void BlockedClients::blockOnList(ClientSession* client, std::vector<std::string> keys,
                                 bool fromLeft, int64_t timeoutMs) {
    std::lock_guard<std::mutex> lock(mutex_);
    listWaiters_.push_back(
        ListBlockedClient{client, std::move(keys), fromLeft, timeoutMs, timeutil::steadyMs()});
}

void BlockedClients::blockOnStreams(ClientSession* client, std::vector<std::string> keys,
                                    std::vector<StreamId> lastIds, int64_t count,
                                    int64_t timeoutMs) {
    std::lock_guard<std::mutex> lock(mutex_);
    streamWaiters_.push_back(StreamBlockedClient{client, std::move(keys), std::move(lastIds),
                                                 count, timeoutMs, timeutil::steadyMs()});
}

int BlockedClients::notifyList(const std::string& key, DataStore& store) {
    std::lock_guard<std::mutex> lock(mutex_);
    int served = 0;

    // Lock order is always blocked-then-store. Every caller releases the store
    // lock before reaching here, so this is the only nesting that occurs.
    for (auto it = listWaiters_.begin(); it != listWaiters_.end();) {
        if (std::find(it->keys.begin(), it->keys.end(), key) == it->keys.end()) {
            ++it;
            continue;
        }

        // Take the first of this waiter's keys that currently has an element.
        // Checking every key, not just the one that triggered the notification,
        // is what makes `BLPOP a b` on an already-populated `b` work.
        std::string servedKey, value;
        bool servedHere = store.write([&](DataStore::Map& data) {
            for (const auto& k : it->keys) {
                auto dit = data.find(k);
                if (dit == data.end() || dit->second.type != DataType::LIST) continue;
                purgeIfExpired(data, k, timeutil::nowMs());
                if (dit == data.end() || dit->second.listValue.empty()) continue;
                servedKey = k;
                if (it->fromLeft) {
                    value = dit->second.listValue.front();
                    dit->second.listValue.pop_front();
                } else {
                    value = dit->second.listValue.back();
                    dit->second.listValue.pop_back();
                }
                if (dit->second.listValue.empty()) data.erase(dit);
                return true;
            }
            return false;
        });

        if (!servedHere) {
            ++it;
            continue;
        }

        io::sendAll(it->client->fd(), resp::array({resp::bulkString(servedKey),
                                                   resp::bulkString(value)}));
        it = listWaiters_.erase(it);
        served++;
    }

    return served;
}

int BlockedClients::notifyStreams(const std::string& key, DataStore& store) {
    std::lock_guard<std::mutex> lock(mutex_);
    int served = 0;

    for (auto it = streamWaiters_.begin(); it != streamWaiters_.end();) {
        size_t keyIndex = SIZE_MAX;
        for (size_t i = 0; i < it->keys.size(); i++) {
            if (it->keys[i] == key) {
                keyIndex = i;
                break;
            }
        }
        if (keyIndex == SIZE_MAX) {
            ++it;
            continue;
        }
        if (keyIndex >= it->lastIds.size()) {
            ++it;
            continue;
        }

        const StreamId after = it->lastIds[keyIndex];
        const int64_t count = it->count;
        const std::string clientKey = key;

        bool gotData = store.read([&](const DataStore::Map& data) {
            auto dit = data.find(clientKey);
            if (dit == data.end() || dit->second.type != DataType::STREAM) return false;
            for (const auto& e : dit->second.streamValue) {
                if (streamIdLess(after, StreamId{e.timestamp, e.sequence})) return true;
            }
            return false;
        });
        if (!gotData) {
            ++it;
            continue;
        }

        // Collect and send outside the store lock so a slow peer cannot stall
        // every other writer.
        std::string frame = store.read([&](const DataStore::Map& data) {
            auto dit = data.find(clientKey);
            if (dit == data.end()) return std::string();
            std::vector<std::string> entries;
            int64_t taken = 0;
            for (const auto& e : dit->second.streamValue) {
                if (!streamIdLess(after, StreamId{e.timestamp, e.sequence})) continue;
                std::vector<std::string> fields;
                fields.reserve(e.fields.size() * 2);
                for (const auto& [f, v] : e.fields) {
                    fields.push_back(resp::bulkString(f));
                    fields.push_back(resp::bulkString(v));
                }
                entries.push_back(resp::array({resp::bulkString(e.id), resp::array(fields)}));
                if (count > 0 && ++taken >= count) break;
            }
            if (entries.empty()) return std::string();
            return resp::array({resp::array({resp::bulkString(clientKey), resp::array(entries)})});
        });

        if (frame.empty()) {
            ++it;
            continue;
        }

        io::sendAll(it->client->fd(), frame);
        it = streamWaiters_.erase(it);
        served++;
    }

    return served;
}

void BlockedClients::expireTimedOut() {
    const int64_t now = timeutil::steadyMs();
    const std::string nothing = resp::nullArray();

    std::lock_guard<std::mutex> lock(mutex_);

    for (auto it = listWaiters_.begin(); it != listWaiters_.end();) {
        if (it->timeoutMs >= 0 && now - it->startedAtMs >= it->timeoutMs) {
            io::sendAll(it->client->fd(), nothing);
            it = listWaiters_.erase(it);
        } else {
            ++it;
        }
    }

    for (auto it = streamWaiters_.begin(); it != streamWaiters_.end();) {
        if (it->timeoutMs >= 0 && now - it->startedAtMs >= it->timeoutMs) {
            io::sendAll(it->client->fd(), nothing);
            it = streamWaiters_.erase(it);
        } else {
            ++it;
        }
    }
}

void BlockedClients::removeClient(ClientSession* client) {
    std::lock_guard<std::mutex> lock(mutex_);
    listWaiters_.erase(std::remove_if(listWaiters_.begin(), listWaiters_.end(),
                                      [client](const ListBlockedClient& w) {
                                          return w.client == client;
                                      }),
                       listWaiters_.end());
    streamWaiters_.erase(std::remove_if(streamWaiters_.begin(), streamWaiters_.end(),
                                        [client](const StreamBlockedClient& w) {
                                            return w.client == client;
                                        }),
                         streamWaiters_.end());
}

bool BlockedClients::empty() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return listWaiters_.empty() && streamWaiters_.empty();
}

}  // namespace redis
