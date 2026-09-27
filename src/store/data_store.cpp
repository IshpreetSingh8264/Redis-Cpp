#include "store/data_store.hpp"

#include <algorithm>

#include "utils/time.hpp"

namespace redis {

bool purgeIfExpired(DataStore::Map& data, const std::string& key, int64_t nowMs) {
    auto it = data.find(key);
    if (it == data.end()) return false;
    if (!it->second.isExpired(nowMs)) return false;
    data.erase(it);
    return true;
}

bool typeMatches(const RedisValue& v, DataType expected) {
    return v.type == expected || v.type == DataType::NONE;
}

size_t DataStore::size() const {
    const int64_t now = timeutil::nowMs();
    return read([&](const Map& d) {
        size_t n = 0;
        for (const auto& [k, v] : d) {
            if (!v.isExpired(now)) n++;
        }
        return n;
    });
}

void DataStore::clear() { write([](Map& d) { d.clear(); }); }

std::vector<std::pair<std::string, RedisValue>> DataStore::snapshot() const {
    const int64_t now = timeutil::nowMs();
    return read([&](const Map& d) {
        std::vector<std::pair<std::string, RedisValue>> out;
        out.reserve(d.size());
        for (const auto& [k, v] : d) {
            if (!v.isExpired(now)) out.emplace_back(k, v);
        }
        return out;
    });
}

std::vector<std::string> DataStore::collectExpired() const {
    const int64_t now = timeutil::nowMs();
    return read([&](const Map& d) {
        std::vector<std::string> out;
        for (const auto& [k, v] : d) {
            if (v.isExpired(now)) out.push_back(k);
        }
        return out;
    });
}

void DataStore::removeKeys(const std::vector<std::string>& keys) {
    write([&](Map& d) {
        for (const auto& k : keys) d.erase(k);
    });
}

}  // namespace redis
