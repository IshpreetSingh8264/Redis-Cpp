#include "persistence/rdb_manager.hpp"

#include <filesystem>
#include <fstream>
#include <vector>

#include "persistence/rdb_reader.hpp"
#include "persistence/rdb_writer.hpp"
#include "store/data_store.hpp"
#include "types/server_config.hpp"

namespace redis {

std::string RdbManager::path() const {
    return config_->dir + "/" + config_->dbfilename;
}

bool RdbManager::save(const DataStore& store, std::string& error) const {
    error.clear();

    rdb::RdbWriter writer;
    for (const auto& [key, value] : store.snapshot()) writer.writeEntry(key, value);
    const std::string blob = writer.finish();

    // Write to a sibling temp file and rename, so a crash mid-save cannot
    // leave a half-written dump where a good one used to be.
    const std::string target = path();
    const std::string temp = target + ".tmp";
    try {
        std::filesystem::create_directories(config_->dir);
        {
            std::ofstream out(temp, std::ios::binary | std::ios::trunc);
            if (!out.is_open()) {
                error = "cannot open " + temp + " for writing";
                return false;
            }
            out.write(blob.data(), static_cast<std::streamsize>(blob.size()));
            if (!out.good()) {
                error = "write to " + temp + " failed";
                return false;
            }
        }
        std::error_code ec;
        std::filesystem::rename(temp, target, ec);
        if (ec) {
            error = "cannot rename " + temp + " to " + target + ": " + ec.message();
            return false;
        }
    } catch (const std::exception& e) {
        error = e.what();
        return false;
    }
    return true;
}

bool RdbManager::load(DataStore& store, int& loaded, std::string& error) const {
    loaded = 0;
    error.clear();

    std::ifstream in(path(), std::ios::binary);
    if (!in.is_open()) return false;  // "no dump file" is not an error

    std::vector<char> blob((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    if (blob.empty()) {
        error = "dump file is empty";
        return false;
    }
    return loadFromBuffer(std::string(blob.data(), blob.size()), store, loaded, error);
}

bool RdbManager::loadFromBuffer(const std::string& blob, DataStore& store, int& loaded,
                               std::string& error) const {
    loaded = 0;
    error.clear();

    rdb::RdbReader reader(reinterpret_cast<const uint8_t*>(blob.data()), blob.size());
    rdb::Entries entries;
    std::string readError;
    if (!reader.load(entries, readError)) {
        error = readError;
        return false;
    }
    error = readError;  // non-empty only for the non-fatal checksum warning

    store.write([&](DataStore::Map& data) {
        for (auto& [key, value] : entries) data[std::move(key)] = std::move(value);
    });
    loaded = static_cast<int>(entries.size());
    return true;
}

}  // namespace redis
