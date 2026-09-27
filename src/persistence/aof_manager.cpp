#include "persistence/aof_manager.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>

#include "persistence/rdb_manager.hpp"
#include "types/services.hpp"
#include "persistence/rdb_writer.hpp"
#include "protocol/resp.hpp"
#include "store/data_store.hpp"
#include "types/server_config.hpp"
#include "utils/strutil.hpp"

namespace redis {

namespace {

constexpr const char* kManifestName = "appendonly.aof.manifest";
constexpr const char* kBasePart = "appendonly.aof.1.base.rdb";
constexpr const char* kIncrPart = "appendonly.aof.1.incr.aof";

}  // namespace

std::string AofManager::dirPath() const {
    return config_->dir + "/" + config_->appenddirname;
}
std::string AofManager::manifestPath() const { return dirPath() + "/" + kManifestName; }
std::string AofManager::basePath() const { return dirPath() + "/" + kBasePart; }
std::string AofManager::incrPath() const { return dirPath() + "/" + kIncrPart; }

bool AofManager::isOpen() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return fd_ >= 0;
}

size_t AofManager::bufferedBytes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (fd_ < 0) return 0;
    struct stat st;
    if (fstat(fd_, &st) != 0) return 0;
    return static_cast<size_t>(st.st_size);
}

std::string AofManager::configGet(const std::string& param) const {
    const std::string p = strutil::toLower(param);
    if (p == "appendonly") return config_->appendonly ? "yes" : "no";
    if (p == "appendfilename") return config_->appendfilename;
    if (p == "appenddirname") return config_->appenddirname;
    return "";
}

bool AofManager::configSet(const std::string& param, const std::string& value,
                           std::string& error) {
    const std::string p = strutil::toLower(param);
    const std::string v = strutil::toLower(value);

    if (p == "appendfilename") {
        config_->appendfilename = value;
        return true;
    }
    if (p == "appenddirname") {
        config_->appenddirname = value;
        return true;
    }
    if (p == "appendonly") {
        if (v == "yes") {
            if (config_->appendonly) return true;
            // Turning AOF on mid-session means taking a base snapshot of
            // everything written so far, or every one of those keys would be
            // missing after the next restart.
            std::error_code ec;
            std::filesystem::create_directories(dirPath(), ec);
            if (ec) {
                error = "cannot create " + dirPath() + ": " + ec.message();
                return false;
            }
            DataStore& store = *services_->store;
            if (!writeFreshBase(store, error)) return false;
            if (!writeManifest(error)) return false;
            config_->appendonly = true;
            std::string openError;
            if (!open(store, openError)) {
                config_->appendonly = false;
                error = openError;
                return false;
            }
            return true;
        }
        if (v == "no") {
            config_->appendonly = false;
            close();
            return true;
        }
        error = "argument couldn't be parsed into an integer";
        return false;
    }

    error = "Unknown option or number of arguments for CONFIG SET - '" + param + "'";
    return false;
}

bool AofManager::writeManifest(std::string& error) const {
    std::ofstream out(manifestPath(), std::ios::trunc);
    if (!out.is_open()) {
        error = "cannot open " + manifestPath() + " for writing";
        return false;
    }
    out << "file " << kBasePart << " seq 1 type b\n";
    out << "file " << kIncrPart << " seq 1 type i\n";
    if (!out.good()) {
        error = "write to " + manifestPath() + " failed";
        return false;
    }
    return true;
}

bool AofManager::writeFreshBase(DataStore& store, std::string& error) const {
    // The base part is a real RDB, but it is named and placed by the AOF, not
    // by the RDB layer: it is the first entry in the manifest and it lives
    // inside the appendonly directory. Delegating to RdbManager::save() would
    // write it to <dir>/<dbfilename> instead, where nothing would ever look
    // for it.
    const std::string target = dirPath() + "/" + kBasePart;
    try {
        std::filesystem::create_directories(dirPath());
        std::ofstream out(target, std::ios::binary | std::ios::trunc);
        if (!out.is_open()) {
            error = "cannot open " + target + " for writing";
            return false;
        }
        rdb::RdbWriter writer;
        for (const auto& [key, value] : store.snapshot()) writer.writeEntry(key, value);
        const std::string blob = writer.finish();
        out.write(blob.data(), static_cast<std::streamsize>(blob.size()));
        if (!out.good()) {
            error = "write to " + target + " failed";
            return false;
        }
    } catch (const std::exception& e) {
        error = e.what();
        return false;
    }
    return true;
}

std::vector<std::string> AofManager::manifestEntries() const {
    std::vector<std::string> parts;
    std::ifstream in(manifestPath());
    if (!in.is_open()) return parts;
    std::string line;
    while (std::getline(in, line)) {
        auto fields = strutil::splitWhitespace(line);
        if (fields.size() >= 2 && fields[0] == "file") parts.push_back(fields[1]);
    }
    return parts;
}

bool AofManager::readManifest(std::vector<std::string>& parts) const {
    parts = manifestEntries();
    return !parts.empty();
}

bool AofManager::open(DataStore& store, std::string& error) {
    RdbManager rdb_(config_);
    error.clear();
    if (!config_->appendonly) return true;

    std::error_code ec;
    std::filesystem::create_directories(dirPath(), ec);
    if (ec) {
        error = "cannot create " + dirPath() + ": " + ec.message();
        return false;
    }

    std::vector<std::string> parts;
    const bool hadManifest = readManifest(parts);

    if (!hadManifest) {
        // First boot with AOF enabled: take a base snapshot of whatever is in
        // memory (the RDB file loaded at startup, or nothing) and start the
        // incremental part empty.
        if (!writeFreshBase(store, error)) return false;
        if (!writeManifest(error)) return false;
        parts = {kBasePart, kIncrPart};
    }

    // The base has to be applied before the incremental part, because the
    // incremental part assumes the snapshot is already there.
    for (const auto& part : parts) {
        const std::string full = dirPath() + "/" + part;
        if (part.size() > 4 && part.compare(part.size() - 4, 4, ".rdb") == 0) {
            std::ifstream in(full, std::ios::binary);
            if (!in.is_open()) {
                if (std::filesystem::exists(full, ec)) {
                    error = "cannot read AOF base part " + part;
                    return false;
                }
                continue;  // advertised but not written: nothing to apply
            }
            const std::string blob((std::istreambuf_iterator<char>(in)),
                                   std::istreambuf_iterator<char>());
            int applied = 0;
            std::string warning;
            // The base part is loaded from this exact path, not from
            // <dir>/<dbfilename>: the AOF names and places its own parts, and
            // loading the other file would silently apply the wrong snapshot.
            if (!rdb_.loadFromBuffer(blob, store, applied, warning)) {
                error = "cannot load AOF base part " + part + ": " + warning;
                return false;
            }
            if (!warning.empty()) error = warning;  // non-fatal, e.g. a checksum note
            continue;
        }
        if (!replayIncrement(full, error)) return false;
    }

    // Keep the incremental part open so every write is one write(2).
    std::lock_guard<std::mutex> lock(mutex_);
    fd_ = ::open(incrPath().c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd_ < 0) {
        error = "cannot open " + incrPath() + " for appending: " + std::strerror(errno);
        return false;
    }
    return true;
}

bool AofManager::replayIncrement(const std::string& path, std::string& error) const {
    std::error_code ec;
    if (!std::filesystem::exists(path, ec)) return true;

    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) {
        error = "cannot read " + path;
        return false;
    }
    std::string blob((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());

    // Each record is a RESP array; run them in order. A trailing partial
    // record means the process died mid-append -- that record was never
    // acknowledged to a client, so dropping it is correct.
    size_t pos = 0;
    while (pos < blob.size()) {
        resp::ParseResult r = resp::parseCommand(blob.substr(pos));
        if (!r.ok) break;
        if (r.consumed == 0) break;
        pos += r.consumed;
        if (r.args.empty()) continue;
        if (applyCommand) applyCommand(r.args);
    }
    return true;
}

bool AofManager::rewrite(DataStore& store, std::string& error) {
    std::error_code ec;

    // The new sequence number is one past the highest already on disk, so a
    // manifest left over from an interrupted rewrite is never mistaken for the
    // current one.
    int64_t highest = 0;
    for (const auto& part : manifestEntries()) {
        // "appendonly.aof.<seq>.<kind>"
        const size_t firstDot = part.find('.');
        const size_t secondDot = part.find('.', firstDot == std::string::npos ? 0 : firstDot + 1);
        if (firstDot == std::string::npos || secondDot == std::string::npos) continue;
        int64_t seq = 0;
        if (strutil::parseInt64(part.substr(firstDot + 1, secondDot - firstDot - 1), seq)) {
            highest = std::max(highest, seq);
        }
    }
    const int64_t nextSequence = highest + 1;
    const std::string baseName =
        "appendonly.aof." + std::to_string(nextSequence) + ".base.rdb";
    const std::string incrName =
        "appendonly.aof." + std::to_string(nextSequence) + ".incr.aof";

    rdb::RdbWriter writer;
    for (const auto& [key, value] : store.snapshot()) writer.writeEntry(key, value);
    const std::string blob = writer.finish();

    // New parts first, manifest last. The manifest is the only thing that says
    // which parts are current, so publishing it early would advertise a base
    // that does not exist yet.
    {
        std::ofstream out(dirPath() + "/" + baseName, std::ios::binary | std::ios::trunc);
        if (!out.is_open()) {
            error = "cannot write " + baseName;
            return false;
        }
        out.write(blob.data(), static_cast<std::streamsize>(blob.size()));
        if (!out.good()) {
            error = "write to " + baseName + " failed";
            return false;
        }
    }
    { std::ofstream fresh(dirPath() + "/" + incrName, std::ios::binary | std::ios::trunc); }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
        fd_ = ::open((dirPath() + "/" + incrName).c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd_ < 0) {
            error = "cannot open " + incrName + " for appending: " + std::strerror(errno);
            return false;
        }
    }

    {
        std::ofstream manifest(manifestPath(), std::ios::trunc);
        if (!manifest.is_open()) {
            error = "cannot write " + manifestPath();
            return false;
        }
        manifest << "file " << baseName << " seq " << nextSequence << " type b\n";
        manifest << "file " << incrName << " seq " << nextSequence << " type i\n";
    }

    for (const auto& part : manifestEntries()) {
        if (part == baseName || part == incrName) continue;
        std::filesystem::remove(dirPath() + "/" + part, ec);
    }
    return true;
}

void AofManager::append(const std::vector<std::string>& args) {
    if (!config_->appendonly) return;
    std::lock_guard<std::mutex> lock(mutex_);
    if (fd_ < 0) return;
    const std::string frame = resp::encodeCommand(args);
    size_t written = 0;
    while (written < frame.size()) {
        ssize_t n = ::write(fd_, frame.data() + written, frame.size() - written);
        if (n > 0) {
            written += static_cast<size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        break;  // the AOF is a cache of the keyspace, not the source of truth
    }
}

void AofManager::close() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (fd_ >= 0) {
        ::fsync(fd_);
        ::close(fd_);
        fd_ = -1;
    }
}

}  // namespace redis
