#include "commands/auth_manager.hpp"

#include <algorithm>

#include "types/server_config.hpp"
#include "utils/strutil.hpp"

namespace redis {

void AuthManager::bootstrap() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (users_.empty()) users_["default"] = User{};
    if (!config_->requirepass.empty()) {
        // --requirepass is shorthand for "this user has this password".
        User& slot = users_[config_->user];
        slot.passwords.insert(config_->requirepass);
        slot.nopass = false;
        slot.flags.erase("nopass");
    }
}

bool AuthManager::enforcementEnabled() const {
    if (!config_->requirepass.empty()) return true;
    std::lock_guard<std::mutex> lock(mutex_);
    // Only the user an *unauthenticated* connection becomes matters. Giving
    // some other user a password does not lock everyone else out; treating it
    // as if it did meant a single `ACL SETUSER bob >secret` turned NOAUTH on
    // for every connection in the server, including ones that never wanted to
    // be bob.
    if (config_->user != "default") return true;  // the default user is disabled
    auto it = users_.find("default");
    if (it == users_.end()) return false;
    return !it->second.nopass || !it->second.passwords.empty();
}

bool AuthManager::authenticate(const std::string& user, const std::string& password) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = users_.find(user);
    if (it == users_.end()) return false;
    if (it->second.nopass) return true;
    if (it->second.passwords.count(password)) return true;
    // With --requirepass set, AUTH <password> is also accepted as the default
    // user, which is what every client that only knows one secret expects.
    return !config_->requirepass.empty() && user == "default" &&
           password == config_->requirepass;
}

bool AuthManager::userExists(const std::string& user) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return users_.count(user) != 0;
}

std::vector<std::string> AuthManager::usernames() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> out;
    for (const auto& [name, user] : users_) out.push_back(name);
    return out;
}

bool AuthManager::passwordsOf(const std::string& user, std::vector<std::string>& out) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = users_.find(user);
    if (it == users_.end()) return false;
    out.assign(it->second.passwords.begin(), it->second.passwords.end());
    return true;
}

bool AuthManager::hasNoPasswords(const std::string& user) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = users_.find(user);
    return it != users_.end() && it->second.passwords.empty();
}

void AuthManager::setPassword(const std::string& user, const std::string& password) {
    std::lock_guard<std::mutex> lock(mutex_);
    User& slot = users_[user];
    slot.passwords.insert(password);
    slot.nopass = false;
    slot.flags.erase("nopass");
}

void AuthManager::clearPasswords(const std::string& user) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = users_.find(user);
    if (it == users_.end()) return;
    it->second.passwords.clear();
}

void AuthManager::setNopass(const std::string& user, bool nopass) {
    std::lock_guard<std::mutex> lock(mutex_);
    User& slot = users_[user];
    slot.nopass = nopass;
    if (nopass) slot.flags.insert("nopass");
    else slot.flags.erase("nopass");
}

bool AuthManager::nopass(const std::string& user) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = users_.find(user);
    return it != users_.end() && it->second.nopass;
}

std::set<std::string> AuthManager::flagsOf(const std::string& user) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = users_.find(user);
    if (it == users_.end()) return {};
    return it->second.flags;
}

void AuthManager::setFlag(const std::string& user, const std::string& flag, bool on) {
    std::lock_guard<std::mutex> lock(mutex_);
    User& slot = users_[user];
    const std::string lowered = strutil::toLower(flag);
    if (on) slot.flags.insert(lowered);
    else slot.flags.erase(lowered);
}

bool AuthManager::deleteUser(const std::string& user) {
    std::lock_guard<std::mutex> lock(mutex_);
    return users_.erase(user) != 0;
}

void AuthManager::addUser(const std::string& user) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (users_.count(user) == 0) users_[user] = User{};
}

}  // namespace redis
