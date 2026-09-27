/**
 * auth_manager.hpp -- passwords and ACL users.
 *
 * Two levels, as in Redis 6+: a server-wide `requirepass`, and per-user
 * entries that carry a password set and a set of enabled command categories.
 * Only the subset of ACL that is actually useful without a command table is
 * modelled -- see auth_commands.cpp for what is and is not answered.
 */
#ifndef REDIS_COMMANDS_AUTH_MANAGER_HPP
#define REDIS_COMMANDS_AUTH_MANAGER_HPP

#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

namespace redis {

class ServerConfig;

class AuthManager {
public:
    explicit AuthManager(ServerConfig* config) : config_(config) {}

    /// Create the `default` user and apply --requirepass if one was given.
    void bootstrap();

    /// True when a password is required at all. With no requirepass and no user
    /// passwords, the server is open and every connection starts authenticated.
    bool enforcementEnabled() const;

    bool authenticate(const std::string& user, const std::string& password);
    bool userExists(const std::string& user) const;
    std::vector<std::string> usernames() const;
    bool passwordsOf(const std::string& user, std::vector<std::string>& out) const;
    bool hasNoPasswords(const std::string& user) const;
    void setPassword(const std::string& user, const std::string& password);
    void clearPasswords(const std::string& user);
    void setNopass(const std::string& user, bool nopass);
    bool nopass(const std::string& user) const;
    /// User flags such as on, off, allkeys, noauth, sanitize-payload.
    std::set<std::string> flagsOf(const std::string& user) const;
    void setFlag(const std::string& user, const std::string& flag, bool on);
    bool deleteUser(const std::string& user);
    void addUser(const std::string& user);

private:
    struct User {
        std::set<std::string> passwords;  // stored as given; this is a learning
                                           // project, not a credential store
        bool nopass = true;
        std::set<std::string> flags = {"on", "nopass", "sanitize-payload"};
    };

    const ServerConfig* config_;
    mutable std::mutex mutex_;
    std::map<std::string, User> users_;
};

}  // namespace redis

#endif  // REDIS_COMMANDS_AUTH_MANAGER_HPP
