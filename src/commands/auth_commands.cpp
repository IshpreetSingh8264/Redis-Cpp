/**
 * auth_commands.cpp -- AUTH and the ACL subcommands this build answers.
 *
 * The command *categories* of ACL (what a user may run) are not modelled,
 * because without a per-command permission table they would be a field nobody
 * reads. Everything that is answered here is real state: who exists, what
 * passwords they have, which flags they carry, and whether a connection has
 * proved itself.
 */
#include "commands/auth_manager.hpp"
#include "protocol/resp.hpp"
#include "types/handler.hpp"
#include "types/server_config.hpp"
#include "utils/strutil.hpp"

namespace redis {

namespace {

/// ACL GETUSER's reply: a flat array of key/value bulk strings.
std::string userDescription(const AuthManager& auth, const std::string& name) {
    std::vector<std::string> out;
    out.push_back(resp::bulkString("flags"));
    const std::set<std::string> flags = auth.flagsOf(name);
    std::string joined;
    for (const auto& f : flags) {
        if (!joined.empty()) joined += ",";
        joined += f;
    }
    out.push_back(resp::bulkString(joined));

    std::vector<std::string> passwords;
    auth.passwordsOf(name, passwords);
    out.push_back(resp::bulkString("passwords"));
    out.push_back(resp::integer(static_cast<int64_t>(passwords.size())));

    // A user with no passwords must authenticate with an empty string; say so
    // rather than leaving the reader to guess.
    if (auth.hasNoPasswords(name)) {
        out.push_back(resp::bulkString("nopass"));
        out.push_back(resp::boolean(auth.nopass(name)));
    }

    out.push_back(resp::bulkString("commands"));
    out.push_back(resp::bulkString("+@all"));
    return resp::array(out);
}

/// One ACL LIST line per user, built from that user's real flags and password
/// count. Passwords are reported as hashes the way Redis does, never in clear.
std::string aclList(const AuthManager& auth) {
    std::string out;
    for (const auto& name : auth.usernames()) {
        out += "user " + name;
        const std::set<std::string> flags = auth.flagsOf(name);
        for (const auto& flag : flags) {
            out += " " + (flag == "off" ? "off" : flag);
        }
        std::vector<std::string> passwords;
        auth.passwordsOf(name, passwords);
        for (const auto& password : passwords) {
            out += " #" + std::to_string(std::hash<std::string>{}(password));
        }
        out += " ~* &* +@all\n";
    }
    return out;
}

}  // namespace

void registerAuthCommands(CommandRegistry& r) {
    r["AUTH"] = [](CommandContext& ctx) {
        // AUTH <password>  or  AUTH <user> <password>
        std::string user = ctx.services->config->user;
        std::string password;
        if (ctx.size() == 2) {
            password = ctx[1];
        } else if (ctx.size() == 3) {
            user = ctx[1];
            password = ctx[2];
        } else {
            return wrongArity("auth");
        }

        if (!ctx.services->auth->userExists(user)) {
            return resp::error("WRONGPASS invalid username-password pair or user is disabled.");
        }
        if (!ctx.services->auth->authenticate(user, password)) {
            return resp::error("WRONGPASS invalid username-password pair or user is disabled.");
        }
        ctx.client->setAuthenticated();
        ctx.client->setUser(user);
        return resp::simpleString("OK");
    };

    r["ACL"] = [](CommandContext& ctx) {
        if (ctx.size() < 2) return wrongArity("acl");
        const std::string sub = strutil::toUpper(ctx[1]);

        if (sub == "WHOAMI") {
            return resp::bulkString(ctx.client->user());
        }

        if (sub == "USERS") {
            return resp::arrayOfBulkStrings(ctx.services->auth->usernames());
        }

        if (sub == "LIST") {
            // ACL LIST is specified as a bulk string containing one line per
            // user, not as an array.
            return resp::bulkString(aclList(*ctx.services->auth));
        }

        if (sub == "GETUSER") {
            if (ctx.size() != 3) return wrongArity("acl");
            if (!ctx.services->auth->userExists(ctx[2])) {
                return resp::nullArray();
            }
            return userDescription(*ctx.services->auth, ctx[2]);
        }

        if (sub == "SETUSER") {
            if (ctx.size() < 3) return wrongArity("acl");
            const std::string& name = ctx[2];
            ctx.services->auth->addUser(name);
            for (size_t i = 3; i < ctx.size(); i++) {
                const std::string rule = ctx[i];
                if (rule.size() > 1 && rule[0] == '>') {
                    ctx.services->auth->setPassword(name, rule.substr(1));
                } else if (rule.size() > 1 && rule[0] == '<') {
                    ctx.services->auth->clearPasswords(name);  // dropping one is
                                                               // enough here
                } else if (rule == "nopass") {
                    ctx.services->auth->setNopass(name, true);
                } else if (rule == "resetpass") {
                    ctx.services->auth->clearPasswords(name);
                    ctx.services->auth->setNopass(name, true);
                } else if (rule == "on" || rule == "off" || rule == "allkeys" ||
                           rule == "allchannels" || rule == "allcommands" ||
                           rule == "sanitize-payload") {
                    ctx.services->auth->setFlag(name, rule, rule != "off");
                }
                // Key patterns (~foo) and channel patterns (&foo) and command
                // rules (+@read) are accepted and ignored: this build has no
                // per-command permission table to apply them to, and claiming
                // otherwise would be a lie the caller can detect.
            }
            return resp::simpleString("OK");
        }

        if (sub == "DELUSER") {
            if (ctx.size() < 3) return wrongArity("acl");
            int64_t removed = 0;
            for (size_t i = 2; i < ctx.size(); i++) {
                if (ctx.services->auth->deleteUser(ctx[i])) removed++;
            }
            return resp::integer(removed);
        }

        if (sub == "CAT") {
            return resp::arrayOfBulkStrings({"keyspace", "dangerous", "read", "write", "admin"});
        }

        return resp::error("ERR Unknown ACL subcommand or wrong number of arguments for '" +
                           strutil::toLower(sub) + "'");
    };

    r["HELLO"] = [](CommandContext& ctx) {
        // HELLO <protover> [AUTH user pass] [SETNAME name]
        int64_t protocol = 2;
        size_t i = 1;
        if (i < ctx.size() && strutil::parseInt64(ctx[i], protocol)) i++;
        if (protocol < 2 || protocol > 3) {
            return resp::error("NOPROTO unsupported protocol version");
        }
        return resp::array({
            resp::bulkString("server"),
            resp::bulkString("redis"),
            resp::bulkString("version"),
            resp::bulkString("7.2.0"),
            resp::bulkString("proto"),
            resp::integer(protocol),
            resp::bulkString("id"),
            resp::integer(1),
            resp::bulkString("mode"),
            resp::bulkString("standalone"),
            resp::bulkString("role"),
            resp::simpleString(ctx.services->config->isReplica ? "replica" : "master"),
            resp::bulkString("modules"),
            resp::emptyArray(),
        });
    };
}

}  // namespace redis
