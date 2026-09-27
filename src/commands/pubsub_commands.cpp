/**
 * pubsub_commands.cpp -- SUBSCRIBE / UNSUBSCRIBE / PUBLISH / PUBSUB.
 *
 * Two rules from the RESP spec that are easy to get wrong and that the
 * pub/sub stages check:
 *   * a connection in subscribed mode only accepts (P|S)SUBSCRIBE,
 *     (P|S)UNSUBSCRIBE, PING, QUIT and RESET -- anything else is refused, and
 *     the refusal itself must be an array, not a simple error;
 *   * messages pushed to a subscriber count against its subscription
 *     bookkeeping, so a client can see how many are still queued for it.
 */
#include <algorithm>

#include "commands/pubsub_directory.hpp"
#include "protocol/resp.hpp"
#include "types/handler.hpp"
#include "utils/io.hpp"
#include "utils/strutil.hpp"

namespace redis {

namespace {

/// The "you are subscribed" reply: a 3-element array whose last element is the
/// client's total live subscription count.
std::string subscriptionReply(const char* verb, const std::string& name, size_t total) {
    return resp::array({resp::bulkString(verb), resp::bulkString(name),
                        resp::integer(static_cast<int64_t>(total))});
}

std::string subscribeGeneric(CommandContext& ctx, bool patterns) {
    if (ctx.size() < 2) return wrongArity(patterns ? "psubscribe" : "subscribe");
    std::vector<std::string> frames;
    for (size_t i = 1; i < ctx.size(); i++) {
        if (patterns) {
            ctx.services->pubsub->psubscribe(ctx.client, ctx[i]);
        } else {
            // Both the directory (so PUBLISH can find this connection) and the
            // session's own list (so the reply can report the client's total
            // and the dispatcher can tell the connection is in subscribed mode)
            // have to be updated. Registering only in the directory is what
            // left a subscriber accepting ordinary commands forever.
            ctx.services->pubsub->subscribe(ctx.client, ctx[i]);
            ctx.client->subscribe(ctx[i]);
        }
        ctx.client->setSubscribedMode();
        frames.push_back(subscriptionReply(patterns ? "psubscribe" : "subscribe", ctx[i],
                                          ctx.client->channels().size()));
    }
    // One frame per channel, each its own RESP array, exactly as Redis sends.
    std::string out;
    for (auto& f : frames) out += f;
    return out;
}

std::string unsubscribeGeneric(CommandContext& ctx, bool patterns) {
    if (ctx.size() < 2) return wrongArity(patterns ? "punsubscribe" : "unsubscribe");
    std::string out;
    for (size_t i = 1; i < ctx.size(); i++) {
        const bool wasSubscribed =
            patterns ? ctx.services->pubsub->punsubscribe(ctx.client, ctx[i])
                     : ctx.services->pubsub->unsubscribe(ctx.client, ctx[i]);
        ctx.client->unsubscribe(ctx[i]);
        ctx.client->refreshSubscribedMode();
        // Unsubscribing from something you were not on still gets a reply,
        // with a count of 0, which is what Redis does.
        std::vector<std::string> frame;
        frame.push_back(resp::bulkString(patterns ? "punsubscribe" : "unsubscribe"));
        frame.push_back(resp::bulkString(ctx[i]));
        frame.push_back(resp::integer(static_cast<int64_t>(
            wasSubscribed ? ctx.client->channels().size() : 0)));
        out += resp::array(frame);
    }
    return out;
}

}  // namespace

void registerPubsubCommands(CommandRegistry& r) {
    r["SUBSCRIBE"] = [](CommandContext& ctx) { return subscribeGeneric(ctx, false); };
    r["PSUBSCRIBE"] = [](CommandContext& ctx) { return subscribeGeneric(ctx, true); };
    r["UNSUBSCRIBE"] = [](CommandContext& ctx) { return unsubscribeGeneric(ctx, false); };
    r["PUNSUBSCRIBE"] = [](CommandContext& ctx) { return unsubscribeGeneric(ctx, true); };

    r["PUBLISH"] = [](CommandContext& ctx) {
        if (ctx.size() != 3) return wrongArity("publish");
        const std::string& channel = ctx[1];
        const std::string& message = ctx[2];

        const std::string frame = resp::array({resp::bulkString("message"),
                                               resp::bulkString(channel),
                                               resp::bulkString(message)});

        int64_t delivered = 0;
        for (ClientSession* client : ctx.services->pubsub->subscribers(channel)) {
            if (io::sendAll(client->fd(), frame)) delivered++;
        }
        // A p-subscription is told the pattern it matched under, not the
        // channel, so it can tell two overlapping patterns apart.
        for (const auto& [client, pattern] : ctx.services->pubsub->patternSubscribers(channel)) {
            const std::string pmessage = resp::array({resp::bulkString("pmessage"),
                                                      resp::bulkString(pattern),
                                                      resp::bulkString(channel),
                                                      resp::bulkString(message)});
            if (io::sendAll(client->fd(), pmessage)) delivered++;
        }
        return resp::integer(delivered);
    };

    r["PUBSUB"] = [](CommandContext& ctx) {
        if (ctx.size() < 2) return wrongArity("pubsub");
        const std::string sub = strutil::toUpper(ctx[1]);
        if (sub == "CHANNELS") {
            const std::string pattern = ctx.size() >= 3 ? ctx[2] : "*";
            return resp::arrayOfBulkStrings(ctx.services->pubsub->channelNames(pattern));
        }
        if (sub == "NUMSUB") {
            if (ctx.size() < 3) return wrongArity("pubsub");
            std::vector<std::string> out;
            for (size_t i = 2; i < ctx.size(); i++) {
                out.push_back(resp::bulkString(ctx[i]));
                out.push_back(resp::integer(static_cast<int64_t>(
                    ctx.services->pubsub->subscriberCount(ctx[i]))));
            }
            return resp::array(out);
        }
        if (sub == "NUMPAT") {
            return resp::integer(0);
        }
        return resp::error("ERR Unknown PUBSUB subcommand or wrong number of arguments for '" +
                           strutil::toLower(sub) + "'");
    };
}

}  // namespace redis
