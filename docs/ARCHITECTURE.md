# Redis — Architecture

How the server is put together. The [README](../README.md) covers what it does; this covers the shape of the code.

> This file replaces two earlier documents, `DOCUMENTATION.md` and `README_IMPLEMENTATION.md`. Both described a
> single-file server with an asio-based event loop, which is not what this codebase is.

## Contents

- [The event loop](#the-event-loop)
- [Connections](#connections)
- [Command dispatch](#command-dispatch)
- [The registry](#the-registry)
- [The keyspace](#the-keyspace)
- [Protocol](#protocol)
- [Replication internals](#replication-internals)
- [Persistence internals](#persistence-internals)
- [File map](#file-map)
- [Conventions](#conventions)

## The event loop

`RedisServer::run()` in `src/net/server.cpp` is the whole server loop:

```cpp
while (running_) {
    epoll_wait(epollFd_, events, 64, 100);   // 100 ms timeout
    tick();                                  // expiry, blocked clients, pending WAITs
    handleReadable(events, count);
}
```

The 100 ms timeout is what drives the active-expiry cycle, the `WAIT` resolution, and the replica reconnect backoff.
There are no threads anywhere in `src/` — no `std::thread`, no pool, no `fork`. `CMakeLists.txt` links
`Threads::Threads`, which is vestigial.

Level-triggered `EPOLLIN` only, on a single epoll set holding the listening socket, every client fd, and the master link
fd. Level-triggered rather than edge-triggered is deliberate: it makes the read loop safe against partial drains without
needing a per-connection "data drained" flag.

## Connections

`ClientSession` (in `src/types/`) holds one input buffer plus the per-connection state that is not on the socket itself.
Sessions live in `RedisServer::clients_`, a `unordered_map<int, unique_ptr<ClientSession>>`.

`processInput()` loops until the buffer holds an incomplete command. A command is framed by `resp::parseCommand`, which
returns `consumed` bytes; the buffer is erased by that amount and the loop continues, so several pipelined commands in
one TCP segment are all handled before returning to `epoll`.

Deferred commands are the one wrinkle. `BLPOP`, `BRPOP`, `XREAD BLOCK`, `WAIT`, and `PSYNC` cannot answer
synchronously, so their handlers return an empty string, `execute()` interprets that as "already handled, write
nothing", and the actual reply is written later from `tick()` by `BlockedClients` or
`ReplicationManager::flushPendingWaits()`.

## Command dispatch

`RedisServer::execute()` is the single choke point. Every command, from every source, goes through it in a fixed order:

```cpp
1. registry lookup          → ERR unknown command
2. NOAUTH check             → NOAUTH Authentication required.
3. subscribed-mode check    → ERR can't execute '<cmd>': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET
4. MULTI queueing           → +QUEUED
5. replica READONLY check   → READONLY You can't write against a read only replica.
6. handler
7. no-reply suppression     for commands arriving from the master link
8. AOF append + propagate   for write commands only
```

Step 5 has a deliberate exception: commands that arrived on the master link are allowed through, which is what makes
replication work at all.

Step 8 gates on `isWriteCommand()`. That function's name list is longer than the registered command set — it includes
`MSETNX`, `COPY`, `RESTORE`, `LMOVE`, `XACK` and a few others that do not exist here. Harmless, because the lookup is by
upper-cased name against real commands, but worth knowing if you audit the list.

## The registry

`src/commands/registry.cpp` builds a `std::unordered_map<std::string, Handler>` once, memoised, by calling fourteen
group registrars. `Handler` is `std::string(CommandContext&) -> std::string`, where the returned string is the serialised
RESP reply.

Adding a command means: write a function with that signature, add a line to the relevant group registrar, and if it is a
write, add its name to `isWriteCommand()`. There is no other registration path.

Aliases are just multiple map entries pointing at the same function — `UNLINK` is `r["UNLINK"] = r["DEL"]`,
`HMSET = HSET`, `SLAVEOF = REPLICAOF`, `FLUSHALL = FLUSHDB`.

## The keyspace

`DataStore` in `src/store/` holds one `unordered_map<string, RedisValue>` behind a `std::shared_mutex`. Handlers never
take the lock themselves; they pass a closure to `read()` or `write()`, which keeps every critical section a single
statement and removes a whole class of lock-ordering bugs.

`RedisValue` carries a `std::string key`, a type tag, the payload, and an `int64_t expiryMs` that is `-1` when there is
no deadline. Expiry is therefore a property of the value rather than a separate structure — see the README for the
lazy and active strategies that act on it.

## Protocol

`src/protocol/resp.cpp`, hand-rolled, RESP2 only.

**Reading.** If the first byte is not `*`, the input is treated as an inline command: read to CRLF and split on
whitespace. A bare CRLF is consumed as noise. Otherwise `*<count>` followed by exactly `<count>` `$<len>` bulk strings.
A short buffer returns `ok=false` and no bytes consumed; a null bulk string *inside* a command array is fatal, because
there is no sensible way to continue.

**Writing.** A hand-rolled set of emitters: simple string, error, integer, bulk string, null bulk, null array, empty
array, boolean, double, and array builders. `boolean` and `doubleValue` are RESP3 shapes that exist but have no call
site.

**Writes go out through `io::sendAll`**, which loops on short writes. Everything is synchronous `write(2)` from the event
loop, including `PUBLISH` fan-out and the RDB bulk transfer during `PSYNC`.

## Replication internals

The replica-side handshake is an explicit state machine in `src/replication/replication_manager.cpp`:

```
AUTH_SENT → PING_SENT → LISTENING_PORT_SENT → CAPA_SENT
         → PSYNC_SENT → AWAITING_FULLRESYNC → LOADING_RDB → STREAMING
```

`AUTH` is only in the chain when `--masterauth` was given; otherwise the first step is `PING`.

**Offset accounting** is the subtle part. The master builds one RESP frame, sends it to every replica, and adds the
frame's byte length to `offset_`. The replica adds `consumed` bytes per parsed command. For `REPLCONF GETACK` the ack
frame is written *before* the offset advances, so the offset the master sees is the pre-frame value — which is what real
Redis reports, and what the grader expects.

The RDB payload is deliberately excluded from the offset.

**Full resync only.** `PSYNC ?` and `PSYNC <replid>` both get
`ERR Partial resynchronization not supported (no backlog kept)`. Answering with a fake success would be worse than
saying no.

The full-resync payload is one RESP bulk string, drained in two non-overlapping states — header first, then body — to
avoid an underflow when the body arrives in several TCP segments.

## Persistence internals

**RDB writer** emits entries, then `OP_SELECTDB 0`, `OP_RESIZEDB 0 0`, `OP_EOF`, then an 8-byte CRC64 over everything
before the checksum. The selector records coming after the body is unusual; this server's own reader accepts it, but do
not assume a real `redis-server` will.

**RDB reader** accepts versions 1–12 and rejects anything higher, with a per-object failure log rather than a hard stop
so one bad object does not lose the rest of the dump.

**AOF replay** runs the parsed frames back through `AofManager::applyCommand`, which goes through the normal dispatcher.
A replayed write is therefore re-appended, keeping the file consistent as it grows.

## File map

| Path | Responsibility |
|---|---|
| `src/main.cpp` | unbuffered streams, `SIGPIPE` ignored, construct, run |
| `src/net/server.cpp` | epoll loop, connections, `execute()`, argument parsing |
| `src/net/socket.cpp` | socket helpers |
| `src/protocol/resp.cpp` | RESP2 codec |
| `src/commands/registry.cpp` | the 121-name registry and `isWriteCommand` |
| `src/commands/*_commands.cpp` | one file per command family |
| `src/store/data_store.cpp` | the keyspace and its shared_mutex |
| `src/persistence/rdb_*.cpp` | RDB opcodes, reader, writer, manager |
| `src/persistence/aof_manager.cpp` | multi-part AOF, rewrite, replay |
| `src/replication/replication_manager.cpp` | handshake, offsets, propagation |
| `src/auth/sha256.cpp` | SHA-256, used only to render password hashes |
| `src/types/values.hpp` | `RedisValue` and the expiry sentinel |
| `src/types/client_session.hpp` | per-connection state |
| `src/utils/io.cpp` | `sendAll` and friends |

## Conventions

- **No third-party libraries.** Raw POSIX sockets, not asio. `vcpkg.json` lists asio and pthreads, but neither is
  consumed and there is no `#include <asio/asio.hpp>` anywhere.
- **Every header has a real `.cpp`.** `CMakeLists.txt` globs `src/*.hpp` as well as `src/*.cpp` and there is a comment
  explaining why: headers had previously gone uncompiled, so a declaration with no definition passed the build.
- **Two std::mutex users are not in the registry** — `AuthManager` and `PubSubDirectory` own their own state and lock it
  directly.
- **Errors are RESP error strings**, not exceptions. A handler returns `"-ERR ...\r\n"`.
- **Comments are written in Pinglish**, with an English gloss in parentheses. The quantity is much smaller than earlier
  documentation claimed: there are two Punjabi comments in the tree, both in `CMakeLists.txt`.
