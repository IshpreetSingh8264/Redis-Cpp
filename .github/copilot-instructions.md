# Redis (C++) — Architecture

Guidance for AI agents and humans working in this repository. Read this before
editing anything: several constraints here are not obvious from the code.

## Project overview

A from-scratch Redis server in C++23, built for the CodeCrafters
["Build your own Redis"](https://app.codecrafters.io/courses/redis/overview)
challenge. Implements the RESP protocol, 142 commands across strings, lists,
sets, hashes, sorted sets, streams, transactions, optimistic locking,
replication, RDB and AOF persistence, pub/sub, geospatial and ACL
authentication. Single-threaded, `epoll` event loop, non-blocking sockets.

## Layout

```
src/
  main.cpp            30 lines. Parses argv, ignores SIGPIPE, runs RedisServer.
  types/              enums, value structs, the Handler alias, server config.
  protocol/           RESP encode/decode.
  store/              DataStore, blocked-client registry.
  net/                ClientConnection, ClientManager, RedisServer.
  commands/           the registry + one file per command group.
  persistence/        RDB reader/writer/manager, AOF manager.
  replication/        ReplicationManager, handshake, propagation.
  geo/                geohash encode/decode, GeoPoint.
  auth/               AclUser, AuthManager.
  utils/              io, strutil, time, logger.
tests/                local regression suite (`./run_tests.sh`).
```

`main.cpp` is deliberately tiny. Domain logic belongs in a module, never there.

## Data flow

```
client socket
  -> RedisServer (epoll accept/read/write loop)
  -> ClientManager (per-connection state)
  -> ClientConnection (buffer, read/write, RESP framing)
  -> RedisServer::dispatch
  -> CommandRegistry lookup        <- data, not control flow
  -> Handler: (CommandContext&, args) -> RespValue
  -> store / persistence / replication side effects
  -> RESP encode -> socket
```

## Conventions

- **`namespace redis`** on everything. Only `main.cpp` is at global scope.
- **One concept per file.** No file exceeds 600 lines; most are far smaller.
- **Every header has a matching `.cpp`.** Two deliberate exceptions
  (`lox.hpp`-style contract-only headers) are documented in-file.
- **Commands are a registry, not an if-chain.** Adding a command means adding one
  entry to the group file's registration list. Never extend the dispatch switch.
- **Every handler has the same signature**, aliased once in `types/handler.hpp`.
- **CMake globs with `CONFIGURE_DEPENDS`.** A new `.cpp` is picked up on rebuild.
  `src/*.hpp` **must** stay in the glob — see the history note below.

## Do not do these

- **Do not re-add `.hpp` to the CMake glob carelessly.** At one point the glob was
  `src/*.cpp` only, so ten headers (~9,000 lines) sat in the tree, never compiled,
  never linked. `file(GLOB_RECURSE ... CONFIGURE_DEPENDS src/*.cpp src/*.hpp)` is
  deliberate. See commit `505d974`.
- **Do not delete a file because it looks unused.** Headers that appear
  unreferenced are usually reached through the registry. Build and test first.
- **Do not implement features the course does not have** (e.g. `FAILOVER` with
  automatic promotion, ACL key/channel pattern rules) unless a stage requires it.
  Where something is deliberately absent, the code says so in a comment.

## Testing

```sh
./run_tests.sh                # local regression suite (323 assertions)
codecrafters test -previous   # live harness, unlocked stages only
```

There is **no CTest target in this project** — `run_tests.sh` is the only
entrypoint. (Four sibling projects do wire `ctest`; this one does not, and
adding it is a deliberate non-goal rather than an oversight.)

`codecrafters test` only runs stages your account has unlocked. The remaining
stages are verified locally against CodeCrafters' published `redis-tester`,
which is not vendored into this repo.

## Known limitations

These are real and intentional. Do not "fix" them without a stage that needs it:

- `GEOSEARCH` supports `BYRADIUS` only; `BYBOX` returns an error, never a wrong
  answer.
- `FAILOVER` returns an error — promotion by timeout is not implemented.
- ACL key patterns, channel patterns and per-command rules are parsed and
  **ignored**; the code says so at the parse site.
- RDB listpack / ziplist / quicklist2 / hash-ziplist member encodings are not
  decoded. Plain opcodes 0-5, intset and quicklist-with-ziplist are.
- Replication keeps no backlog, so partial resync reports unsupported rather
  than pretending to succeed.
- `INFO` reports `DATA` type 0; some fields are constants.

## Build

Requires cmake, a C++23 compiler, and pthreads. No third-party dependencies —
zlib is not used, the protocol is hand-rolled.
