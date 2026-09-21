# Redis — C++

A Redis-compatible server in C++23, built for the
[CodeCrafters "Build your own Redis" challenge](https://codecrafters.io/challenges/redis).

121 commands across strings, lists, sets, hashes, sorted sets, streams, geo, pub/sub, transactions, keyspace expiry,
ACLs, RDB snapshots, append-only persistence, and master/replica replication — on a single-threaded `epoll` event loop
using raw POSIX sockets and a hand-rolled RESP codec.

> **Scope note.** This is a serious reimplementation of Redis, not a complete clone. Partial resynchronization, stream
> consumer groups, RESP3 output, `FAILOVER`, and write commands of any kind are not implemented. See
> [Known limitations](#known-limitations).

## Contents

- [Quick start](#quick-start)
- [What it implements](#what-it-implements)
- [Architecture](#architecture)
- [Command reference](#command-reference)
- [Persistence](#persistence)
- [Replication](#replication)
- [Keyspace expiry](#keyspace-expiry)
- [Running the tests](#running-the-tests)
- [CLI options](#cli-options)
- [Known limitations](#known-limitations)
- [Project layout](#project-layout)

## Quick start

Requires CMake 3.13+, a C++23 compiler, and Python 3. There are no third-party dependencies — `vcpkg.json` is vestigial.

```bash
cmake -B build -S .
cmake --build ./build
./build/redis
```

Or, to build and start in one step:

```bash
./your_program.sh
```

```
$ ./build/redis --port 6379
Starting Redis server on port 6379
Loading RDB from ./dump.rdb
Server ready to accept connections
```

```bash
$ redis-cli -p 6379 set greeting "hello"
OK
$ redis-cli -p 6379 get greeting
"hello"
$ redis-cli -p 6379 zadd board 10 alice 20 bob 30 carol
3
$ redis-cli -p 6379 zrangebyscore board 15 30
1) "bob"
2) "carol"
```

## What it implements

| Area | Summary |
|---|---|
| Protocol | RESP2 inbound and outbound, plus inline commands. RESP3 is *not* implemented. |
| Concurrency | One thread, one `epoll` loop, level-triggered. No worker pool, no threads. |
| Persistence | RDB version 12, and the Redis 7 multi-part AOF layout. |
| Replication | `PSYNC`, `REPLCONF`, `WAIT`, full resynchronization only. |
| Auth | `AUTH`, `ACL`, `HELLO`, SHA-256 password hashing for display. |
| Expiry | Lazy expiry on every access, plus a full keyspace sweep every 100 ms. |

## Architecture

```
                    ┌──────────────────────────────────────────┐
   accept / recv ──►│  RedisServer::run()   (src/net/server.cpp)│
                    │                                          │
                    │   epoll_wait(100ms)                       │
                    │     ├─► tick()        expiry, blocked ops │
                    │     └─► handleReadable() per event        │
                    │            └─► processInput()             │
                    │                  └─► execute()  ◄── single │
                    │                        │         choke    │
                    │        ┌───────────────┼──────────────┐   │
                    │        ▼               ▼              ▼   │
                    │   NOAUTH check   MULTI queue   registry  │
                    │                                 lookup    │
                    │                                      │    │
                    │                          ┌───────────┴────┐
                    │                          ▼                ▼
                    │                    AOF append       CommandHandler
                    │                    + replica        (121 commands)
                    │                    propagation
                    └──────────────────────────────────────────┘
```

`RedisServer::execute()` is the single choke point every command passes through, in a fixed order:

1. Registry lookup — unknown names get `ERR unknown command`.
2. `NOAUTH` check for unauthenticated connections.
3. Subscribed-mode refusal.
4. `MULTI` queueing.
5. `READONLY` refusal on a replica.
6. The handler itself.
7. No-reply suppression for commands arriving from the master link.
8. AOF append and replica propagation, for write commands only.

Deferred-reply commands (`BLPOP`, `BRPOP`, `XREAD BLOCK`, `WAIT`, `PSYNC`) signal "already answered" by returning an
empty string. `execute()` writes nothing in that case, and the reply goes out later from `tick()`.

**Locking.** The datastore guards its map with a `std::shared_mutex`; handlers never take it directly, they pass a
closure to `read()` / `write()`. `AuthManager`, `PubSubDirectory`, `BlockedClients`, `AofManager`, and
`ReplicationManager` each have their own `std::mutex`. Writes to sockets are synchronous `write(2)` calls from the
event loop — including `PUBLISH` fan-out and full RDB transfer during `PSYNC`.

## Command reference

121 names are registered in `src/commands/registry.cpp`.

**Strings** — `SET` `GET` `SETNX` `SETXX` `GETSET` `SETEX` `PSETEX` `APPEND` `STRLEN` `GETRANGE` `SETRANGE` `MGET`
`MSET` `INCR` `DECR` `INCRBY` `DECRBY` `INCRBYFLOAT`
: `SET` supports `EX` `PX` `EXAT` `PXAT` `KEEPTTL` `NX` `XX` `GET`.

**Keys** — `DEL` `UNLINK` `EXISTS` `TYPE` `KEYS` `SCAN` `EXPIRE` `PEXPIRE` `EXPIREAT` `PEXPIREAT` `TTL` `PTTL` `PERSIST`
`RENAME` `RENAMENX` `RANDOMKEY` `DBSIZE` `FLUSHDB` `FLUSHALL`

**Lists** — `LPUSH` `RPUSH` `LPUSHX` `RPUSHX` `LPOP` `RPOP` `BLPOP` `BRPOP` `LLEN` `LRANGE` `LINDEX` `LSET` `LREM`
`LTRIM` `LINSERT` `RPOPLPUSH`

**Sets** — `SADD` `SREM` `SMEMBERS` `SISMEMBER` `SCARD` `SPOP` `SRANDMEMBER` `SMOVE` `SDIFF` `SINTER` `SUNION`
`SDIFFSTORE`

**Hashes** — `HSET` `HMSET` `HSETNX` `HGET` `HMGET` `HGETALL` `HKEYS` `HVALS` `HDEL` `HEXISTS` `HLEN` `HSTRLEN`
`HINCRBY`

**Sorted sets** — `ZADD` `ZSCORE` `ZMSCORE` `ZRANK` `ZREVRANK` `ZRANGE` `ZRANGEBYSCORE` `ZREVRANGEBYSCORE` `ZREVRANGE`
`ZCOUNT` `ZCARD` `ZREM` `ZREMRANGEBYRANK` `ZREMRANGEBYSCORE` `ZINCRBY` `ZPOPMIN` `ZPOPMAX`
: `ZADD` supports `NX` `XX` `CH` `GT` `LT`. Tied members sort lexicographically, as Redis does.

**Streams** — `XADD` `XRANGE` `XREVRANGE` `XLEN` `XDEL` `XTRIM` `XREAD` `XINFO`
: `XADD` supports `NOMKSTREAM`, `MAXLEN`, `MINID`. `XREAD` supports `BLOCK`, `COUNT`, `STREAMS`.

**Transactions** — `MULTI` `EXEC` `DISCARD` `WATCH` `UNWUNWATCH` `RESET`
: A violated `WATCH` makes `EXEC` return a null array. `EXECABORT` is not modelled.

**Pub/Sub** — `SUBSCRIBE` `PSUBSCRIBE` `UNSUBSCRIBE` `PUNSUBSCRIBE` `PUBLISH` `PUBSUB`

**Geo** — `GEOADD` `GEOPOS` `GEODIST` `GEOHASH` `GEOSEARCH`
: `GEOSEARCH` accepts `FROMLONLAT` / `FROMMEMBER` with `BYRADIUS` only. `BYBOX` returns an error.

**Server** — `PING` `ECHO` `INFO` `CONFIG` `COMMAND` `CLIENT` `DEBUG` `SHUTDOWN`

**Auth** — `AUTH` `ACL` `HELLO`

**Persistence** — `SAVE` `BGSAVE` `LASTSAVE` `BGREWRITEAOF`

**Replication** — `REPLCONF` `PSYNC` `SYNC` `REPLICAOF` `SLAVEOF` `WAIT` `FAILOVER`
: `FAILOVER` always returns an error. `SLAVEOF` is an alias of `REPLICAOF`.

## Persistence

### RDB

Writes `REDIS0012` and reads versions 1–12. Objects are zlib-compressed; a load-time CRC64 mismatch is a warning, not
a failure. Expiry is stored as `OP_EXPIRETIME_MS` with a little-endian absolute millisecond deadline, and keys already
expired at write time are skipped.

Reads handle the plain encodings plus `SET_INTSET`, `LIST_QUICKLIST`, `LIST_QUICKLIST_2`, and the ziplist and LZF
decompressors. It does not decode zipmap, listpack, or the newer listpack-based hash and list encodings.

`SAVE` writes to `<path>.tmp` and renames, so a crash mid-write cannot corrupt the live dump.

**Streams are not serialised.** A stream key disappears across a restart. This is the most significant gap in
persistence.

### Append-only file

The Redis 7 multi-part layout, under `<dir>/<appenddirname>/`:

```
appendonly.aof.manifest
appendonly.aof.1.base.rdb      a full RDB of the keyspace
appendonly.aof.1.incr.aof      RESP command frames since
```

Every part name is derived from `--appendfilename`. The manifest is written last, after the new parts, and old parts are
deleted only once it lands — so an interrupted rewrite leaves a readable manifest.

On startup the base RDB is loaded and then each `.incr.aof` is replayed through the normal dispatcher. A trailing partial
record is discarded. Only write commands are logged.

`--appendfsync` is parsed, validated, and reported by `CONFIG GET`, but no code path branches on it; every write goes
straight through, and `fsync` happens when the file is closed.

## Replication

A replica walks a handshake state machine: `AUTH` (only when `--masterauth` is set) → `PING` → `REPLCONF
listening-port` → `REPLCONF capa` → `PSYNC ? -1` → await `+FULLRESYNC` → load the RDB payload → stream.

The master advances its offset by the byte length of each command frame it propagates. The replica advances by the bytes
it consumes, and for `REPLCONF GETACK` deliberately sends the ack *before* advancing, so the reported offset is the
pre-frame value — the ordering real Redis uses.

`WAIT` does not sleep. It snapshots the current offset, sends one `REPLCONF GETACK *` to each replica, and parks a
pending reply that `tick()` resolves once enough replicas have acked. The answer is the number of replicas that
acknowledged, not a count clamped to the argument.

Partial resynchronization is refused explicitly rather than faked: `PSYNC ?` returns `ERR Partial resynchronization not
supported (no backlog kept)`, because no backlog is kept.

A replica refuses writes with `READONLY You can't write against a read only replica.`, except for commands arriving from
the master itself.

## Keyspace expiry

Each value carries an absolute wall-clock `expiryMs`; `-1` means no deadline. There is no separate heap and no
time-ordered expiry structure.

- **Lazy** — every read and write path checks `isExpired()` first, so a stale key is never resurrected.
- **Active** — `tick()` sweeps the entire keyspace every 100 ms and removes whatever has expired. There is no sampling
  rate and no per-cycle cap. It is correct, and it is O(keys) per tick.

`TTL` returns `-2` for a missing key, `-1` for no deadline, and otherwise seconds rounded up.

## Running the tests

```bash
cmake -B build -S . && cmake --build ./build
./run_tests.sh          # defaults to port 7799
./run_tests.sh 7800
```

`run_tests.sh` starts `build/redis` against a fresh temporary directory, waits for the port, runs its assertions, prints
`=== N passed, M failed ===`, and exits nonzero on failure. It needs `python3`, `bash`, and a prebuilt `build/redis`.
Replies are rendered by `tools/rt.py`.

Roughly 320 assertions across 17 sections: `core`, `expiry`, `lists`, `sets`, `hashes`, `sorted sets`, `streams`,
`transactions`, `watch aborts`, `server + acl`, `geo`, `rdb round trip`, `aof`, `blocking`, `pubsub`, `auth`, and
`replica is read-only`.

There is no replication integration test beyond the read-only check, no `SCAN` cursor test, and no multi-key `EXEC`
rollback test.

## CLI options

| Flag | Default | Notes |
|---|---|---|
| `--port <n>` | `6379` | Out of range exits 1. |
| `--dir <path>` | `.` | Not `chdir`'d; every path is built from it. |
| `--dbfilename <name>` | `dump.rdb` | |
| `--appendonly <yes\|no>` | `no` | Anything else exits 1. |
| `--appendfilename <name>` | `appendonly.aof` | |
| `--appenddirname <dir>` | `appendonlydir` | |
| `--appendfsync <always\|everysec\|no>` | `everysec` | Validated and reported; never branched on. |
| `--requirepass <secret>` | — | Sugar for a password on the `default` user. |
| `--masterauth <secret>` | — | Sent as `AUTH` during the replica handshake. |
| `--user <name>` | `default` | |
| `--replicaof <host> <port>` | — | `--slaveof` also accepted; either `host port` or one quoted `"host port"`. |

An unrecognised argument prints `Bad argument: <arg>` and exits 1. There is no `--help`.

## Known limitations

Worth stating plainly, since several of these differ from real Redis in ways that will surprise you:

1. **RESP3 is not implemented.** `HELLO 3` is accepted and echoes `proto: 3`, but every reply is then encoded as RESP2.
2. **Streams are not persisted.** No RDB opcode is emitted for them.
3. **No partial resynchronization**, no replication backlog, and no automatic failover.
4. **`BGSAVE` and `BGREWRITEAOF` are synchronous.** They do the work on the event loop and then report success.
5. **Active expiry sweeps the whole keyspace every 100 ms.** No sampling, no budget.
6. **ACLs are parsed but not enforced as permissions.** Key patterns, channel patterns, and command rules are accepted
   and discarded; every authenticated user can run every command.
7. **`ACL LIST` and `ACL GETUSER` hash passwords differently** — `GETUSER` uses SHA-256, `LIST` uses `std::hash`. Only
   the former matches Redis.
8. **No stream consumer groups.** `XGROUP`, `XREADGROUP`, and `XACK` are absent.
9. **`INFO` reports mostly constants.** Uptime, connected clients, used memory, and command counts are all zero or
   placeholders.
10. **Writes are synchronous.** There is no write buffer and no `SO_SNDBUF` tuning, so a slow reader stalls the loop.
11. **No `QUIT` command is registered**, although the server closes the connection on that name — so `QUIT` returns
    `ERR unknown command` and then disconnects.
12. **No RDB auto-save.** `CONFIG GET save` returns a plausible-looking string that nothing acts on.

## Project layout

```
src/
  main.cpp                 startup, signal handling, argument parsing
  net/server.cpp           epoll loop, connection state, execute()
  net/socket.cpp           socket helpers
  protocol/resp.cpp        RESP2 encode/decode
  commands/                the registry and one file per command family
  store/                   the keyspace and its shared_mutex
  persistence/             RDB reader/writer/manager, AOF manager
  replication/             handshake, offset accounting, propagation
  auth/                    SHA-256
  types/                   value model, client session, errors
  utils/                   io, string helpers, time
run_tests.sh               the only test entry point
tools/rt.py                RESP renderer used by run_tests.sh
```

## Licence

No licence file is present in this repository. Add one before redistributing.
