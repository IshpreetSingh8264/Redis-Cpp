# Redis Implementation in C++ 🚀

## Paaji eh hai Codecrafters Redis Challenge da solution!
**(Bro this is the Codecrafters Redis Challenge solution!)**

A complete, production-grade Redis implementation in C++23 with **Pinglish comments** (Punjabi + English) throughout the codebase for extra fun! 🎉

---

## 📋 Table of Contents

1. [Features](#-features)
2. [Architecture](#-architecture)
3. [File Structure](#-file-structure)
4. [Building](#-building)
5. [Running](#-running)
6. [Supported Commands](#-supported-commands)
7. [Replication](#-replication)
8. [Persistence](#-persistence)
9. [Authentication](#-authentication)
10. [Testing](#-testing)

---

## ✨ Features

### Data Structures
- **Strings** - SET, GET, INCR, DECR, MSET, MGET, APPEND, STRLEN
- **Lists** - LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, LINDEX, LREM, BLPOP, BRPOP
- **Sets** - SADD, SMEMBERS, SISMEMBER, SREM, SCARD
- **Sorted Sets** - ZADD, ZSCORE, ZRANK, ZRANGE, ZCOUNT, ZCARD, ZREM
- **Hashes** - HSET, HGET, HGETALL, HDEL, HEXISTS, HLEN
- **Streams** - XADD, XRANGE, XREAD, XLEN

### Server Features
- **Pub/Sub** - SUBSCRIBE, UNSUBSCRIBE, PUBLISH
- **Transactions** - MULTI, EXEC, DISCARD
- **Replication** - Master-Replica sync with PSYNC/REPLCONF
- **Persistence** - RDB file format (save/load)
- **Authentication** - AUTH command, ACL support
- **Geospatial** - GEOADD, GEOPOS, GEODIST, GEORADIUS, GEOHASH

### Performance
- **Non-blocking I/O** with epoll
- **Thread-safe** data store with shared_mutex
- **Edge-triggered** event handling
- **Efficient** RESP protocol parsing

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                           main.cpp                               │
│                   (Entry point, CLI parsing)                     │
└─────────────────────────────────┬───────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                          server.hpp                              │
│        (RedisServer - event loop, client management)             │
└─────────────┬───────────────┬───────────────┬───────────────────┘
              │               │               │
              ▼               ▼               ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────────────┐
│  commands.hpp   │ │  client.hpp     │ │    replication.hpp      │
│ (Cmd Handler)   │ │ (Connections)   │ │  (Master/Replica sync)  │
└────────┬────────┘ └─────────────────┘ └─────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│                        data_store.hpp                            │
│        (Thread-safe storage for all data types)                  │
└─────────────────────────────────────────────────────────────────┘
         ▲
         │
┌────────┴────────────────────────────────────────────────────────┐
│                                                                  │
│   ┌────────────────┐  ┌────────────────┐  ┌──────────────────┐  │
│   │ resp_parser.hpp│  │ geospatial.hpp │  │rdb_persistence.hpp│  │
│   │(RESP Protocol) │  │(Geo commands)  │  │(Save/Load RDB)   │  │
│   └────────────────┘  └────────────────┘  └──────────────────┘  │
│                                                                  │
│   ┌────────────────┐  ┌────────────────┐                        │
│   │authentication. │  │  common.hpp    │                        │
│   │hpp (ACL/AUTH)  │  │(Utils, Types)  │                        │
│   └────────────────┘  └────────────────┘                        │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

---

## 📁 File Structure

```
src/
├── main.cpp              # Entry point, CLI argument parsing
├── server.hpp            # Main server class, event loop
├── common.hpp            # Constants, types, utility functions
├── resp_parser.hpp       # RESP protocol parser/serializer
├── data_store.hpp        # Thread-safe data storage
├── client.hpp            # Client connection management
├── commands.hpp          # Command handler implementation
├── replication.hpp       # Master/Replica replication
├── authentication.hpp    # AUTH and ACL implementation
├── geospatial.hpp        # Geo commands (GEOADD, etc.)
└── rdb_persistence.hpp   # RDB file save/load
```

---

## 🔨 Building

### Prerequisites
- C++23 compatible compiler (GCC 13+, Clang 16+)
- CMake 3.16+
- vcpkg (for dependencies)

### Dependencies (via vcpkg)
- asio
- pthreads

### Build Commands

```bash
# Clone and enter directory
cd codecrafters-redis-cpp

# Configure
cmake -B build -S .

# Build
cmake --build build

# Or simply use the provided script
./your_program.sh
```

---

## 🚀 Running

### Basic Usage

```bash
# Default port 6379
./your_program.sh

# Custom port
./your_program.sh --port 6380

# As a replica
./your_program.sh --port 6380 --replicaof localhost 6379

# With RDB persistence
./your_program.sh --dir /tmp/redis --dbfilename dump.rdb

# With authentication
./your_program.sh --requirepass mysecretpassword
```

### CLI Options

| Option | Description | Default |
|--------|-------------|---------|
| `--port <port>` | Port to listen on | 6379 |
| `--replicaof <host> <port>` | Connect as replica to master | - |
| `--dir <path>` | RDB file directory | . |
| `--dbfilename <name>` | RDB filename | dump.rdb |
| `--requirepass <pass>` | Require AUTH password | - |
| `--help` | Show help | - |

---

## 📜 Supported Commands

### Strings
| Command | Syntax | Description |
|---------|--------|-------------|
| SET | `SET key value [EX s] [PX ms] [NX\|XX]` | Set string value |
| GET | `GET key` | Get string value |
| INCR | `INCR key` | Increment by 1 |
| DECR | `DECR key` | Decrement by 1 |
| INCRBY | `INCRBY key increment` | Increment by amount |
| DECRBY | `DECRBY key decrement` | Decrement by amount |
| MSET | `MSET key val [key val ...]` | Set multiple keys |
| MGET | `MGET key [key ...]` | Get multiple keys |
| APPEND | `APPEND key value` | Append to string |
| STRLEN | `STRLEN key` | Get string length |

### Lists
| Command | Syntax | Description |
|---------|--------|-------------|
| LPUSH | `LPUSH key val [val ...]` | Push to head |
| RPUSH | `RPUSH key val [val ...]` | Push to tail |
| LPOP | `LPOP key [count]` | Pop from head |
| RPOP | `RPOP key [count]` | Pop from tail |
| LRANGE | `LRANGE key start stop` | Get range |
| LLEN | `LLEN key` | Get length |
| LINDEX | `LINDEX key index` | Get by index |
| LREM | `LREM key count elem` | Remove elements |
| BLPOP | `BLPOP key [key ...] timeout` | Blocking pop (head) |
| BRPOP | `BRPOP key [key ...] timeout` | Blocking pop (tail) |

### Sets
| Command | Syntax | Description |
|---------|--------|-------------|
| SADD | `SADD key member [member ...]` | Add members |
| SMEMBERS | `SMEMBERS key` | Get all members |
| SISMEMBER | `SISMEMBER key member` | Check membership |
| SREM | `SREM key member [member ...]` | Remove members |
| SCARD | `SCARD key` | Get cardinality |

### Sorted Sets
| Command | Syntax | Description |
|---------|--------|-------------|
| ZADD | `ZADD key [NX\|XX] [GT\|LT] score member ...` | Add with score |
| ZSCORE | `ZSCORE key member` | Get score |
| ZRANK | `ZRANK key member` | Get rank |
| ZRANGE | `ZRANGE key start stop [WITHSCORES]` | Get range |
| ZCOUNT | `ZCOUNT key min max` | Count in range |
| ZCARD | `ZCARD key` | Get cardinality |
| ZREM | `ZREM key member [member ...]` | Remove members |

### Hashes
| Command | Syntax | Description |
|---------|--------|-------------|
| HSET | `HSET key field value [field value ...]` | Set fields |
| HGET | `HGET key field` | Get field |
| HGETALL | `HGETALL key` | Get all fields |
| HDEL | `HDEL key field [field ...]` | Delete fields |
| HEXISTS | `HEXISTS key field` | Check field exists |
| HLEN | `HLEN key` | Get field count |

### Streams
| Command | Syntax | Description |
|---------|--------|-------------|
| XADD | `XADD key ID field value ...` | Add entry |
| XRANGE | `XRANGE key start end [COUNT n]` | Get range |
| XREAD | `XREAD [COUNT n] [BLOCK ms] STREAMS key ... ID ...` | Read entries |
| XLEN | `XLEN key` | Get length |

### Key Commands
| Command | Syntax | Description |
|---------|--------|-------------|
| DEL | `DEL key [key ...]` | Delete keys |
| EXISTS | `EXISTS key [key ...]` | Check existence |
| TYPE | `TYPE key` | Get type |
| KEYS | `KEYS pattern` | Find keys |
| EXPIRE | `EXPIRE key seconds` | Set TTL (seconds) |
| PEXPIRE | `PEXPIRE key milliseconds` | Set TTL (ms) |
| TTL | `TTL key` | Get TTL (seconds) |
| PTTL | `PTTL key` | Get TTL (ms) |

### Pub/Sub
| Command | Syntax | Description |
|---------|--------|-------------|
| SUBSCRIBE | `SUBSCRIBE channel [channel ...]` | Subscribe |
| UNSUBSCRIBE | `UNSUBSCRIBE [channel ...]` | Unsubscribe |
| PUBLISH | `PUBLISH channel message` | Publish message |

### Transactions
| Command | Syntax | Description |
|---------|--------|-------------|
| MULTI | `MULTI` | Start transaction |
| EXEC | `EXEC` | Execute transaction |
| DISCARD | `DISCARD` | Discard transaction |

### Geospatial
| Command | Syntax | Description |
|---------|--------|-------------|
| GEOADD | `GEOADD key lon lat member ...` | Add location |
| GEOPOS | `GEOPOS key member [member ...]` | Get position |
| GEODIST | `GEODIST key m1 m2 [unit]` | Get distance |
| GEORADIUS | `GEORADIUS key lon lat radius unit` | Search radius |
| GEOHASH | `GEOHASH key member [member ...]` | Get hash |

### Server/Admin
| Command | Syntax | Description |
|---------|--------|-------------|
| PING | `PING [message]` | Test connection |
| ECHO | `ECHO message` | Echo message |
| INFO | `INFO [section]` | Server info |
| CONFIG | `CONFIG GET/SET param` | Config management |
| DBSIZE | `DBSIZE` | Key count |
| FLUSHDB | `FLUSHDB` | Clear database |
| AUTH | `AUTH [username] password` | Authenticate |
| CLIENT | `CLIENT ID/LIST/SETNAME/GETNAME` | Client info |

---

## 🔄 Replication

### Master-Replica Setup

```bash
# Start master
./your_program.sh --port 6379

# Start replica (separate terminal)
./your_program.sh --port 6380 --replicaof localhost 6379
```

### How It Works

1. **Handshake**: Replica sends PING, then REPLCONF commands
2. **PSYNC**: Replica requests synchronization
3. **RDB Transfer**: Master sends full RDB snapshot
4. **Command Propagation**: Master forwards write commands

### Replication Commands
- `REPLCONF` - Replication configuration
- `PSYNC` - Partial/Full sync request
- `WAIT` - Wait for replica acknowledgment

---

## 💾 Persistence

### RDB Files

RDB (Redis Database) files store a point-in-time snapshot.

```bash
# Configure RDB location
./your_program.sh --dir /data --dbfilename dump.rdb
```

### Features
- Automatic loading on startup
- Binary format for efficiency
- Supports all data types
- Expiry preservation

### RDB Format
```
[REDIS][VERSION][AUX_FIELDS][DB_DATA][EOF][CHECKSUM]
```

---

## 🔐 Authentication

### Simple AUTH

```bash
# Start with password
./your_program.sh --requirepass mypassword

# Connect and authenticate
redis-cli
> AUTH mypassword
OK
```

### ACL (Access Control List)

```
ACL SETUSER username on >password ~keys:* +@all
ACL LIST
ACL WHOAMI
```

---

## 🧪 Testing

### Using redis-cli

```bash
# Connect
redis-cli -p 6379

# Test commands
127.0.0.1:6379> SET foo bar
OK
127.0.0.1:6379> GET foo
"bar"
127.0.0.1:6379> LPUSH mylist a b c
(integer) 3
127.0.0.1:6379> LRANGE mylist 0 -1
1) "c"
2) "b"
3) "a"
```

### Codecrafters Testing

```bash
# Run codecrafters tests
codecrafters test
```

---

## 📝 Code Comments Style

This project uses **Pinglish** (Punjabi + English) comments for fun! 🎉

Example:
```cpp
// Paaji eh function keys find karda hai pattern naal match hunde
// (Bro this function finds keys matching the pattern)
std::vector<std::string> keys(const std::string& pattern) const {
    // Saari keys check karo te match hundi wapas karo
    // (Check all keys and return matching ones)
    ...
}
```

---

## 📄 License

MIT License - Feel free to use and modify!

---

## 🙏 Credits

- **Codecrafters** - For the amazing challenge
- **Redis** - For the inspiration
- **ASIO** - For async I/O

---

**Made with ❤️ and lots of chai! ☕**

*"Jiven Redis kaam karda, ohi implement kita hai!"*
*(We implemented exactly how Redis works!)*
