# Redis Implementation in C++ 🚀

## Paaji eh hai Codecrafters Redis Challenge da solution!
**(Bro this is the Codecrafters Redis Challenge solution!)**

A complete Redis implementation in C++23 with **Pinglish comments** (Punjabi + English) throughout the codebase for extra fun! 🎉

---

## ✨ Features

### Implemented Commands

| Category | Commands |
|----------|----------|
| **Strings** | SET, GET, INCR |
| **Lists** | LPUSH, RPUSH, LRANGE |
| **Streams** | XADD, XRANGE, XREAD (with BLOCK) |
| **Server** | PING, ECHO, INFO, CONFIG, KEYS, TYPE |
| **Replication** | REPLCONF, PSYNC, WAIT |

### Architecture Features

- **Non-blocking I/O** with epoll
- **Thread-safe** data store with shared_mutex
- **RDB file loading** for persistence
- **Master-Replica** replication support
- **RESP protocol** parser

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                          main.cpp                                │
│           (Complete Redis Server - Single File)                  │
│                                                                  │
│  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐        │
│  │ Data Store    │  │ RESP Parser   │  │ RDB Loader    │        │
│  │ (thread-safe) │  │               │  │               │        │
│  └───────────────┘  └───────────────┘  └───────────────┘        │
│                                                                  │
│  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐        │
│  │ Command       │  │ Replication   │  │ Event Loop    │        │
│  │ Handlers      │  │ (PSYNC)       │  │ (epoll)       │        │
│  └───────────────┘  └───────────────┘  └───────────────┘        │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🔧 Building

```bash
# Build with CMake
cd codecrafters-redis-cpp
cmake -B build -S .
cmake --build build

# Or use the Codecrafters script
./your_program.sh
```

---

## 🚀 Running

### Basic Usage

```bash
# Start server on default port (6379)
./build/redis

# Start on custom port
./build/redis --port 6380

# Load RDB from directory
./build/redis --dir /tmp/redis-data --dbfilename dump.rdb
```

### Replication

```bash
# Start master
./build/redis --port 6379

# Start replica (connects to master)
./build/redis --port 6380 --replicaof localhost 6379
```

---

## 📝 Command Reference

### String Commands

#### PING
```
PING [message]
```
Returns PONG or echoes the message.

**Example:**
```bash
> PING
PONG
> PING "hello paaji"
hello paaji
```

#### SET
```
SET key value [EX seconds] [PX milliseconds] [NX|XX]
```
Set a key to a value with optional expiration.

**Options:**
- `EX seconds` - Set expiry in seconds
- `PX milliseconds` - Set expiry in milliseconds
- `NX` - Only set if key doesn't exist
- `XX` - Only set if key exists

**Example:**
```bash
> SET mykey "hello"
OK
> SET counter 100 EX 60
OK
```

#### GET
```
GET key
```
Get the value of a key.

**Example:**
```bash
> GET mykey
hello
> GET nonexistent
(nil)
```

#### INCR
```
INCR key
```
Increment the integer value of a key by one.

**Example:**
```bash
> SET counter 10
OK
> INCR counter
11
> INCR counter
12
```

---

### List Commands

#### LPUSH
```
LPUSH key value [value ...]
```
Insert values at the head of the list.

**Example:**
```bash
> LPUSH mylist "world"
1
> LPUSH mylist "hello"
2
```

#### RPUSH
```
RPUSH key value [value ...]
```
Insert values at the tail of the list.

**Example:**
```bash
> RPUSH mylist "foo"
3
```

#### LRANGE
```
LRANGE key start stop
```
Get a range of elements from a list.

**Example:**
```bash
> LRANGE mylist 0 -1
1) "hello"
2) "world"
3) "foo"
```

---

### Stream Commands

#### XADD
```
XADD key ID field value [field value ...]
```
Add an entry to a stream.

**ID formats:**
- `*` - Auto-generate ID
- `<timestamp>-*` - Use timestamp, auto-generate sequence
- `<timestamp>-<sequence>` - Explicit ID

**Example:**
```bash
> XADD mystream * field1 value1 field2 value2
1234567890-0
> XADD mystream 1-0 foo bar
1-0
```

#### XRANGE
```
XRANGE key start end [COUNT count]
```
Get a range of entries from a stream.

**Example:**
```bash
> XRANGE mystream - +
1) 1) "1-0"
   2) 1) "foo"
      2) "bar"
> XRANGE mystream 0-0 1-0
...
```

#### XREAD
```
XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] ID [ID ...]
```
Read entries from streams.

**Example:**
```bash
> XREAD STREAMS mystream 0
1) 1) "mystream"
   2) 1) 1) "1-0"
         2) 1) "foo"
            2) "bar"
> XREAD BLOCK 5000 STREAMS mystream $
(blocking...)
```

---

### Server Commands

#### ECHO
```
ECHO message
```
Echo the message back.

**Example:**
```bash
> ECHO "Hello Paaji!"
Hello Paaji!
```

#### INFO
```
INFO [section]
```
Get server information.

**Example:**
```bash
> INFO replication
# Replication
role:master
master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb
master_repl_offset:0
```

#### CONFIG GET
```
CONFIG GET parameter
```
Get configuration parameter value.

**Example:**
```bash
> CONFIG GET dir
1) "dir"
2) "/tmp/redis-data"
> CONFIG GET dbfilename
1) "dbfilename"
2) "dump.rdb"
```

#### KEYS
```
KEYS pattern
```
Find all keys matching a pattern.

**Example:**
```bash
> KEYS *
1) "mykey"
2) "counter"
> KEYS my*
1) "mykey"
```

#### TYPE
```
TYPE key
```
Get the type of a key.

**Example:**
```bash
> TYPE mykey
string
> TYPE mylist
list
> TYPE nonexistent
none
```

---

### Replication Commands

#### WAIT
```
WAIT numreplicas timeout
```
Wait for replicas to acknowledge writes.

**Example:**
```bash
> SET foo bar
OK
> WAIT 1 1000
1
```

---

## 🔄 Replication Flow

```
Master                          Replica
  │                               │
  │ <──────── PING ──────────────│
  │ ────────── PONG ─────────────>│
  │                               │
  │ <─────── REPLCONF ───────────│
  │ ──────────  OK  ─────────────>│
  │                               │
  │ <────────── PSYNC ───────────│
  │ ─────── FULLRESYNC ──────────>│
  │ ──────── RDB file ───────────>│
  │                               │
  │ (connected replicas)          │
  │                               │
  │ ──── propagate writes ───────>│
  │                               │
```

---

## 💾 RDB Persistence

The server loads data from RDB files on startup:

```bash
./build/redis --dir /tmp --dbfilename dump.rdb
```

Supported RDB features:
- String values
- Expiry timestamps (seconds and milliseconds)
- AUX metadata fields

---

## 🧪 Testing

### Manual Testing

```bash
# Start server
./build/redis &

# Test with netcat
printf '*1\r\n$4\r\nPING\r\n' | nc -q1 localhost 6379

# Test SET and GET
printf '*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n' | nc -q1 localhost 6379
printf '*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n' | nc -q1 localhost 6379
```

### Using redis-cli

```bash
redis-cli -p 6379 PING
redis-cli -p 6379 SET hello world
redis-cli -p 6379 GET hello
```

---

## 📜 Pinglish Comments Guide

Throughout the code, you'll find comments in Pinglish format:

```cpp
// Paaji eh function value set karda hai
// (Bro this function sets the value)
void setValue(...) { ... }
```

Key Pinglish terms:
- **Paaji/Paai** = Bro
- **Oye** = Hey
- **Ki haal hai** = How's it going
- **Sab theek** = All good
- **Koi nahi** = No worries
- **Wadiya** = Great/Excellent

---

## 📁 Project Structure

```
codecrafters-redis-cpp/
├── src/
│   └── main.cpp          # Complete Redis server
├── CMakeLists.txt        # Build configuration
├── your_program.sh       # Codecrafters run script
└── README_IMPLEMENTATION.md
```

---

## 🎯 Codecrafters Challenge Stages

This implementation covers:

1. ✅ Bind to a port
2. ✅ Respond to PING
3. ✅ Respond to multiple PINGs
4. ✅ Handle concurrent clients
5. ✅ Implement ECHO command
6. ✅ Implement SET & GET commands
7. ✅ Expiry for SET
8. ✅ Multiple clients
9. ✅ RDB config
10. ✅ RDB read key
11. ✅ RDB read string
12. ✅ RDB read multiple keys
13. ✅ RDB read with expiry
14. ✅ Replication - INFO
15. ✅ Replication - REPLCONF, PSYNC
16. ✅ Replication - handshake
17. ✅ Replication - ACK
18. ✅ Streams - XADD, XRANGE, XREAD

---

## 🤝 Contributing

Feel free to add more commands or improve the implementation!
Remember to add Pinglish comments - that's the fun part! 😄

---

## 📄 License

This is part of the Codecrafters Redis challenge.

---

**Made with ❤️ and lots of Pinglish comments!**

*Paaji, coding mein maza aana chahida hai!*
*(Bro, coding should be fun!)*
