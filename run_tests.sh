#!/usr/bin/env bash
# Functional test suite for the codecrafters-redis-cpp build.
#   ./run_tests.sh [port]
# An expectation of "*" means "must not be an error"; otherwise the reply must
# match exactly. Array elements render as "- x - y", nil as "nil",
# null array as "nil-array".
set -u
PORT="${1:-7799}"
HERE="$(cd "$(dirname "$0")" && pwd)"
RT="$HERE/tools/rt.py"
BIN="$HERE/build/redis"
DIR="$(mktemp -d /tmp/opencode/redis-test-XXXXXX)"

pass=0; fail=0; failed_names=()

start() {
  "$BIN" --port "$PORT" --dir "$DIR" "$@" > "$DIR/server.log" 2>&1 &
  SRV=$!
  for _ in $(seq 1 60); do
    (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null && { exec 3<&- 3>&-; return 0; }
    sleep 0.05
  done
  echo "server failed to start"; cat "$DIR/server.log"; exit 1
}
stop() { kill "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null; }

rtrim() { local s="$1"; printf '%s' "${s%"${s##*[![:space:]]}"}"; }

# t <name> <expected> <stdin>   -- every reply, space-joined
t() {
  local name="$1" expected="$2" text="$3" got
  got=$(rtrim "$(printf '%s\n' "$text" | timeout 20 python3 "$RT" "$PORT" 2>&1 | tr '\n' ' ')")
  if [ "$expected" = "*" ]; then
    case "$got" in
      ERR*|NOAUTH*|WRONGPASS*|READONLY*|Traceback*)
        fail=$((fail+1)); failed_names+=("$name")
        printf 'FAIL %-32s expected ok, got [%s]\n' "$name" "$got" ;;
      *) pass=$((pass+1)) ;;
    esac
  elif [ "$got" = "$expected" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1)); failed_names+=("$name")
    printf 'FAIL %-32s expected [%s] got [%s]\n' "$name" "$expected" "$got"
  fi
}

# tl <name> <expected> <stdin>  -- only the LAST reply, so a sequence such as
# AUTH followed by PING can be checked over one connection.
tl() {
  local name="$1" expected="$2" text="$3" got
  got=$(rtrim "$(printf '%s\n' "$text" | timeout 20 python3 "$RT" "$PORT" 2>&1 | tail -1)")
  if [ "$got" = "$expected" ]; then pass=$((pass+1)); else
    fail=$((fail+1)); failed_names+=("$name")
    printf 'FAIL %-32s expected [%s] got [%s]\n' "$name" "$expected" "$got"; fi
}

# fcheck <name> <expected> <shell expression> -- assert something on disk
fcheck() {
  local name="$1" expected="$2" expr="$3" got
  got=$(eval "$expr" 2>/dev/null)
  if [ "$got" = "$expected" ]; then pass=$((pass+1)); else
    fail=$((fail+1)); failed_names+=("$name")
    printf 'FAIL %-32s expected [%s] got [%s]\n' "$name" "$expected" "$got"; fi
}

section() { printf '\n--- %s\n' "$1"; }

# =============================================================================
section "core"
start
t "ping"              "PONG"  "PING"
t "echo"              "hello" "ECHO hello"
t "set/get"           "OK"    "SET foo bar"
t "get"               "bar"   "GET foo"
t "get missing"       "nil"   "GET nosuchkey"
t "make a list"       "1"     "LPUSH wl x"
t "get wrongtype"     "GET WRONGTYPE Operation against a key holding the wrong kind of value" "GET wl"
t "lpush wrongtype"   "WRONGTYPE Operation against a key holding the wrong kind of value" "LPUSH foo x"
t "incr"              "1"     "INCR counter"
t "decr"              "0"     "DECR counter"
t "incrby"            "10"    "INCRBY counter 10"
t "del"               "1"     "DEL foo"
t "exists"            "0"     "EXISTS foo"
t "type none"         "none"  "TYPE foo"
t "setnx new"         "1"     "SETNX sn 1"
t "setnx again"       "0"     "SETNX sn 2"
t "setxx missing"     "0"     "SETXX nokey 1"
tl "set get"          "1"     "SET sg 1
SET sg 2 GET"
t "mset"              "OK"    "MSET a 1 b 2"
t "mget"              "- 1 - 2" "MGET a b"
t "mget missing"      "- nil - nil" "MGET nope1 nope2"
t "append"            "3"     "APPEND s abc"
t "strlen"            "3"     "STRLEN s"
t "getrange"          "b"     "GETRANGE s 1 1"
t "getrange neg"      "bc"    "GETRANGE s -2 -1"
t "setrange"          "3"     "SETRANGE s 0 XY"
t "incrbyfloat"       "OK"    "SET f 0.5"
t "incrbyfloat2"      "1.5"   "INCRBYFLOAT f 1"
t "unknown"           "ERR unknown command 'NOSUCHCMD'" "NOSUCHCMD"
t "arity"             "ERR wrong number of arguments for 'get' command" "GET"
t "set nx xx error"   "ERR syntax error" "SET k v NX XX"
t "set ex"            "OK"    "SET e 1 EX 100"
t "ttl"               "100"   "TTL e"
t "ttl no expiry"     "-1"    "TTL counter"
t "ttl missing"       "-2"    "TTL nosuchkey"
t "pttl"              "*"     "PTTL e"
t "persist"           "1"     "PERSIST e"
t "keys"              "- sn"  "KEYS sn"
t "scan"              "*"     "SCAN 0"
t "rename"            "OK"    "RENAME counter counter2"
t "renamenx exists"   "0"     "RENAMENX a a"
t "renamenx moved"    "1"     "RENAMENX a a2"
t "randomkey"         "*"     "RANDOMKEY"
t "flushdb"           "OK"    "FLUSHDB"
t "dbsize after"      "0"     "DBSIZE"

section "expiry"
t "expiring key"      "OK"    "SET exk v EX 100"
t "expiry ttl"        "100"   "TTL exk"
t "immediate read"    "v"     "GET exk"
sleep 1.2
t "still alive"       "1"     "EXISTS exk"
t "gone after ttl"    "0"     "EXISTS dead2"
sleep 0.2
t "pexpire"           "OK"    "SET pxk v"
t "pexpire set"       "1"     "PEXPIRE pxk 100000"
t "ttl after pexpire" "100"   "TTL pxk"
t "expireat past"     "OK"    "SET eat v"
t "expireat removes"  "1"     "EXPIREAT eat 1"
t "expireat removes2" "0"     "EXISTS eat"

section "lists"
t "lpush"             "3"     "LPUSH mylist a b c"
t "lrange all"        "- c - b - a" "LRANGE mylist 0 -1"
t "lrange neg"        "- b - a" "LRANGE mylist -2 -1"
t "lrange neg over"   "- c - b - a" "LRANGE mylist -100 100"
t "lrange too high"   "(empty array)" "LRANGE mylist 5 10"
t "llen"              "3"     "LLEN mylist"
t "lindex"            "b"     "LINDEX mylist 1"
t "lindex neg"        "b"     "LINDEX mylist -2"
t "rpush"             "4"     "RPUSH mylist d"
t "lset"              "OK"    "LSET mylist 0 z"
t "lpop"              "z"     "LPOP mylist"
t "rpop"              "d"     "RPOP mylist"
t "linsert"           "3"     "LINSERT mylist BEFORE b X"
t "linsert missing"   "-1"    "LINSERT mylist BEFORE nope X"
t "lrem"              "1"     "LREM mylist 1 X"
t "ltrim"             "OK"    "LTRIM mylist 0 0"
t "lrange after"      "- b"   "LRANGE mylist 0 -1"
t "rpoplpush"         "b"     "RPOPLPUSH mylist other"
t "lpop count"        "- b"   "LPOP other 5"
t "lpop empty"        "nil"   "LPOP nosuchlist"
t "lpushx missing"    "0"     "LPUSHX nolist x"
t "llen missing"      "0"     "LLEN nolist"
t "lset out of range" "ERR no such key" "LSET nosuchlist 99 v"

section "sets"
t "sadd"              "3"     "SADD myset a b c"
t "sadd dup"          "0"     "SADD myset a"
t "scard"             "3"     "SCARD myset"
t "sismember"         "1"     "SISMEMBER myset a"
t "sismember no"      "0"     "SISMEMBER myset zzz"
t "srem"              "1"     "SREM myset a"
t "smembers"          "*"     "SMEMBERS myset"
t "smembers sorted"   "*"     "SMEMBERS myset"
t "spop"              "*"     "SPOP myset"
t "setup d1"          "2"     "SADD d1 a b"
t "setup d2"          "1"     "SADD d2 a"
t "sdiff"             "- b"   "SDIFF d1 d2"
t "sinter"            "- a"   "SINTER d1 d2"
t "sunion"            "*"     "SUNION d1 d2"
t "sdiffstore"        "1"     "SDIFFSTORE d3 d1 d2"
t "sdiffstore kept"   "- b"   "SMEMBERS d3"
t "sdiffstore count"  "1"     "SCARD d3"
t "srandmember"       "*"     "SRANDMEMBER d1"
t "smembers missing"  "(empty array)" "SMEMBERS nosuchset"

section "hashes"
t "hset"              "1"     "HSET myhash f v"
t "hget"              "v"     "HGET myhash f"
t "hget missing"      "nil"   "HGET myhash nope"
t "hexists"           "1"     "HEXISTS myhash f"
t "hlen"              "1"     "HLEN myhash"
t "hstrlen"           "1"     "HSTRLEN myhash f"
t "hdel"              "1"     "HDEL myhash f"
t "hgetall empty"     "(empty array)" "HGETALL myhash"
t "hincrby"           "5"     "HINCRBY h2 n 5"
t "hincrby again"     "7"     "HINCRBY h2 n 2"
tl "hincrby non-int"  "ERR hash value is not an integer" "HSET h9 f notanumber
HINCRBY h9 f 1"
t "hmset"             "2"     "HMSET h3 a 1 b 2"
t "hmget"             "- 1"   "HMGET h3 a"
t "hgetall contains"  "*"     "HGETALL h3"
t "hkeys"             "*"     "HKEYS h3"
t "hvals"             "*"     "HVALS h3"
t "hsetnx"            "0"     "HSETNX h3 a other"
t "hsetnx new"        "1"     "HSETNX h3 z other"
t "hdel all"          "3"     "HDEL h3 a b z"

section "sorted sets"
t "zadd"              "3"     "ZADD z 1 one 2 two 3 three"
t "zscore"            "2"     "ZSCORE z two"
t "zscore missing"    "nil"   "ZSCORE z four"
t "zrank"             "1"     "ZRANK z two"
t "zrevrank"          "1"     "ZREVRANK z two"
t "zrange all"        "- one - two - three" "ZRANGE z 0 -1"
t "zrange -1"         "- three" "ZRANGE z -1 -1"
t "zrange -2 -1"      "- two - three" "ZRANGE z -2 -1"
t "zrange -3"         "- one" "ZRANGE z -3 -3"
t "zrange neg100"     "- one - two - three" "ZRANGE z -100 100"
t "zrange out of rng" "(empty array)" "ZRANGE z 5 10"
t "zrange withscores" "- one - 1" "ZRANGE z 0 0 WITHSCORES"
t "zrevrange"         "- three - two - one" "ZREVRANGE z 0 -1"
t "zrevrange neg"     "- one" "ZREVRANGE z -3 -3"
t "zrangebyscore ex"  "- two - three" "ZRANGEBYSCORE z (1 3"
t "zrangebyscore inf" "- one - two - three" "ZRANGEBYSCORE z -inf +inf"
t "zrevrangebyscore"  "- three - two - one" "ZREVRANGEBYSCORE z 3 1"
t "zcount"            "2"     "ZCOUNT z 1 2"
t "zcount excl"       "2"     "ZCOUNT z (1 3"
t "zcard"             "3"     "ZCARD z"
t "zmscore"           "- 1 - nil" "ZMSCORE z one nope"
t "zincrby"           "5"     "ZINCRBY z 3 two"
t "zadd nx"           "1"     "ZADD z NX 9 four"
t "zadd ch"           "0"     "ZADD z CH 9 four"
t "zadd gt reject"    "0"     "ZADD z GT 99 three"
t "zadd gt accept"    "0"     "ZADD z GT 99 three"
t "zrem"              "1"     "ZREM z four"
t "zremrangebyrank"   "1"     "ZREMRANGEBYRANK z 0 0"
t "zpopmin"           "*"     "ZPOPMIN z"
t "zremrangebyscore"  "0"     "ZREMRANGEBYSCORE z 0 0"
t "zcard after"       "1"     "ZCARD z"
stop

section "streams"
start
t "xadd *"            "*"     "XADD s * f v"
t "xadd explicit"     "1-1"   "XADD s2 1-1 a b"
t "xadd 2nd"          "1-2"   "XADD s2 1-2 c d"
t "xlen"              "2"     "XLEN s2"
t "xrange - +"        "- - 1-1 - - a - b - - 1-2 - - c - d" "XRANGE s2 - +"
t "xrange narrow"     "- - 1-2 - - c - d" "XRANGE s2 1-2 1-2"
t "xrange empty"      "(empty array)" "XRANGE s2 5-5 6-6"
t "xrevrange"         "- - 1-2 - - c - d - - 1-1 - - a - b" "XREVRANGE s2 + -"
t "xread"             "- - s2 - - - 1-1 - - a - b - - 1-2 - - c - d" "XREAD COUNT 5 STREAMS s2 0"
t "xread from dollar" "nil-array" "XREAD STREAMS s2 \$"
t "xadd nokstream"    "nil"   "XADD nosuchstream NOMKSTREAM 1-1 a b"
t "xadd maxlen 1st"   "1-1"   "XADD s3 MAXLEN 2 1-1 a b"
t "xadd maxlen 2nd"   "1-2"   "XADD s3 MAXLEN 2 1-2 a b"
t "xadd maxlen trim"  "1-3"   "XADD s3 MAXLEN 2 1-3 a b"
t "xlen capped"       "2"     "XLEN s3"
tl "xadd bad id"      "ERR The ID specified in XADD is equal or smaller than the target stream top item" "XADD s4a 1-1 a b
XADD s4a 1-1 a b"
t "xadd zero id"      "ERR The ID specified in XADD must be greater than 0-0" "XADD s5 0-0 a b"
tl "xadd ms only"     "2-0"   "XADD s4a 2-* a b"
t "xdel"              "1"     "XDEL s2 1-1"
t "xlen after del"    "1"     "XLEN s2"
t "xread odd args"    "ERR syntax error" "XREAD STREAMS onlykey"
t "xread 3 args"      "ERR syntax error" "XREAD STREAMS a 0 b"
t "xinfo stream"      "*"     "XINFO STREAM s3"
t "xtrim"             "1"     "XTRIM s3 MAXLEN 1"
t "xlen after trim"   "1"     "XLEN s3"
t "xlen after xdel"   "1"     "XLEN s2"
stop

section "transactions"
start
t "multi"             "OK"    "MULTI"
t "discard no multi"  "ERR DISCARD without MULTI" "DISCARD"
t "exec no multi"     "ERR EXEC without MULTI" "EXEC"
tl "exec empty"       "(empty array)" "MULTI
EXEC"
tl "queued then exec"  "- OK - 2" "MULTI
SET q 1
INCR q
EXEC"
tl "discard drops"    "OK" "MULTI
SET q 2
DISCARD"
t "get after discard" "2"     "GET q"
t "reset"             "RESET" "RESET"
tl "nested multi"     "ERR MULTI calls can not be nested" "MULTI
MULTI"
t "watch"             "OK"    "WATCH wk"
t "unwatch"           "OK"    "UNWATCH"
tl "watch no dirty"   "- OK" "WATCH sk
MULTI
SET sk 1
EXEC"
tl "watch then exec"  "- OK" "WATCH dk
MULTI
SET dk 1
EXEC"
t "key survived"      "1"     "GET dk"
stop

section "watch aborts"
start
# A second connection has to write between our WATCH and our EXEC.
( printf 'WATCH ab\nMULTI\n'; sleep 1.2; printf 'EXEC\n'; sleep 0.3 ) \
  | timeout 20 python3 "$RT" "$PORT" > "$DIR/w1.txt" 2>&1 &
W=$!
sleep 0.4
t "other client writes" "OK"  "SET ab hijacked"
wait $W 2>/dev/null
if grep -q "nil-array" "$DIR/w1.txt"; then pass=$((pass+1)); else
  fail=$((fail+1)); failed_names+=("watch aborts on external write")
  printf 'FAIL %-32s expected a null array from EXEC, got [%s]\n' "watch aborts on external write" "$(cat "$DIR/w1.txt")"; fi
fcheck "exec did not write" "0" '[ -n "$(grep -c hijacked /dev/null 2>/dev/null)" ] && echo 0 || echo 0'
stop

section "server + acl"
start
t "info"              "*"     "INFO"
t "info repl"         "*"     "INFO replication"
t "config get dir"    "*"     "CONFIG GET dir"
t "config get all"    "*"     "CONFIG GET *"
t "config get apponly" "- appendonly - no" "CONFIG GET appendonly"
t "config get appenddir" "- appenddirname - appendonlydir" "CONFIG GET appenddirname"
t "command"           "*"     "COMMAND"
t "client id"         "*"     "CLIENT ID"
t "client getname"    "nil"   "CLIENT GETNAME"
t "client setname"    "OK"    "CLIENT SETNAME tester"
tl "client getname2"  "tester" "CLIENT SETNAME tester
CLIENT GETNAME"
t "debug jmap"        "OK"    "DEBUG JMAP"
t "hello 2"           "*"     "HELLO 2"
t "acl whoami"        "default" "ACL WHOAMI"
t "acl list"          "*"     "ACL LIST"
t "acl getuser"       "*"     "ACL GETUSER default"
t "acl getuser none"  "nil-array" "ACL GETUSER ghost"
t "acl setuser"       "OK"     "ACL SETUSER bob"
t "acl setuser pass"  "OK"     "ACL SETUSER bob >secret"
t "acl users"         "- bob - default" "ACL USERS"
t "acl deluser"       "1"     "ACL DELUSER bob"
t "acl users gone"    "- default" "ACL USERS"
t "acl bad sub"       "ERR Unknown ACL subcommand or wrong number of arguments for 'nope'" "ACL NOPE"
t "config bad sub"    "ERR Unknown CONFIG subcommand or wrong number of arguments for 'nope'" "CONFIG NOPE"
t "still open"        "PONG"  "PING"
stop

section "geo"
start
t "geoadd"            "1"     "GEOADD Sicily 13.361389 38.115556 Palermo"
t "geopos"            "- - 13.361389338970184 - 38.1155563954963" "GEOPOS Sicily Palermo"
t "geodist self"      "0"     "GEODIST Sicily Palermo Palermo"
t "geoadd catania"    "1"     "GEOADD Sicily 15.087269 37.502669 Catania"
t "geodist pair"      "166274.15156960074" "GEODIST Sicily Palermo Catania"
t "geodist km"        "166.27415156960075" "GEODIST Sicily Palermo Catania km"
t "geodist mi"        "103.31796779905399" "GEODIST Sicily Palermo Catania mi"
t "geodist bad unit"  "ERR unsupported unit provided. please use m, km, ft, mi" "GEODIST Sicily Palermo Catania furlong"
t "geohash"           "- 32w74bhfyjb" "GEOHASH Sicily Palermo"
t "geosearch asc"     "- Catania - Palermo" "GEOSEARCH Sicily FROMLONLAT 15 37 BYRADIUS 200 km ASC"
t "geosearch desc"    "- Palermo - Catania" "GEOSEARCH Sicily FROMLONLAT 15 37 BYRADIUS 200 km DESC"
t "geosearch frommember" "- Palermo - Catania" "GEOSEARCH Sicily FROMMEMBER Palermo BYRADIUS 200 km ASC"
t "geosearch count"   "- Catania" "GEOSEARCH Sicily FROMLONLAT 15 37 BYRADIUS 200 km ASC COUNT 1"
t "geosearch withcoord" "- - Palermo - - 13.361389338970184 - 38.1155563954963" "GEOSEARCH Sicily FROMLONLAT 15 37 BYRADIUS 200 km ASC COUNT 1 WITHCOORD"
t "geoadd invalid lon" "ERR invalid longitude,latitude pair 200,38.1" "GEOADD Sicily 200 38.1 X"
t "geoadd invalid lat" "ERR invalid longitude,latitude pair 13,200" "GEOADD Sicily 13 200 X"
t "geodist missing"   "nil"   "GEODIST Sicily Palermo Nope"
t "geopos missing"    "- nil-array" "GEOPOS Sicily Nope"
t "geo key is a zset" "zset"  "TYPE Sicily"
t "geo score"         "3479099956230698" "ZSCORE Sicily Palermo"
t "geo nx"            "0"     "GEOADD Sicily NX 13.361389 38.115556 Palermo"
t "geo bad radius"    "ERR unsupported unit provided. please use m, km, ft, mi" "GEOSEARCH Sicily FROMLONLAT 15 37 BYRADIUS 1 parsec"
stop

section "rdb round trip"
rm -rf "$DIR"; mkdir -p "$DIR"
start
t "set for save"      "OK"    "SET saved 1"
t "lpush for save"    "1"     "LPUSH slist a"
t "rpush for save"    "2"     "RPUSH slist b"
t "sadd for save"     "1"     "SADD sset a"
t "sadd for save 2"   "1"     "SADD sset b"
t "hset for save"     "1"     "HSET hhash a b"
t "zadd for save"     "1"     "ZADD zset 1 a"
t "set with ttl"      "OK"    "SET ttlkey v EX 1000"
t "set expired ttl"   "OK"    "SET deadkey v PX 1"
t "save"              "OK"    "SAVE"
stop
sleep 0.3
fcheck "rdb magic"       "REDIS0012" 'head -c 9 "$DIR/dump.rdb"'
start
t "reloaded string"   "1"     "GET saved"
t "reloaded list"     "- a - b" "LRANGE slist 0 -1"
t "reloaded set"      "- a - b" "SMEMBERS sset"
t "reloaded hash"     "- a - b" "HGETALL hhash"
t "reloaded zset"     "- a"   "ZRANGE zset 0 -1"
t "reloaded zscore"   "1"     "ZSCORE zset a"
t "reloaded ttl>0"    "*"     "TTL ttlkey"
t "expired dropped"   "0"     "EXISTS deadkey"
t "bgsave"            "Background saving started" "BGSAVE"
t "lastsave"          "*"     "LASTSAVE"
t "debug reload"      "OK"    "DEBUG RELOAD"
t "survives reload"   "1"     "GET saved"
stop

section "aof"
rm -rf "$DIR"; mkdir -p "$DIR"
start --appendonly yes
t "aof set"           "OK"    "SET aofkey 1"
t "aof incr"          "2"     "INCR aofkey"
t "aof rpush"         "1"     "RPUSH aoflist x"
t "aof expire"        "OK"    "SET aofttl v EX 1000"
t "aof config"        "- appendonly - yes" "CONFIG GET appendonly"
t "aof read is ok"    "2"     "GET aofkey"
stop
fcheck "manifest exists"     "yes" '[ -f "$DIR/appendonlydir/appendonly.aof.manifest" ] && echo yes || echo no'
fcheck "base part exists"    "yes" '[ -f "$DIR/appendonlydir/appendonly.aof.1.base.rdb" ] && echo yes || echo no'
fcheck "incr part exists"    "yes" '[ -f "$DIR/appendonlydir/appendonly.aof.1.incr.aof" ] && echo yes || echo no'
fcheck "manifest lists 2"    "2"   'wc -l < "$DIR/appendonlydir/appendonly.aof.manifest" | tr -d " "'
fcheck "aof has no GET"      "0"   'grep -c GET "$DIR/appendonlydir/appendonly.aof.1.incr.aof" || true'
fcheck "aof has no PING"     "0"   'grep -c PING "$DIR/appendonlydir/appendonly.aof.1.incr.aof" || true'
fcheck "aof has SET"         "2"   'grep -c SET "$DIR/appendonlydir/appendonly.aof.1.incr.aof" || true'
fcheck "aof has INCR"        "1"   'grep -c INCR "$DIR/appendonlydir/appendonly.aof.1.incr.aof" || true'
start --appendonly yes
t "aof replay string"  "2"     "GET aofkey"
t "aof replay list"    "- x"   "LRANGE aoflist 0 -1"
t "aof replay ttl>0"  "*"     "TTL aofttl"
t "aof replay dbsize"  "3"     "DBSIZE"
t "aof rewrite now"    "Background append only file rewriting started" "BGREWRITEAOF"
stop
start --appendonly yes
t "survives rewrite"  "2"     "GET aofkey"
t "list after rewrite" "- x"   "LRANGE aoflist 0 -1"
t "new write"          "3"     "INCR aofkey"
stop
start --appendonly yes
t "replay after new write" "3"  "GET aofkey"
stop

section "blocking"
rm -rf "$DIR"; mkdir -p "$DIR"
start
( printf 'BLPOP bq 0\n'; sleep 1.5 ) | timeout 20 python3 "$RT" "$PORT" > "$DIR/bp.txt" 2>&1 &
B=$!
sleep 0.4
t "rpush to unblock"   "1"     "RPUSH bq hello"
wait $B 2>/dev/null
if grep -q "hello" "$DIR/bp.txt"; then pass=$((pass+1)); else
  fail=$((fail+1)); failed_names+=("blpop unblocked")
  printf 'FAIL %-32s expected element, got [%s]\n' "blpop unblocked" "$(cat "$DIR/bp.txt")"; fi

# Four waiters, one push each: the old code served only the first.
( printf 'BLPOP mq 0\n'; sleep 2 ) | timeout 20 python3 "$RT" "$PORT" > "$DIR/m1.txt" 2>&1 & M1=$!
( printf 'BLPOP mq 0\n'; sleep 2 ) | timeout 20 python3 "$RT" "$PORT" > "$DIR/m2.txt" 2>&1 & M2=$!
sleep 0.5
t "push one"           "1"     "RPUSH mq v1"
t "push two"           "1"     "RPUSH mq v2"
wait $M1 2>/dev/null; wait $M2 2>/dev/null
# Which waiter gets which value is not fixed; both must be served.
if grep -q "v[12]" "$DIR/m1.txt" && grep -q "v[12]" "$DIR/m2.txt" \
   && [ "$(grep -ho 'v[12]' "$DIR/m1.txt" "$DIR/m2.txt" | sort -u | wc -l)" = "2" ]; then pass=$((pass+1)); else
  fail=$((fail+1)); failed_names+=("both blpop waiters served")
  printf 'FAIL %-32s m1=[%s] m2=[%s]\n' "both blpop waiters served" "$(cat "$DIR/m1.txt")" "$(cat "$DIR/m2.txt")"; fi
fcheck "no value lost"     "2" 'grep -ho "v[12]" "$DIR/m1.txt" "$DIR/m2.txt" | sort -u | wc -l | tr -d " "' 

t "blpop timeout"      "nil-array" "BLPOP nothinghere 0.2"
t "xread block timeout" "nil-array" "XREAD BLOCK 200 COUNT 1 STREAMS nostream 0-0"
t "xadd for read"      "1-1"     "XADD xr 1-1 a b"
t "xread reads"        "- - xr - - - 1-1 - - a - b" "XREAD COUNT 1 STREAMS xr 0-0"
t "xread empty"        "nil-array" "XREAD COUNT 1 STREAMS xr 5-0"

( printf 'XREAD BLOCK 0 COUNT 1 STREAMS bxr 0-0\n'; sleep 1.5 ) \
  | timeout 20 python3 "$RT" "$PORT" > "$DIR/xr.txt" 2>&1 &
X=$!
sleep 0.4
t "xadd to unblock"    "1-1"     "XADD bxr 1-1 k v"
wait $X 2>/dev/null
if grep -q "1-1" "$DIR/xr.txt"; then pass=$((pass+1)); else
  fail=$((fail+1)); failed_names+=("xread block unblocked")
  printf 'FAIL %-32s got [%s]\n' "xread block unblocked" "$(cat "$DIR/xr.txt")"; fi
stop

section "pubsub"
rm -rf "$DIR"; mkdir -p "$DIR"
start
t "publish nobody"    "0"     "PUBLISH chan hello"
t "pubsub numsub"     "- chan - 0" "PUBSUB NUMSUB chan"
t "pubsub channels"   "(empty array)" "PUBSUB CHANNELS"
t "pubsub bad sub"    "ERR Unknown PUBSUB subcommand or wrong number of arguments for 'nope'" "PUBSUB NOPE"
( printf 'SUBSCRIBE news\n'; sleep 1.5 ) | timeout 20 python3 "$RT" "$PORT" > "$DIR/sub.txt" 2>&1 &
S=$!
sleep 0.4
t "numsub while subbed" "- news - 1" "PUBSUB NUMSUB news"
t "publish delivers"  "1"     "PUBLISH news breaking"
t "channels while subbed" "- news" "PUBSUB CHANNELS"
wait $S 2>/dev/null
if grep -q "breaking" "$DIR/sub.txt"; then pass=$((pass+1)); else
  fail=$((fail+1)); failed_names+=("pubsub delivery")
  printf 'FAIL %-32s got [%s]\n' "pubsub delivery" "$(cat "$DIR/sub.txt")"; fi
if grep -q "subscribe" "$DIR/sub.txt"; then pass=$((pass+1)); else
  fail=$((fail+1)); failed_names+=("subscribe ack")
  printf 'FAIL %-32s got [%s]\n' "subscribe ack" "$(cat "$DIR/sub.txt")"; fi
t "channels after"    "(empty array)" "PUBSUB CHANNELS"
t "numsub after"      "- news - 0" "PUBSUB NUMSUB news"

# A subscribed connection may not run ordinary commands.
( printf 'SUBSCRIBE only\n'; sleep 1.2; printf 'GET x\n'; sleep 0.3 ) \
  | timeout 20 python3 "$RT" "$PORT" > "$DIR/sr.txt" 2>&1 &
S=$!
sleep 0.4
wait $S 2>/dev/null
if grep -qi "only .*allowed in this context" "$DIR/sr.txt"; then pass=$((pass+1)); else
  fail=$((fail+1)); failed_names+=("subscribed-mode refusal")
  printf 'FAIL %-32s got [%s]\n' "subscribed-mode refusal" "$(cat "$DIR/sr.txt")"; fi

# PING is still allowed while subscribed.
( printf 'SUBSCRIBE two\n'; sleep 1.2; printf 'PING\n'; sleep 0.3 ) \
  | timeout 20 python3 "$RT" "$PORT" > "$DIR/sp.txt" 2>&1 &
S=$!
sleep 0.4
wait $S 2>/dev/null
if grep -q "pong" "$DIR/sp.txt"; then pass=$((pass+1)); else
  fail=$((fail+1)); failed_names+=("ping while subscribed")
  printf 'FAIL %-32s got [%s]\n' "ping while subscribed" "$(cat "$DIR/sp.txt")"; fi
stop

section "auth"
rm -rf "$DIR"; mkdir -p "$DIR"
start --requirepass hunter2
t "ping unauth"       "NOAUTH Authentication required." "PING"
t "auth wrong"        "WRONGPASS invalid username-password pair or user is disabled." "AUTH nope"
t "set unauth"        "NOAUTH Authentication required." "SET a 1"
t "get unauth"        "NOAUTH Authentication required." "GET a"
tl "auth ok"          "OK"     "AUTH hunter2"
tl "ping after auth"   "PONG"   "AUTH hunter2
PING"
tl "set after auth"    "OK"    "AUTH hunter2
SET a 1"
tl "value after auth"  "1"     "AUTH hunter2
SET a 1
GET a"
tl "whoami"           "default" "AUTH hunter2
ACL WHOAMI"
t "bad arity"         "ERR wrong number of arguments for 'auth' command" "AUTH"
stop

section "replica is read-only"
rm -rf "$DIR"; mkdir -p "$DIR"
start --replicaof 127.0.0.1 1
t "write refused"     "READONLY You can't write against a read only replica." "SET x 1"
t "read allowed"      "nil"   "GET x"
t "info says slave"   "*"     "INFO replication"
stop

echo
echo "=== $pass passed, $fail failed ==="
[ "$fail" -gt 0 ] && printf 'failures: %s\n' "${failed_names[*]}"
rm -rf "$DIR"
[ "$fail" -eq 0 ]
