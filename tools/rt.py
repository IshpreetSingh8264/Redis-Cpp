#!/usr/bin/env python3
"""Raw RESP client for verifying the codecrafters-redis-cpp build.

Usage:
  rt.py <port> <<'EOF'
  SET foo bar
  GET foo
  EOF
Each line is one command, whitespace-split (use quotes for args with spaces).
Prints the decoded reply for each. `EXIT` quits.
"""
import socket
import sys


def encode(args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        b = a.encode()
        out += b"$%d\r\n%s\r\n" % (len(b), b)
    return out


class Reader:
    def __init__(self, sock):
        self.s = sock
        self.buf = b""

    def line(self):
        while b"\r\n" not in self.buf:
            d = self.s.recv(65536)
            if not d:
                raise EOFError
            self.buf += d
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def exact(self, n):
        """Read n payload bytes plus the trailing CRLF."""
        while len(self.buf) < n + 2:
            d = self.s.recv(65536)
            if not d:
                raise EOFError
            self.buf += d
        out, self.buf = self.buf[:n], self.buf[n + 2:]
        return out

    def reply(self):
        ln = self.line()
        t, rest = ln[:1], ln[1:]
        if t == b"+":
            return ("status", rest.decode())
        if t == b"-":
            return ("error", rest.decode())
        if t == b":":
            return ("int", int(rest))
        if t == b"$":
            n = int(rest)
            if n == -1:
                return ("nil",)
            return ("bulk", self.exact(n).decode("utf-8", "replace"))
        if t == b"*":
            n = int(rest)
            if n == -1:
                return ("nil-array",)
            return ("array", [self.reply() for _ in range(n)])
        if t == b"_":
            n = int(rest)
            if n == -1:
                return ("nil",)
            return ("null", self.exact(n).decode("utf-8", "replace"))
        if t == b"#":
            return ("bool", rest == b"t")
        if t == b",":
            return ("double", rest.decode())
        if t == b"(":
            return ("bignum", rest.decode())
        return ("raw", repr(ln))


def show(v):
    """Render a reply on one line. Array elements are separated by "|" and
    prefixed with "- ", so nested arrays stay unambiguous."""
    if v[0] == "nil":
        return "nil"
    if v[0] == "nil-array":
        return "nil-array"
    if v[0] == "array":
        if not v[1]:
            return "(empty array)"
        return " ".join("- " + show(x) for x in v[1])
    if v[0] == "bulk" and "\n" in v[1]:
        return "bulk:" + repr(v[1])
    if v[0] == "raw":
        return v[1]
    return " ".join(str(x) for x in v[1:])


def main():
    # --echo is off by default: tests compare replies, and a command echoed back
    # with spaces in it defeats any prefix-stripping on the caller side.
    argv = [a for a in sys.argv[1:] if a != "--echo"]
    echo = "--echo" in sys.argv
    port = int(argv[0])
    host = argv[1] if len(argv) > 1 else "127.0.0.1"
    s = socket.create_connection((host, port), timeout=15)
    s.settimeout(15)
    r = Reader(s)
    import shlex

    for line in sys.stdin:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        args = shlex.split(line)
        if not args:
            continue
        s.sendall(encode(args))
        try:
            reply = show(r.reply())
            print(("%s -> %s" % (line, reply)) if echo else reply)
        except (EOFError, socket.timeout) as e:
            if echo:
                print("%s -> <no reply: %s>" % (line, e))
            break

    # Drain anything the server pushed after the last command -- a pub/sub
    # message or an unblocked BLPOP arrives without the client asking.
    s.settimeout(0.4)
    while True:
        try:
            print(show(r.reply()))
        except (EOFError, socket.timeout, BlockingIOError):
            break
    s.close()


main()
