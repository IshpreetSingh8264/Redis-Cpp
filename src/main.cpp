/**
 * main.cpp -- bootstrap.
 *
 * Rule 1: a thin entry point. Parse the arguments, construct the server, run
 * it, exit with its status. There is no domain logic here and there is nothing
 * in this file that a command could be implemented in.
 */
#include <csignal>
#include <cstdlib>
#include <iostream>

#include "net/server.hpp"
#include "types/server_config.hpp"

int main(int argc, char** argv) {
    // Every line the server prints has to reach the harness before the process
    // is judged, so do not let it sit in a buffer.
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    // A replica whose master hangs up mid-write must get EPIPE, not death.
    std::signal(SIGPIPE, SIG_IGN);

    redis::ServerConfig config;
    if (!redis::RedisServer::parseArguments(argc, argv, config)) return 1;

    redis::RedisServer server(std::move(config));
    server.run();
    return 0;
}
