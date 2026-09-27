cmake_minimum_required(VERSION 3.13)

project(redis-starter-cpp)

# NOTE: src/*.hpp MUST stay in this glob. The previous `src/*.cpp`-only glob meant
# every header in the project was never compiled -- a whole architecture sat
# on disk, switched off, and nobody noticed.
file(GLOB_RECURSE SOURCE_FILES CONFIGURE_DEPENDS src/*.cpp src/*.hpp)

set(CMAKE_CXX_STANDARD 23) # Enable the C++23 standard
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(THREADS_PREFER_PTHREAD_FLAG ON)

find_package(Threads REQUIRED)

# Note: This implementation uses raw POSIX sockets instead of ASIO
# for better compatibility and learning purposes
# Paaji raw sockets use kar rahe - asio di zaroorat nahi!
# (We're using raw sockets - no need for asio!)

add_executable(redis ${SOURCE_FILES})

# Headers are included by their path from src/, so that the include graph reads
# the same way as the directory tree.
target_include_directories(redis PRIVATE src)

target_link_libraries(redis PRIVATE Threads::Threads)