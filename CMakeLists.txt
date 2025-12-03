cmake_minimum_required(VERSION 3.10)
project(vll_db_project)

set(CMAKE_CXX_STANDARD 17)

include_directories(${CMAKE_SOURCE_DIR}/src)

find_package(Threads REQUIRED)

# Build microbenchmark executable
add_executable(bench_microbenchmark
    bench/microbenchmark.cpp
    src/core/vll_stman.cpp
    src/concurrency/vll.cpp
    src/concurrency/lock_manager_2pl.cpp
)

target_link_libraries(bench_microbenchmark PRIVATE Threads::Threads)