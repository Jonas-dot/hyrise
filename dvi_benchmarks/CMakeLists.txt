# Standalone BTree Benchmarks
# NO Hyrise dependencies - pure C++ for accurate benchmarking
#
# Structure:
# src/     - BTree implementations and benchmark drivers
# tests/   - Unit tests
# results/ - Output snapshots (latest/)

cmake_minimum_required(VERSION 3.16)
project(dvi_benchmarks)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG -march=native")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

find_package(Threads REQUIRED)

# Main benchmark binary (standalone, no Hyrise dependency)
add_executable(btree_benchmark
    src/standalone_benchmark/btree_benchmark.cpp
    src/standalone_btree/without_dependency_validation/btree_olc.cpp
)
target_include_directories(btree_benchmark PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src/standalone_btree/without_dependency_validation
    ${CMAKE_CURRENT_SOURCE_DIR}/src/standalone_btree/with_dependency_validation
)
target_link_libraries(btree_benchmark Threads::Threads)

# Unit tests (standalone binary with custom test framework)
add_executable(btree_validation_test
    tests/btree_with_validation_test.cpp
    src/standalone_btree/without_dependency_validation/btree_olc.cpp
)
target_include_directories(btree_validation_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src/standalone_btree/without_dependency_validation
    ${CMAKE_CURRENT_SOURCE_DIR}/src/standalone_btree/with_dependency_validation
)
target_link_libraries(btree_validation_test Threads::Threads)

message(STATUS "Building DVI standalone benchmarks")
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")