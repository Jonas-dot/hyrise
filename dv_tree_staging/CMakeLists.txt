cmake_minimum_required(VERSION 3.18)

project(hyrise_dv_tree_staging LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

# The core under src/storage/dependency_validation/ is a byte-identical copy of
# hyrise/src/lib/storage/dependency_validation/ (index core only, no Hyrise
# integration glue). shim/types.hpp replaces Hyrise's types.hpp, which is the
# core's single external dependency. Keep the copies verbatim so diffing against
# the in-hyrise version stays trivial.
set(DV_TREE_SOURCES
    src/storage/dependency_validation/dv_tree.cpp
    src/storage/dependency_validation/dv_types.cpp
    src/storage/dependency_validation/key_normalization.cpp
    src/storage/dependency_validation/btree/btree_adapter.cpp
    src/storage/dependency_validation/btree/tum_btree/btree.cpp
)

function(configure_dv_tree_target target)
    target_include_directories(${target}
        PUBLIC src shim
    )
    target_compile_options(${target} PRIVATE -Wall -Wextra -pedantic)
    target_link_libraries(${target} PUBLIC Threads::Threads)
endfunction()

# Production-shaped library: the white-box test API stays private.
add_library(dv_tree_staging STATIC ${DV_TREE_SOURCES})
configure_dv_tree_target(dv_tree_staging)

# The standalone regressions intentionally use the DV_TESTING inspection hooks.
add_library(dv_tree_staging_test_support STATIC ${DV_TREE_SOURCES})
configure_dv_tree_target(dv_tree_staging_test_support)
target_compile_definitions(dv_tree_staging_test_support PUBLIC DV_TESTING)
target_include_directories(dv_tree_staging_test_support PUBLIC tests)

add_executable(dv_tree_unit_tests tests/unit/unit_tests.cpp)
target_link_libraries(dv_tree_unit_tests PRIVATE dv_tree_staging_test_support)

add_executable(dv_tree_concurrency_tests
    tests/concurrency/concurrency_tests.cpp
)
target_link_libraries(dv_tree_concurrency_tests PRIVATE dv_tree_staging_test_support)

enable_testing()
add_test(NAME dv_tree_unit_tests COMMAND dv_tree_unit_tests)
add_test(NAME dv_tree_concurrency_tests COMMAND dv_tree_concurrency_tests)
