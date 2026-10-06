# DV-Tree standalone mirror

This directory is the standalone, Hyrise-independent build of the DV-Tree
index core. Since the integration (Phases 0-14) completed, its role has
inverted compared to the original staging area: the authoritative
implementation now lives in `../src/lib/storage/dependency_validation/`, and
this directory mirrors it verbatim so the index can be built, tested, and
audited without any of Hyrise.

- Hyrise's top-level CMake files do not include this directory;
- every file under `src/storage/dependency_validation/` is a **byte-identical
  copy** of its counterpart in `../src/lib/storage/dependency_validation/`
  (index core only — the Hyrise integration glue such as the operators'
  staging, write set, commit coordinator, bootstrap, and value normalization
  is deliberately absent);
- `shim/types.hpp` stands in for Hyrise's `types.hpp`, the core's single
  external dependency (`CommitID` and its constants). Nothing else from
  Hyrise is referenced.

## Syncing with the in-Hyrise core

After changing the core inside Hyrise, re-copy the files and re-run the tests
here. The copies must stay verbatim (`cmp` clean) so diffing against the
in-Hyrise version stays trivial; anything standalone-specific belongs in
`shim/` or `tests/`, never in the copied core.

## Layout

```text
src/storage/dependency_validation/  byte-identical copy of the in-Hyrise core
shim/types.hpp                      minimal stand-in for Hyrise's types.hpp
tests/unit/                         deterministic and public-boundary regressions
tests/concurrency/                  OLC and commit-order concurrency regressions
docs/                               architecture and MVCC/SI correctness contracts
CMakeLists.txt                      independent C++23 build; not included by Hyrise
```

The Hyrise integration plan and phased checklist are project-internal notes and
live outside this tree, in `../dv_tree_notes/`.

## Build and test

From this directory:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

The `dv_tree_staging` target compiles the production-shaped library without
white-box hooks. Tests link a separate `dv_tree_staging_test_support` target
with `DV_TESTING` enabled; `dv_tree_public_api_check` compiles the smoke test
without `DV_TESTING` to guard the production-shaped public surface.

For a ThreadSanitizer run of the concurrency suite:

```sh
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_CXX_FLAGS="-fsanitize=thread" -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=thread"
cmake --build build-tsan --parallel
./build-tsan/dv_tree_concurrency_tests
```

## Provenance and licensing

`src/storage/dependency_validation/btree/tum_btree/` is an adapted, privately
owned copy of the basic variable-key layout from the TUM
[`btree-cpp`](https://github.com/m-mueller678/btree-cpp) research artifact,
used as DV-Tree's structural page engine. The takeover boundary and the
DV-Tree-owned modifications are recorded in that directory's
[README](src/storage/dependency_validation/btree/tum_btree/README.md).

Key normalization takes its ordering rules from DuckDB's sort-key encoding and
from OrderedCode-style zero escaping; no DuckDB code is copied.

**Before publishing this tree outside the research project, the upstream
`btree-cpp` licensing and redistribution terms must be confirmed and a matching
`LICENSE`/attribution file added at the repository root.** This directory
currently carries no license file of its own.
