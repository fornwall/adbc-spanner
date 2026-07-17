# Builds the canonical Apache Arrow ADBC C++ validation suite against the
# adbc-spanner driver (loaded as a shared library through the ADBC driver
# manager). See README.md for how to run it.
#
# Everything external is fetched and built from source at a pinned arrow-adbc
# release, so the only host requirements are a C++17 compiler, CMake and git.
cmake_minimum_required(VERSION 3.20)
project(adbc_spanner_validation LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Optional sanitizers for the whole C++ side of the suite (this harness + the ADBC driver
# manager + the adbc_validation object library + vendored nanoarrow/fmt). Set e.g.
# `-DSPANNER_VALIDATION_SANITIZE=address,undefined` (the run script forwards
# ADBC_VALIDATION_SANITIZE). The flags are added *before* FetchContent below so the fetched
# arrow-adbc sources inherit them too.
#
# In the common case the Rust cdylib itself is NOT instrumented — it is built separately by cargo
# (stable) and dlopen-loaded by the driver manager. That still catches the memory bugs that matter
# at the C ABI boundary: ASan's malloc/free/memcpy interceptors are process-wide, and Rust's default
# allocator is libc malloc, so double-free / heap-overflow / use-after-free on the C-ABI structs
# shared across the boundary (ArrowArray/ArrowSchema release callbacks, AdbcError private_data)
# are reported — with symbolized Rust frames — even though the cdylib is uninstrumented. Errors
# that never touch an interceptor (a plain out-of-bounds store inside Rust) are NOT visible this way.
#
# The run script's rust-asan path closes that gap: it builds the cdylib itself with nightly Rust's
# `-Zsanitizer=address` (+ `-Zbuild-std`), so this CMake side is compiled with clang (CC/CXX) and
# `-fsanitize=address` to share the SAME LLVM compiler-rt ASan runtime as the instrumented cdylib
# (a gcc libasan build would abort at startup when mixed). This CMakeLists needs no special handling
# for that — it just honours SPANNER_VALIDATION_SANITIZE below; the compiler choice comes from CC/CXX.
#
# That rust-asan path also carries a positive control (a canary/tripwire): the run script additionally
# builds the cdylib with `--cfg asan_canary` (enabling an intentionally-out-of-bounds Rust symbol,
# src/asan_canary.rs) and, via adbc-validation/asan_canary.cc, calls it from a clang
# `-fsanitize=address` program against a C++-allocated buffer — instrumented Rust writing one byte
# past C++-allocated heap — asserting ASan reports the cross-boundary heap-buffer-overflow. If it does
# NOT fire, the cdylib is not ASan-armed and the leg fails loudly instead of passing as a no-op. That
# canary is an ad-hoc clang compile in the run script, deliberately NOT a CMake target here, so it
# never perturbs this cached arrow-adbc build tree or its cache key.
set(SPANNER_VALIDATION_SANITIZE ""
    CACHE STRING "comma-separated -fsanitize list for the C++ suite (e.g. address,undefined)")
if(SPANNER_VALIDATION_SANITIZE)
  message(STATUS "Sanitizers: -fsanitize=${SPANNER_VALIDATION_SANITIZE}")
  add_compile_options(-fsanitize=${SPANNER_VALIDATION_SANITIZE}
                      -fno-omit-frame-pointer -fno-sanitize-recover=all)
  add_link_options(-fsanitize=${SPANNER_VALIDATION_SANITIZE})
endif()

# TEMPORARILY pinned to a fornwall/arrow-adbc fork branch that carries one
# unmerged upstream PR, apache/arrow-adbc#4534. As soon as that PR merges,
# revert GIT_REPOSITORY (below) to apache/arrow-adbc.git and bump the tag onto a
# `main` rev containing it — the fork is the *only* reason this points anywhere
# but apache/arrow-adbc.
#
# This is the same *family* as the adbc_core / adbc_ffi git pin the driver
# builds on (see Cargo.toml), but the two revisions are bumped independently
# and may differ — this one only selects the C++ validation-suite sources.
# Its apache/arrow-adbc base (rather than the 0.23 release tag) carries five
# C++ validation fixes this suite relies on:
# apache/arrow-adbc#4493 — TestSqlPartitionedInts now allows >1 partition, so
# SpannerStatementTest.SqlPartitionedInts is gate-enforced instead of excluded;
# apache/arrow-adbc#4496 — the `DriverQuirks::RewriteSql(query_id, default_sql)`
# hook itself (first used to rewrite the `CAST(1.5 AS FLOAT)` in
# TestSqlQueryFloats / TestSqlSchemaFloats to GoogleSQL `FLOAT64`);
# apache/arrow-adbc#4504, which adds the `DriverQuirks::QuoteIdentifier(name)`
# hook (the Spanner quirks override it to GoogleSQL backticks, so the
# ingest/prepare readback queries are no longer blocked on ANSI
# double-quoting); and apache/arrow-adbc#4514, which routes the statement
# tests' remaining hardcoded dialect-sensitive SQL (non-Spanner CREATE TABLEs,
# INSERTs without a column list, `SELECT *` ingest readbacks, NULLS
# FIRST/LAST) through RewriteSql under stable query ids; and
# apache/arrow-adbc#4534, which makes the two TestSqlPrepareUpdate readbacks
# order-deterministic (`ORDER BY <col> ASC NULLS FIRST` plus sorted expected
# vectors) so a dialect quirk can make them pass. The Spanner quirks
# override RewriteSql (spanner_validation.cc) to substitute GoogleSQL per query
# id, which unlocks most of the former ingest-readback and non-Spanner-DDL
# exclusions (see EXCLUDED in scripts/run-adbc-validation.sh for the
# survivors). Bump the SHA to advance the suite.
#
# FORCE so this pin always wins over a value cached in a restored/incremental
# build tree: a plain `set(... CACHE STRING)` is a no-op when the entry already
# exists, so a stale `CMakeCache.txt` (e.g. one restored from a run on the old
# rev) would silently keep the previous tag and rebuild against the wrong
# arrow-adbc source. FORCE makes a rev bump here authoritative.
set(ARROW_ADBC_TAG "4dc3be825cafb4357a91fae720c6de57594976fe"
    CACHE STRING "arrow-adbc revision (TEMP: fornwall fork branch, see above)" FORCE)

include(FetchContent)

# Build only what the validation suite needs: the driver manager (which provides
# the flat C ADBC API and dlopen-loads our cdylib) and, via ADBC_BUILD_TESTS, the
# `adbc_validation` object library and a fetched GoogleTest. All other drivers off.
# fmt / nanoarrow / toml++ are vendored inside arrow-adbc (vendored copies on by
# default), so no system packages are required.
set(ADBC_DRIVER_MANAGER ON CACHE BOOL "" FORCE)
set(ADBC_BUILD_TESTS ON CACHE BOOL "" FORCE)
# Use arrow-adbc's vendored fmt / nanoarrow so no system packages are needed.
set(ADBC_WITH_VENDORED_FMT ON CACHE BOOL "" FORCE)
set(ADBC_WITH_VENDORED_NANOARROW ON CACHE BOOL "" FORCE)
set(ADBC_BUILD_SHARED ON CACHE BOOL "" FORCE)
set(ADBC_BUILD_STATIC OFF CACHE BOOL "" FORCE)
set(ADBC_DRIVER_SQLITE OFF CACHE BOOL "" FORCE)
set(ADBC_DRIVER_POSTGRESQL OFF CACHE BOOL "" FORCE)
set(ADBC_DRIVER_FLIGHTSQL OFF CACHE BOOL "" FORCE)
set(ADBC_DRIVER_SNOWFLAKE OFF CACHE BOOL "" FORCE)
set(ADBC_DRIVER_BIGQUERY OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
  arrow_adbc
  # TEMPORARY: fornwall/arrow-adbc fork branch for unmerged apache/arrow-adbc#4534;
  # revert to https://github.com/apache/arrow-adbc.git once #4534 merges (see above).
  GIT_REPOSITORY https://github.com/fornwall/arrow-adbc.git
  GIT_TAG ${ARROW_ADBC_TAG}
  # NOT GIT_SHALLOW: ARROW_ADBC_TAG is a specific `main` commit, and a shallow clone
  # (`--depth 1`) fetches only upstream's current tip — so checking out the pinned SHA
  # works only while it happens to be that tip and breaks the moment upstream `main`
  # advances past it (a cold clone then fails with "unable to read tree <sha>"). A full
  # clone reliably contains the pinned commit. This runs only on a cache miss (a rev bump
  # or a new runner), so the extra history costs nothing on the common cached path.
  # The C/C++ project lives under c/ in the monorepo.
  SOURCE_SUBDIR c)
# Keep the suite hermetic: arrow-adbc's `find_package(GTest)` (AdbcDefines.cmake)
# would otherwise pick up any *installed* GTest config package (e.g. a
# Homebrew/linuxbrew googletest). Besides linking an arbitrary host googletest,
# the imported GTest::* targets a config package creates are directory-scoped to
# the fetched subproject and invisible up here, so the target_link_libraries
# below would fail with "GTest::gtest_main ... not found". Disabling the
# find_package forces arrow-adbc's from-source googletest fallback (the CI path
# everywhere), whose targets are real and global.
set(CMAKE_DISABLE_FIND_PACKAGE_GTest TRUE)

FetchContent_MakeAvailable(arrow_adbc)

add_executable(spanner_validation spanner_validation.cc)
target_link_libraries(spanner_validation PRIVATE adbc_validation
                                                 adbc_driver_manager_shared
                                                 GTest::gtest_main)

# Warning hygiene for OUR C++ only, mirroring `clippy -D warnings` on the Rust side.
# Deliberately target-scoped rather than add_compile_options(): the FetchContent-built
# arrow-adbc / googletest sources are third-party and must not inherit -Werror (their
# warnings are not ours to fix, and a new compiler release would break the build). Their
# headers are already included SYSTEM here and via arrow-adbc's own targets, so warnings
# from them stay silent in this target too. -Wconversion / -Wsign-conversion earn their
# place in a harness that shuffles int64_t / size_t / enum codes across the C ABI.
# gcc/clang only, like the -fsanitize flags above.
target_compile_options(
  spanner_validation PRIVATE -Wall -Wextra -Wpedantic -Wshadow -Wconversion
                             -Wsign-conversion -Werror)
# Match the include layout the in-tree driver tests use.
target_include_directories(
  spanner_validation SYSTEM
  PRIVATE "${arrow_adbc_SOURCE_DIR}/c" "${arrow_adbc_SOURCE_DIR}/c/include"
          "${arrow_adbc_SOURCE_DIR}/c/vendor" "${arrow_adbc_SOURCE_DIR}/c/driver")

include(GoogleTest)
enable_testing()
gtest_discover_tests(spanner_validation)
