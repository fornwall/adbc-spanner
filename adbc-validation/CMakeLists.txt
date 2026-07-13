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

# Pin to an apache/arrow-adbc `main` revision, matching the family of the
# adbc_core / adbc_ffi git pin the driver builds on (see Cargo.toml). Tracking
# `main` (rather than the 0.23 release tag) picks up two C++ validation fixes:
# apache/arrow-adbc#4493 — TestSqlPartitionedInts now allows >1 partition, so
# SpannerStatementTest.SqlPartitionedInts is gate-enforced instead of excluded —
# and apache/arrow-adbc#4496, which adds the general
# `DriverQuirks::RewriteSql(query_id, default_sql)` hook. The Spanner quirks
# override it (spanner_validation.cc) to rewrite the `CAST(1.5 AS FLOAT)` in
# TestSqlQueryFloats / TestSqlSchemaFloats to GoogleSQL `FLOAT64`, so both cases
# are gate-enforced instead of excluded. Bump the SHA to advance the suite.
#
# FORCE so this pin always wins over a value cached in a restored/incremental
# build tree: a plain `set(... CACHE STRING)` is a no-op when the entry already
# exists, so a stale `CMakeCache.txt` (e.g. one restored from a run on the old
# rev) would silently keep the previous tag and rebuild against the wrong
# arrow-adbc source. FORCE makes a rev bump here authoritative.
set(ARROW_ADBC_TAG "138861516b5818ca0ab638678932418c652fa2f6"
    CACHE STRING "arrow-adbc main revision" FORCE)

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
  GIT_REPOSITORY https://github.com/apache/arrow-adbc.git
  GIT_TAG ${ARROW_ADBC_TAG}
  # NOT GIT_SHALLOW: ARROW_ADBC_TAG is a specific `main` commit, and a shallow clone
  # (`--depth 1`) fetches only upstream's current tip — so checking out the pinned SHA
  # works only while it happens to be that tip and breaks the moment upstream `main`
  # advances past it (a cold clone then fails with "unable to read tree <sha>"). A full
  # clone reliably contains the pinned commit. This runs only on a cache miss (a rev bump
  # or a new runner), so the extra history costs nothing on the common cached path.
  # The C/C++ project lives under c/ in the monorepo.
  SOURCE_SUBDIR c)
FetchContent_MakeAvailable(arrow_adbc)

add_executable(spanner_validation spanner_validation.cc)
target_link_libraries(spanner_validation PRIVATE adbc_validation
                                                 adbc_driver_manager_shared
                                                 GTest::gtest_main)
# Match the include layout the in-tree driver tests use.
target_include_directories(
  spanner_validation SYSTEM
  PRIVATE "${arrow_adbc_SOURCE_DIR}/c" "${arrow_adbc_SOURCE_DIR}/c/include"
          "${arrow_adbc_SOURCE_DIR}/c/vendor" "${arrow_adbc_SOURCE_DIR}/c/driver")

include(GoogleTest)
enable_testing()
gtest_discover_tests(spanner_validation)
