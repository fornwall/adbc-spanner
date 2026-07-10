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
# The Rust cdylib itself is NOT instrumented — it is built separately by cargo (stable) and
# dlopen-loaded by the driver manager. That still catches the memory bugs that matter at the C
# ABI boundary: ASan's malloc/free/memcpy interceptors are process-wide, and Rust's default
# allocator is libc malloc, so double-free / heap-overflow / use-after-free on the C-ABI structs
# shared across the boundary (ArrowArray/ArrowSchema release callbacks, AdbcError private_data)
# are reported — with symbolized Rust frames — even though the cdylib is uninstrumented. Errors
# that never touch an interceptor (a plain out-of-bounds store inside Rust) would need a nightly
# `-Zsanitizer=address` cdylib build, which is out of scope here (cargo-fuzz already ASan-covers
# the offline parser paths).
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
# `main` (rather than the 0.23 release tag) picks up the C++ validation fix in
# apache/arrow-adbc#4493 — TestSqlPartitionedInts now allows >1 partition, so
# SpannerStatementTest.SqlPartitionedInts is gate-enforced instead of excluded.
# Bump the SHA to advance the suite.
#
# FORCE so this pin always wins over a value cached in a restored/incremental
# build tree: a plain `set(... CACHE STRING)` is a no-op when the entry already
# exists, so a stale `CMakeCache.txt` (e.g. one restored from a run on the old
# rev) would silently keep the previous tag and rebuild against the wrong
# arrow-adbc source. FORCE makes a rev bump here authoritative.
set(ARROW_ADBC_TAG "f9886ddca13a2ea2f044a453216b3973b66cd98f"
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
  GIT_SHALLOW TRUE
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
