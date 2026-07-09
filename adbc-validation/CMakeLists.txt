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

# Temporary pin to the arrow-adbc fork branch that adds the float-cast-type quirk
# (`DriverQuirks::FloatCastTypeName`, fornwall/arrow-adbc#7, based on the same
# apache `main` rev the driver's adbc_core/adbc_ffi build on). Revert to a
# versioned release tag once that quirk lands upstream.
#
# FORCE so this pin always wins over a value cached in a restored/incremental
# build tree: a plain `set(... CACHE STRING)` is a no-op when the entry already
# exists, so a stale `CMakeCache.txt` (e.g. seeded via the CI cache restore-keys
# from a run on the old rev) would silently keep the previous tag and rebuild
# against the wrong arrow-adbc source. FORCE makes a rev bump here authoritative.
set(ARROW_ADBC_REPOSITORY "https://github.com/fornwall/arrow-adbc.git"
    CACHE STRING "arrow-adbc git repository" FORCE)
set(ARROW_ADBC_TAG "010cbf38be8dce3e4fa4b3592e25aff8dbe1bc3a"
    CACHE STRING "arrow-adbc git rev (fornwall/arrow-adbc#7)" FORCE)

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
  GIT_REPOSITORY ${ARROW_ADBC_REPOSITORY}
  GIT_TAG ${ARROW_ADBC_TAG}
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
