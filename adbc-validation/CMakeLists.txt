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

# Temporary pin to the arrow-adbc fork branch that relaxes TestSqlPartitionedInts'
# single-partition assumption (fornwall/arrow-adbc#9, based on the same apache
# `main` rev the driver's adbc_core/adbc_ffi build on). Revert to a versioned
# release tag once that change lands upstream.
set(ARROW_ADBC_REPOSITORY "https://github.com/fornwall/arrow-adbc.git"
    CACHE STRING "arrow-adbc git repository")
set(ARROW_ADBC_TAG "d16588b5dbc8720ba559fc538c2ea434599df82e"
    CACHE STRING "arrow-adbc git rev (fornwall/arrow-adbc#9)")

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
