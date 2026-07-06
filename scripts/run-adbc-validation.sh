#!/usr/bin/env bash
#
# Build the adbc-spanner cdylib and the canonical Apache Arrow ADBC C++
# validation suite (see adbc-validation/), then run the suite against the driver
# loaded through the ADBC driver manager.
#
# With no Spanner target configured it starts a throwaway emulator (via
# scripts/with-emulator.sh) and creates the test instance/database itself, so:
#
#   scripts/run-adbc-validation.sh            # emulator, gated (CI) subset
#   scripts/run-adbc-validation.sh --full     # emulator, every test (exploration)
#   SPANNER_EMULATOR_HOST=localhost:9010 scripts/run-adbc-validation.sh
#   SPANNER_GCP_DATABASE=proj.inst.db scripts/run-adbc-validation.sh
#
# By default only the DatabaseTest + ConnectionTest suites run, minus a small,
# documented set of known Spanner conformance gaps (see adbc-validation/README.md).
# `--full` runs the whole suite (StatementTest included) for local exploration;
# expect failures/skips there.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Fixed emulator identifiers, matching tests/integration.rs.
EMULATOR_DATABASE="projects/test-project/instances/test-instance/databases/adbc-test"

FULL=0
[ "${1:-}" = "--full" ] && FULL=1

# The gated subset. DatabaseTest + ConnectionTest pass in full (lifecycle +
# metadata). From StatementTest we gate the cases that pass cleanly today, as an
# explicit allowlist rather than an exclude list: the remaining cases fail in
# ways that abort the process (the upstream adbc_ffi error release is not
# idempotent, so an error-path assertion double-frees), so a negative filter
# could not stay green. The excluded cases are documented in the README.
GATED_FILTER='SpannerDatabaseTest.*:SpannerConnectionTest.*'
GATED_FILTER+=':SpannerStatementTest.NewInit'
GATED_FILTER+=':SpannerStatementTest.Release'
GATED_FILTER+=':SpannerStatementTest.SqlPrepareGetParameterSchema'
GATED_FILTER+=':SpannerStatementTest.SqlPrepareUpdateNoParams'
GATED_FILTER+=':SpannerStatementTest.SqlPrepareErrorParamCountMismatch'
GATED_FILTER+=':SpannerStatementTest.SqlQueryErrors'
GATED_FILTER+=':SpannerStatementTest.SqlSchemaInts'
GATED_FILTER+=':SpannerStatementTest.SqlSchemaStrings'
GATED_FILTER+=':SpannerStatementTest.SqlSchemaErrors'
GATED_FILTER+=':SpannerStatementTest.ConcurrentStatements'
GATED_FILTER+=':SpannerStatementTest.ResultIndependence'
GATED_FILTER+=':SpannerStatementTest.ResultInvalidation'

# No target configured: run under a throwaway emulator, then re-enter this script.
if [ -z "${SPANNER_EMULATOR_HOST:-}" ] && [ -z "${SPANNER_GCP_DATABASE:-}" ]; then
  exec "$REPO_ROOT/scripts/with-emulator.sh" "$0" "$@"
fi

echo ">> building the adbc-spanner cdylib"
cargo build

echo ">> building the ADBC validation harness (fetches arrow-adbc + googletest)"
BUILD_DIR="${ADBC_VALIDATION_BUILD_DIR:-$REPO_ROOT/adbc-validation/build}"
cmake -S "$REPO_ROOT/adbc-validation" -B "$BUILD_DIR" \
  -DCMAKE_BUILD_TYPE=Release -DCMAKE_POLICY_VERSION_MINIMUM=3.5 >/dev/null
cmake --build "$BUILD_DIR" --target spanner_validation -j"$(nproc 2>/dev/null || echo 2)"

# Resolve the target database, creating it on the emulator (which starts empty).
if [ -n "${SPANNER_EMULATOR_HOST:-}" ]; then
  export ADBC_SPANNER_DATABASE="$EMULATOR_DATABASE"
  rest_host="${SPANNER_EMULATOR_HOST%:*}"
  rest="http://${rest_host}:${SPANNER_EMULATOR_REST_PORT:-9020}"
  echo ">> creating emulator instance/database via the admin REST API ($rest)"
  curl -sf -X POST "$rest/v1/projects/test-project/instances" \
    -H 'Content-Type: application/json' \
    -d '{"instanceId":"test-instance","instance":{"config":"projects/test-project/instanceConfigs/emulator-config","displayName":"adbc","nodeCount":1}}' \
    >/dev/null 2>&1 || true
  curl -sf -X POST "$rest/v1/projects/test-project/instances/test-instance/databases" \
    -H 'Content-Type: application/json' \
    -d '{"createStatement":"CREATE DATABASE `adbc-test`"}' >/dev/null 2>&1 || true
  # Wait for the database to be listable before connecting.
  for _ in $(seq 1 40); do
    if curl -sf "$rest/v1/projects/test-project/instances/test-instance/databases" 2>/dev/null \
        | grep -q 'adbc-test'; then
      break
    fi
    sleep 0.25
  done
else
  # Real Cloud Spanner target: project.instance.database -> the driver's URI form.
  IFS='.' read -r p i d <<<"$SPANNER_GCP_DATABASE"
  export ADBC_SPANNER_DATABASE="projects/$p/instances/$i/databases/$d"
fi

export ADBC_SPANNER_LIBRARY="$REPO_ROOT/target/debug/libadbc_spanner.so"
[ -f "$ADBC_SPANNER_LIBRARY" ] || ADBC_SPANNER_LIBRARY="$REPO_ROOT/target/debug/libadbc_spanner.dylib"

echo ">> ADBC_SPANNER_LIBRARY=$ADBC_SPANNER_LIBRARY"
echo ">> ADBC_SPANNER_DATABASE=$ADBC_SPANNER_DATABASE"

if [ "$FULL" -eq 1 ]; then
  echo ">> running the FULL validation suite (expect Spanner-specific failures/skips)"
  # Per-test process isolation via ctest, so a failing assertion (which aborts the
  # process — see the README note on the upstream non-idempotent error release)
  # only fails that one test rather than hiding the rest.
  ctest --test-dir "$BUILD_DIR" --output-on-failure || true
else
  echo ">> running the gated validation subset"
  "$BUILD_DIR/spanner_validation" --gtest_filter="$GATED_FILTER"
fi
