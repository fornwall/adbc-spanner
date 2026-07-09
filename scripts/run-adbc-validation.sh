#!/usr/bin/env bash
#
# Build the adbc-spanner cdylib and the canonical Apache Arrow ADBC C++
# validation suite (see adbc-validation/), then run the suite against the driver
# loaded through the ADBC driver manager.
#
# With no Spanner target configured it starts a throwaway emulator (via
# scripts/with-emulator.sh) and creates the test instance/database itself, so:
#
#   scripts/run-adbc-validation.sh            # emulator, CI checks (gate + guards)
#   scripts/run-adbc-validation.sh --full     # emulator, every test (exploration)
#   scripts/run-adbc-validation.sh --check-drift  # build + stale guard only (no DB)
#   SPANNER_EMULATOR_HOST=localhost:9010 scripts/run-adbc-validation.sh
#   SPANNER_GCP_DATABASE=proj.inst.db scripts/run-adbc-validation.sh
#
# The default (CI) path is driven by a single EXCLUDED list of the cases that are
# known-not-passing / not-applicable to Spanner (documented in
# adbc-validation/README.md), and runs three checks against it:
#
#   1. Gate            — run everything EXCEPT the excluded cases
#                        (`--gtest_filter=-<EXCLUDED>`); it must pass. A brand-new
#                        upstream case isn't excluded, so it runs here and auto-
#                        enrolls: if it fails, CI goes red until it is fixed or
#                        added to EXCLUDED with a reason.
#   2. Expected-failure guard — run ONLY the excluded cases and assert none of
#                        them actually PASSED (an excluded test that starts passing
#                        must be removed from EXCLUDED so the gate enforces it).
#   3. Stale guard     — assert every EXCLUDED entry still exists upstream, so a
#                        rename/removal (e.g. after bumping ARROW_ADBC_TAG) can't
#                        leave the list rotting.
#
# `--full` runs the whole suite (every case, per-test isolation) for local
# exploration; expect Spanner-specific failures/skips there.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Fixed emulator identifiers, matching tests/integration.rs.
EMULATOR_DATABASE="projects/test-project/instances/test-instance/databases/adbc-test"

BUILD_DIR="${ADBC_VALIDATION_BUILD_DIR:-$REPO_ROOT/.adbc-validation-build}"

FULL=0
CHECK_DRIFT_ONLY=0
for arg in "$@"; do
  case "$arg" in
    --full) FULL=1 ;;
    --check-drift) CHECK_DRIFT_ONLY=1 ;;
    *)
      echo "!! unknown argument: $arg" >&2
      echo "usage: $0 [--full | --check-drift]" >&2
      exit 2
      ;;
  esac
done

# ---------------------------------------------------------------------------
# EXCLUDED: the single source of truth for cases that are known-not-passing or
# not-applicable to Spanner's model. Everything NOT in this list is expected to
# pass (or self-skip) and is enforced by the gate, so new upstream cases run
# automatically. Every entry is grouped by bucket with a reason; each is enforced
# to (a) still exist upstream (stale guard) and (b) still fail-or-skip, never pass
# (expected-failure guard). DatabaseTest + ConnectionTest are fully passing and so
# never appear here.
# ---------------------------------------------------------------------------
EXCLUDED=(
  # --- Bucket 1: suite-internal non-Spanner DDL -------------------------------
  # These cases issue hardcoded CREATE TABLE with no primary key, double-quoted
  # identifiers, and INT/TEXT/FLOAT types — not valid Spanner DDL (which needs
  # INT64/FLOAT64, a PRIMARY KEY, backtick quoting) and there is no quirks hook.
  'SpannerStatementTest.SqlBind'
  'SpannerStatementTest.SqlPrepareSelectParams'
  'SpannerStatementTest.SqlPrepareUpdate'
  'SpannerStatementTest.SqlPrepareUpdateStream'
  'SpannerStatementTest.SqlQueryEmpty'
  'SpannerStatementTest.SqlQueryFloats'
  'SpannerStatementTest.SqlQueryInsertRollback'
  'SpannerStatementTest.SqlQueryRowsAffectedDelete'
  'SpannerStatementTest.SqlQueryRowsAffectedDeleteStream'
  'SpannerStatementTest.SqlSchemaFloats'
  # Transactions is NOT excluded: Spanner has no transactional DDL (DDL auto-commits
  # via the admin API and cannot be rolled back), so the case can never apply. The
  # `ddl_implicit_commit_txn` quirk makes it self-skip, which the gate tolerates —
  # no expected-failure bookkeeping needed.

  # --- Bucket 2: ingest readback (and non-applicable ingest variants) ---------
  # Create-mode ingest is supported (synthetic adbc_ingest_key UUID PK), so these
  # now *run* instead of skipping, but they read back via a hardcoded double-quoted
  # `SELECT * FROM "bulk_ingest" ...` (not GoogleSQL, no quirks hook), and `SELECT *`
  # would also surface the synthetic key column, breaking the single-column
  # assertions. The type/temp/target variants that Spanner does not model (float16,
  # temporary tables, ...) self-skip; either way they are not gated.
  # (SqlIngest{Table,Column}Escaping are NOT here: they create-mode ingest with no
  # readback, so with the create default they pass cleanly and are gate-enforced;
  # identifier-escaping is additionally covered natively in tests/integration.rs.)
  # (SqlIngestErrors is NOT here: it exercises only the ingest error paths, with no
  # readback and no non-Spanner DDL, so it passes cleanly and is enforced by the gate.)
  'SpannerStatementTest.SqlIngestAppend'
  'SpannerStatementTest.SqlIngestBinary'
  'SpannerStatementTest.SqlIngestBinaryView'
  'SpannerStatementTest.SqlIngestBool'
  'SpannerStatementTest.SqlIngestCreateAppend'
  'SpannerStatementTest.SqlIngestDate32'
  'SpannerStatementTest.SqlIngestDuration'
  'SpannerStatementTest.SqlIngestFixedSizeBinary'
  'SpannerStatementTest.SqlIngestFloat16'
  'SpannerStatementTest.SqlIngestFloat32'
  'SpannerStatementTest.SqlIngestFloat64'
  'SpannerStatementTest.SqlIngestInt16'
  'SpannerStatementTest.SqlIngestInt32'
  'SpannerStatementTest.SqlIngestInt64'
  'SpannerStatementTest.SqlIngestInt8'
  'SpannerStatementTest.SqlIngestInterval'
  'SpannerStatementTest.SqlIngestLargeBinary'
  'SpannerStatementTest.SqlIngestLargeString'
  'SpannerStatementTest.SqlIngestListOfInt32'
  'SpannerStatementTest.SqlIngestListOfString'
  'SpannerStatementTest.SqlIngestMultipleConnections'
  'SpannerStatementTest.SqlIngestPrimaryKey'
  'SpannerStatementTest.SqlIngestReplace'
  'SpannerStatementTest.SqlIngestSample'
  'SpannerStatementTest.SqlIngestString'
  'SpannerStatementTest.SqlIngestStringDictionary'
  'SpannerStatementTest.SqlIngestStringView'
  'SpannerStatementTest.SqlIngestTargetCatalog'
  'SpannerStatementTest.SqlIngestTargetCatalogSchema'
  'SpannerStatementTest.SqlIngestTargetSchema'
  'SpannerStatementTest.SqlIngestTemporary'
  'SpannerStatementTest.SqlIngestTemporaryAppend'
  'SpannerStatementTest.SqlIngestTemporaryExclusive'
  'SpannerStatementTest.SqlIngestTemporaryReplace'
  'SpannerStatementTest.SqlIngestTimestamp'
  'SpannerStatementTest.SqlIngestTimestampTz'
  'SpannerStatementTest.SqlIngestUInt16'
  'SpannerStatementTest.SqlIngestUInt32'
  'SpannerStatementTest.SqlIngestUInt64'
  'SpannerStatementTest.SqlIngestUInt8'
  'SpannerStatementTest.TestSqlIngestStreamZeroArrays'

  # --- Bucket 3: ECANCELED through the C stream -------------------------------
  # SqlQueryCancel requires the result stream's get_next to return exactly
  # ECANCELED (125) after a cancel, but arrow-rs's FFI_ArrowArrayStream exporter
  # (used by adbc_ffi) can only map errors to ENOSYS/ENOMEM/EIO/EINVAL, so no Rust
  # driver behind adbc_ffi can emit 125 today. Cancellation itself works and is
  # sticky; covered natively by cancel_between_stream_chunks_cancels_the_next_fetch
  # in tests/integration.rs.
  'SpannerStatementTest.SqlQueryCancel'

  # --- Bucket 4: rigid single-partition assumption ----------------------------
  # SqlPartitionedInts hardcodes ASSERT_EQ(1, num_partitions) for `SELECT 42`, but
  # Spanner's partitionQuery may return more (the emulator returns 2). The driver's
  # partition round-trip is covered by execute_partitions_round_trip in
  # tests/integration.rs.
  'SpannerStatementTest.SqlPartitionedInts'
)

# The colon-joined --gtest_filter value for the EXCLUDED set. Prefixed with `-`
# it negates (the gate: run everything else); bare it selects only these (the
# expected-failure guard).
EXCLUDED_FILTER="$(IFS=:; printf '%s' "${EXCLUDED[*]}")"

# ---------------------------------------------------------------------------

build_harness() {
  echo ">> building the adbc-spanner cdylib"
  cargo build

  echo ">> building the ADBC validation harness (fetches arrow-adbc + googletest)"
  cmake -S "$REPO_ROOT/adbc-validation" -B "$BUILD_DIR" \
    -DCMAKE_BUILD_TYPE=Release -DCMAKE_POLICY_VERSION_MINIMUM=3.5 >/dev/null
  cmake --build "$BUILD_DIR" --target spanner_validation -j"$(nproc 2>/dev/null || echo 2)"
}

# Parse `Suite.\n  Case\n ...` from `--gtest_list_tests` into fully-qualified
# Suite.Case names (one per line). Lists only; connects to no database.
list_available_cases() {
  "$BUILD_DIR/spanner_validation" --gtest_list_tests 2>/dev/null | awk '
    /^[A-Za-z]/          { suite = $1; next }
    /^[[:space:]]/ && NF { print suite $1 }
  '
}

# Stale guard: every EXCLUDED entry must still be a case the harness exposes, so a
# rename/removal upstream (e.g. after bumping ARROW_ADBC_TAG) is caught instead of
# silently rotting. Needs no database (`--gtest_list_tests` only enumerates).
run_stale_guard() {
  echo ">> stale guard: every EXCLUDED entry must still exist upstream"
  local bin="$BUILD_DIR/spanner_validation"
  if [ ! -x "$bin" ]; then
    echo "!! $bin not found; build the harness first" >&2
    return 1
  fi

  local -a available=()
  mapfile -t available < <(list_available_cases)
  if [ "${#available[@]}" -eq 0 ]; then
    echo "!! could not enumerate any test cases from $bin --gtest_list_tests" >&2
    return 1
  fi

  local -A avail=()
  local t
  for t in "${available[@]}"; do avail["$t"]=1; done

  local -a stale=()
  local e
  for e in "${EXCLUDED[@]}"; do
    [ -n "${avail[$e]:-}" ] || stale+=("$e")
  done

  if [ "${#stale[@]}" -ne 0 ]; then
    echo "!! ${#stale[@]} EXCLUDED entr(y/ies) no longer exposed by the harness" >&2
    echo "   (renamed/removed upstream? bumped ARROW_ADBC_TAG? — clean up EXCLUDED):" >&2
    printf '     %s\n' "${stale[@]}" >&2
    return 1
  fi

  echo ">> stale guard OK: all ${#EXCLUDED[@]} EXCLUDED entries still present upstream"
  return 0
}

# Gate: run every case EXCEPT the excluded ones; it must pass. This is where a
# newly-added upstream case auto-enrolls (it isn't excluded, so it runs here).
run_gate() {
  echo ">> gate: running every non-EXCLUDED case (must all pass or self-skip)"
  "$BUILD_DIR/spanner_validation" --gtest_filter="-$EXCLUDED_FILTER"
}

# Expected-failure guard (xfail-strict): run ONLY the excluded cases and assert
# none of them actually PASSED. An excluded case that starts passing must be
# removed from EXCLUDED so the gate enforces it. The run itself exits non-zero
# (the excluded cases fail), so its status is captured, not propagated. The
# pass/fail/skip discriminator comes from the JUnit XML: gtest writes each
# <testcase> opening tag on one line; a case that ran and passed is
# result="completed" with no <failure> child (a self-closing <testcase .../>),
# a failure adds a <failure> child, and a skip is result="skipped" — so a skipped
# excluded case does NOT count as passing.
run_xfail_guard() {
  echo ">> expected-failure guard: no EXCLUDED case may actually pass"
  local bin="$BUILD_DIR/spanner_validation"
  local xml="$BUILD_DIR/excluded-results.xml"
  rm -f "$xml"

  local rc=0
  "$bin" --gtest_filter="$EXCLUDED_FILTER" --gtest_output="xml:$xml" >/dev/null 2>&1 || rc=$?
  if [ ! -f "$xml" ]; then
    echo "!! expected-failure guard: harness produced no XML at $xml (exit $rc)" >&2
    return 1
  fi

  local -a xpass=()
  mapfile -t xpass < <(awk '
    /<testcase /{
      cls = ""; nm = ""; res = ""; failed = 0; open = 1
      if (match($0, /classname="[^"]*"/)) cls = substr($0, RSTART + 11, RLENGTH - 12)
      if (match($0, /name="[^"]*"/))      nm  = substr($0, RSTART + 6,  RLENGTH - 7)
      if (match($0, /result="[^"]*"/))    res = substr($0, RSTART + 8,  RLENGTH - 9)
      # A self-closing <testcase .../> has no children => it ran and passed.
      if ($0 ~ /\/>[[:space:]]*$/) { if (res == "completed") print cls "." nm; open = 0 }
      next
    }
    open && /<failure/       { failed = 1 }
    open && /<\/testcase>/   { if (res == "completed" && failed == 0) print cls "." nm; open = 0 }
  ' "$xml")

  if [ "${#xpass[@]}" -ne 0 ]; then
    echo "!! ${#xpass[@]} EXCLUDED test(s) now PASS — remove them from EXCLUDED so the" >&2
    echo "   gate enforces them (an expected-failure that started passing):" >&2
    printf '     %s\n' "${xpass[@]}" >&2
    return 1
  fi

  echo ">> expected-failure guard OK: all ${#EXCLUDED[@]} EXCLUDED cases still fail or skip"
  return 0
}

# --check-drift: build the harness and run only the stale guard. This needs no
# emulator/database, so it runs before the emulator gate below and exits.
if [ "$CHECK_DRIFT_ONLY" -eq 1 ]; then
  build_harness
  run_stale_guard
  exit $?
fi

# No target configured: run under a throwaway emulator, then re-enter this script.
if [ -z "${SPANNER_EMULATOR_HOST:-}" ] && [ -z "${SPANNER_GCP_DATABASE:-}" ]; then
  exec "$REPO_ROOT/scripts/with-emulator.sh" "$0" "$@"
fi

build_harness

# The stale guard needs no database, so run it before the emulator DB creation.
# Skip it under --full (which is exploratory and runs every case anyway).
if [ "$FULL" -eq 0 ]; then
  run_stale_guard
fi

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
  # shellcheck disable=SC2016  # the backticks are literal Spanner DDL inside JSON, not a subshell.
  curl -sf -X POST "$rest/v1/projects/test-project/instances/test-instance/databases" \
    -H 'Content-Type: application/json' \
    -d '{"createStatement":"CREATE DATABASE `adbc-test`"}' >/dev/null 2>&1 || true
  # Wait for the database to be listable before connecting. The creation calls above
  # are deliberately idempotent (|| true), so this wait is the actual gate — and it
  # must fail loudly if the database never appears, rather than let the suite run
  # against a database that does not exist.
  ready=0
  for _ in $(seq 1 40); do
    if curl -sf "$rest/v1/projects/test-project/instances/test-instance/databases" 2>/dev/null \
        | grep -q 'adbc-test'; then
      ready=1
      break
    fi
    sleep 0.25
  done
  if [ "$ready" -ne 1 ]; then
    echo "!! emulator database adbc-test did not become listable at $rest within 10s" >&2
    echo "!! (is the emulator at SPANNER_EMULATOR_HOST=$SPANNER_EMULATOR_HOST healthy?)" >&2
    exit 1
  fi
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
  # Gate first (must pass), then assert the excluded set still all fails/skips.
  run_gate
  run_xfail_guard
fi
