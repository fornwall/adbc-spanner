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
# Two sanitizer knobs (see the SANITIZE / RUST_SANITIZE block below):
#   ADBC_VALIDATION_SANITIZE=address,undefined scripts/run-adbc-validation.sh  # C++ side only
#   ADBC_VALIDATION_SANITIZE=address ADBC_VALIDATION_RUST_SANITIZE=address \
#     scripts/run-adbc-validation.sh   # + the Rust cdylib itself (nightly -Zsanitizer=address,
#                                      #   C++ side auto-built with clang to match the runtime)
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

# Optional sanitizers for the C++ side of the suite. Set e.g.
# ADBC_VALIDATION_SANITIZE=address,undefined to build the harness + arrow-adbc +
# driver manager with -fsanitize=... By default the Rust cdylib is loaded uninstrumented (see
# the note in adbc-validation/CMakeLists.txt); ASan's process-wide malloc/free/memcpy
# interceptors still catch double-free / overflow / use-after-free on the C-ABI structs
# the driver hands across the boundary.
SANITIZE="${ADBC_VALIDATION_SANITIZE:-}"

# Optionally instrument the Rust cdylib ITSELF with a sanitizer. Set
# ADBC_VALIDATION_RUST_SANITIZE=address to build the cdylib with nightly Rust's
# `-Zsanitizer=address` (plus `-Zbuild-std`, so the standard library is instrumented too —
# otherwise ASan misreports std allocations), catching memory bugs *inside* Rust — out-of-bounds
# stores, use-after-free on Rust-owned heap — that the C-side-only ASan above cannot see. Only
# `address` is supported (Rust has no `-Zsanitizer=undefined` cdylib story). This REQUIRES
# ADBC_VALIDATION_SANITIZE to include `address` too: the main C++ test executable must own the ASan
# runtime, and both sides must share the SAME runtime — Rust's `-Zsanitizer=address` uses LLVM
# compiler-rt ASan, so the C++ side must be compiled with clang (gcc's libasan is a different,
# incompatible runtime that aborts at startup when mixed). We therefore default CC/CXX to clang
# below when this is set (honouring an explicit CC/CXX if you export your own).
RUST_SANITIZE="${ADBC_VALIDATION_RUST_SANITIZE:-}"

# Host target triple. `-Zbuild-std` needs an explicit --target, which relocates the cdylib to
# target/<triple>/debug/; resolved once here and reused for both the build and the library path.
RUST_TARGET="$(rustc -vV | sed -n 's/host: //p')"

# When instrumenting the Rust side, compile the C++ side with clang so both link the SAME
# compiler-rt ASan runtime (mixing gcc's libasan with compiler-rt ASan in one process is unsound
# and aborts at startup). Only default these — respect a caller-provided CC/CXX.
if [ -n "$RUST_SANITIZE" ]; then
  export CC="${CC:-clang}"
  export CXX="${CXX:-clang++}"
fi

# Keep sanitized and plain build trees separate so switching between them doesn't force a
# full arrow-adbc rebuild (and so a cached CI tree can't mix flags). The Rust-instrumented leg
# additionally compiles the C++ side with a different compiler (clang vs gcc), so it needs its
# own tree distinct from the C-side-only `-san` one. Overridable as before.
build_suffix=""
[ -n "$SANITIZE" ] && build_suffix="-san"
[ -n "$RUST_SANITIZE" ] && build_suffix="-rustsan"
BUILD_DIR="${ADBC_VALIDATION_BUILD_DIR:-$REPO_ROOT/.adbc-validation-build$build_suffix}"

# Under sanitizers, disable LeakSanitizer: the driver's shared Tokio runtime, gRPC connection
# pools and lazily-initialized globals are intentionally process-lifetime and would otherwise
# swamp the run with non-actionable "leaks" — even more so once the Rust side is instrumented too.
# Memory-error checks (ASan) and UB checks (UBSan) stay fatal so a real bug fails the run. These
# are exported for both the build (gtest test discovery runs the binary) and the run itself.
if [ -n "$SANITIZE" ] || [ -n "$RUST_SANITIZE" ]; then
  export ASAN_OPTIONS="detect_leaks=0:abort_on_error=1:${ASAN_OPTIONS:-}"
  export UBSAN_OPTIONS="print_stacktrace=1:halt_on_error=1:${UBSAN_OPTIONS:-}"
fi

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
  # --- Bucket 1: readbacks that assert insertion order -------------------------
  # Both cases create `bulk_ingest` via create-mode ingest, INSERT more rows via
  # bound params, and then assert the `SELECT *` readback returns ALL rows in
  # insertion order. The SpannerQuirks::RewriteSql overrides
  # (apache/arrow-adbc#4514 routes the SQL; see spanner_validation.cc) fix the SQL itself —
  # the INSERT gets its required column list, the readback dodges the synthetic
  # adbc_ingest_key column — but no SQL can recover insertion order from a
  # Spanner table: rows come back in primary-key order and the synthetic key is
  # a random UUID. The final CompareArray on row order is what still fails.
  #
  # The rest of the former non-Spanner-DDL bucket is NOT here anymore: SqlBind,
  # SqlQueryEmpty, SqlQueryInsertRollback, SqlQueryRowsAffectedDelete{,Stream}
  # (hardcoded CREATE TABLE with INT/INTEGER/TEXT and no PRIMARY KEY) and
  # SqlPrepareSelectParams (a bare `SELECT @p0, @p1` whose parameter types
  # Spanner cannot infer) are rewritten to valid GoogleSQL via RewriteSql and
  # now pass, gate-enforced.
  #
  # SqlQueryFloats / SqlSchemaFloats are NOT here either: their only
  # Spanner-incompatible bit was the bare `CAST(1.5 AS FLOAT)` query, rewritten
  # to GoogleSQL `FLOAT64` (apache/arrow-adbc#4496), so both pass and are
  # gate-enforced.
  'SpannerStatementTest.SqlPrepareUpdate'
  'SpannerStatementTest.SqlPrepareUpdateStream'
  # Transactions is NOT excluded: it expects read-your-writes of an uncommitted
  # ingest, which the driver's buffer-and-commit manual transactions (one kind of
  # work per transaction — queries or DML) reject with InvalidState, so the case
  # can never apply. The `ddl_implicit_commit_txn` quirk makes it self-skip
  # (see the comment on the quirk in adbc-validation/spanner_validation.cc), which
  # the gate tolerates — no expected-failure bookkeeping needed.

  # --- Bucket 2: Arrow types the driver cannot map to a Spanner column ---------
  # These three cannot pass, for two distinct reasons:
  #
  #   - UInt64 (ingest-time "cannot create a Spanner column"): u64::MAX (1.8e19)
  #     exceeds i64::MAX (9.2e18), so it cannot widen to INT64 losslessly (a
  #     wrapping cast would silently corrupt large values). It *would* fit Spanner
  #     NUMERIC, but that reads back as Decimal128, not the INT64 the shared
  #     IngestSelectRoundTripType quirk (and the driver's uniform integer→INT64
  #     model) expects — and the suite's SchemaField cannot even express
  #     Decimal128(precision, scale) to assert such a round-trip. So it is unmapped.
  #
  #   - Interval / Duration (emulator limitation): both would map to a Spanner
  #     INTERVAL column, but the Cloud Spanner *emulator* — which is what every CI
  #     suite runs against — does not support the INTERVAL column type. A create-
  #     mode ingest that declares an INTERVAL column fails at CREATE TABLE with a
  #     backend GOOGLESQL_RET_CHECK ("IsSupportedColumnType"), so neither case can
  #     pass in CI regardless of driver support. (Arrow Interval(MonthDayNano) is a
  #     clean 1:1 with Spanner INTERVAL on *real* Spanner — same months/days/nanos
  #     model — but that is unverifiable here; Arrow Duration additionally has no
  #     fixed-unit counterpart in Spanner and the suite's ValidateIngestedTemporalData
  #     FAILs any non-TIMESTAMP temporal readback.)
  #
  # NOT here anymore (now mapped and gate-enforced): SqlIngestUInt8/UInt16/UInt32
  # widen losslessly to INT64 (u8/u16/u32 max < i64::MAX); SqlIngestFixedSizeBinary
  # maps to BYTES like the other binary kinds (readback Binary via
  # IngestSelectRoundTripType).
  #
  # The rest of the former ingest-readback bucket is NOT here anymore: the
  # readback SQL is routed through RewriteSql (apache/arrow-adbc#4514), and the
  # Spanner rewrites select the ingested column(s) explicitly (dodging the
  # synthetic adbc_ingest_key column) and drop the NULLS FIRST/LAST the emulator
  # rejects, so the whole SqlIngest type family plus Append/Replace/CreateAppend/
  # MultipleConnections/Sample now pass and are gate-enforced (with
  # IngestSelectRoundTripType declaring the INT64/STRING/BYTES widenings and
  # ValidateIngestedTemporalData checking the Timestamp readback values).
  #
  # NOT here (self-skip via a quirk, and the gate tolerates skips, so no bookkeeping):
  # SqlIngestFloat16 (Spanner has no float16 type), SqlIngestTemporary{,Append,
  # Replace,Exclusive} (Spanner has no temporary tables), and SqlIngestPrimaryKey
  # (PrimaryKeyIngestTableDdl returns nullopt: the case append-ingests rows
  # omitting the primary key and expects auto-assigned ascending keys, which
  # Spanner cannot do — no ordered auto-increment, and sequences are bit-reversed).
  # (SqlIngest{TargetCatalog,TargetSchema,TargetCatalogSchema} are NOT here: their
  # quirks are declared true, but the cases only ingest and never read back, so with
  # the create default they pass cleanly and are gate-enforced.)
  # (SqlIngest{Table,Column}Escaping are NOT here either: they create-mode ingest with
  # no readback, so with the create default they pass cleanly and are gate-enforced;
  # identifier-escaping is additionally covered natively in tests/integration.rs.)
  # (SqlIngestErrors is NOT here: it exercises only the ingest error paths, with no
  # readback and no non-Spanner DDL, so it passes cleanly and is enforced by the gate.)
  'SpannerStatementTest.SqlIngestDuration'
  'SpannerStatementTest.SqlIngestInterval'
  'SpannerStatementTest.SqlIngestUInt64'

  # --- Bucket 3: empty-stream ingest -------------------------------------------
  # Ingesting a stream with zero batches fails with InvalidState "cannot ingest:
  # no data has been bound" — the driver requires bound data before an ingest
  # executes, upstream expects a zero-row ingest to succeed and create the
  # table. A driver-side gap (no SQL involved), guarded here until fixed.
  'SpannerStatementTest.TestSqlIngestStreamZeroArrays'

  # --- Bucket 4: ECANCELED through the C stream -------------------------------
  # SqlQueryCancel requires the result stream's get_next to return exactly
  # ECANCELED (125) after a cancel, but arrow-rs's FFI_ArrowArrayStream exporter
  # (used by adbc_ffi) can only map errors to ENOSYS/ENOMEM/EIO/EINVAL, so no Rust
  # driver behind adbc_ffi can emit 125 today. Cancellation itself works and is
  # sticky; covered natively by cancel_between_stream_chunks_cancels_the_next_fetch
  # in tests/integration.rs.
  'SpannerStatementTest.SqlQueryCancel'
)
# SqlPartitionedInts is NOT excluded: it used to hardcode ASSERT_EQ(1, num_partitions)
# for `SELECT 42` (Spanner's partitionQuery may return more — the emulator returns 2),
# but apache/arrow-adbc#4493 relaxed it to allow >=1 partitions and assert on the union
# of all of them, so it now passes and is gate-enforced (see ARROW_ADBC_TAG in
# adbc-validation/CMakeLists.txt). The round-trip is additionally covered by
# execute_partitions_round_trip in tests/integration.rs.

# The colon-joined --gtest_filter value for the EXCLUDED set. Prefixed with `-`
# it negates (the gate: run everything else); bare it selects only these (the
# expected-failure guard).
EXCLUDED_FILTER="$(IFS=:; printf '%s' "${EXCLUDED[*]}")"

# ---------------------------------------------------------------------------

# Cross-boundary ASan canary (rust-asan leg only). A *passing* rust-asan leg is no proof the Rust
# instrumentation is armed: if the -Zsanitizer=address / -Zbuild-std wiring silently regressed (a
# flag typo, a toolchain skew, an ABI mismatch that disarms rather than aborts) the suite would
# still go green and give false confidence. So, before running the suite, call an
# intentionally-out-of-bounds Rust symbol (adbc_spanner_asan_canary, compiled only under
# --cfg asan_canary) from a clang -fsanitize=address C++ program against a buffer the C++ side
# allocated — i.e. instrumented Rust writing one byte past C++-allocated heap, the exact
# cross-boundary shape this leg exists to cover. ASan MUST report a heap-buffer-overflow; if it does
# NOT, the cdylib is not ASan-armed and the whole leg is a no-op, so we fail loudly and go red.
run_rust_asan_canary() {
  local lib="$REPO_ROOT/target/$RUST_TARGET/debug/libadbc_spanner.so"
  local src="$REPO_ROOT/adbc-validation/asan_canary.cc"
  local bin="$BUILD_DIR/asan_canary"
  mkdir -p "$BUILD_DIR"

  echo ">> ASan canary: compiling the cross-boundary tripwire (${CXX:-clang++} -fsanitize=address)"
  "${CXX:-clang++}" -std=c++17 -g -O0 -fsanitize=address -fno-omit-frame-pointer \
    "$src" -o "$bin" -ldl

  echo ">> ASan canary: running it against $lib (expecting a heap-buffer-overflow)"
  # ASAN_OPTIONS carries abort_on_error=1, so a detected overflow aborts the process (non-zero
  # exit); capture that rather than letting `set -e` kill the script before we can assert on it.
  local out rc=0
  out="$("$bin" "$lib" 2>&1)" || rc=$?

  if [ "$rc" -ne 0 ] \
      && printf '%s' "$out" | grep -q 'AddressSanitizer' \
      && printf '%s' "$out" | grep -q 'heap-buffer-overflow'; then
    echo ">> ASan canary OK: cross-boundary heap-buffer-overflow reported (exit $rc) — the cdylib IS ASan-armed"
    printf '%s\n' "$out" | grep -E 'heap-buffer-overflow|adbc_spanner_asan_canary' | head -n 4 \
      | sed 's/^/     /'
    return 0
  fi

  echo "!! ASan canary did NOT trip — the Rust cdylib is not ASan-armed; the rust-asan leg is a no-op" >&2
  echo "!! (a -Zsanitizer=address / -Zbuild-std regression would let real in-Rust memory bugs pass silently)" >&2
  echo "!! canary exit=$rc; full output follows:" >&2
  printf '%s\n' "$out" | sed 's/^/     /' >&2
  return 1
}

build_harness() {
  if [ -n "$RUST_SANITIZE" ]; then
    # Nightly + build-std so the cdylib AND std are instrumented; the artifact lands under
    # target/<triple>/debug/ because -Zbuild-std forces an explicit --target. `--cfg asan_canary`
    # additionally compiles the test-only ASan tripwire (src/asan_canary.rs) into THIS build only —
    # a bare --cfg is set nowhere else (not by `cargo build`, not by `--all-features`), so the
    # intentionally-out-of-bounds symbol never leaks into a shipped cdylib.
    echo ">> building the adbc-spanner cdylib with -Zsanitizer=$RUST_SANITIZE (nightly, -Zbuild-std, --target $RUST_TARGET, --cfg asan_canary)"
    RUSTFLAGS="-Zsanitizer=$RUST_SANITIZE --cfg asan_canary ${RUSTFLAGS:-}" \
      cargo +nightly build -Zbuild-std --target "$RUST_TARGET"
    # Positive control: prove the freshly-built cdylib is ACTUALLY ASan-armed before running the
    # (slow) suite, so a silently-disarmed leg fails fast here instead of going green as a no-op.
    run_rust_asan_canary
  else
    echo ">> building the adbc-spanner cdylib"
    cargo build
  fi

  echo ">> building the ADBC validation harness (fetches arrow-adbc + googletest)"
  [ -n "$SANITIZE" ] && echo ">> sanitizers: -fsanitize=$SANITIZE (C++ side${CC:+, CC=$CC CXX=$CXX})"
  [ -n "$RUST_SANITIZE" ] && echo ">> cdylib is ASan-instrumented (Rust -Zsanitizer=$RUST_SANITIZE); C++ side shares its compiler-rt runtime"
  cmake -S "$REPO_ROOT/adbc-validation" -B "$BUILD_DIR" \
    -DCMAKE_BUILD_TYPE=Release -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
    -DSPANNER_VALIDATION_SANITIZE="$SANITIZE" >/dev/null
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
  export ADBC_SPANNER_URI="spanner:///$EMULATOR_DATABASE"
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
  export ADBC_SPANNER_URI="spanner:///projects/$p/instances/$i/databases/$d"
fi

# The instrumented leg builds with an explicit --target, so its artifact lives under
# target/<triple>/debug/ rather than target/debug/.
if [ -n "$RUST_SANITIZE" ]; then
  export ADBC_SPANNER_LIBRARY="$REPO_ROOT/target/$RUST_TARGET/debug/libadbc_spanner.so"
else
  export ADBC_SPANNER_LIBRARY="$REPO_ROOT/target/debug/libadbc_spanner.so"
  [ -f "$ADBC_SPANNER_LIBRARY" ] || ADBC_SPANNER_LIBRARY="$REPO_ROOT/target/debug/libadbc_spanner.dylib"
fi

echo ">> ADBC_SPANNER_LIBRARY=$ADBC_SPANNER_LIBRARY"
echo ">> ADBC_SPANNER_URI=$ADBC_SPANNER_URI"

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
