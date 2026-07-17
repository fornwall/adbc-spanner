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
# The default (CI) path is driven by the single EXCLUDED list below (cases known
# not to pass on Spanner; see adbc-validation/README.md) and runs three checks:
#
#   1. Gate            — everything EXCEPT the excluded cases must pass or skip;
#                        new upstream cases auto-enroll here.
#   2. Expected-failure guard — no excluded case may actually pass.
#   3. Stale guard     — every EXCLUDED entry must still exist upstream.
#
# `--full` runs the whole suite (per-test isolation) for local exploration;
# expect Spanner-specific failures/skips there.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Fixed emulator identifiers, matching tests/integration.rs.
EMULATOR_DATABASE="projects/test-project/instances/test-instance/databases/adbc-test"

# Sanitizers for the C++ side (harness + arrow-adbc + driver manager), e.g.
# ADBC_VALIDATION_SANITIZE=address,undefined. The Rust cdylib stays uninstrumented (see
# adbc-validation/CMakeLists.txt), but ASan's process-wide malloc/free/memcpy interceptors
# still catch double-free / overflow / use-after-free on the C-ABI structs crossing the
# boundary.
SANITIZE="${ADBC_VALIDATION_SANITIZE:-}"

# ADBC_VALIDATION_RUST_SANITIZE=address instruments the cdylib ITSELF via nightly
# `-Zsanitizer=address` + `-Zbuild-std` (std must be instrumented too, else ASan misreports
# std allocations), catching memory bugs *inside* Rust that the C-side-only ASan cannot see.
# Only `address` is supported (no `-Zsanitizer=undefined` cdylib story). REQUIRES
# ADBC_VALIDATION_SANITIZE to include `address`: the C++ executable owns the runtime and both
# sides must share it — Rust uses compiler-rt ASan, so the C++ side needs clang (gcc's libasan
# is incompatible and aborts at startup when mixed), hence the CC/CXX clang default below.
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

# One build tree per flag combination, so switching legs doesn't force a full arrow-adbc
# rebuild and a cached CI tree can't mix flags. The Rust-instrumented leg compiles the C++ side
# with clang rather than gcc, so it needs a tree of its own distinct from the C-side-only one.
build_suffix=""
[ -n "$SANITIZE" ] && build_suffix="-san"
[ -n "$RUST_SANITIZE" ] && build_suffix="-rustsan"
BUILD_DIR="${ADBC_VALIDATION_BUILD_DIR:-$REPO_ROOT/.adbc-validation-build$build_suffix}"

# Disable LeakSanitizer: the driver's shared Tokio runtime, gRPC connection pools and lazy
# globals are intentionally process-lifetime and would swamp the run with non-actionable
# "leaks". ASan memory errors and UBSan reports stay fatal. Exported for both the build (gtest
# discovery runs the binary) and the run.
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
# not-applicable to Spanner. Everything NOT listed must pass (or self-skip) and is
# gate-enforced, so new upstream cases run automatically. Each entry is enforced to
# still exist upstream (stale guard) and to still fail-or-skip (expected-failure
# guard). Cases needing only SQL adaptation are handled by SpannerQuirks::RewriteSql
# in adbc-validation/spanner_validation.cc, not by exclusion; cases a quirk makes
# self-skip need no entry here either (the gate tolerates skips).
# ---------------------------------------------------------------------------
EXCLUDED=(
  # --- Arrow types the driver cannot map to a Spanner column ------------------
  # Duration / Interval: unmapped, so ingest fails with "cannot create a Spanner
  #   column for Arrow type ...". The natural target, Spanner's INTERVAL column, is
  #   unsupported by the emulator (CREATE TABLE fails with a GOOGLESQL_RET_CHECK on
  #   IsSupportedColumnType), so neither could pass in CI even with driver support.
  #   Interval(MonthDayNano) is a clean 1:1 with Spanner INTERVAL on real Spanner and
  #   is a candidate for a future PR; Duration has no fixed-unit Spanner counterpart
  #   (one INTERVAL column reads back as only one Arrow type) and the suite's
  #   ValidateIngestedTemporalData FAILs any non-TIMESTAMP temporal readback.
  # UInt64: u64::MAX exceeds i64::MAX, so it cannot widen to INT64 losslessly. It
  #   would fit NUMERIC, but that reads back as Decimal128, not the INT64 the shared
  #   IngestSelectRoundTripType quirk expects — and the suite's SchemaField cannot
  #   express Decimal128(precision, scale) to assert such a round-trip.
  'SpannerStatementTest.SqlIngestDuration'
  'SpannerStatementTest.SqlIngestInterval'
  'SpannerStatementTest.SqlIngestUInt64'

  # --- ECANCELED through the C stream -----------------------------------------
  # SqlQueryCancel wants get_next to return exactly ECANCELED (125), but arrow-rs's
  # FFI_ArrowArrayStream exporter (used by adbc_ffi) can only map errors to
  # ENOSYS/ENOMEM/EIO/EINVAL, so no Rust driver behind adbc_ffi can emit 125 today.
  # Cancellation itself works and is sticky; covered natively by
  # cancel_between_stream_chunks_cancels_the_next_fetch in tests/integration.rs.
  'SpannerStatementTest.SqlQueryCancel'
)

# The colon-joined --gtest_filter value for the EXCLUDED set. Prefixed with `-`
# it negates (the gate: run everything else); bare it selects only these (the
# expected-failure guard).
EXCLUDED_FILTER="$(IFS=:; printf '%s' "${EXCLUDED[*]}")"

# ---------------------------------------------------------------------------

# Cross-boundary ASan canary (rust-asan leg only). A green rust-asan leg is no proof the Rust
# instrumentation is armed — a -Zsanitizer/-Zbuild-std regression disarms it silently. So call an
# intentionally-out-of-bounds Rust symbol (adbc_spanner_asan_canary, compiled only under
# --cfg asan_canary) from a clang -fsanitize=address program against a C++-allocated buffer: the
# exact cross-boundary shape this leg exists to cover. ASan MUST report a heap-buffer-overflow;
# if it does not, the leg is a no-op and we fail loudly.
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
    # Nightly + build-std so the cdylib AND std are instrumented; -Zbuild-std forces an explicit
    # --target, so the artifact lands under target/<triple>/debug/. `--cfg asan_canary` compiles
    # the test-only tripwire (src/asan_canary.rs) into THIS build only — nothing else sets that
    # cfg, so the out-of-bounds symbol never leaks into a shipped cdylib.
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

# Expected-failure guard (xfail-strict): run ONLY the excluded cases and assert none
# PASSED — one that starts passing must be removed from EXCLUDED so the gate enforces it.
# The run exits non-zero by design, so its status is captured, not propagated. The
# discriminator comes from the JUnit XML (gtest writes each <testcase> opening tag on one
# line): passed = result="completed" with no <failure> child, skipped = result="skipped" —
# so a skipped excluded case does not count as passing.
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
  # The creation calls above are deliberately idempotent (|| true), so this wait is the
  # actual gate: fail loudly rather than run the suite against a database that isn't there.
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
  # Per-test process isolation via ctest, so a failing assertion (which aborts the process
  # — see the README note on the upstream non-idempotent error release) only fails that one
  # test rather than hiding the rest.
  ctest --test-dir "$BUILD_DIR" --output-on-failure || true
else
  # Gate first (must pass), then assert the excluded set still all fails/skips.
  run_gate
  run_xfail_guard
fi
