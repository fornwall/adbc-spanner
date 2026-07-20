#!/usr/bin/env bash
#
# Run the ADBC Driver Foundry validation suite (https://github.com/adbc-drivers/validation)
# against the adbc-spanner driver. This is a *type/feature coverage* harness — complementary to
# scripts/run-adbc-validation.sh, which runs Apache arrow-adbc's C++ C-ABI conformance suite.
#
# The suite is driver-agnostic: it loads our cdylib through the ADBC driver manager. It is not
# related to driverbase-rs (a Rust authoring framework we do not use).
#
# With no Spanner target configured it starts a throwaway emulator (via scripts/with-emulator.sh),
# creates the test instance/database itself, then runs the suite:
#
#   scripts/run-foundry-validation.sh                      # emulator
#   SPANNER_EMULATOR_HOST=localhost:9010 scripts/run-foundry-validation.sh
#   scripts/run-foundry-validation.sh -k connection        # pass extra pytest args
#
# The suite's base query corpus assumes a portable SQL dialect (no mandatory PRIMARY KEY,
# INTEGER/BIGINT type names, positional $1 parameters); the Spanner-dialect overrides under
# foundry-validation/queries/spanner/ cover the corpus, so this run GATES CI
# (foundry-validation.yml). Every case passes or skips with a reason — no expected failures;
# see foundry-validation/README.md.
#
# -e matters here: without it a failed `cargo build` let the suite proceed and
# validate a *stale* previously-built cdylib — plausible-looking results for old code.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Pinned so the corpus/behaviour is reproducible; bump deliberately. Points at the
# upstream adbc-drivers/validation suite: the test_rows_affected DDL-override hook we
# rely on landed upstream (#249), and the create-mode synthetic-column quirk is handled
# by a driver-side test override (see tests/test_connection.py, adbc-drivers/validation#250)
# rather than a shared-suite feature — so no fork is needed.
VALIDATION_REF="${ADBC_VALIDATION_REF:-dbc6857ff7ab7c43e98d7729a63ee8d9303ac1f9}"
VALIDATION_REPO="${ADBC_VALIDATION_REPO:-adbc-drivers/validation}"
PYTHON="${PYTHON:-python3}"
EMULATOR_DATABASE="projects/test-project/instances/test-instance/databases/adbc-test"

# No target configured: run under a throwaway emulator, then re-enter this script.
if [ -z "${SPANNER_EMULATOR_HOST:-}" ] && [ -z "${SPANNER_GCP_DATABASE:-}" ]; then
  exec "$REPO_ROOT/scripts/with-emulator.sh" "$0" "$@"
fi

echo ">> building the adbc-spanner cdylib"
cargo build

# The pin must hold even when *some* version is already installed: import-existence
# alone silently kept whatever ref happened to be present, defeating "pinned so the
# corpus/behaviour is reproducible". pip records the resolved commit in the
# installed distribution's direct_url metadata, which `pip freeze` reports as
# `pkg @ git+URL@<sha>` — compare that against the pin and reinstall on mismatch.
installed_ref="$("$PYTHON" -m pip freeze 2>/dev/null \
  | sed -n 's/^adbc[-_]drivers[-_]validation @ git+.*@//p' | head -n 1)"
if [ "$installed_ref" != "$VALIDATION_REF" ]; then
  echo ">> installing the validation suite at pinned $VALIDATION_REF (installed: ${installed_ref:-none})"
  "$PYTHON" -m pip install --quiet \
    "adbc_drivers_validation @ git+https://github.com/${VALIDATION_REPO}@${VALIDATION_REF}" \
    pyarrow pytest
fi

# Resolve the target database, creating it on the emulator (which starts empty).
if [ -n "${SPANNER_EMULATOR_HOST:-}" ]; then
  export ADBC_SPANNER_URI="spanner:///$EMULATOR_DATABASE"
  rest="http://${SPANNER_EMULATOR_HOST%:*}:${SPANNER_EMULATOR_REST_PORT:-9020}"
  echo ">> creating emulator instance/database via the admin REST API ($rest)"
  curl -sf -X POST "$rest/v1/projects/test-project/instances" \
    -H 'Content-Type: application/json' \
    -d '{"instanceId":"test-instance","instance":{"config":"projects/test-project/instanceConfigs/emulator-config","displayName":"adbc","nodeCount":1}}' \
    >/dev/null 2>&1 || true
  curl -sf -X POST "$rest/v1/projects/test-project/instances/test-instance/databases" \
    -H 'Content-Type: application/json' \
    -d '{"createStatement":"CREATE DATABASE `adbc-test`"}' >/dev/null 2>&1 || true
  # The creation calls above are deliberately idempotent (|| true: the instance and
  # database may already exist from a previous run), so this wait is the actual
  # gate — and it must fail loudly if the database never appears, rather than let
  # pytest run against a database that does not exist.
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
  IFS='.' read -r p i d <<<"$SPANNER_GCP_DATABASE"
  export ADBC_SPANNER_URI="spanner:///projects/$p/instances/$i/databases/$d"
fi

echo ">> ADBC_SPANNER_URI=$ADBC_SPANNER_URI"
echo ">> running the Foundry validation suite"
cd "$REPO_ROOT/foundry-validation"
exec "$PYTHON" -m pytest -p no:cacheprovider "$@"
