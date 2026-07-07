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
# NOTE: the suite's base query corpus assumes a portable SQL dialect (no mandatory PRIMARY KEY,
# INTEGER/BIGINT type names, positional $1 parameters). Spanner diverges on all three, so many
# type/* cases currently error/fail until Spanner-dialect overrides are added under
# python/validation/queries/spanner/. See python/validation/README.md. This script therefore does
# not gate CI; it is an exploratory coverage harness.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Pinned so the corpus/behaviour is reproducible; bump deliberately.
VALIDATION_REF="${ADBC_VALIDATION_REF:-575a41bfd96b09d9d8a057d0ea1e66a27a315475}"
PYTHON="${PYTHON:-python3}"
EMULATOR_DATABASE="projects/test-project/instances/test-instance/databases/adbc-test"

# No target configured: run under a throwaway emulator, then re-enter this script.
if [ -z "${SPANNER_EMULATOR_HOST:-}" ] && [ -z "${SPANNER_GCP_DATABASE:-}" ]; then
  exec "$REPO_ROOT/scripts/with-emulator.sh" "$0" "$@"
fi

echo ">> building the adbc-spanner cdylib"
cargo build

echo ">> ensuring the validation suite is installed (pinned $VALIDATION_REF)"
if ! "$PYTHON" -c "import adbc_drivers_validation" >/dev/null 2>&1; then
  "$PYTHON" -m pip install --quiet \
    "adbc_drivers_validation @ git+https://github.com/adbc-drivers/validation@${VALIDATION_REF}" \
    pyarrow pytest
fi

# Resolve the target database, creating it on the emulator (which starts empty).
if [ -n "${SPANNER_EMULATOR_HOST:-}" ]; then
  export ADBC_SPANNER_DATABASE="$EMULATOR_DATABASE"
  rest="http://${SPANNER_EMULATOR_HOST%:*}:${SPANNER_EMULATOR_REST_PORT:-9020}"
  echo ">> creating emulator instance/database via the admin REST API ($rest)"
  curl -sf -X POST "$rest/v1/projects/test-project/instances" \
    -H 'Content-Type: application/json' \
    -d '{"instanceId":"test-instance","instance":{"config":"projects/test-project/instanceConfigs/emulator-config","displayName":"adbc","nodeCount":1}}' \
    >/dev/null 2>&1 || true
  curl -sf -X POST "$rest/v1/projects/test-project/instances/test-instance/databases" \
    -H 'Content-Type: application/json' \
    -d '{"createStatement":"CREATE DATABASE `adbc-test`"}' >/dev/null 2>&1 || true
  for _ in $(seq 1 40); do
    curl -sf "$rest/v1/projects/test-project/instances/test-instance/databases" 2>/dev/null \
      | grep -q 'adbc-test' && break
    sleep 0.25
  done
else
  IFS='.' read -r p i d <<<"$SPANNER_GCP_DATABASE"
  export ADBC_SPANNER_DATABASE="projects/$p/instances/$i/databases/$d"
fi

echo ">> ADBC_SPANNER_DATABASE=$ADBC_SPANNER_DATABASE"
echo ">> running the Foundry validation suite"
cd "$REPO_ROOT/python/validation"
exec "$PYTHON" -m pytest -p no:cacheprovider "$@"
