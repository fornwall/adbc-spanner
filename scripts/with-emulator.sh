#!/usr/bin/env bash
#
# Run a command with a throwaway Cloud Spanner emulator available.
#
# Starts the official emulator in Docker, waits for its gRPC port, exports
# SPANNER_EMULATOR_HOST, runs the command passed as arguments, then tears the
# emulator down again.
#
# Usage:
#   scripts/with-emulator.sh cargo test --test emulator -- --nocapture
#   scripts/with-emulator.sh cargo test        # run the whole suite against the emulator
#
set -euo pipefail

IMAGE="${SPANNER_EMULATOR_IMAGE:-gcr.io/cloud-spanner-emulator/emulator}"
CONTAINER="${SPANNER_EMULATOR_CONTAINER:-adbc-spanner-emulator}"
GRPC_PORT="${SPANNER_EMULATOR_GRPC_PORT:-9010}"
REST_PORT="${SPANNER_EMULATOR_REST_PORT:-9020}"

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <command> [args...]" >&2
  exit 2
fi

# Some environments configure a broken Docker credential helper for gcr.io; the
# emulator image is public, so use a clean, empty Docker config to bypass it.
DOCKER_CONFIG="$(mktemp -d)"
export DOCKER_CONFIG
echo '{}' > "$DOCKER_CONFIG/config.json"

cleanup() {
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  rm -rf "$DOCKER_CONFIG"
}
trap cleanup EXIT

echo ">> starting Spanner emulator ($IMAGE) as '$CONTAINER'"
docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
docker run -d --name "$CONTAINER" \
  -p "${GRPC_PORT}:9010" -p "${REST_PORT}:9020" \
  "$IMAGE" >/dev/null

echo ">> waiting for gRPC port ${GRPC_PORT}"
for _ in $(seq 1 30); do
  if bash -c "echo > /dev/tcp/127.0.0.1/${GRPC_PORT}" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

export SPANNER_EMULATOR_HOST="localhost:${GRPC_PORT}"
echo ">> SPANNER_EMULATOR_HOST=${SPANNER_EMULATOR_HOST}"
echo ">> running: $*"
"$@"
