#!/usr/bin/env bash
#
# Run a command with a throwaway Cloud Spanner emulator available.
#
# Starts the official emulator in Docker, waits for its gRPC port, exports
# SPANNER_EMULATOR_HOST, runs the command passed as arguments, then tears the
# emulator down again.
#
# Usage:
#   scripts/with-emulator.sh cargo test --test integration -- --nocapture
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

# The forwarded TCP port opens ~1s before the emulator's admin API is actually
# serving RPCs. Proceeding at TCP-open makes the test's `create_instance` fire too
# early, fail, and (being best-effort) get silently dropped — leaving no instance
# and a confusing "Instance not found" later. Gate on a real admin response instead:
# the emulator's REST API returns 200 for a config listing only once it is ready.
if command -v curl >/dev/null 2>&1; then
  echo ">> waiting for the emulator admin API (REST port ${REST_PORT})"
  for _ in $(seq 1 120); do
    if [ "$(curl -s -o /dev/null -w '%{http_code}' \
        "http://127.0.0.1:${REST_PORT}/v1/projects/emulator/instanceConfigs" 2>/dev/null)" = "200" ]; then
      break
    fi
    sleep 0.5
  done
else
  echo ">> curl not found; sleeping briefly for the admin API to come up" >&2
  sleep 3
fi

export SPANNER_EMULATOR_HOST="localhost:${GRPC_PORT}"
echo ">> SPANNER_EMULATOR_HOST=${SPANNER_EMULATOR_HOST}"
echo ">> running: $*"
"$@"
