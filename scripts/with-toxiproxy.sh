#!/usr/bin/env bash
#
# Run a command with a Cloud Spanner emulator reachable *through a Toxiproxy TCP
# fault-injection proxy*, so a test can inject transport-level faults (latency,
# connection resets, timeouts) between the driver and the emulator's gRPC port.
#
# Topology:
#
#   driver ─(data-plane gRPC)─▶ Toxiproxy listener ─(upstream)─▶ emulator :9010
#                                 :$TOXIPROXY_LISTEN_PORT          (container IP)
#                                      ▲
#                                      │ HTTP admin API (:$TOXIPROXY_ADMIN_PORT)
#                                 the test adds/removes toxics here
#
#   setup (create instance/DB/table, admin API) ─────────────▶ emulator :9010 direct
#
# WHY setup bypasses the proxy: the google-cloud-rust admin client only remaps the
# emulator's admin port when the gRPC endpoint ends exactly in `:9010` (→ `:9020`).
# Pointed at any other endpoint (e.g. the proxy's port) DDL/DB-creation sends its
# admin HTTP request to the wrong port and fails. So the emulator keeps its
# *internal* :9010/:9020 and we reach it directly by the container's IP for schema
# setup, while only the fault-injected data-plane query/DML traffic goes through
# Toxiproxy (data-plane gRPC is not admin-remapped, so proxying it is fine).
#
# To avoid host-port clashes with other concurrent emulators, the emulator
# publishes NO host ports; it is reached by its docker-bridge container IP.
#
# It exports, for the child command:
#   SPANNER_EMULATOR_HOST   — the *proxy* listener (the driver's data plane)
#   SPANNER_EMULATOR_DIRECT — the emulator's real `<ip>:9010` (schema setup only)
#   TOXIPROXY_URL           — the Toxiproxy HTTP admin API base
#   TOXIPROXY_PROXY         — the proxy name the test toggles toxics on
#
# then runs the command and tears both containers down again. It mirrors the
# structure and readiness-gating of scripts/with-emulator.sh.
#
# Usage:
#   scripts/with-toxiproxy.sh cargo test --test resilience -- --nocapture --test-threads=1
#
# Honest scope: Toxiproxy injects *transport* faults only (latency, bandwidth,
# resets, timeouts) — it cannot emit gRPC statuses such as ABORTED, so this
# harness does NOT exercise Spanner's ABORTED-driven transaction replay. See
# tests/RESILIENCE.md.
set -euo pipefail

# --- Emulator (no host ports; reached by container IP on internal :9010/:9020) ---
EMULATOR_IMAGE="${SPANNER_EMULATOR_IMAGE:-gcr.io/cloud-spanner-emulator/emulator}"
EMULATOR_CONTAINER="${SPANNER_EMULATOR_CONTAINER:-adbc-emu-fault}"

# --- Toxiproxy ---
TOXIPROXY_IMAGE="${TOXIPROXY_IMAGE:-ghcr.io/shopify/toxiproxy}"
TOXIPROXY_CONTAINER="${TOXIPROXY_CONTAINER:-adbc-toxiproxy-fault}"
# Port the driver connects to (the proxy listener), on the host.
TOXIPROXY_LISTEN_PORT="${TOXIPROXY_LISTEN_PORT:-8666}"
# Port of the Toxiproxy HTTP admin API, on the host.
TOXIPROXY_ADMIN_PORT="${TOXIPROXY_ADMIN_PORT:-8475}"
# Name of the proxy the test toggles toxics on.
TOXIPROXY_PROXY="${TOXIPROXY_PROXY:-spanner}"

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <command> [args...]" >&2
  exit 2
fi

# Some environments configure a broken Docker credential helper for gcr.io; both
# images are public, so use a clean, empty Docker config to bypass it.
DOCKER_CONFIG="$(mktemp -d)"
export DOCKER_CONFIG
echo '{}' > "$DOCKER_CONFIG/config.json"

cleanup() {
  docker rm -f "$TOXIPROXY_CONTAINER" >/dev/null 2>&1 || true
  docker rm -f "$EMULATOR_CONTAINER" >/dev/null 2>&1 || true
  rm -rf "$DOCKER_CONFIG"
}
trap cleanup EXIT

# --- 1. Start the emulator (internal :9010/:9020, no host publishing) -------------
echo ">> starting Spanner emulator ($EMULATOR_IMAGE) as '$EMULATOR_CONTAINER'"
docker rm -f "$EMULATOR_CONTAINER" >/dev/null 2>&1 || true
docker run -d --name "$EMULATOR_CONTAINER" "$EMULATOR_IMAGE" >/dev/null

# The emulator's container IP on the docker bridge — distinct per container, so
# concurrent emulators never collide, and it preserves the internal :9010 the admin
# port remap depends on.
EMULATOR_IP=""
for _ in $(seq 1 30); do
  EMULATOR_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$EMULATOR_CONTAINER" 2>/dev/null || true)"
  if [ -n "$EMULATOR_IP" ]; then
    break
  fi
  sleep 0.5
done
if [ -z "$EMULATOR_IP" ]; then
  echo "could not determine the emulator container IP" >&2
  docker logs "$EMULATOR_CONTAINER" >&2 || true
  exit 1
fi
echo ">> emulator container IP: ${EMULATOR_IP} (gRPC :9010, admin REST :9020)"

# Gate on a real admin response, not just TCP-open (see with-emulator.sh for why).
if command -v curl >/dev/null 2>&1; then
  echo ">> waiting for the emulator admin API (http://${EMULATOR_IP}:9020)"
  ready=""
  for _ in $(seq 1 120); do
    if [ "$(curl -s -o /dev/null -w '%{http_code}' \
        "http://${EMULATOR_IP}:9020/v1/projects/emulator/instanceConfigs" 2>/dev/null)" = "200" ]; then
      ready="yes"
      break
    fi
    sleep 0.5
  done
  if [ -z "$ready" ]; then
    echo "emulator admin API did not become ready" >&2
    docker logs "$EMULATOR_CONTAINER" >&2 || true
    exit 1
  fi
else
  echo ">> curl not found; sleeping briefly for the admin API to come up" >&2
  sleep 3
fi

# --- 2. Start Toxiproxy (host network so its listener is reachable on 127.0.0.1) --
echo ">> starting Toxiproxy ($TOXIPROXY_IMAGE) as '$TOXIPROXY_CONTAINER'"
docker rm -f "$TOXIPROXY_CONTAINER" >/dev/null 2>&1 || true
docker run -d --name "$TOXIPROXY_CONTAINER" --network host \
  "$TOXIPROXY_IMAGE" \
  -host 0.0.0.0 -port "${TOXIPROXY_ADMIN_PORT}" >/dev/null

echo ">> waiting for the Toxiproxy admin API (port ${TOXIPROXY_ADMIN_PORT})"
TOXIPROXY_URL="http://127.0.0.1:${TOXIPROXY_ADMIN_PORT}"
ready=""
for _ in $(seq 1 60); do
  if [ "$(curl -s -o /dev/null -w '%{http_code}' "${TOXIPROXY_URL}/version" 2>/dev/null)" = "200" ]; then
    ready="yes"
    break
  fi
  sleep 0.5
done
if [ -z "$ready" ]; then
  echo "Toxiproxy admin API did not become ready" >&2
  docker logs "$TOXIPROXY_CONTAINER" >&2 || true
  exit 1
fi

# --- 3. Create the proxy: host listener → emulator gRPC (by container IP) ---------
# Toxiproxy runs in the host network namespace, which can reach the emulator's
# docker-bridge IP directly.
echo ">> creating proxy '${TOXIPROXY_PROXY}': 127.0.0.1:${TOXIPROXY_LISTEN_PORT} -> ${EMULATOR_IP}:9010"
curl -s -X DELETE "${TOXIPROXY_URL}/proxies/${TOXIPROXY_PROXY}" >/dev/null 2>&1 || true
create_code="$(curl -s -o /dev/null -w '%{http_code}' -X POST "${TOXIPROXY_URL}/proxies" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"${TOXIPROXY_PROXY}\",\"listen\":\"127.0.0.1:${TOXIPROXY_LISTEN_PORT}\",\"upstream\":\"${EMULATOR_IP}:9010\",\"enabled\":true}")"
if [ "$create_code" != "201" ] && [ "$create_code" != "200" ]; then
  echo "failed to create Toxiproxy proxy (HTTP ${create_code})" >&2
  docker logs "$TOXIPROXY_CONTAINER" >&2 || true
  exit 1
fi

# --- 4. Run the command ----------------------------------------------------------
# Data plane (driver-under-test) → proxy; schema setup → emulator :9010 direct.
export SPANNER_EMULATOR_HOST="127.0.0.1:${TOXIPROXY_LISTEN_PORT}"
export SPANNER_EMULATOR_DIRECT="${EMULATOR_IP}:9010"
export TOXIPROXY_URL
export TOXIPROXY_PROXY
echo ">> SPANNER_EMULATOR_HOST=${SPANNER_EMULATOR_HOST} (driver data plane, via Toxiproxy)"
echo ">> SPANNER_EMULATOR_DIRECT=${SPANNER_EMULATOR_DIRECT} (schema setup, direct)"
echo ">> TOXIPROXY_URL=${TOXIPROXY_URL} TOXIPROXY_PROXY=${TOXIPROXY_PROXY}"
echo ">> running: $*"
"$@"
