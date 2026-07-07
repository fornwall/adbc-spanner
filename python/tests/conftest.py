"""Shared fixtures for the emulator end-to-end tests.

These tests only run when ``SPANNER_EMULATOR_HOST`` points at a Spanner emulator
(matching the Rust integration test); otherwise they self-skip so a plain
``pytest`` is green everywhere.

The ADBC driver connects to an existing *database*, but it cannot create the
enclosing instance/database (those are instance-admin operations). We bootstrap
them here with plain REST calls to the emulator's admin API (port 9020) using
only the standard library — no ``google-cloud-*`` Python dependency — reusing the
same ids and ``emulator-config`` the Rust setup uses.
"""

import json
import os
import time
import urllib.error
import urllib.request

import pytest

PROJECT = "test-project"
INSTANCE = "test-instance"
DATABASE = "adbc-test"


def _rest_base():
    """Base URL of the emulator's REST admin API.

    ``SPANNER_EMULATOR_HOST`` is the gRPC endpoint (``host:9010``); the REST admin
    API lives on ``host:9020`` (override the port with ``SPANNER_EMULATOR_REST_PORT``).
    """
    grpc = os.environ["SPANNER_EMULATOR_HOST"]
    host = grpc.rsplit(":", 1)[0] or "localhost"
    port = os.environ.get("SPANNER_EMULATOR_REST_PORT", "9020")
    return f"http://{host}:{port}"


def _post(url, body):
    """POST JSON; treat 409 (already exists) as success so setup is idempotent."""
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        url, data=data, method="POST", headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:
        if exc.code == 409:
            return None
        raise


def _await_operation(op):
    """Poll a long-running operation to completion (emulator ops are near-instant)."""
    if not op or op.get("done"):
        return
    name = op.get("name")
    if not name:
        return
    for _ in range(120):
        with urllib.request.urlopen(f"{_rest_base()}/v1/{name}", timeout=30) as resp:
            got = json.loads(resp.read() or b"{}")
        if got.get("done"):
            if "error" in got:
                raise RuntimeError(f"operation {name} failed: {got['error']}")
            return
        time.sleep(0.5)
    raise RuntimeError(f"operation {name} did not complete in time")


@pytest.fixture(scope="session")
def emulator_database():
    """Ensure the instance + database exist and return the database path."""
    if not os.environ.get("SPANNER_EMULATOR_HOST"):
        pytest.skip("SPANNER_EMULATOR_HOST not set; skipping emulator end-to-end tests")

    base = _rest_base()
    _await_operation(
        _post(
            f"{base}/v1/projects/{PROJECT}/instances",
            {
                "instanceId": INSTANCE,
                "instance": {
                    "config": f"projects/{PROJECT}/instanceConfigs/emulator-config",
                    "displayName": "ADBC Python test instance",
                    "nodeCount": 1,
                },
            },
        )
    )
    _await_operation(
        _post(
            f"{base}/v1/projects/{PROJECT}/instances/{INSTANCE}/databases",
            {"createStatement": f"CREATE DATABASE `{DATABASE}`"},
        )
    )
    return f"projects/{PROJECT}/instances/{INSTANCE}/databases/{DATABASE}"
