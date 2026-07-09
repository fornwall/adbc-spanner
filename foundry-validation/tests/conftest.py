import os
import sys
from pathlib import Path

import adbc_drivers_validation.model
import adbc_drivers_validation.tests.conftest
import pytest
from adbc_drivers_validation.tests.conftest import (  # noqa: F401
    conn,
    conn_factory,
    db_kwargs,
    manual_test,
    noci,
)

from . import spanner


def pytest_collection_modifyitems(
    session: pytest.Session, config: pytest.Config, items: list[pytest.Item]
) -> None:
    # Delegate to the suite's own hook (it filters out the interactive test_repl
    # unless --repl is passed, adds JUnit metadata, etc.). It is imported as a
    # module here rather than registered as a plugin, so it only runs if we call it.
    adbc_drivers_validation.tests.conftest.pytest_collection_modifyitems(
        session, config, items
    )


def pytest_addoption(parser):
    adbc_drivers_validation.tests.conftest.pytest_addoption(parser)
    parser.addoption("--vendor-version", action="store", default="emulator")


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    # Fail loudly on a "green" run that exercised no real coverage.
    #
    # The suite reports Spanner's expected SQL-dialect divergences as per-case
    # *skips* (each with a reason), and pytest exits 0 when every collected case is
    # skipped. So an all-skip run is indistinguishable, by exit code alone, from a
    # broken harness that silently skipped everything (e.g. the driver failing to
    # load, or a fixture misconfiguration) — the exact "harness breakage looks like
    # an expected dialect skip" hole that the gating workflow otherwise leaves open.
    #
    # When FOUNDRY_VALIDATION_REQUIRE_PASSES is set — the CI workflow sets it, so
    # ad-hoc local `-k` runs (which may legitimately select only skipped cases) are
    # unaffected — require that at least one collected case actually passed. This
    # mirrors the env-gated fail-loud convention of the ADBC_TEST_REQUIRE_TARGET
    # skip guard used by the Rust/Python integration suites.
    if exitstatus != int(pytest.ExitCode.OK):
        return
    if not os.environ.get("FOUNDRY_VALIDATION_REQUIRE_PASSES"):
        return
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    passed = len(reporter.stats.get("passed", [])) if reporter is not None else 0
    if session.testscollected > 0 and passed == 0:
        message = (
            f"FOUNDRY_VALIDATION_REQUIRE_PASSES: collected {session.testscollected} "
            "case(s) but none passed — every case skipped or errored. This usually "
            "means the harness itself is broken (driver failed to load, connection "
            "setup failed), not an expected Spanner SQL-dialect skip."
        )
        if reporter is not None:
            reporter.write_line(message, red=True)
        else:  # pragma: no cover - terminal reporter is a default plugin
            print(message)
        session.exitstatus = int(pytest.ExitCode.TESTS_FAILED)


@pytest.fixture(scope="session")
def driver(request, pytestconfig) -> adbc_drivers_validation.model.DriverQuirks:
    return spanner.get_quirks(pytestconfig.getoption("vendor_version"))


@pytest.fixture(scope="session")
def driver_path(driver) -> str:
    ext = {"win32": "dll", "darwin": "dylib"}.get(sys.platform, "so")
    # Built cdylib at the repo root: foundry-validation/tests -> repo root.
    return str(Path(__file__).resolve().parents[2] / f"target/debug/libadbc_spanner.{ext}")
