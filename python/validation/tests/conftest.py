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


@pytest.fixture(scope="session")
def driver(request, pytestconfig) -> adbc_drivers_validation.model.DriverQuirks:
    return spanner.get_quirks(pytestconfig.getoption("vendor_version"))


@pytest.fixture(scope="session")
def driver_path(driver) -> str:
    ext = {"win32": "dll", "darwin": "dylib"}.get(sys.platform, "so")
    # Built cdylib at the repo root: python/validation/tests -> repo root.
    return str(Path(__file__).resolve().parents[3] / f"target/debug/libadbc_spanner.{ext}")
