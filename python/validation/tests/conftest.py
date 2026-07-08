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

# Failures pending the two suite changes staged at fornwall/validation (to be submitted
# upstream to adbc-drivers/validation); resolve these when bumping VALIDATION_REF past
# their merge. strict=True so a fixed test fails the run until its xfail is removed —
# each entry must be dropped in the same commit that bumps the pin and adds the matching
# SpannerQuirks hookup (see python/validation/README.md).
_PENDING_UPSTREAM_XFAILS = {
    # fornwall/validation#1: route the test's hardcoded `CREATE TABLE (id INT)` through
    # query_override (Spanner needs INT64 + PRIMARY KEY); quirks hookup: a
    # "TestStatement.test_rows_affected" query_override.
    "test_rows_affected": "suite hardcodes portable CREATE TABLE DDL (fornwall/validation#1)",
    # fornwall/validation#2: let the strict column-list assertions ignore declared
    # synthetic ingest columns; quirks hookup:
    # bulk_ingest_synthetic_columns=["adbc_ingest_key"].
    "test_get_objects_column_filter_table": (
        "create-mode ingest adds the synthetic adbc_ingest_key column (fornwall/validation#2)"
    ),
    "test_get_objects_column_filter_table_name": (
        "create-mode ingest adds the synthetic adbc_ingest_key column (fornwall/validation#2)"
    ),
}


def pytest_collection_modifyitems(
    session: pytest.Session, config: pytest.Config, items: list[pytest.Item]
) -> None:
    adbc_drivers_validation.tests.conftest.pytest_collection_modifyitems(
        session, config, items
    )
    for item in items:
        # Exact match on the parametrization-stripped name: _filter_table is a
        # prefix of _filter_table_name, so no substring matching.
        reason = _PENDING_UPSTREAM_XFAILS.get(item.name.split("[")[0])
        if reason:
            item.add_marker(pytest.mark.xfail(strict=True, reason=reason))


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
