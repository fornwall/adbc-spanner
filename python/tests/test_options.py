"""Offline tests for credential kwargs and the typed option-key enums."""

import inspect

import adbc_driver_spanner
import adbc_driver_spanner.dbapi
from adbc_driver_spanner import (
    ConnectionOptions,
    DatabaseOptions,
    StatementOptions,
)


def test_connect_functions_take_db_kwargs():
    """Both connect entry points expose the raw-option escape hatch and nothing
    else credential-shaped — all options travel through db_kwargs now."""
    for fn in (adbc_driver_spanner.connect, adbc_driver_spanner.dbapi.connect):
        params = inspect.signature(fn).parameters
        assert "db_kwargs" in params
        # The friendly per-credential kwargs were removed in favour of db_kwargs.
        assert "access_token" not in params
        assert "keyfile" not in params
        assert "uri" not in params


def test_option_enums_are_exported_and_well_formed():
    for enum_cls in (DatabaseOptions, ConnectionOptions, StatementOptions):
        values = [member.value for member in enum_cls]
        # No duplicate keys within a level.
        assert len(values) == len(set(values))
        # Every key uses a known ADBC prefix.
        assert all(v.startswith(("spanner.", "adbc.", "uri")) for v in values)


def test_option_enum_values_are_usable_as_kwargs_keys():
    # The whole point of the enums: .value is the raw string db_kwargs expects.
    assert DatabaseOptions.ACCESS_TOKEN.value == "spanner.auth.access_token"
    assert ConnectionOptions.READ_STALENESS.value == "spanner.read.staleness"
    assert StatementOptions.ROWS_PER_BATCH.value == "spanner.rows_per_batch"
