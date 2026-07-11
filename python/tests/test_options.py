"""Offline tests for credential kwargs and the typed option-key enums."""

import inspect

import adbc_driver_spanner
import adbc_driver_spanner.dbapi
from adbc_driver_spanner import (
    ConnectionOptions,
    DatabaseOptions,
    StatementOptions,
    option_kwargs,
)


def test_connect_functions_accept_access_token():
    """Regression: the low-level connect() once referenced access_token without
    declaring it as a parameter, so any call raised NameError."""
    for fn in (adbc_driver_spanner.connect, adbc_driver_spanner.dbapi.connect):
        assert "access_token" in inspect.signature(fn).parameters


def test_option_kwargs_maps_credentials():
    opts = option_kwargs(
        "projects/p/instances/i/databases/d",
        keyfile="/k.json",
        access_token="ya29.tok",
        impersonate_target_principal="sa@p.iam.gserviceaccount.com",
        impersonate_delegates=["a@p.iam", "b@p.iam"],
        impersonate_scopes="https://www.googleapis.com/auth/cloud-platform",
        impersonate_lifetime=1800,
    )
    assert opts["spanner.database"] == "projects/p/instances/i/databases/d"
    assert opts["spanner.auth.keyfile"] == "/k.json"
    assert opts["spanner.auth.access_token"] == "ya29.tok"
    assert opts["spanner.auth.impersonate.target_principal"] == "sa@p.iam.gserviceaccount.com"
    # A sequence of delegates/scopes is rendered as a comma-separated string.
    assert opts["spanner.auth.impersonate.delegates"] == "a@p.iam,b@p.iam"
    assert opts["spanner.auth.impersonate.scopes"] == (
        "https://www.googleapis.com/auth/cloud-platform"
    )
    assert opts["spanner.auth.impersonate.lifetime"] == "1800"


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
