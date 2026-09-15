import adbc_drivers_validation.tests.connection

from . import spanner


def pytest_generate_tests(metafunc) -> None:
    quirks = [spanner.get_quirks(metafunc.config.getoption("vendor_version"))]
    return adbc_drivers_validation.tests.connection.generate_tests(quirks, metafunc)


# The shared connection suite is inherited wholesale — nothing here needs adapting for
# Spanner. (Until 0.7 the two `get_objects` tests that assert the *exact* ingested column
# list were overridden to filter out the synthetic `adbc_ingest_key` primary-key column
# create-mode ingest used to add; the create modes now declare no primary key at all, so
# an ingested table's columns are exactly the ingested ones — see
# adbc-drivers/validation#250 for the discussion of the alternative, a shared-suite
# `bulk_ingest_synthetic_column` feature flag.)
TestConnection = adbc_drivers_validation.tests.connection.TestConnection
