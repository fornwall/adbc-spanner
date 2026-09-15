import adbc_drivers_validation.tests.ingest

from . import spanner

# Which ingest cases `test_create_long_values` runs on — it ingests 1 KiB…128 KiB
# values, so it only makes sense for the variable-length string/binary types. The
# suite's default is just {string, binary}; Spanner's STRING(MAX)/BYTES(MAX) columns
# take the large- and view-layout variants through the same driver code path
# (`bind::cell_value`), so exercise all six. Sizes top out at 128 KiB, well under
# Spanner's 10 MiB per-cell limit.
LONG_VALUE_QUERIES = {
    "ingest/string",
    "ingest/large_string",
    "ingest/string_view",
    "ingest/binary",
    "ingest/large_binary",
    "ingest/binary_view",
}


def pytest_generate_tests(metafunc) -> None:
    quirks = [spanner.get_quirks(metafunc.config.getoption("vendor_version"))]
    return adbc_drivers_validation.tests.ingest.generate_tests(
        quirks, metafunc, long_value_queries=LONG_VALUE_QUERIES
    )


class TestIngest(adbc_drivers_validation.tests.ingest.TestIngest):
    pass
