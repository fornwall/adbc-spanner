import adbc_drivers_validation.tests.ingest

from . import spanner


def pytest_generate_tests(metafunc) -> None:
    quirks = [spanner.get_quirks(metafunc.config.getoption("vendor_version"))]
    return adbc_drivers_validation.tests.ingest.generate_tests(quirks, metafunc)


class TestIngest(adbc_drivers_validation.tests.ingest.TestIngest):
    pass
