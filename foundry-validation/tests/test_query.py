import adbc_drivers_validation.tests.query

from . import spanner


def pytest_generate_tests(metafunc) -> None:
    quirks = [spanner.get_quirks(metafunc.config.getoption("vendor_version"))]
    return adbc_drivers_validation.tests.query.generate_tests(quirks, metafunc)

from adbc_drivers_validation.tests.query import TestQuery  # noqa: E402,F401
