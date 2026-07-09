import adbc_drivers_validation.tests.statement

from . import spanner


def pytest_generate_tests(metafunc) -> None:
    quirks = [spanner.get_quirks(metafunc.config.getoption("vendor_version"))]
    return adbc_drivers_validation.tests.statement.generate_tests(quirks, metafunc)

from adbc_drivers_validation.tests.statement import TestStatement  # noqa: E402,F401
