"""Import-level smoke tests that don't need a Spanner instance."""

import adbc_driver_spanner
import adbc_driver_spanner.dbapi


def test_entrypoint_constant():
    assert adbc_driver_spanner.ENTRYPOINT == "AdbcSpannerInit"


def test_version_is_present():
    assert isinstance(adbc_driver_spanner.__version__, str)
    assert adbc_driver_spanner.__version__


def test_dbapi_exposes_connect():
    assert callable(adbc_driver_spanner.dbapi.connect)


def test_missing_library_raises_clearly():
    """When no bundled lib is present (source checkout), the error is actionable."""
    import pathlib

    here = pathlib.Path(adbc_driver_spanner.__file__).parent
    has_lib = any(
        (here / n).is_file()
        for n in ("libadbc_spanner.so", "libadbc_spanner.dylib", "adbc_spanner.dll")
    )
    if has_lib:
        # A real wheel is installed; just confirm the path resolves.
        assert adbc_driver_spanner._driver_path()
    else:
        import pytest

        with pytest.raises(RuntimeError, match="no bundled Spanner driver library"):
            adbc_driver_spanner._driver_path()
