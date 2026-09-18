"""Import-level smoke tests that don't need a Spanner instance."""

import spanner_adbc
import spanner_adbc.dbapi


def test_entrypoint_constant():
    assert spanner_adbc.ENTRYPOINT == "AdbcSpannerInit"


def test_version_is_present():
    assert isinstance(spanner_adbc.__version__, str)
    assert spanner_adbc.__version__


def test_dbapi_exposes_connect():
    assert callable(spanner_adbc.dbapi.connect)


def test_missing_library_raises_clearly():
    """When no bundled lib is present (source checkout), the error is actionable."""
    import pathlib

    here = pathlib.Path(spanner_adbc.__file__).parent
    has_lib = any(
        (here / n).is_file()
        for n in ("libspanner_adbc.so", "libspanner_adbc.dylib", "spanner_adbc.dll")
    )
    if has_lib:
        # A real wheel is installed; just confirm the path resolves.
        assert spanner_adbc._driver_path()
    else:
        import pytest

        with pytest.raises(RuntimeError, match="no bundled Spanner driver library"):
            spanner_adbc._driver_path()
