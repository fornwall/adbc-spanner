"""Tests for the ADBC driver manifest this package generates.

The offline tests check the manifest's shape against the specification
(``docs/source/format/driver_manifests.rst`` in apache/arrow-adbc). The emulator
test is the one that actually proves discovery works: it writes a manifest into
a scratch directory, points ``ADBC_DRIVER_PATH`` at it, and connects through the
plain ``adbc_driver_manager`` — once by ``driver="spanner"`` and once by URI
scheme alone, with no ``driver`` option at all.
"""

import os
import tomllib

import pytest

import adbc_driver_spanner
from adbc_driver_spanner import manifest as manifest_mod

# Architecture / OS names from the spec's authoritative platform-tuple table.
_SPEC_OSES = {"linux", "macos", "windows", "freebsd", "openbsd"}
_SPEC_ARCHES = {
    "x86",
    "amd64",
    "arm",
    "armbe",
    "arm64",
    "arm64be",
    "s390x",
    "powerpc",
    "powerpc64",
    "powerpc64le",
    "riscv",
    "riscv64",
    "sparc",
    "sparc64",
    "wasm32",
    "wasm64",
}
_SPEC_ENVS = {"musl", "mingw"}


def _has_library() -> bool:
    try:
        adbc_driver_spanner._driver_path()
    except RuntimeError:
        return False
    return True


needs_library = pytest.mark.skipif(
    not _has_library(),
    reason="no bundled driver library in this checkout/install",
)


def test_platform_tuple_matches_the_spec_vocabulary():
    tuple_ = manifest_mod.platform_tuple()
    os_name, _, rest = tuple_.partition("_")
    arch, _, env = rest.partition("_")
    assert os_name in _SPEC_OSES, tuple_
    assert arch in _SPEC_ARCHES, tuple_
    if env:
        assert env in _SPEC_ENVS, tuple_


@needs_library
def test_manifest_parses_and_has_the_required_keys():
    doc = tomllib.loads(manifest_mod.manifest_text())

    # `manifest_version`, if present, must be 1; managers reject anything higher.
    assert doc["manifest_version"] == 1
    # The only *required* key per the spec.
    shared = doc["Driver"]["shared"]
    assert isinstance(shared, str)
    assert os.path.isabs(shared), shared
    assert os.path.isfile(shared), shared
    assert shared == adbc_driver_spanner._driver_path()

    assert doc["Driver"]["entrypoint"] == adbc_driver_spanner.ENTRYPOINT
    assert doc["version"] == adbc_driver_spanner.__version__
    assert doc["ADBC"]["version"] == "1.1.0"


@needs_library
def test_install_writes_a_manifest_named_for_the_driver(tmp_path):
    written = manifest_mod.install(tmp_path)
    # The base name is what the driver manager matches against the driver name
    # and against a connection URI's scheme, so it must be exactly this.
    assert written.name == "spanner.toml"
    assert written.parent == tmp_path
    assert tomllib.loads(written.read_text())["Driver"]["shared"]

    # Re-running overwrites rather than failing: that is the upgrade path.
    again = manifest_mod.install(tmp_path)
    assert again == written


def test_manifest_escapes_backslashes(tmp_path):
    """A Windows path must survive the round trip through TOML."""
    text = manifest_mod.manifest_text(r"C:\Program Files\ADBC\adbc_spanner.dll")
    shared = tomllib.loads(text)["Driver"]["shared"]
    assert shared.endswith(r"ADBC\adbc_spanner.dll")
    assert "\\\\" not in shared


def test_cli_path_and_print(capsys):
    assert manifest_mod.main(["path"]) == 0
    printed = capsys.readouterr().out.strip()
    assert printed.endswith(os.sep + "spanner.toml")

    if _has_library():
        assert manifest_mod.main(["print"]) == 0
        assert tomllib.loads(capsys.readouterr().out)["Driver"]["shared"]


def test_default_install_dir_is_a_searched_directory():
    """The default target must be a directory a driver manager actually searches.

    In a venv the Python driver manager adds ``<sys.prefix>/etc/adbc/drivers``;
    outside one, the user config directory is searched under the default load
    flags.
    """
    import sys

    directory = manifest_mod.default_install_dir()
    if sys.prefix != sys.base_prefix:
        assert directory == __import__("pathlib").Path(sys.prefix) / "etc/adbc/drivers"
    else:
        assert "adbc" in str(directory).lower()


@needs_library
def test_manifest_makes_the_driver_discoverable_by_name(
    emulator_database, tmp_path, monkeypatch
):
    """End-to-end: resolve the driver through a manifest, not a library path."""
    pytest.importorskip("pyarrow")  # dbapi result fetching needs it
    import adbc_driver_manager.dbapi

    manifest_mod.install(tmp_path)
    monkeypatch.setenv("ADBC_DRIVER_PATH", str(tmp_path))

    db_kwargs = {"spanner.emulator": "true"}
    uri = f"spanner:///{emulator_database}"

    # (1) By driver name.
    with adbc_driver_manager.dbapi.connect(
        driver="spanner", uri=uri, db_kwargs=db_kwargs, autocommit=True
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)

    # (2) By URI scheme alone — no `driver` option at all. This is what the
    # manifest buys over the bundled-path `adbc_driver_spanner.connect()`.
    with adbc_driver_manager.dbapi.connect(
        uri=uri, db_kwargs=db_kwargs, autocommit=True
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)
