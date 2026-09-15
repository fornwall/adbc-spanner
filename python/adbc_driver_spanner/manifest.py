"""Generate and install an ADBC *driver manifest* for the bundled Spanner driver.

A driver manifest is a small TOML file that lets any ADBC driver manager find a
driver **by name** instead of by an explicit path to a shared library::

    adbc_driver_manager.dbapi.connect(driver="spanner", uri="spanner:///...")

and — because a driver manager with no ``driver`` option falls back to the
*scheme* of the ``uri`` option, and this driver's connection URIs already use
the ``spanner://`` scheme — by URI alone::

    adbc_driver_manager.dbapi.connect(uri="spanner:///projects/p/instances/i/databases/d")

See the ADBC specification, ``docs/source/format/driver_manifests.rst``.

Why this is not a plain data file in the wheel
----------------------------------------------

The manifest's ``Driver.shared`` value is passed to ``dlopen``/``LoadLibraryExW``
**verbatim**: neither the C nor the Rust driver manager resolves it relative to
the manifest file. So a manifest must contain the *absolute* path of the shared
library, which is only known once the wheel is installed. A manifest shipped
pre-baked in the wheel would therefore point nowhere. Instead this module writes
one at install time::

    python -m adbc_driver_spanner.manifest install
    # or, equivalently, the console script:
    adbc-driver-spanner-install-manifest

By default it writes to a directory the driver manager already searches (see
:func:`default_install_dir`).
"""

from __future__ import annotations

import argparse
import os
import pathlib
import platform
import sys
import sysconfig
import typing

from . import ENTRYPOINT, __version__, _driver_path

__all__ = [
    "MANIFEST_NAME",
    "default_install_dir",
    "install",
    "manifest_text",
    "platform_tuple",
]

#: The manifest's file name. The *base name* is what a driver manager matches
#: against the driver name (and against a connection URI's scheme), so calling
#: it anything else breaks ``driver="spanner"``.
MANIFEST_NAME = "spanner.toml"

# Architecture names from the spec's authoritative platform-tuple table, keyed by
# the values `platform.machine()` reports. Only the architectures this project
# builds for are listed; anything else is reported as an error rather than
# guessed at.
_ARCHES = {
    "x86_64": "amd64",
    "amd64": "amd64",
    "aarch64": "arm64",
    "arm64": "arm64",
}

_OSES = {
    "linux": "linux",
    "darwin": "macos",
    "win32": "windows",
    "cygwin": "windows",
    "freebsd": "freebsd",
    "openbsd": "openbsd",
}


def _is_musl() -> bool:
    """Whether this interpreter runs against musl libc rather than glibc.

    The driver manager appends ``_musl`` to the platform tuple when *it* was
    compiled against musl. The driver manager reaching this code is the
    ``adbc_driver_manager`` wheel installed next to this package, so it matches
    the interpreter's own libc.
    """
    if sys.platform != "linux":
        return False
    # The platform tag is the most direct signal: CPython built on Alpine
    # reports e.g. "linux-x86_64" but its wheel tags are musllinux_*, and
    # `sysconfig.get_platform()` on a musl build carries "musl" on the
    # distributions that set it.
    if "musl" in (sysconfig.get_platform() or ""):
        return True
    try:
        # glibc reports e.g. ("glibc", "2.35"); a musl build reports ("", "").
        name, _version = platform.libc_ver()
    except OSError:  # pragma: no cover - platform.libc_ver is very forgiving
        return False
    return not name


def platform_tuple() -> str:
    """The ``<os>_<arch>[_<env>]`` tuple the driver manager looks up.

    Mirrors ``InternalAdbcCurrentArch()`` in the C driver manager
    (``c/driver_manager/adbc_driver_manager.cc``) and ``arch_triplet()`` in the
    Rust one.
    """
    os_name = _OSES.get(sys.platform)
    if os_name is None and sys.platform.startswith("freebsd"):
        os_name = "freebsd"
    machine = platform.machine().lower()
    arch = _ARCHES.get(machine)
    if os_name is None or arch is None:
        raise RuntimeError(
            "adbc_driver_spanner: cannot derive an ADBC platform tuple for "
            f"sys.platform={sys.platform!r} machine={platform.machine()!r}; "
            "this platform is not one the Spanner driver ships binaries for."
        )
    suffix = "_musl" if os_name == "linux" and _is_musl() else ""
    return f"{os_name}_{arch}{suffix}"


def _toml_string(value: str) -> str:
    """Render a TOML basic string.

    Basic (double-quoted) strings are used rather than literal ones because a
    Windows install path contains backslashes *and* may contain a single quote,
    which a TOML literal string cannot escape.
    """
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def manifest_text(library: typing.Union[str, "os.PathLike[str]", None] = None) -> str:
    """Render a driver manifest for this installation.

    Parameters
    ----------
    library:
        Absolute path to the driver shared library. Defaults to the library
        bundled in this wheel.

    Notes
    -----
    ``Driver.shared`` is emitted as a plain *string* rather than a table of
    platform tuples. The spec allows either ("``Driver.shared`` ... must either
    be a string (single path) or a table of platform-specific paths"), and a
    manifest generated for one installed wheel describes exactly one platform,
    so the string form cannot disagree with the driver manager about how this
    platform is spelled.
    """
    path = pathlib.Path(library) if library is not None else pathlib.Path(_driver_path())
    path = path.resolve()
    return f"""\
# ADBC driver manifest for Google Cloud Spanner.
#
# Generated by `python -m adbc_driver_spanner.manifest install` for the library
# bundled in the adbc-driver-spanner wheel. Regenerate it after upgrading,
# reinstalling or moving the Python environment: the path below is absolute and
# is passed to the dynamic loader verbatim.
#
# Platform tuple at generation time: {platform_tuple()}

manifest_version = 1

name = "Google Cloud Spanner"
publisher = "fornwall"
license = "Apache-2.0"
version = {_toml_string(__version__)}
url = "https://github.com/fornwall/adbc-spanner"
source = "adbc-driver-spanner (Python wheel)"

[ADBC]
version = "1.1.0"

[Driver]
entrypoint = {_toml_string(ENTRYPOINT)}
shared = {_toml_string(str(path))}
"""


def default_install_dir() -> pathlib.Path:
    """The directory to install the manifest into by default.

    Inside a virtual environment this is ``<sys.prefix>/etc/adbc/drivers``,
    which the Python driver manager adds to its search paths automatically
    (``adbc_driver_manager/_lib.pyx`` does this whenever
    ``sys.prefix != sys.base_prefix``). That keeps the manifest scoped to the
    same environment as the wheel it points into.

    Outside a venv it is the driver manager's *user* config directory, which is
    searched when the ``SEARCH_USER`` load flag is set (it is, by default):

    * Linux: ``$XDG_CONFIG_HOME/adbc/drivers``, else ``~/.config/adbc/drivers``
    * macOS: ``~/Library/Application Support/ADBC/Drivers``
    * Windows: ``%LOCALAPPDATA%\\ADBC\\Drivers``
    """
    if sys.prefix != sys.base_prefix:
        return pathlib.Path(sys.prefix) / "etc" / "adbc" / "drivers"

    if sys.platform == "win32":
        local = os.environ.get("LOCALAPPDATA")
        if not local:
            raise RuntimeError(
                "adbc_driver_spanner: %LOCALAPPDATA% is not set, so the driver "
                "manager's user config directory cannot be located; pass an "
                "explicit directory instead."
            )
        return pathlib.Path(local) / "ADBC" / "Drivers"

    if sys.platform == "darwin":
        return pathlib.Path.home() / "Library" / "Application Support" / "ADBC" / "Drivers"

    xdg = os.environ.get("XDG_CONFIG_HOME")
    base = pathlib.Path(xdg) if xdg else pathlib.Path.home() / ".config"
    return base / "adbc" / "drivers"


def install(
    directory: typing.Union[str, "os.PathLike[str]", None] = None,
    *,
    library: typing.Union[str, "os.PathLike[str]", None] = None,
) -> pathlib.Path:
    """Write ``spanner.toml`` into *directory* and return the path written.

    *directory* defaults to :func:`default_install_dir` and is created if it
    does not exist. An existing manifest is overwritten, so re-running this
    after an upgrade is the supported way to refresh a stale path.
    """
    target_dir = pathlib.Path(directory) if directory is not None else default_install_dir()
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / MANIFEST_NAME
    target.write_text(manifest_text(library), encoding="utf-8")
    return target


def main(argv: typing.Optional[typing.Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m adbc_driver_spanner.manifest",
        description=(
            "Install an ADBC driver manifest so the Spanner driver resolves by "
            'name (driver="spanner") or by URI scheme (uri="spanner:///...").'
        ),
    )
    sub = parser.add_subparsers(dest="command")

    p_install = sub.add_parser("install", help="write the manifest (default)")
    p_install.add_argument(
        "-d",
        "--dir",
        dest="directory",
        default=None,
        help=(
            f"directory to write {MANIFEST_NAME} into (default: a directory the "
            "driver manager searches; see the `path` subcommand)"
        ),
    )
    sub.add_parser("print", help="print the manifest to stdout without writing it")
    sub.add_parser("path", help="print the default install path and exit")

    args = parser.parse_args(argv)
    command = args.command or "install"

    if command == "print":
        sys.stdout.write(manifest_text())
        return 0
    if command == "path":
        print(default_install_dir() / MANIFEST_NAME)
        return 0

    written = install(getattr(args, "directory", None))
    print(f"Wrote {written}")
    print(
        'The Spanner driver now resolves as driver="spanner" and from '
        'uri="spanner:///..." for this environment.'
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised via the console script
    raise SystemExit(main())
