"""Setuptools shim for building platform wheels.

Metadata lives in ``pyproject.toml``; this file exists only to:

1. Mark the distribution as non-pure (it ships a compiled shared library), so
   the wheel gets a platform tag instead of ``any``.
2. Force the ``py3-none-<platform>`` tag: the wheel contains no Python
   extension module, so it works on any Python 3 ABI — we just need the right
   *platform* tag, which CI passes via ``--plat-name``.
"""

from setuptools import setup
from setuptools.dist import Distribution

try:  # pragma: no cover - depends on installed wheel version
    from wheel.bdist_wheel import bdist_wheel
except ImportError:  # newer setuptools vendors it here
    from setuptools.command.bdist_wheel import bdist_wheel


class BinaryDistribution(Distribution):
    """Force a platform (non-pure) wheel even without an ext module."""

    def has_ext_modules(self) -> bool:  # noqa: D401
        return True


class WheelPy3None(bdist_wheel):
    """Emit a ``py3-none-<platform>`` tag (ABI-agnostic, platform-specific)."""

    def get_tag(self):
        _python, _abi, plat = super().get_tag()
        return "py3", "none", plat


setup(distclass=BinaryDistribution, cmdclass={"bdist_wheel": WheelPy3None})
