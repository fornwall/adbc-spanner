"""Execute the README's Cookbook snippets against the emulator, so the docs can't drift.

Each ```python fenced block in ../README.md is extracted and run as-is (blocks tagged
`# docs-test: skip` on their first line, or that don't open a connection, are ignored). The only
rewrite is transparent: `spanner.connect(...)` is redirected at the emulator test database, so the
illustrative `projects/my-project/...` path in the docs stays readable. Self-skips without
`SPANNER_EMULATOR_HOST` (see conftest).
"""

import pathlib
import re

import pytest

pytest.importorskip("pyarrow")  # every snippet returns Arrow

README = pathlib.Path(__file__).resolve().parents[1] / "README.md"


def _runnable_blocks():
    text = README.read_text()
    blocks = re.findall(r"```python\n(.*?)```", text, re.DOTALL)
    runnable = []
    for block in blocks:
        if block.lstrip().startswith("# docs-test: skip"):
            continue
        if "spanner.connect" not in block:
            continue
        runnable.append(block)
    return runnable


RUNNABLE = _runnable_blocks()


def test_readme_has_runnable_snippets():
    # Guard against a parsing/format change silently dropping every example.
    assert RUNNABLE, "no runnable ```python blocks found in README.md"


@pytest.fixture(scope="module")
def cookbook_env(emulator_database):
    """Seed the `Singers` table and redirect `spanner.connect` at the emulator database."""
    import adbc_driver_spanner.dbapi as sp
    from adbc_driver_spanner import DatabaseOptions

    conn = sp.connect(
        db_kwargs={
            DatabaseOptions.URI.value: f"spanner:///{emulator_database}",
            DatabaseOptions.EMULATOR.value: "true",
        },
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS Singers")
            cur.execute(
                "CREATE TABLE Singers ("
                "  SingerId INT64 NOT NULL,"
                "  FirstName STRING(MAX),"
                ") PRIMARY KEY (SingerId)"
            )
            cur.execute("INSERT INTO Singers (SingerId, FirstName) VALUES (1, 'Alice'), (2, 'Bob')")
    finally:
        conn.close()

    original = sp.connect

    def redirected(db_kwargs=None, **kwargs):
        # Override the illustrative uri with the emulator database and force
        # emulator mode, preserving any other db_kwargs the snippet set.
        merged = dict(db_kwargs or {})
        merged[DatabaseOptions.URI.value] = f"spanner:///{emulator_database}"
        merged[DatabaseOptions.EMULATOR.value] = "true"
        return original(db_kwargs=merged, **kwargs)

    sp.connect = redirected
    try:
        yield
    finally:
        sp.connect = original


@pytest.mark.parametrize(
    "code",
    RUNNABLE,
    ids=[f"block{i}" for i in range(len(RUNNABLE))],
)
def test_readme_snippet(code, cookbook_env):
    try:
        exec(compile(code, "<README snippet>", "exec"), {"__name__": "__readme__"})
    except ModuleNotFoundError as exc:
        pytest.skip(f"optional dependency not installed: {exc.name}")
