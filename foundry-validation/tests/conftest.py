import os
import sys
from pathlib import Path

import adbc_drivers_validation.model
import adbc_drivers_validation.tests.conftest
import pytest
from adbc_drivers_validation.tests.conftest import (  # noqa: F401
    conn,
    conn_factory,
    db_kwargs,
    manual_test,
    noci,
)

from . import spanner


def pytest_collection_modifyitems(
    session: pytest.Session, config: pytest.Config, items: list[pytest.Item]
) -> None:
    # Delegate to the suite's own hook (it filters out the interactive test_repl
    # unless --repl is passed, adds JUnit metadata, etc.). It is imported as a
    # module here rather than registered as a plugin, so it only runs if we call it.
    adbc_drivers_validation.tests.conftest.pytest_collection_modifyitems(
        session, config, items
    )


def pytest_addoption(parser):
    adbc_drivers_validation.tests.conftest.pytest_addoption(parser)
    parser.addoption("--vendor-version", action="store", default="emulator")


# The committed skip-inventory baseline (see _skip_baseline_guard below and the
# "Skip-inventory baseline guard" section of foundry-validation/README.md).
SKIP_BASELINE_PATH = Path(__file__).resolve().parents[1] / "skip_baseline.txt"

_SKIP_BASELINE_HEADER = """\
# Foundry validation skip-inventory baseline.
#
# The sorted set of pytest nodeids expected to SKIP on a full suite run. One nodeid
# per line, optionally followed by "  # <reason>" for reviewer readability. The
# comparison key is the NODEID ALONE — the trailing reason is not compared (reasons
# live in the .txtcase files and are maintained there; comparing them here would
# couple this guard to unrelated reason edits).
#
# Why it exists: the suite reports Spanner's SQL-dialect divergences as per-case
# skips. Without a pinned inventory, bumping ADBC_VALIDATION_REF can silently slip a
# new upstream case into skip with no coverage, and a case that starts passing can
# stay silently skipped (its skip never removed to enable it). This baseline pins
# exactly which cases skip so either drift fails CI.
#
# Guard: foundry-validation/tests/conftest.py, enforced only when
# FOUNDRY_VALIDATION_CHECK_SKIP_BASELINE is set (the CI workflow sets it). It fails a
# full run on either an *unlisted skip* (skipping now, not in this file) or a *stale
# entry* (in this file, not skipping now — it passes/fails/was renamed).
#
# Regenerate after a legitimate skip-set change (rewrites this file from the run):
#   FOUNDRY_UPDATE_SKIP_BASELINE=1 scripts/run-foundry-validation.sh
"""


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    _require_passes_guard(session, exitstatus)
    _skip_baseline_guard(session)


def _require_passes_guard(session: pytest.Session, exitstatus: int) -> None:
    # Fail loudly on a "green" run that exercised no real coverage.
    #
    # The suite reports Spanner's expected SQL-dialect divergences as per-case
    # *skips* (each with a reason), and pytest exits 0 when every collected case is
    # skipped. So an all-skip run is indistinguishable, by exit code alone, from a
    # broken harness that silently skipped everything (e.g. the driver failing to
    # load, or a fixture misconfiguration) — the exact "harness breakage looks like
    # an expected dialect skip" hole that the gating workflow otherwise leaves open.
    #
    # When FOUNDRY_VALIDATION_REQUIRE_PASSES is set — the CI workflow sets it, so
    # ad-hoc local `-k` runs (which may legitimately select only skipped cases) are
    # unaffected — require that at least one collected case actually passed. This
    # mirrors the env-gated fail-loud convention of the ADBC_TEST_REQUIRE_TARGET
    # skip guard used by the Rust/Python integration suites.
    if exitstatus != int(pytest.ExitCode.OK):
        return
    if not os.environ.get("FOUNDRY_VALIDATION_REQUIRE_PASSES"):
        return
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    passed = len(reporter.stats.get("passed", [])) if reporter is not None else 0
    if session.testscollected > 0 and passed == 0:
        message = (
            f"FOUNDRY_VALIDATION_REQUIRE_PASSES: collected {session.testscollected} "
            "case(s) but none passed — every case skipped or errored. This usually "
            "means the harness itself is broken (driver failed to load, connection "
            "setup failed), not an expected Spanner SQL-dialect skip."
        )
        if reporter is not None:
            reporter.write_line(message, red=True)
        else:  # pragma: no cover - terminal reporter is a default plugin
            print(message)
        session.exitstatus = int(pytest.ExitCode.TESTS_FAILED)


def _skip_reason(report: pytest.TestReport) -> str:
    # A skip report's longrepr is a (path, lineno, "Skipped: <reason>") tuple.
    longrepr = getattr(report, "longrepr", None)
    if isinstance(longrepr, tuple) and len(longrepr) == 3:
        reason = str(longrepr[2])
        prefix = "Skipped: "
        if reason.startswith(prefix):
            reason = reason[len(prefix) :]
        return reason.strip()
    return ""


def _collect_skipped(session: pytest.Session) -> dict[str, str]:
    # Map each skipped nodeid to its (last-seen) reason. Keyed by nodeid so a case
    # reported skipped in more than one phase collapses to a single entry.
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    skipped: dict[str, str] = {}
    if reporter is None:  # pragma: no cover - terminal reporter is a default plugin
        return skipped
    for report in reporter.stats.get("skipped", []):
        skipped[report.nodeid] = _skip_reason(report)
    return skipped


def _load_baseline(path: Path) -> set[str]:
    # One nodeid per line. Blank lines and `#` comment lines are ignored; a trailing
    # "  # <reason>" annotation is stripped so only the nodeid is compared.
    nodeids: set[str] = set()
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        nodeid = line.split(" #", 1)[0].strip()
        if nodeid:
            nodeids.add(nodeid)
    return nodeids


def _write_baseline(path: Path, skipped: dict[str, str]) -> None:
    lines = [_SKIP_BASELINE_HEADER]
    for nodeid in sorted(skipped):
        reason = skipped[nodeid]
        lines.append(f"{nodeid}  # {reason}" if reason else nodeid)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _skip_baseline_guard(session: pytest.Session) -> None:
    # Pin *which* cases skip, so a suite-ref bump can't silently slip a new case into
    # skip (no coverage) or leave a now-passing case silently skipped (its .txtcase
    # skip never removed). This is the Foundry analog of the closed-set/xfail-strict
    # guard in scripts/run-adbc-validation.sh for the C++ suite, adapted to the
    # skip-based pytest model.
    #
    # Regenerate mode (FOUNDRY_UPDATE_SKIP_BASELINE) takes precedence: it rewrites the
    # baseline from this run instead of comparing — how a maintainer updates the file
    # after a legitimate skip-set change.
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")

    def emit(message: str) -> None:
        if reporter is not None:
            reporter.write_line(message, red=True)
        else:  # pragma: no cover - terminal reporter is a default plugin
            print(message)

    if os.environ.get("FOUNDRY_UPDATE_SKIP_BASELINE"):
        skipped = _collect_skipped(session)
        _write_baseline(SKIP_BASELINE_PATH, skipped)
        if reporter is not None:
            reporter.write_line(
                f"FOUNDRY_UPDATE_SKIP_BASELINE: wrote {len(skipped)} nodeid(s) to "
                f"{SKIP_BASELINE_PATH.name}",
                green=True,
            )
        return

    if not os.environ.get("FOUNDRY_VALIDATION_CHECK_SKIP_BASELINE"):
        return
    # A `-k`/`-m`-filtered run collects only part of the suite, so its skipped set is
    # not the full inventory — never enforce against a subset even if the env var
    # leaked in. (CI runs unfiltered; this is belt-and-suspenders for local runs.)
    if getattr(session.config.option, "keyword", "") or getattr(
        session.config.option, "markexpr", ""
    ):
        return
    if not SKIP_BASELINE_PATH.exists():
        emit(
            "FOUNDRY_VALIDATION_CHECK_SKIP_BASELINE: no baseline at "
            f"{SKIP_BASELINE_PATH} — regenerate with "
            "FOUNDRY_UPDATE_SKIP_BASELINE=1 scripts/run-foundry-validation.sh"
        )
        session.exitstatus = int(pytest.ExitCode.TESTS_FAILED)
        return

    baseline = _load_baseline(SKIP_BASELINE_PATH)
    live = set(_collect_skipped(session))
    unlisted = sorted(live - baseline)
    stale = sorted(baseline - live)
    if not unlisted and not stale:
        return

    if unlisted:
        emit(
            "FOUNDRY_VALIDATION_CHECK_SKIP_BASELINE: "
            f"{len(unlisted)} case(s) skipped but NOT in the baseline (unlisted "
            "skip) — a new or regressed case is skipping without being acknowledged. "
            "Triage it (fix it, or add it to the baseline with a reason via "
            "FOUNDRY_UPDATE_SKIP_BASELINE=1):"
        )
        for nodeid in unlisted:
            emit(f"  + {nodeid}")
    if stale:
        emit(
            "FOUNDRY_VALIDATION_CHECK_SKIP_BASELINE: "
            f"{len(stale)} baseline entr(y/ies) did NOT skip this run (stale entry) "
            "— the case no longer skips. If it now passes, remove its .txtcase skip "
            "and regenerate the baseline to enable the coverage; if it now fails or "
            "was renamed upstream, investigate:"
        )
        for nodeid in stale:
            emit(f"  - {nodeid}")
    session.exitstatus = int(pytest.ExitCode.TESTS_FAILED)


@pytest.fixture(scope="session")
def driver(request, pytestconfig) -> adbc_drivers_validation.model.DriverQuirks:
    return spanner.get_quirks(pytestconfig.getoption("vendor_version"))


@pytest.fixture(scope="session")
def driver_path(driver) -> str:
    ext = {"win32": "dll", "darwin": "dylib"}.get(sys.platform, "so")
    # Built cdylib at the repo root: foundry-validation/tests -> repo root.
    return str(Path(__file__).resolve().parents[2] / f"target/debug/libadbc_spanner.{ext}")
