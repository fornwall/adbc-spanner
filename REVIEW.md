# Repo review — prioritized findings

*Review date: 2026-07-07, against main at f7364e8.*

Overall: the driver is in very good shape — the type mapping, dense-union metadata assembly,
transaction model, streaming reader, and test suite (property-based round-trips, FFI conformance,
C++ validation, Python cookbook tests) are all solid. The issues below are ranked by how likely
they are to bite a real user. (All P1 and P2 findings from the original review have been fixed.)

## P3 — improvements worth queuing

- **Python packaging polish**: wheel ships no LICENSE text despite embedding aws-lc etc.
  (Apache-2.0 §4 gap); `setuptools>=64` floor is too low for the PEP 639 string license (needs
  ≥77); `adbc-driver-manager` dependency has no version floor; dev `_version.py` has drifted
  (0.3.9 vs 0.5.0) — wire it into cargo-release's `pre-release-replacements`.
- **macOS deployment target**: the `macosx_10_12` tag is asserted, not enforced — export
  `MACOSX_DEPLOYMENT_TARGET` in the build (aws-lc's cmake defaults from the host).
- **Test/fuzz upkeep**: `AdbcDdl` scratch table is never dropped (leaks into a real
  `SPANNER_GCP_DATABASE`); no integration round-trip for JSON/`arrow.json` or FLOAT32 columns;
  `ensure_database_once` poisons its mutex on setup panic (the file already solves this pattern
  for `serial_guard`); fuzz corpus is discarded every run — cache it so coverage accumulates;
  `ci.yml` clippy omits `--all-features` so the `fuzzing` module is never linted; the `like` fuzz
  oracle's `Regex::new().unwrap()` can panic on `CompiledTooBig` if `-max_len` is ever raised.

## Verified non-issues

The PyPI trusted-publishing setup is correct and least-privilege; the tag-vs-Cargo.toml version
gate works; `deny.toml` correctly restricts git sources to the documented pins; and
`like_match`/`split_statements` are already adversarial-input-aware within the syntax they know.
