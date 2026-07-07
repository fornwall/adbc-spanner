# Repo review — prioritized findings

*Review date: 2026-07-07, against main at f7364e8.*

Overall: the driver is in very good shape — the type mapping, dense-union metadata assembly,
transaction model, streaming reader, and test suite (property-based round-trips, FFI conformance,
C++ validation, Python cookbook tests) are all solid. The issues below are ranked by how likely
they are to bite a real user.

## P1 — fix soon (user-facing breakage or wrong deliverables)

**1. Linux wheels are tagged `manylinux_2_35` but built on glibc 2.39** —
`.github/workflows/libraries.yml:176` hardcodes the tag while the build runs on `ubuntu-latest`,
which is now 24.04 (glibc 2.39). If the binary picks up any post-2.35 versioned symbol, pip on
Ubuntu 22.04 / Debian 12 installs the wheel fine and then `dlopen` fails at connect time. Build in
a manylinux container or pin `ubuntu-22.04`, and add an `auditwheel show` check so the tag can't
silently drift again when runners move to 26.04.

**2. `cancel()` is silently lost between chunk fetches** — `src/runtime.rs:40`:
`Notify::notify_waiters()` wakes only currently-parked waiters and stores nothing. If a caller
cancels while the streaming reader is between fetches (converting rows, or the consumer is
processing a batch), the signal evaporates and the query streams to completion — contradicting the
module docs. Make cancellation sticky (`AtomicBool` + `Notify`, or
`tokio_util::sync::CancellationToken`).

**3. Malformed values silently become NULL on the read path** — `src/conversion.rs`: parse
failures for INT64, FLOAT, DATE, NUMERIC and base64 BYTES all funnel through `Option` → null slot,
so an unexpected wire value reads back as SQL NULL with no error. The timestamp path already does
the right thing (errors loudly on unrepresentable values, `conversion.rs:353`); the other types
should be consistent — corrupt-looking data should fail, not fabricate NULLs.

**4. `INSERT/UPDATE/DELETE ... THEN RETURN` rows are discarded** — `src/statement.rs:369`:
anything classified as DML by `is_dml` returns an empty reader from `execute()`. Spanner supports
`THEN RETURN`, so a client running `INSERT ... THEN RETURN id` gets an empty result with no error.
Either detect `THEN RETURN` and route through a result-returning path, or reject it explicitly.

## P2 — real defects with narrower blast radius

**5. SQL lexers miss GoogleSQL raw and triple-quoted strings** — `src/ddl.rs:51`: in `r'C:\'` the
backslash isn't an escape, so the splitter eats the closing quote and mangles the batch;
`'''don't; stop'''` splits mid-literal. Same blind spots in `named_parameters` (`src/bind.rs`).
Handle `r`/`b` prefixes and `'''`/`"""`.

**6. Statement hints defeat statement classification** — `first_keyword("@{HINT=X} UPDATE ...")`
returns `None` (`src/ddl.rs:112`), so hinted DML entering via `execute()` is sent to a read-only
transaction, which Spanner rejects. Skip a leading `@{…}` block.

**7. Identifier quoting is inconsistent and uses the wrong escape** — `qualified_table`
(`src/connection.rs:995`) interpolates caller-supplied schema/table names with no escaping (a name
with a backtick breaks `get_table_schema` and then gets mislabeled `NotFound`), while the one
escape that exists (`connection.rs:367` and `bind::quote_ident`) uses MySQL-style backtick
doubling — GoogleSQL wants backslash escapes. One shared, correct `quote_ident` fixes all three
sites. Related: `insert_sql` (`src/bind.rs:396`) writes parameter references as
`@<raw column name>`, so a column name that isn't a valid identifier produces invalid SQL — the
`` a`b `` test at `bind.rs:461` currently asserts the broken output. Binding ingest params
positionally (`@p0, @p1`) would decouple param names from column names.

**8. Release job can attach unchecksummed wheels** — `libraries.yml:94`: the `release` job
downloads *all* artifacts with `merge-multiple` while `python-wheels` runs in parallel; depending
on timing, wheels land in `dist/` and get attached to the GitHub Release without matching the
`sha256sum adbc-spanner-*` glob. Add a `pattern:` filter.

**9. CI supply-chain hygiene** — the release-critical actions are pinned to mutable refs
(`softprops/action-gh-release@v3` with `contents: write`, `pypa/gh-action-pypi-publish@release/v1`
— a *branch* — with `id-token: write`); pin those to commit SHAs. And `ci.yml`,
`adbc-validation.yml`, `fuzz.yml` have no `permissions:` block at all — add `contents: read` like
the other two workflows already do.

**10. gRPC error fidelity** — `src/error.rs`: `ABORTED` (Spanner's routine "retry me" signal) maps
to `Status::Internal`, indistinguishable from a driver bug when the r/w runner exhausts retries
under contention; and `from_spanner` leaves `vendor_code` at zero when it could carry the numeric
gRPC code for callers' retry logic.

**11. Untested data-loss path** — the "re-enabling autocommit commits buffered DML" branch
(`src/connection.rs:721`) has zero coverage; the one toggle test deliberately buffers nothing
(`tests/integration.rs:486`). A regression that *discarded* the buffer instead of committing would
pass the whole suite.

**12. Emulator scripts fail open** — `scripts/with-emulator.sh:44–64`: both readiness loops fall
through silently on timeout and run the tests against a dead port (the ci.yml copy of this loop
fails correctly). `run-foundry-validation.sh` also lacks `-e`, so a failed build validates a stale
`.so`, and its `VALIDATION_REF` pin only applies on first install.

## P3 — improvements worth queuing

- **Bind coverage**: no `Utf8View`/`BinaryView` (increasingly what polars/new pyarrow emit), no
  `List` → Spanner `ARRAY` params, no `Int8`/`Date64`. ARRAY binding is the most-asked-for gap for
  a Spanner driver.
- **Ingest modes**: only `append` is supported; `create`/`replace` (DDL from the Arrow schema) is
  the highest-value feature add. At minimum, add a test that non-append modes get a clean
  `NotImplemented` (currently untested).
- **Python packaging polish**: wheel ships no LICENSE text despite embedding aws-lc etc.
  (Apache-2.0 §4 gap); `setuptools>=64` floor is too low for the PEP 639 string license (needs
  ≥77); `adbc-driver-manager` dependency has no version floor; dev `_version.py` has drifted
  (0.3.9 vs 0.5.0) — wire it into cargo-release's `pre-release-replacements`.
- **macOS deployment target**: the `macosx_10_12` tag is asserted, not enforced — export
  `MACOSX_DEPLOYMENT_TARGET` in the build (aws-lc's cmake defaults from the host).
- **Statistics performance**: `collect_statistics` rescans the whole COLUMNS batch per table,
  O(tables × total columns) — the batch is already sorted, group it once. Also guard the `as i32`
  list-offset accumulation in `statistics.rs`/`objects.rs` and replace the schema-shape
  `expect`/`unreachable!` panics with `Status::Internal` errors (this is a cdylib; panics unwind
  toward the C ABI).
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
