# Foundry validation harness

> Part of the testing overview in [docs/testing.md](../docs/testing.md).

An adapter that runs the **ADBC Driver Foundry** validation suite
([adbc-drivers/validation](https://github.com/adbc-drivers/validation)) against the adbc-spanner
driver. It is a **type/feature coverage matrix** — complementary to, not a replacement for, the
Apache arrow-adbc C++ conformance suite we run in `adbc-validation/` (`adbc-validation.yml`).

The suite is driver-agnostic: it loads our built cdylib through the ADBC driver manager and runs a
corpus of declarative query cases. **It has nothing to do with `driverbase-rs`** (a Rust driver
*authoring* framework we don't use) — the only coupling is the ADBC C ABI.

## Running it

```sh
scripts/run-foundry-validation.sh            # starts a throwaway emulator, runs the suite
scripts/run-foundry-validation.sh -k ingest  # extra args are forwarded to pytest
```

The script builds the driver, installs the (pinned) validation package, bootstraps the emulator
instance/database, and runs pytest. The `Foundry validation` workflow runs it as a **gating CI
job** on pushes to main and pull requests (plus *workflow_dispatch*): every case passes or skips
with a reason — there are **no expected failures**.

## Layout

- `tests/spanner.py` — `SpannerQuirks`: the driver descriptor (feature matrix, connection options,
  identifier quoting, `bind_parameter` → `@pN`, table-not-found detection).
- `tests/conftest.py` + `tests/test_*.py` — thin glue that re-exports the suite's shared fixtures
  and test classes and points `driver_path` at `target/debug/libadbc_spanner.*`.
- `queries/spanner/` — Spanner-dialect **overrides** overlaid on the suite's base corpus (see below).

## Status

The plumbing is solid (connection/metadata and many query cases pass). The suite's **base query
corpus assumes a portable SQL dialect that Spanner diverges from**, so per-category
`queries/spanner/` overrides are added incrementally.

**`type/bind/*` is done** (all pass or `skip`): the driver binds parameters positionally when the
bound column names don't match the query's `@names`, so no per-case column renaming is needed — each
override just supplies a Spanner `setup_query` (mandatory `PRIMARY KEY`, native type names) and an
explicit `INSERT` column list. `FLOAT32`/`FLOAT64` — which Spanner forbids as a primary key —
round-trip by adding a synthetic UUID key column (defaulted, so the bind `INSERT` still supplies
only `res`), and `BinaryView`/`Utf8View` params round-trip too (the driver binds the Arrow view
layouts like their offset kin, `src/bind.rs`). `timestamptz_ns` round-trips in the default
(nanosecond) mode — its values sit at the i64 nanosecond boundaries (~1677 / ~2262), which *are*
in range — and `timestamptz_us` (whose values reach `9999-12-31`) round-trips by setting
`spanner.max_timestamp_precision=microseconds` (a per-case `setup.connection.options`, reverted
after) so the read returns `Timestamp(Microsecond, "UTC")` over Spanner's full 0001–9999 range.
Cases Spanner cannot round-trip are `skip`ped with a reason: narrower integers (read back as
`INT64`), `DECIMAL(p,s)` (Spanner `NUMERIC` is fixed 38,9), `TIME`-of-day / `float16` /
`fixed_size_binary` (no Spanner type), the tz-naive `timestamp_*` variants (Spanner `TIMESTAMP` is
UTC-aware), and the coarser-unit `timestamptz_s`/`_ms` (Spanner resolves to sub-second, so a
second/millisecond read-back unit never matches).

**`type/select/*` is done** (all pass or `skip`): each override supplies a Spanner `setup_query` —
`idx` is the `PRIMARY KEY` and `res` the native-typed value column, with literals adjusted for
Spanner (`X'..'` → `FROM_HEX('..')`, `\`/`'` escaping in strings). Because `idx` is the key here,
`FLOAT32`/`FLOAT64` round-trip (unlike in `type/bind`). `timestamptz` also round-trips: the case
includes `9999-12-31`, so the override reads it under `spanner.max_timestamp_precision=microseconds`
(a per-case `setup.connection.options`, reverted after) → `Timestamp(Microsecond, "UTC")` over
Spanner's full 0001–9999 range. `skip`ped: narrower integers (→ `INT64`), `DECIMAL` (fixed 38,9),
`TIME` (no type), and the tz-naive `timestamp` (Spanner `TIMESTAMP` is UTC-aware, so a no-timezone
read-back never matches).

**`type/literal/*` is done** (all pass or `skip`): each case selects a typed literal/cast; the base
corpus uses portable-SQL type names Spanner rejects (`SMALLINT`/`INT`/`BIGINT`/`REAL`/`DOUBLE
PRECISION`/`NUMERIC(p,s)`/`TIME`/`TIMESTAMP WITH TIME ZONE`), so each override supplies the
Spanner-native equivalent. Ported: `int64` (`CAST(.. AS INT64)`), `float32` (`FLOAT32`), `float64`
(`FLOAT64`), and — unlike `type/select`, whose values hit `9999-12-31` — `timestamp`/`timestamptz`,
whose literals (`2023-05-15`) are inside Arrow's nanosecond range; both map to Spanner's only
timestamp type (a UTC-aware absolute instant → Arrow `Timestamp(Nanosecond, "UTC")`), so the override
pins the literal to `+00` and expects UTC nanoseconds. `skip`ped: narrower integers (→ `INT64`),
`DECIMAL` (fixed 38,9), and `TIME` (no type).

**`ingest/*` is done** (all pass or `skip`): this one needed a *driver* change, not fixtures — the
suite ingests with `mode="create"`, so the driver now builds the table from the ingest data's Arrow
schema, adding a synthetic `adbc_ingest_key` UUID primary key (Spanner requires one; the ingest
`INSERT`s omit it so the `DEFAULT (GENERATE_UUID())` fills it). `append`/`create`/`create_append`/
`replace` are all supported. `skip`ped: narrower integers (→ `INT64`), all `DECIMAL` variants (fixed
38,9), `TIME`/view/fixed-size-binary (no type), tz-naive `timestamp` (Spanner `TIMESTAMP` is
UTC-aware), and `timestamptz` at non-nanosecond units (Spanner returns nanosecond).

**`connection`/`statement` metadata is mostly done.** Fixed via quirks config (real `get_info`
values, `current_catalog`/`current_schema` = `""`, a Spanner `sample_table` `query_override`) and two
small driver changes: an unrecognised option now reports `NotImplemented` (not `InvalidArguments`),
and the connection reports the current catalog/schema as `""` (Spanner's single unnamed catalog and
default schema). Both conformance suites model current catalog/schema the same way — report the value
when the driver declares support, else `NOT_FOUND` — so declaring support in both harnesses (the
Foundry `current_catalog`/`current_schema` and the C++ `supports_metadata_current_catalog()`) keeps
them consistent.

**`test_get_statistics` passes.** The suite calls `get_statistics` with `approximate=True`, and the
driver serves the same exact aggregate-scan statistics as for `approximate=false`. That is
spec-conformant — `approximate=True` merely *allows* approximate/out-of-date values, and exact
values always satisfy it (Spanner keeps no cheap pre-computed statistics, so there is nothing
cheaper to serve); each row reports `statistic_is_approximate=false`.

**Spanner-specific test adaptations (formerly strict xfails).** Two cases needed adapting for
Spanner. Neither requires a fork any more — `VALIDATION_REF` pins the plain upstream
`adbc-drivers/validation` suite (`scripts/run-foundry-validation.sh`) — and both now **pass**, so
there are no strict xfails left:

- `test_get_objects_column_filter_table` / `_table_name` — tables created by `mode="create"` ingest
  carry the synthetic `adbc_ingest_key` primary-key column, which `get_objects` faithfully lists; the
  cases' strict column-list assertions expected only the data columns.
  Handled **in this repo**, not the shared suite: `tests/test_connection.py` subclasses the suite's
  `TestConnection` and overrides just these two tests to drop `adbc_ingest_key` before the strict
  assertions (`SYNTHETIC_INGEST_COLUMN`). The membership-based filter tests already pass and are
  inherited unchanged. This is the driver-side alternative to a shared
  `bulk_ingest_synthetic_column` feature flag — see the discussion on
  [adbc-drivers/validation#250](https://github.com/adbc-drivers/validation/pull/250). If the suite
  ever renames these two methods or reshapes their assertions, the overrides silently go stale — keep
  them in lockstep with the pin.
- `test_rows_affected` — the suite hardcoded portable `CREATE TABLE (id INT)`; Spanner needs a
  `PRIMARY KEY` and `INT64`. The suite now routes that DDL through
  `query_override("TestStatement.test_rows_affected.create_table", …)`
  ([#249](https://github.com/adbc-drivers/validation/pull/249), merged upstream); quirks hookup: that
  override rewrites `(id INT)` → `(id INT64, adbc_pk STRING(36) DEFAULT (GENERATE_UUID())) PRIMARY KEY (adbc_pk)`.

Known remaining gaps (documented, not yet addressed):

- `test_get_objects_constraints_foreign` — `SpannerQuirks` implements the suite's
  `sample_ddl_constraints` hook (Spanner DDL: `INT64`, trailing `PRIMARY KEY`, table-level
  `FOREIGN KEY`), so the fixture no longer errors, and the driver reports the constraints
  faithfully — the FK shapes are exact, and declared key order is preserved (`PRIMARY KEY (b, a)`
  reports `["b", "a"]`), matching the suite's non-normalized defaults. `_primary` now **passes**
  (the driver reports `constraint_column_usage` as NULL, not `[]`, for non-FK constraints). Only
  `_foreign` stays skipped (feature gated off): it is upstream-unpassable for Spanner — every
  Spanner table has a mandatory primary key, and even the empty `PRIMARY KEY ()` singleton form
  still produces a `PK_<table>` row in `INFORMATION_SCHEMA.TABLE_CONSTRAINTS` (verified on the
  emulator), so the FK tables report two constraints (PK + FK) where the suite asserts exactly one.
