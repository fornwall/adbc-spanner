# Foundry validation harness

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
layouts like their offset kin, `src/bind.rs`). Cases Spanner cannot round-trip are `skip`ped with a
reason: narrower integers (read back as `INT64`), `DECIMAL(p,s)` (Spanner `NUMERIC` is fixed 38,9),
`TIME`-of-day / `float16` / `fixed_size_binary` (no Spanner type), and timestamps outside Arrow's
nanosecond range.

**`type/select/*` is done** (all pass or `skip`): each override supplies a Spanner `setup_query` —
`idx` is the `PRIMARY KEY` and `res` the native-typed value column, with literals adjusted for
Spanner (`X'..'` → `FROM_HEX('..')`, `\`/`'` escaping in strings). Because `idx` is the key here,
`FLOAT32`/`FLOAT64` round-trip (unlike in `type/bind`). `skip`ped: narrower integers (→ `INT64`),
`DECIMAL` (fixed 38,9), `TIME` (no type), and `timestamp`/`timestamptz` (the cases include
`9999-12-31`, outside Arrow's nanosecond range).

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

**Fork-pinned suite fixes (formerly strict xfails).** Three cases needed suite changes that are not
yet in the upstream `adbc-drivers/validation` corpus; both are now landed on the
[fornwall/validation](https://github.com/fornwall/validation) fork that `VALIDATION_REF` pins
(`scripts/run-foundry-validation.sh`), staged ahead of an upstream submission. They are wired up in
`SpannerQuirks` and now **pass**, so there are no strict xfails left. When `VALIDATION_REF` bumps to
a suite that drops these changes (or renames the hooks), the matching quirks hookups below break —
keep them in lockstep with the pin:

- `test_get_objects_column_filter_table` / `_table_name` — tables created by `mode="create"` ingest
  carry the synthetic `adbc_ingest_key` column, which `get_objects` faithfully lists; the cases'
  strict column-list assertions expected only the data columns.
  Suite fix: the `bulk_ingest_synthetic_column` `DriverFeatures` field the strict assertions filter
  out; quirks hookup: `bulk_ingest_synthetic_column="adbc_ingest_key"`.
- `test_rows_affected` — the suite hardcoded portable `CREATE TABLE (id INT)` with no override
  hook; Spanner needs a `PRIMARY KEY` and `INT64`.
  Suite fix: the DDL is routed through `query_override("TestStatement.test_rows_affected.create_table", …)`;
  quirks hookup: that override rewrites `(id INT)` → `(id INT64) PRIMARY KEY (id)`.

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
