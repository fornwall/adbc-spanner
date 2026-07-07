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
instance/database, and runs pytest. It is **not wired into gating CI** (see status below); the
`Foundry validation` workflow runs it on demand via *workflow_dispatch*.

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
explicit `INSERT` column list. Cases Spanner cannot round-trip are `skip`ped with a reason: narrower
integers (read back as `INT64`), `DECIMAL(p,s)` (Spanner `NUMERIC` is fixed 38,9), `FLOAT` (cannot be
a primary key), `TIME`-of-day / `float16` / `fixed_size_binary` / Arrow view types (no Spanner type),
and timestamps outside Arrow's nanosecond range.

**`type/select/*` is done** (all pass or `skip`): each override supplies a Spanner `setup_query` —
`idx` is the `PRIMARY KEY` and `res` the native-typed value column, with literals adjusted for
Spanner (`X'..'` → `FROM_HEX('..')`, `\`/`'` escaping in strings). Because `idx` is the key here,
`FLOAT32`/`FLOAT64` round-trip (unlike in `type/bind`). `skip`ped: narrower integers (→ `INT64`),
`DECIMAL` (fixed 38,9), `TIME` (no type), and `timestamp`/`timestamptz` (the cases include
`9999-12-31`, outside Arrow's nanosecond range).

Still to port, each the same shape — a Spanner `setup_query` override (`PRIMARY KEY` + native type
names) plus `skip`s for unsupported types:

- `queries/spanner/ingest/*` — bulk-ingest cases (the driver builds the `INSERT` from the bound
  data's column names, so mostly the table DDL needs the dialect override).
- A handful of `connection`/`statement` metadata cases.

Once a category is green, consider a gating CI job for that subset.
