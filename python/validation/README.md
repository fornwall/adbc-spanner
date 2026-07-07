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

## Status — dialect porting needed

The suite runs and the plumbing is solid (connection/metadata and many query cases pass), but its
**base query corpus assumes a portable SQL dialect that Spanner diverges from**, so a chunk of the
`type/*` cases currently error or fail. Three gaps, all addressable under `queries/spanner/`:

1. **Mandatory `PRIMARY KEY`.** Base setups like `CREATE TABLE t (idx INTEGER, res BIGINT)` are
   rejected by Spanner (*"Must specify either table or column PRIMARY KEY"*). Each type case needs a
   Spanner `setup_query` override.
2. **Type names.** `INTEGER`/`BIGINT`/`VARCHAR` → Spanner `INT64`/`STRING(MAX)`/etc.
3. **Named parameters.** The suite emits positional `$1`; Spanner is named-only. `bind_parameter`
   already maps `$N` → `@pN`, but the driver binds a batch **column** to `@<column-name>`, so the
   `type/bind/*` cases need the bind column renamed to `pN` (per-case override) or a driver-side
   positional-binding enhancement.

Bulk **ingest** cases largely work as-is, because the driver builds the `INSERT` from the bound
Arrow data's own column names.

### Roadmap (incremental follow-up)

- Add `queries/spanner/type/select/*` and `queries/spanner/type/ingest/*` `setup_query` overrides
  (PRIMARY KEY + Spanner type names). Mark genuinely-unsupported Arrow types (`uint*`, `float16`,
  time-of-day, `fixed_size_binary`, views) `skip`/`hide` in `query.toml`.
- Decide on named-parameter binding: per-case bind overrides vs. a driver enhancement.
- Once a category is green, flip it on and consider a gating CI job for that subset.
