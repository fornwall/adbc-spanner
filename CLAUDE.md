# CLAUDE.md

Guidance for working in this repository.

## What this is

`adbc-spanner` is a Rust [ADBC](https://arrow.apache.org/adbc/) (Arrow Database Connectivity) driver
for Google Cloud Spanner. It implements the native Rust `adbc_core` traits on top of the official
`google-cloud-spanner` preview client, returning query results as Apache Arrow record batches. It
also builds a C-ABI **cdylib** that any ADBC driver manager can load.

## Common commands

```sh
cargo build                 # builds the rlib and the cdylib (libadbc_spanner.so/.dylib/.dll)
cargo test                  # unit tests + doctest; the emulator integration test self-skips
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all --check     # CI enforces formatting

# Run everything, including the Spanner emulator integration test:
scripts/with-emulator.sh cargo test
```

CI enforces `fmt --check`, `clippy -D warnings`, unit tests, and the emulator integration test, so
run those before pushing.

## Architecture

The ADBC object hierarchy, one module each:

```
SpannerDriver ──▶ SpannerDatabase ──▶ SpannerConnection ──▶ SpannerStatement
```

- `src/driver.rs` — `SpannerDriver` + `SpannerDatabase`; option/config plumbing (database path,
  endpoint, emulator, `SPANNER_EMULATOR_HOST`) and building the Spanner `DatabaseClient`.
- `src/connection.rs` — `SpannerConnection`: transaction mode (autocommit default or manual
  buffer-and-commit), `get_table_types` / `get_table_schema`.
- `src/statement.rs` — `execute` (query → Arrow), `execute_update` (DML/DDL, incl. `;`-batches and
  bound params), `execute_schema` (PLAN-only schema), parameter binding / bulk ingest.
- `src/conversion.rs` — Spanner result set → Arrow schema + typed arrays (the type mapping lives
  here).
- `src/runtime.rs` — a shared Tokio runtime; the ADBC traits are sync while the Spanner client is
  async, so every call bridges via `runtime.block_on(...)`. The runtime is created once by the
  driver and shared via `Arc` into every database/connection/statement.
- `src/ffi.rs` — `adbc_ffi::export_driver!(AdbcSpannerInit, SpannerDriver)`; the C entrypoint of the
  shared library. Gated behind the default `ffi` feature.
- `src/error.rs` — helpers to build `adbc_core` errors; `from_spanner` is generic over `Display`
  because the client surfaces several distinct gax error types.

Key design points:

- **Sync-over-async bridge.** ADBC traits are synchronous; each method does `runtime.block_on`. Do
  not add a second runtime — reuse the shared one.
- **Transactions.** Autocommit by default: queries use a single-use read-only transaction; DML
  (including a `;`-separated batch via `ExecuteBatchDml`) uses a read/write runner. Setting
  `adbc.connection.autocommit=false` enters manual mode, which **buffers** DML and applies the whole
  batch atomically in one read/write transaction on `commit` (`rollback` discards it). The client
  exposes no manual begin/commit handle, so buffer-and-replay is what makes manual transactions both
  possible and retry-safe. In manual mode `execute_update` returns `None` (count unknown until
  commit); queries and DDL still run immediately.
- **Arrow version.** `arrow-array`/`arrow-schema` are pinned to the range `adbc_core` allows
  (`>=53.1, <59`) so the `RecordBatch`/`Schema`/`RecordBatchReader` types unify across crates.

## The google-cloud-spanner preview crate

This uses the **googleapis preview** client `google-cloud-spanner` (crate description "Google Cloud
Client Libraries for Rust - Spanner"). Beware: `docs.rs/.../latest` and web summaries often surface
an **older, unrelated** yoshidan-style API (`Client::new`, `client.single()`, `add_param`) — do not
trust those. For ground truth, read the extracted source under
`~/.cargo/git/checkouts/google-cloud-rust-*/` (the git dependency's checkout), or the crates.io
source for `adbc_core-0.23.0` and `adbc_ffi-0.23.0`.

**Temporary git pin.** `Cargo.toml` pins the whole `google-cloud-*` family (spanner, auth, lro, both
admin crates) to a `google-cloud-rust` git revision, because native `STRUCT` mapping needs
`Type::struct_type()`, which is on `main` but not yet in a crates.io release. Consequences: the crate
**cannot be published to crates.io** until that ships (revert to versioned deps then); and locally,
this machine's global git config rewrites `https://github.com` to SSH, so cargo fetches fail unless
you set `CARGO_NET_GIT_FETCH_WITH_CLI=true` plus a `GIT_CONFIG_*` identity `insteadOf` override for
the fork URL (see the session notes / the `with-emulator.sh` invocations). CI is unaffected (clean
git config, public repo over HTTPS).

The TLS stack is hardwired to `aws-lc` (via `tonic/tls-aws-lc`, `rustls/aws_lc_rs`, and the auth
id-token backend) — there is no `ring` option. This is why the release CI builds natively per arch
and installs NASM on Windows.

## Testing against the emulator

- `tests/emulator.rs` skips itself unless `SPANNER_EMULATOR_HOST` is set, so plain `cargo test` is
  green everywhere.
- It creates the instance/database/table via the admin clients
  (`spanner.instance_admin_builder()` / `database_admin_builder()` → `create_instance` /
  `create_database().set_extra_statements(..).poller().until_done()`), then exercises the driver
  (DML insert + typed SELECT).
- `scripts/with-emulator.sh <cmd>` runs the emulator in Docker, exports the env var, runs the
  command, and tears it down. It also works around a broken gcr.io Docker credential helper by using
  a clean empty `DOCKER_CONFIG` (the emulator image is public).

## Shared library (loadable driver)

The `cdylib` exports `AdbcSpannerInit` (+ an `AdbcDriverInit` fallback) so ADBC driver managers can
load it. Verify locally with:

```sh
cargo build --release
nm -D --defined-only target/release/libadbc_spanner.so | grep AdbcSpannerInit
```

`.github/workflows/libraries.yml` builds the library natively for linux x86-64, linux aarch64, macOS
arm64 and windows x86-64 on pushes to main, pull requests and tags; artifacts attach to every run,
and on `v*` tags they are attached to the GitHub Release.

## Releasing

Releases use [`cargo-release`](https://github.com/crate-ci/cargo-release), configured under
`[package.metadata.release]` in `Cargo.toml`.

```sh
cargo release patch            # dry run (default) — preview only
cargo release patch --execute  # bump + commit + publish to crates.io + tag vX.Y.Z + push
```

Pushing the `vX.Y.Z` tag triggers `libraries.yml` to attach the platform shared libraries to the
GitHub Release. So: `cargo release --execute` owns versioning + crates.io publish + tagging; CI owns
building and attaching the binaries. They do not overlap.

## Conventions / gotchas

- Match surrounding style; keep `fmt`/`clippy` clean (CI fails otherwise).
- Supported so far: queries, DML, DDL (via admin `UpdateDatabaseDdl`), manual transactions
  (buffer-and-commit), native Arrow types for DATE/TIMESTAMP/NUMERIC, parameter binding + bulk
  ingest, `get_table_types`/`get_table_schema`, and keyfile/keyfile_json auth.
- Still returning `NotImplemented` (keep the pattern until implemented): `get_info`, `get_objects`,
  `get_statistics`, Substrait, partitioned execution, and mapping ARRAY/STRUCT to Arrow
  `List`/`Struct` (currently JSON `Utf8`).
- Commits in this environment may need `-c commit.gpgsign=false` if no signing agent is present.
