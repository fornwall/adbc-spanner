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
- `src/statement.rs` — `execute` (query → streaming Arrow reader), `execute_update` (DML/DDL, incl.
  `;`-batches and bound params), `execute_schema` (PLAN-only schema), parameter binding / bulk
  ingest.
- `src/conversion.rs` — Spanner result set → Arrow schema + typed arrays (the type mapping lives
  here), plus `SpannerBatchReader`, the streaming `RecordBatchReader` that `execute` returns (pulls
  rows in bounded chunks of `spanner.rows_per_batch`, default 8192).
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

## Testing against the emulator (or a real instance)

- `tests/integration.rs` skips itself unless a target is configured, so plain `cargo test` is green
  everywhere. Two targets are supported (`test_target()` resolves them; emulator wins if both set):
  - `SPANNER_EMULATOR_HOST` — a local **emulator** (fixed `test-project`/`test-instance`/`adbc-test`
    ids, all created by the test).
  - `SPANNER_GCP_DATABASE` — a **real** Cloud Spanner database, `project.instance.database` form,
    reached with Application Default Credentials (`gcloud auth application-default login` / a
    service-account key). The instance must already exist; the test best-effort creates the database
    and `CREATE TABLE IF NOT EXISTS Singers`, and cleans up its own scratch tables, so it is safe to
    re-run against a persistent database. No driver change was needed — `SpannerDatabase::connect`
    already falls back to ADC when there is no emulator host and no keyfile.
- Setup creates the database/table via the admin clients (`instance_admin_builder()` /
  `database_admin_builder()` → `create_instance` [emulator only] / `create_database(..).poller()`),
  then exercises the driver (DML insert + typed SELECT). It runs once per binary behind a mutex
  (`ensure_database_once`) so the two parallel tests don't race on setup.
- `scripts/with-emulator.sh <cmd>` runs the emulator in Docker, exports the env var, runs the
  command, and tears it down. It waits for the **admin API** (a REST 200 on `instanceConfigs`), not
  just the gRPC TCP port — the forwarded port opens ~1s before the emulator is actually serving, and
  starting the test that early made `create_instance` fail silently → "Instance not found". It also
  works around a broken gcr.io Docker credential helper with a clean empty `DOCKER_CONFIG` (the
  emulator image is public).
- **The emulator gRPC endpoint must sit on port `9010`.** The pinned `google-cloud-rust` client
  derives the admin/REST endpoint by literal-substring-replacing `9010`→`9020` in the gRPC endpoint
  (see `.../google-cloud-rust-*/tests/spanner/src/client.rs`), so on any *other* gRPC port the admin
  request is sent to the gRPC port and every DDL / `create_database` fails with `error sending
  request ... /ddl`. So `SPANNER_EMULATOR_HOST` may use any *host* but the *port* must be `9010`
  (admin REST on `9020`); the driver has no override. To run several emulators concurrently (e.g.
  parallel test worktrees) without host-port clashes, start each container with **no** `-p` publish
  and connect via its docker-network IP on the internal `9010`/`9020` — distinct IP per container,
  ports stay `9010`/`9020` so the remap works. `SPANNER_EMULATOR_REST_PORT` (read by the Python
  `conftest.py`, not the driver) can still move the REST admin port.

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

**Always cut releases with [`cargo-release`](https://github.com/crate-ci/cargo-release)** (configured
under `[package.metadata.release]` in `Cargo.toml`) — never bump the version / commit / tag by hand.
Hand-rolling a release risks a malformed tag or a version that disagrees with `Cargo.toml` (which the
`python-wheels` job rejects), and there is nothing to gain: cargo-release does the exact same steps
deterministically.

```sh
cargo release patch            # dry run (default) — preview only
cargo release patch --execute  # bump + commit "Release X.Y.Z" + tag vX.Y.Z + push
```

**crates.io publishing is off**, via `publish = false` in the release config (the git-pinned
`google-cloud-*` deps can't be published — see the dependency note above). So `cargo release --execute`
does **not** touch crates.io — it only versions, commits, tags and pushes. Note the dry-run still
prints a `Publishing adbc-spanner` heading; that is just the step label, not an actual `cargo publish`,
so it is not a reason to avoid cargo-release. Re-enable `publish` once those deps are versioned.

Pushing the `vX.Y.Z` tag triggers `libraries.yml` to build + attach the platform shared libraries to
the GitHub Release and to build + publish the Python wheels to PyPI. So: `cargo release --execute` owns
versioning + tagging; CI owns building, attaching binaries, and publishing wheels. They do not overlap.

### Python package (`python/`)

`python/` is a separate PyPI distribution, `adbc-driver-spanner` — a data-only wheel that bundles the
prebuilt cdylib and drives it through `adbc_driver_manager` (DBAPI 2.0 + Arrow). It links nothing
against Python, so there is no PyO3/maturin build; `python/setup.py` just forces a
`py3-none-<platform>` tag and CI copies the right `.so`/`.dylib`/`.dll` in before packaging.

The same `vX.Y.Z` tag drives it — no separate command. `libraries.yml` has two extra jobs after the
library `build`:

- `python-wheels` reuses the per-platform artifacts `build` already produced, repackaging each into a
  wheel on one Linux runner (no compilation). It derives the version from `Cargo.toml` and **fails the
  release if the tag disagrees with the crate version**, so crate/tag/wheel can't drift.
  `adbc_driver_spanner/_version.py` (checked in) is only a dev fallback; CI overwrites it.
- `python-publish` (tags only) uploads to PyPI via **Trusted Publishing (OIDC)** — no token/secret.
  It uses `permissions: id-token: write` and the `pypi` GitHub environment.

Unlike the crate, the wheel ships a compiled binary, so the git-pinned `google-cloud-rust` dependency
(which blocks `cargo publish`) does **not** block PyPI — the Python package can release independently.

**One-time PyPI setup (before the first tag):** register a *pending publisher* at
<https://pypi.org/manage/account/publishing/> with project `adbc-driver-spanner`, owner `fornwall`,
repo `adbc-spanner`, workflow `libraries.yml`, environment `pypi` (all must match exactly), then
create the `pypi` GitHub environment (Settings → Environments), ideally restricted to `v*` tags. See
`python/README.md` for usage.

## Conventions / gotchas

- Match surrounding style; keep `fmt`/`clippy` clean (CI fails otherwise).
- Supported so far: streaming queries (`execute` returns a lazy `SpannerBatchReader` that converts
  bounded row chunks to Arrow on demand; chunk size via `spanner.rows_per_batch`), DML, DDL (via
  admin `UpdateDatabaseDdl`), manual transactions
  (buffer-and-commit), native Arrow types for DATE/TIMESTAMP/NUMERIC and native `List`/`Struct` for
  ARRAY/STRUCT, parameter binding (by column name, else positionally) + bulk ingest (append and
  create/create_append/replace — the create modes build the table from the ingest data's Arrow schema
  with a synthetic `adbc_ingest_key` UUID primary key, since Spanner requires one), `get_info` (static
  driver/vendor metadata),
  `get_objects` (incl. foreign-key `constraint_column_usage`), `get_table_types`/`get_table_schema`,
  `get_parameter_schema`, best-effort `Connection`/`Statement::cancel` (a shared `CancelSignal`
  interrupts an in-flight `block_on` op), keyfile/keyfile_json auth (credential-type auto-detected
  from the JSON `"type"`), and service-account impersonation
  (`spanner.impersonate.target_principal` enables it; optional `spanner.impersonate.delegates`
  [comma-separated chain], `spanner.impersonate.scopes` [comma-separated, defaults to
  cloud-platform], `spanner.impersonate.lifetime` [seconds, default 3600] — layered on top of the
  base credentials via `google-cloud-auth`'s `impersonated::Builder::from_source_credentials`,
  aligned with the BigQuery ADBC driver's `impersonate.*` group).
  (`get_statistics` computes exact `ROW_COUNT`/`NULL_COUNT`/`DISTINCT_COUNT` via one aggregate scan
  per table — see `src/statistics.rs`; `approximate=true` returns nothing since Spanner has no cheap
  stats. `get_statistic_names` returns an empty, correctly-typed result set.)
- Partitioned execution (`execute_partitions`/`read_partition`): `execute_partitions` opens a batch
  read-only transaction (`DatabaseClient::batch_read_only_transaction`), calls `partition_query`, and
  serialises each `google_cloud_spanner::batch::Partition` (which carries its session + transaction
  id + partition token, and is `serde`-serializable) into an opaque ADBC descriptor. Schema comes
  from a separate `QueryMode::Plan` probe. `read_partition` deserialises a descriptor and calls
  `Partition::execute` on the connection's client, streaming rows to Arrow via the same
  `stream_query` path as `execute`. This works because the client's session is **multiplexed** and
  `Arc`-shared across the connection's cloned `DatabaseClient`s, so a descriptor stays valid after
  the producing statement is gone. `spanner.data_boost_enabled` (statement option) bakes Data
  Boost into each descriptor; `spanner.max_partitions` hints the partition count. The emulator
  supports the Partition RPCs (it ignores Data Boost) — covered by `execute_partitions_round_trip` in
  `tests/integration.rs`.
- Still returning `NotImplemented` (keep the pattern until implemented): Substrait
  (`set_substrait_plan`) — Spanner executes GoogleSQL/PostgreSQL text, not Substrait plans.
- Commits in this environment may need `-c commit.gpgsign=false` if no signing agent is present.
