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
  endpoint, emulator, `SPANNER_EMULATOR_HOST`), `spanner:` connection-URI parsing
  (path = database, `//host` authority = endpoint, query params = database options, expanded
  eagerly on set so precedence is last-writer-wins), and building the Spanner `DatabaseClient`.
- `src/connection.rs` — `SpannerConnection`: transaction mode (autocommit default or manual
  buffer-and-commit), `get_table_types` / `get_table_schema`.
- `src/statement.rs` — `execute` (query → streaming Arrow reader), `execute_update` (DML/DDL, incl.
  `;`-batches and bound params), `execute_schema` (PLAN-only schema), parameter binding / bulk
  ingest.
- `src/conversion.rs` — Spanner result set → Arrow schema + typed arrays (the type mapping lives
  here), plus `SpannerBatchReader`, the streaming `RecordBatchReader` that `execute` returns (pulls
  rows in bounded chunks of `spanner.rows_per_batch`, default 8192; a background task on the shared
  runtime prefetches the next chunk while the consumer processes the current one — depth-1, via
  `spawn_prefetch` in `src/runtime.rs`; cancel aborts the task, drop aborts its `JoinHandle`).
- `src/runtime.rs` — a shared Tokio runtime; the ADBC traits are sync while the Spanner client is
  async, so every call bridges via `runtime.block_on(...)`. The runtime is created once by the
  driver and shared via `Arc` into every database/connection/statement.
- `src/ffi.rs` — `adbc_ffi::export_driver!(AdbcSpannerInit, SpannerDriver)`; the C entrypoint of the
  shared library. Gated behind the default `ffi` feature.
- `src/error.rs` — helpers to build `adbc_core` errors. `from_spanner` takes the concrete
  `google_cloud_spanner::Error`: it maps the gRPC code onto the closest ADBC status, keeps the
  numeric code in `vendor_code`, and forwards any `google.rpc.Status` details (ErrorInfo/BadRequest
  on INVALID_ARGUMENT, QuotaFailure on RESOURCE_EXHAUSTED, PreconditionFailure on
  FAILED_PRECONDITION, …; RetryInfo on ABORTED is forwarded too but rarely surfaces — the client's
  transaction runner retries aborts internally, consuming its `retryDelay`) into `Error.details` —
  key = lowercased proto type name
  (`google.rpc.retryinfo`), value = the detail's ProtoJSON bytes (no `-bin` suffix; the pinned
  client's detail types have no binary-proto encoding). `from_builder` stays generic over
  `Display` for the status-less client-builder errors.

Key design points:

- **Sync-over-async bridge.** ADBC traits are synchronous; each method does `runtime.block_on`. Do
  not add a second runtime — reuse the shared one.
- **Transactions.** Autocommit by default: queries use a single-use read-only transaction; DML
  (including a `;`-separated batch via `ExecuteBatchDml`) uses a read/write runner. Setting
  `adbc.connection.autocommit=false` enters manual mode, which **buffers** DML and applies the whole
  batch atomically in one read/write transaction on `commit` (`rollback` discards it). The client
  exposes no manual begin/commit handle, so buffer-and-replay is what makes manual transactions both
  possible and retry-safe. In manual mode `execute_update` returns `None` (count unknown until
  commit); queries and DDL still run immediately — so a manual transaction has **no
  read-your-writes** (`INSERT` → `SELECT COUNT(*)` returns the pre-insert count) and **DML/DDL
  reorder** (DDL issued after buffered DML executes before it). Both consequences are documented
  user-facing (README.md Transactions bullet, python/README.md "Manual transactions" section with a
  CI-executed example, `SpannerConnection` rustdoc + lib.rs crate docs); rejecting/warning on
  queries while DML is buffered was deliberately not done, and the proper fix waits on the client
  exposing begin/commit handles. The standard
  `adbc.connection.transaction.isolation_level` option is honoured for read/write transactions:
  `serializable` and `repeatable_read` map to the client's `IsolationLevel` (applied via
  `TransactionRunnerBuilder::set_isolation_level`), `default` leaves the database default, and the
  other spec levels are rejected with `NotImplemented`. The standard `adbc.connection.readonly`
  option (default `false`) makes a connection reject all writes — DML/DDL/ingest fail with
  `InvalidState`, queries still run; the flag is a shared `Arc<AtomicBool>` that statements read at
  execution time, so toggling it on the connection immediately affects existing statements too.
- **Stale reads.** Read-only queries default to a strong bound. `spanner.read.staleness`
  (`exact:<duration>` / `max:<duration>`) and `spanner.read.timestamp` (RFC3339, optionally prefixed
  `read:` / `min:`) request a non-strong `TimestampBound` — parsed in `src/staleness.rs` (`ReadBound`
  / `ReadStaleness`, unit-tested offline) and applied at the `single_use()` query sites plus the
  partition batch read-only transaction via `staleness::single_use`. A bound (parameterized) query
  over several bound rows runs all its per-row statements in **one** multi-use read-only
  transaction pinned at the same bound (streaming via `BoundQueryBatchReader` in
  `src/conversion.rs`); since Spanner accepts the bounded-staleness kinds only on single-use
  transactions, `max:`/`min:` are pinned there to their most-stale legal equivalent
  (`ReadBound::pinned_for_multi_use`). Both options exist at connection
  **and** statement level (statement inherits the connection's, then overrides); they are mutually
  exclusive (set one to `""` to unset), and round-trip through `get_option`.
- **Arrow version.** `arrow-array`/`arrow-schema`/`arrow-buffer` are pinned to the range the (git)
  `adbc_core` allows (`>=58, <60`) so the `RecordBatch`/`Schema`/`RecordBatchReader` types unify
  across crates.

## The google-cloud-spanner preview crate

This uses the **googleapis preview** client `google-cloud-spanner` (crate description "Google Cloud
Client Libraries for Rust - Spanner"). Beware: `docs.rs/.../latest` and web summaries often surface
an **older, unrelated** yoshidan-style API (`Client::new`, `client.single()`, `add_param`) — do not
trust those. For ground truth, read the extracted source under
`~/.cargo/git/checkouts/google-cloud-rust-*/` (the git dependency's checkout); `adbc_core` /
`adbc_ffi` are likewise git-pinned, so their ground truth is the `arrow-adbc-*` checkout under the
same directory (the fork tracks close to the 0.23.0 release).

**Temporary git pins (two families).** `Cargo.toml` pins two dependency families to git revisions,
and **each is independently a crates.io publish blocker** — the crate cannot be published until
*both* are reverted to versioned releases:

1. The whole `google-cloud-*` family (spanner, auth, lro, `wkt`, both admin crates + `gax`)
   is pinned to a `google-cloud-rust` git revision, because native `STRUCT` mapping needs
   `Type::struct_type()`, which is on `main` but not yet in a crates.io release.
2. `adbc_core` and `adbc_ffi` (and the dev-dependency `adbc_driver_manager`) are pinned to a
   `fornwall/arrow-adbc` fork git revision — all three must share the *same* rev — carrying two
   fixes not yet in the 0.23 crates.io release: an idempotent `release_ffi_error` (no double-free on
   the standard release-twice idiom) and `AdbcStatementExecuteQuery` writing `rows_affected = -1` on
   the query path (submitted upstream as arrow-adbc PR #4469). Because a git source will not unify
   with the crates.io `= "0.23"` release, downstream crates must also take `adbc_core` from this
   same git rev (see `README.md`).

**Revert checklist — the single anchor.** The two revs are spread across ~9 `Cargo.toml` dependency
lines plus `deny.toml` plus the docs; this list is the one place that enumerates every edit needed to
revert a family to versioned crates.io releases. Current pinned revs:

- `google-cloud-rust`: `3872d2885c6da3a9463e85b50bf1fe8e9ddc1fa1`
- `fornwall/arrow-adbc`: `786e7f3488eb71b200ece775b027a647cf42db9e`

**Invariant:** the three arrow-adbc crates (`adbc_core`, `adbc_ffi`, `adbc_driver_manager`) must
always share ONE rev; the eight `google-cloud-rust` crates likewise share ONE rev. When reverting,
touch *every* location for that family in lockstep:

- `Cargo.toml` `[dependencies]` — arrow-adbc: `adbc_core`, `adbc_ffi`; google-cloud:
  `google-cloud-spanner`, `google-cloud-auth`, `google-cloud-lro`, `google-cloud-gax` (this last
  names `rpc::StatusDetails` so `from_spanner` can forward `google.rpc.Status` details),
  `google-cloud-wkt` (names the `Duration` type `set_max_commit_delay` takes for
  `spanner.max_commit_delay`).
- `Cargo.toml` `[dev-dependencies]` — arrow-adbc: `adbc_driver_manager`; google-cloud:
  `google-cloud-spanner-admin-instance-v1`, `google-cloud-spanner-admin-database-v1`,
  `spanner-grpc-mock` (the mock-server harness of `tests/mock_spanner.rs`;
  note it is `publish = false` upstream and will never be on crates.io — when the family reverts
  to versioned releases this one stays a git pin, so check whether `cargo publish` tolerates a
  version-less git dev-dependency before flipping `publish` back on). (There is no `[patch]`
  section.)
- `deny.toml` `allow-git` — drop the repo URL for each family once it no longer has any git dep.
- `README.md` — the quickstart `adbc_core = { git = … rev = "786e7f3…" }` block and the surrounding
  "not on crates.io" / *Type mapping* notes that name the pins.
- `CLAUDE.md` — this section (both the "Temporary git pins" note and this checklist); once *both*
  families are versioned, also re-enable `publish` (below) and revisit the `arrow-array`/`-schema`/
  `-buffer` `>=58, <60` range, which exists only to unify with the git `adbc_core`.
- `Cargo.toml` `[package.metadata.release]` `publish = false` — flip back to `true` only after
  *both* families are off git (each is independently a publish blocker).

Locally, this machine's global git config rewrites `https://github.com` to SSH, so cargo fetches
fail unless you set `CARGO_NET_GIT_FETCH_WITH_CLI=true` plus a `GIT_CONFIG_*` identity `insteadOf`
override for the fork URLs (see the session notes / the `with-emulator.sh` invocations). CI is
unaffected (clean git config, public repo over HTTPS).

The TLS stack is hardwired to `aws-lc` (via `tonic/tls-aws-lc`, `rustls/aws_lc_rs`, and the auth
id-token backend) — there is no `ring` option. This is why the release CI builds per arch on its own
runner (aws-lc-rs cross-compiles poorly — even ARM64 Windows uses a native runner; only macOS x86-64
is cross-compiled, off the universal Apple toolchain) and installs NASM only on Windows x86-64.

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
- **Opt-in end-to-end auth tests** (`auth_end_to_end` module in `tests/integration.rs`) exercise the
  `spanner.keyfile` and `spanner.impersonate.target_principal` credential paths against a **real**
  database (the emulator refuses these credentials) with a trivial `SELECT 1`. They self-skip cleanly
  when their env vars are unset, so `cargo test` stays green without credentials. They read
  `SPANNER_GCP_DATABASE` (the real target, reused) plus `SPANNER_TEST_KEYFILE` (path to a
  service-account JSON key → `keyfile_auth_end_to_end`) and/or
  `SPANNER_TEST_IMPERSONATE_TARGET_PRINCIPAL` (a principal to impersonate, base creds from ADC →
  `impersonation_auth_end_to_end`).
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

`.github/workflows/libraries.yml` builds the library for **eight** targets on pushes to main, pull
requests and tags: linux x86-64 glibc (`ubuntu-22.04`), linux aarch64 glibc (`ubuntu-22.04-arm`),
macOS arm64
(`macos-14`), macOS x86-64 (**cross-compiled** on the same `macos-14` arm64 runner, since the Apple
toolchain is universal), windows x86-64 (`windows-latest`) and windows aarch64 (`windows-11-arm`,
native). Every other target is built natively on its own runner. Artifacts attach to every run, and
on `v*` tags they are attached to the GitHub Release.

The remaining two targets, linux **musl** (Alpine, for the `musllinux_1_2` wheels) on x86-64 **and**
aarch64, are built by a separate `build-musl` job rather than the matrix: a musl **cdylib** must link
*dynamically* (rustc drops the cdylib crate-type under the musl target's default `crt-static`), which
needs a musl-native `libgcc` that Ubuntu's `musl-tools` does not ship (a cross-linked `.so` pulls
glibc's libc/libgcc instead — because aws-lc-rs, the hardwired TLS backend, has no pure-Rust option,
the C is unavoidable). So `build-musl` compiles inside a digest-pinned `rust:alpine` container
(native musl gcc/libgcc) via `docker run` with `RUSTFLAGS=-Ctarget-feature=-crt-static`; only the
compile runs in the container, while checkout/package/upload run on the host so the pinned JS actions
work. It is a two-leg matrix — x86-64 on `ubuntu-22.04`, aarch64 on `ubuntu-22.04-arm` — each running
the matching arch of the multi-arch `rust:alpine` image natively (no musl cross-toolchain). `release`
and `python-wheels` `needs:` this job in addition to `build`.

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

**crates.io publishing is off**, via `publish = false` in the release config (the git-pinned deps —
both the `google-cloud-*` family and `adbc_core`/`adbc_ffi` — can't be published; see the two-pin
dependency note above). So `cargo release --execute`
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

Unlike the crate, the wheel ships a compiled binary, so the git-pinned dependencies (the
`google-cloud-rust` and `arrow-adbc` pins, which block `cargo publish`) do **not** block PyPI — the
Python package can release independently.

**One-time PyPI setup (before the first tag):** register a *pending publisher* at
<https://pypi.org/manage/account/publishing/> with project `adbc-driver-spanner`, owner `fornwall`,
repo `adbc-spanner`, workflow `libraries.yml`, environment `pypi` (all must match exactly), then
create the `pypi` GitHub environment (Settings → Environments), ideally restricted to `v*` tags. See
`python/README.md` for usage.

## Conventions / gotchas

- Match surrounding style; keep `fmt`/`clippy` clean (CI fails otherwise).
- Supported so far: streaming queries (`execute` returns a lazy `SpannerBatchReader` that converts
  bounded row chunks to Arrow on demand — chunk size via `spanner.rows_per_batch` — with a
  background task prefetching the next chunk ahead of the consumer), DML, DDL (via
  admin `UpdateDatabaseDdl`), manual transactions
  (buffer-and-commit), native Arrow types for DATE/TIMESTAMP/NUMERIC and native `List`/`Struct` for
  ARRAY/STRUCT, parameter binding (positional by default — the *i*-th bound column binds the *i*-th
  distinct `@param` in query order, column names ignored; the `adbc.statement.bind_by_name`
  statement option [SQLite reference-driver convention, a boolean defaulting to `false`] set to
  `true` forces strict by-name binding [order-independent; an unmatched column → `InvalidArguments`];
  an `arrow.json`-tagged string
  column binds as a `JSON`-typed param — Spanner won't coerce STRING params into JSON columns — and
  ingest create modes map it to a `JSON` column) + bulk ingest (append and
  create/create_append/replace — the create modes build the table via admin DDL from the ingest
  data's Arrow schema with a synthetic `adbc_ingest_key` UUID primary key, since Spanner requires
  one — or, when `spanner.ingest.primary_key` [statement option; comma-separated existing columns,
  `""` unsets, round-trips via `get_option`] is set, key on those existing columns in the given
  order and add no synthetic column [`bind::create_table_sql`; a named column absent from the ingest
  schema → `InvalidArguments`, and it is ignored by `append`]; the rows themselves ship as native
  **insert mutations** — `bind::insert_mutation`, reusing
  the same `cell_value` Arrow→Spanner mapping as parameter binding — not per-row `INSERT` DML, so
  nothing is SQL-parsed/planned per row but `INSERT` semantics are kept (duplicate PK →
  `AlreadyExists` naming the target table; `create` mode onto an existing table likewise remaps to
  `AlreadyExists`); autocommit ingests are built and committed chunk by chunk via
  `DatabaseClient::write_only_transaction` under Spanner's per-commit limits — `IngestChunkBudget`
  in `src/statement.rs`, ~rows × columns mutations + an approximate byte budget — so a multi-chunk
  ingest commits per chunk and is not atomic as a whole (a mid-ingest chunk failure reports the
  exact row count the earlier chunks already committed), while manual-mode ingests buffer their
  mutations unchunked (`TxnState::pending_mutations`) and commit atomically **with** the buffered
  DML in one read/write transaction via `ReadWriteTransaction::buffer` — note Spanner applies
  buffered mutations at commit, after the transaction's DML executes), `get_info` (static
  driver/vendor metadata),
  `get_objects` (incl. foreign-key `constraint_column_usage`), `get_table_types`/`get_table_schema`,
  `get_parameter_schema`, `Connection`/`Statement::cancel` (a shared, sticky `CancelSignal`
  interrupts an in-flight `block_on` op and stays latched — so a cancel between the chunk fetches of
  a streamed result still cancels the next fetch — until the object's next operation resets it),
  keyfile/keyfile_json auth (credential-type auto-detected
  from the JSON `"type"`), OAuth access-token auth
  (`spanner.access_token` — a caller-supplied bearer token sent verbatim with no refresh via a
  minimal custom `google-cloud-auth` `CredentialsProvider`, `StaticTokenCredentials` in
  `src/driver.rs`; mutually exclusive with keyfile/impersonation and refused in emulator mode, the
  keyfile-guard pattern), and service-account impersonation
  (`spanner.impersonate.target_principal` enables it; optional `spanner.impersonate.delegates`
  [comma-separated chain], `spanner.impersonate.scopes` [comma-separated, defaults to
  cloud-platform], `spanner.impersonate.lifetime` [seconds, default 3600] — layered on top of the
  base credentials via `google-cloud-auth`'s `impersonated::Builder::from_source_credentials`,
  the `impersonate.*` naming following gcloud's `--impersonate-service-account` / that builder — NOT
  the ADBC BigQuery driver, which has no impersonation options), and request priority / tags
  (`spanner.request.priority` [`low`/`medium`/`high`] and `spanner.request.tag` at connection +
  statement level [statement inherits, then overrides; `""` unsets — the staleness pattern],
  `spanner.transaction.tag` connection-only; parsed/applied via `RequestConfig` in `src/request.rs`
  — every user statement builder goes through `SpannerStatement::sql_builder`, `run_batch_dml`
  tags the `ExecuteBatchDml` batch and the runner [commit priority + transaction tag; the client
  has no batch-level priority setter], driver-internal metadata queries stay untagged), directed
  reads (`spanner.directed_read` at connection + statement level [statement inherits, then overrides;
  `""` unsets — the staleness pattern; round-trip via `get_option`] — a replica selection for
  read-only queries parsed by `DirectedRead`/`parse` in `src/directed_read.rs` [unit-tested offline]
  with the grammar `<mode>[:<sel>,...][;auto_failover_disabled]` where `<mode>` is `include`/`exclude`
  and each `<sel>` is `<location>[:<type>]`/`:<type>` with `<type>` ∈ `read_write`/`read_only`/`any`;
  built into the client `DirectedReadOptions` and applied via `StatementBuilder::set_directed_read_options`
  only on the read-only query paths — `SpannerStatement::read_sql_builder` [= `sql_builder` + directed
  reads] feeds the main `execute` query, the bound-query path, `execute_partitions`, and the
  `execute_schema` PLAN probe; DML/DDL keep the plain `sql_builder` since Spanner rejects directed
  reads on a read/write transaction), commit
  batching (`spanner.max_commit_delay` at connection + statement level [statement inherits, then
  overrides; `""` unsets — the staleness pattern; a duration in `0..=500ms` parsed with the shared
  `staleness::parse_duration` grammar, out-of-range/malformed → `InvalidArguments`; round-trips via
  `get_option`] — stored on `RequestConfig` in `src/request.rs` and applied as the client's
  `set_max_commit_delay` [a `google_cloud_wkt::Duration`] at the same read/write **commit** sites
  the runner / write-only builders cover: autocommit DML, the `ExecuteBatchDml` batch runner, the
  manual-mode commit, and the ingest write-only txn — i.e. `RequestConfig::{apply_to_runner,
  apply_to_write_only}`), query
  optimizer options (`spanner.query.optimizer_version` and
  `spanner.query.optimizer_statistics_package` at connection + statement level [statement inherits,
  then overrides; `""` unsets — the staleness pattern; opaque pass-through strings, round-trip via
  `get_option`] — `QueryOptionsConfig` in `src/query_options.rs` sets `QueryOptions` on the query
  statement builder via `SpannerStatement::sql_builder`), and RPC
  timeouts (`spanner.rpc.timeout_seconds.{query,update,fetch}` at connection + statement level
  [statement inherits, then overrides; `""` unsets, `0` disables; f64 seconds, finite +
  non-negative, round-trip via `get_option`/`get_option_double` — `RpcTimeouts` in
  `src/timeout.rs`, naming parallels Flight SQL's `adbc.flight.sql.rpc.timeout_seconds.*`] —
  enforced as overall `tokio::time::timeout` deadlines (`timeout::with_timeout`) mapped to
  `Status::Timeout`: query = initial execute + first chunk (plus the `execute_schema`/
  `execute_partitions` probes and `read_partition`'s initial fetch) **and the driver-internal
  metadata reads** (`get_objects`/`collect_objects`, `get_statistics`/`collect_statistics` — both
  its discovery fetch and aggregate-scan phases, `get_table_schema`, the ingest `table_exists`
  probe), fetch = each later chunk [inside the `spawn_prefetch` task, and each `next_bound_chunk`
  of a bound-query stream], update = DML/batch-DML/manual-commit/ingest-chunk paths **and DDL**
  (`run_ddl`'s admin `UpdateDatabaseDdl` call plus its LRO poll loop). So no driver-side network
  path is left unbounded; unlike the tags/priority options (which leave metadata queries
  untagged), the timeouts do bound them), and retry tuning
  (`spanner.retry.{max_attempts,max_elapsed_seconds}` at connection + statement level [statement
  inherits, then overrides; `""` unsets — the staleness/timeout pattern; round-trip via
  `get_option`/`get_option_int`/`get_option_double`] — `RetryConfig` in `src/retry.rs` bounds the
  pinned client's gax retry policy by layering `RetryPolicyExt::{with_attempt_limit,with_time_limit}`
  on a driver-local copy of the client's private `SpannerRetryPolicy` (so the
  transport-error-on-idempotent retry is preserved, not dropped), applied via the builders'
  `with_retry_policy` / `with_begin_retry_policy` / `with_commit_retry_policy` at the same sites the
  request tags cover [`sql_builder`, `run_batch_txn`'s runner + `ExecuteBatchDml` batch, the ingest
  write-only txn]; unset = the client's own unbounded policy, so it is purely opt-in. Bounds the
  *per-attempt* retry loop, complementary to the *overall* RPC-timeout deadlines; custom *backoff*
  via `with_backoff_policy` is a deliberate non-goal for now. The transaction-level abort retry
  stays at the client default).
  (`get_statistics` computes exact `ROW_COUNT`/`NULL_COUNT`/`DISTINCT_COUNT` via one aggregate scan
  per table — see `src/statistics.rs`; `approximate=true` serves the same exact stats (exact values
  always satisfy an approximate request; Spanner has no cheaper source), with
  `statistic_is_approximate=false` on every row. `get_statistic_names` returns an empty,
  correctly-typed result set.)
- Partitioned execution (`execute_partitions`/`read_partition`): `execute_partitions` opens a batch
  read-only transaction (`DatabaseClient::batch_read_only_transaction`), calls `partition_query`, and
  serialises each `google_cloud_spanner::batch::Partition` (which carries its session + transaction
  id + partition token, and is `serde`-serializable) into an opaque ADBC descriptor — a versioned
  JSON envelope `{"v":1,"partition":<Partition serde form>}` (`encode_partition`/`decode_partition`
  in `src/connection.rs`; a missing or unsupported version is a clean `InvalidArguments` — the bare
  `Partition` layout is a client-crate compatibility surface we don't control, while descriptors
  travel between processes and driver versions). Schema comes
  from a separate `QueryMode::Plan` probe. `read_partition` deserialises a descriptor and calls
  `Partition::execute` on the connection's client, streaming rows to Arrow via the same
  `stream_query` path as `execute`. This works because the client's session is **multiplexed** and
  `Arc`-shared across the connection's cloned `DatabaseClient`s, so a descriptor stays valid after
  the producing statement is gone. `spanner.data_boost_enabled` (statement option) bakes Data
  Boost into each descriptor; `spanner.max_partitions` hints the partition count. The emulator
  supports the Partition RPCs (it ignores Data Boost) — covered by `execute_partitions_round_trip` in
  `tests/integration.rs`. **Security caveat:** a descriptor is opaque but *executable* — its serde
  JSON carries the SQL text plus session/transaction identity, so `read_partition` runs whatever it
  contains with the connection's credentials. It is not authenticated (an HMAC envelope was
  considered and deliberately not implemented). Transport descriptors only over trusted channels and
  never execute one from an untrusted source; the `read_partition`/`execute_partitions` rustdoc
  states the same.
- Still returning `NotImplemented` (keep the pattern until implemented): Substrait
  (`set_substrait_plan`) — Spanner executes GoogleSQL/PostgreSQL text, not Substrait plans.
- Commits in this environment may need `-c commit.gpgsign=false` if no signing agent is present.
