# CLAUDE.md

Guidance for working in this repository. It is deliberately short: the detail lives in `docs/`,
`README.md` and module rustdoc — see [Where each subject is
documented](#where-each-subject-is-documented) before adding anything here.

## What this is

`adbc-spanner` is a Rust [ADBC](https://arrow.apache.org/adbc/) (Arrow Database Connectivity) driver
for Google Cloud Spanner. It implements the native Rust `adbc_core` traits on top of the googleapis
preview `google-cloud-spanner` client and returns results as Apache Arrow record batches. It also
builds a C-ABI **cdylib** exporting `AdbcSpannerInit` (plus an `AdbcDriverInit` fallback) that any
ADBC driver manager can load; the export layer is the driver's own code in `src/ffi/`, not a
generated one.

## Common commands

```sh
cargo build                 # rlib + cdylib (libadbc_spanner.so/.dylib/.dll)
cargo test                  # unit tests + doctests; emulator/credential suites self-skip
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all --check     # CI enforces formatting

scripts/with-emulator.sh cargo test                     # + the emulator integration tests
scripts/with-toxiproxy.sh cargo test --test resilience  # transport fault injection
scripts/run-adbc-validation.sh                          # Apache's ADBC C++ validation suite
cargo +nightly fuzz run <target>                        # one fuzz target
```

CI enforces `fmt --check`, `clippy -D warnings`, unit tests and the emulator integration test, so
run those before pushing. `nm -D --defined-only target/release/libadbc_spanner.so | grep
AdbcSpannerInit` checks the cdylib's exports.

## Ground truth for the pinned dependencies

Both dependency families are git-pinned: read the checked-out source, not docs.rs. Use these
**exact** paths — the hashed directories are not unique per repo and each holds many revisions, so a
`google-cloud-rust-*` glob lands on the wrong one:

- Spanner client: `~/.cargo/git/checkouts/google-cloud-rust-897e43a00a59c4d1/ec54ef0/src/`
- ADBC Rust crates: `~/.cargo/git/checkouts/arrow-adbc-cf46b194429f7a74/32c67b0/rust/`
- The C ABI header `src/ffi/abi.rs` transcribes:
  `~/.cargo/git/checkouts/arrow-adbc-cf46b194429f7a74/32c67b0/c/include/arrow-adbc/adbc.h`

**Do not trust `docs.rs/.../latest` or web summaries for the Spanner client.** They surface an
older, unrelated yoshidan-style API (`Client::new`, `client.single()`, `add_param`). The pinned crate
is the googleapis preview client ("Google Cloud Client Libraries for Rust - Spanner").

Locally, this machine's git config rewrites `https://github.com` to SSH, so cargo fetches fail
unless you set `CARGO_NET_GIT_FETCH_WITH_CLI=true` plus a `GIT_CONFIG_*` `insteadOf` override. CI is
unaffected.

## Architecture map

```
SpannerDriver ──▶ SpannerDatabase ──▶ SpannerConnection ──▶ SpannerStatement
```

Unit tests live in a sibling `src/<module>/tests.rs` (e.g. `src/sql/tests.rs`), not inline.

| Module | What lives there |
| --- | --- |
| `lib.rs` | Crate docs, every `OPTION_*` key constant, the `fuzzing` wrapper, drift guards |
| `driver.rs` | `SpannerDriver` + `SpannerDatabase`: config, emulator, building the client |
| `driver/credentials.rs` | The mutually exclusive auth ladder + quota project |
| `driver/uri.rs` | `spanner:` URIs — path = database, `//host` = endpoint, params = options |
| `connection.rs` | The ADBC `Connection` surface: options, metadata entry points, partitions |
| `connection/txn.rs` | Transaction state — `TxnState`/`ManualTxn`, the two-kinds rule, buffering |
| `connection/exec.rs` | Isolation parsing and the three read/write-transaction runners |
| `metadata.rs` | Shared `INFORMATION_SCHEMA` plumbing + the UTF-8-correct `LIKE` matcher |
| `statement.rs` | `execute`/`execute_update`/`execute_schema`/`execute_partitions`, DDL |
| `statement/ingest.rs` | Bulk ingest: chunking, mutations, BatchWrite, the mutation-limit bisect |
| `bind.rs` | Arrow → Spanner values: parameter binding, ingest mutations, `create_table_sql` |
| `conversion.rs` | Result sets → Arrow (the type mapping); the streaming `SpannerBatchReader` |
| `sql.rs` | The one home for SQL text: lexing, splitting, quoting, parameter extraction |
| `objects.rs` | `get_objects`, from `INFORMATION_SCHEMA` |
| `statistics.rs` | `get_statistics` (exact, one aggregate scan per table), `get_statistic_names` |
| `info.rs` | `get_info` — static metadata, no RPC |
| `nested.rs` | Shared builders for those three methods' nested batches |
| `options.rs` | Shared option coercions, `SharedConfig`, the dispatch macro |
| `staleness.rs` | `spanner.read.staleness` → `TimestampBound` |
| `directed_read.rs` | `spanner.directed_read` → `DirectedReadOptions` |
| `query_options.rs` | Query optimizer version / statistics package |
| `request.rs` | Priority, request/transaction tags, commit delay, commit stats |
| `retry.rs` | Attempt/elapsed caps and backoff over the client's policy |
| `timeout.rs` | `spanner.rpc.timeout_seconds.*` deadlines |
| `runtime.rs` | The shared Tokio runtime; `spawn_prefetch` (depth-1 chunk prefetch) |
| `error.rs` | gRPC code → ADBC status, forwarded `google.rpc.Status` details |
| `asan_canary.rs` | Test-only ASan tripwire, `--cfg asan_canary` only |

Driver-internal metadata reads (`get_objects`, `get_statistics`, `get_table_schema`) carry the
request **priority**, replica selection and retry bounds, but stay **untagged**: tags are
user-statement attribution in `QUERY_STATS`/`TRANSACTION_STATS`. The RPC timeouts do bound them.

`src/ffi/` is the C ABI export layer — the whole boundary to a driver manager, and the only `unsafe`
in the crate (default `ffi` feature). Its `mod.rs` module doc walks it in full:

| Module | What lives there |
| --- | --- |
| `mod.rs` | The vtable, `AdbcSpannerInit` + `AdbcDriverInit`; serves ADBC 1.0.0 and 1.1.0 |
| `abi.rs` | adbc.h transcribed — declarations only; layout tests pin sizes/offsets |
| `error.rs` | Errors into the caller's `AdbcError` at its own revision; `ErrorGetDetail*` |
| `guard.rs` | Panic containment, pointer/string conversions, the in/out `length` protocol |
| `handle.rs` | Behind `private_data`: panic poisoning, state `Mutex`, `OptionBuffer`, cancel |
| `options.rs` | `New`/`Release` prologues and the eight option entry points |
| `database.rs`, `connection.rs`, `statement.rs` | Per-object entry points; `shared_driver()` |
| `stream.rs` | Arrow C stream export (cancel → `ECANCELED`), `ErrorFromArrayStream` |
| `import.rs` | Importing bound C arrays/streams, with `ArrayData` validation |
| `roundtrip.rs`, `tests.rs`, `test_support.rs` | Tests through a real driver manager, and helpers |

## Invariants an edit must not break

| Invariant | Guard |
| --- | --- |
| **One `SpannerDriver`, one Tokio runtime per process.** The traits are sync, the client async, so every method bridges with `runtime.block_on`. Never build a second runtime. | `shared_driver()` (`src/ffi/database.rs`) — a `OnceLock`; a failed runtime build is `ADBC_STATUS_INTERNAL`, not a panic. |
| **`unsafe` only under `src/ffi/`.** | `#![deny(unsafe_code)]` + `#![cfg_attr(not(feature = "ffi"), forbid(unsafe_code))]` in `src/lib.rs`, plus the `--no-default-features` CI build. |
| **The three arrow-adbc crates and `ARROW_ADBC_TAG` share ONE rev** — `adbc_core`, dev-deps `adbc_ffi`/`adbc_driver_manager`, and the C++ pin in `adbc-validation/CMakeLists.txt`. | Not machine-checked; the two sides meet only over the C ABI. Bump all four together. |
| **The eight `google-cloud-*` crates share ONE rev**, so their shared internal crates (`gax`, `gaxi`, …) resolve to one copy. | Not machine-checked; bump together. |
| **Every option key is a `pub const OPTION_*` in `src/lib.rs`**, documented in `docs/options.md`. Never spell a key as a literal elsewhere. | `every_handled_option_key_is_documented` (`src/lib.rs`). |
| **A fuzz target is a harness file + a `[[bin]]` in `fuzz/Cargo.toml` + a mention in `docs/testing.md` and here.** CI derives its matrix from the `[[bin]]`s. | `every_fuzz_target_is_wired_and_documented` (`src/lib.rs`). |
| **The emulator's gRPC endpoint must sit on port `9010`.** | See [Testing](#testing-against-the-emulator-or-a-real-database). |
| **DDL is never transaction-aware.** `run_ddl` executes immediately via admin `UpdateDatabaseDdl` whatever the transaction state: it neither fixes a manual transaction's kind nor is rejected by it, `rollback` cannot undo it, and DDL after buffered DML runs *before* it (the **DML/DDL reorder** caveat). | `docs/transactions.md`; cited from `src/statement.rs`. |

## Temporary git pins

`Cargo.toml` pins two dependency families to git revisions. **Each is independently a crates.io
publish blocker** — the crate cannot be published until *both* revert to versioned releases. A git
source does not unify with a crates.io release, so downstream crates must take `adbc_core` from the
same rev (see `README.md`).

| Family | Rev | Why it is still pinned |
| --- | --- | --- |
| `google-cloud-rust` (8 crates) | `ec54ef0ad69ecd24487c1d3b93a2e4082d820b58` | Native `STRUCT` mapping needs `Type::struct_type()`, on `main` but in no release. |
| `apache/arrow-adbc` (3 crates) | `32c67b092c0f7cabf2be75062f001a9e17a48cc1` | `Connection`/`Statement::get_cancel_handle` and the `CancelHandle` trait, added for 0.25.0. |

The pinned arrow-adbc rev is workspace version **0.25.0** (unreleased); the newest release is
**0.24.0**. `InfoCode::Other(u32)` (arrow-adbc#4510), long cited as this pin's reason, **did ship in
0.24.0** — only the cancel-handle API still blocks. Before reverting, diff the released `src/`
against the pinned checkout's `rust/core/src/`.

### Revert checklist

The revs live in 11 `Cargo.toml` dependency lines plus `deny.toml` plus the docs. This list is the
one place enumerating every edit needed to revert a family to versioned crates.io releases; touch
*every* location for that family in lockstep:

- `Cargo.toml` `[dependencies]` — arrow-adbc: `adbc_core` (only; `adbc_ffi` is not a dependency of
  the library). google-cloud: `google-cloud-spanner`, `google-cloud-auth`, `google-cloud-lro`,
  `google-cloud-gax` (names `rpc::StatusDetails`, so `from_spanner` can forward `google.rpc.Status`
  details), `google-cloud-wkt` (names the `Duration` that `set_max_commit_delay` takes).
- `Cargo.toml` `[dev-dependencies]` — arrow-adbc: `adbc_driver_manager`, `adbc_ffi` (its `FFI_Adbc*`
  structs are an independent transcription of the header `src/ffi/abi.rs` transcribes, used by
  `src/ffi/roundtrip.rs` and the raw-`libloading` lifecycle tests in `tests/ffi_lifecycle.rs`, so
  those tests cannot agree with the layer they check by construction). google-cloud:
  `google-cloud-spanner-admin-instance-v1`, `google-cloud-spanner-admin-database-v1`,
  `spanner-grpc-mock` (the `tests/mock_spanner.rs` harness; `publish = false` upstream and never on
  crates.io, so it stays a git pin — check whether `cargo publish` tolerates a version-less git
  dev-dependency before flipping `publish` back on). There is no `[patch]` section.
- `deny.toml` `allow-git` — drop a family's repo URL once it has no git dep left.
- `README.md` — the **Note** callout at the end of the *Type mapping* section, which says the crate
  is not on crates.io and names both pins narratively (no literal rev string there).
- `adbc-validation/CMakeLists.txt` — `ARROW_ADBC_TAG`, the fourth place holding the arrow-adbc rev.
- `CLAUDE.md` — this section.
- `Cargo.toml` `[package.metadata.release]` `publish = false` — flip to `true` only once *both*
  families are off git, and then revisit the `arrow-array`/`-schema`/`-buffer`/`-data` `>=58, <60`
  range, which exists only to unify the Arrow types with the git `adbc_core`.

The TLS stack is hardwired to `aws-lc` (`tonic/tls-aws-lc`, `rustls/aws_lc_rs`, the auth id-token
backend) — there is no `ring` option, which is why release CI builds each arch on its own runner.

## Testing against the emulator (or a real database)

`docs/testing.md` maps every suite and how to run it. What matters here:

- **Targets.** `tests/integration.rs` and `tests/ffi_lifecycle.rs` self-skip unless a target is
  configured, so plain `cargo test` is green everywhere. CI names each emulator-backed binary
  explicitly, so a new `tests/*.rs` does not run there until `ci.yml` lists it. `test_target()` resolves two, emulator first: `SPANNER_EMULATOR_HOST` (a local
  emulator; its fixed `test-project`/`test-instance`/`adbc-test` ids are created by the test) and
  `SPANNER_GCP_DATABASE` (a real database, `project.instance.database`, via ADC). CI sets
  `ADBC_TEST_REQUIRE_TARGET=1` so a skip fails instead; do not set it locally.
- **The emulator's gRPC endpoint must sit on port `9010`.** The client derives the admin/REST
  endpoint in `map_emulator_admin_endpoint` (`src/spanner/src/client.rs`): in emulator mode only, an
  endpoint whose **suffix** is `:9010` gets `:9020` instead. On any other port the admin request goes
  to the gRPC port and every DDL / `create_database` fails with `error sending request ... /ddl`. The
  host is free, the port is not, and the driver has no override. For several emulators at once
  (parallel worktrees), publish no ports and reach each container on its docker-network IP.
- `scripts/with-emulator.sh <cmd>` starts the emulator in Docker, exports the env var, runs the
  command and tears it down. It waits for the **admin API** (a REST 200 on `instanceConfigs`), not
  the gRPC port, which opens ~1s early — starting a test then made `create_instance` fail silently
  with "Instance not found".
- **No CI against a real database.** Every CI functional suite runs against the emulator, so the ADC
  path is covered only by running `SPANNER_GCP_DATABASE=… cargo test --test integration` by hand.
  The opt-in `auth_end_to_end` tests (keyfile, impersonation) are local-only too.

### Fuzzing

`fuzz/` holds the `cargo-fuzz` targets — `sql`, `values`, `like`, `options`, `keyword`, `params`,
`partition`, `staleness`, `directed_read`, `uri` — each a `libfuzzer-sys` harness over the `fuzzing`
wrapper module in `src/lib.rs`. `.github/workflows/fuzz.yml` runs them nightly, deriving its matrix
from the `[[bin]]` declarations. `fuzz/` is a member of the root workspace (rationale in
`Cargo.toml`), so the repo resolves to one `Cargo.lock` and plain cargo commands never build it.

## Releasing

**Always cut releases with [`cargo-release`](https://github.com/crate-ci/cargo-release)** — never
bump the version, commit or tag by hand.

```sh
cargo release patch            # dry run (default)
cargo release patch --execute  # bump + commit + tag vX.Y.Z + push
```

crates.io publishing is off (`publish = false`), so this only versions, commits, tags and pushes;
the tag is what makes CI attach the platform shared libraries and publish the Python wheels.
`CONTRIBUTING.md` has the full policy, including the `version-gate` job that fails a release whose
tag disagrees with the crate version.

## Conventions

- Match surrounding style; keep `fmt` and `clippy` clean (CI fails otherwise).
- Comments explain **why**, not what. Put behavioural detail in rustdoc next to the code, where it
  cannot drift, rather than in this file.
- Do not put review-ticket IDs in prose or comments — they resolve against nothing once the ticket
  is closed. Describe the reason instead.
- Do not narrate removed code. `git log` is the changelog.
- Commits in this environment may need `-c commit.gpgsign=false` if no signing agent is present.

## Where each subject is documented

| Subject | Where |
| --- | --- |
| Every option, its level, grammar and defaults | `docs/options.md`, and the `OPTION_*` constants in `src/lib.rs` |
| Transactions, isolation, the gRPC calls behind each path | `docs/transactions.md`, `src/connection.rs` module doc |
| ADBC concepts and how the driver maps onto them | `docs/adbc.md` |
| Supported functionality, type mapping, auth, quirks | `README.md` |
| Test suites, targets, fault injection, validation harnesses | `docs/testing.md`, `tests/RESILIENCE.md`, `adbc-validation/README.md`, `foundry-validation/README.md` |
| The C ABI export layer and what owning it buys | `src/ffi/mod.rs` module doc |
| Python package usage and publishing | `python/README.md` |
| Contribution, versioning and release policy | `CONTRIBUTING.md` |
| dbt integration notes | `docs/dbt.md` |
