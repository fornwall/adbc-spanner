# CLAUDE.md

`adbc-spanner` is a Rust ADBC driver for Google Cloud Spanner, returning Arrow record batches.
It implements `adbc_core` traits and a hand-written C ABI in `src/ffi/`, exporting
`AdbcSpannerInit` and `AdbcDriverInit` from a cdylib.

## Common commands

Run from the repository root:

```sh
cargo build                                           # rlib + cdylib
cargo test                                            # target-dependent suites self-skip
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all --check
scripts/with-emulator.sh cargo test                    # Docker-backed integration tests
scripts/with-toxiproxy.sh cargo test --test resilience  # transport fault injection
scripts/run-adbc-validation.sh                        # Apache ADBC C++ validation
cargo +nightly fuzz run <target>
```

For code changes, run relevant tests; before pushing, run fmt, clippy, tests and emulator tests.
See [docs/testing.md](docs/testing.md) for additional suites and CI coverage.

## Coding conventions and invariants

- Keep unit tests in sibling `src/<module>/tests.rs` files, not inline.
- Explain non-obvious reasons in comments; keep behavioral detail in nearby rustdoc.
  Omit review-ticket IDs and narratives about removed code; history belongs in git.
- Keep SQL lexing, splitting, quoting and parameter extraction in `src/sql.rs`.
- Reuse the driver's Tokio runtime to bridge synchronous ADBC methods to the async client.
  The C ABI uses one process-wide driver via `shared_driver()` in `src/ffi/database.rs`.
- Confine `unsafe` to `src/ffi/`; preserve the `--no-default-features` build without unsafe code.
- Define option keys as `pub const OPTION_*` in `src/lib.rs`; reuse constants instead of literals
  and document each option in [docs/options.md](docs/options.md).
- DDL executes immediately through the admin API, independently of manual transactions:
  rollback cannot undo it, and DDL runs before buffered DML. See [transactions](docs/transactions.md).
- Internal metadata reads inherit priority, replica selection, retry bounds and RPC timeouts,
  but remain untagged; request/transaction tags attribute user statements.

## Testing against the emulator (or a real database)

- Prefer `scripts/with-emulator.sh <cmd>`: it starts Docker, waits for the admin API and cleans up.
- The emulator's gRPC endpoint must use port `9010`; the pinned client derives REST port `9020`
  from that suffix. For parallel emulators, use separate Docker-network IPs without published ports.
- Tests prefer `SPANNER_EMULATOR_HOST`, then `SPANNER_GCP_DATABASE` (`project.instance.database`,
  using ADC); without a target they self-skip. `ADBC_TEST_REQUIRE_TARGET=1` is for CI, not local use.
- CI runs functional suites against the emulator; real-database and auth end-to-end tests are local.
- Register new emulator-backed test binaries in `.github/workflows/ci.yml`; CI lists them explicitly.

### Fuzzing

Targets: `sql`, `values`, `like`, `options`, `keyword`, `params`, `partition`, `staleness`,
`directed_read`, `uri`. Each needs a harness, a `[[bin]]` in `fuzz/Cargo.toml`, and a mention here
and in `docs/testing.md`; a test checks this, and CI derives its matrix from the manifest.
`fuzz/` shares the root workspace and lockfile but is excluded from default build/test commands.

## Temporary git pins

- Read exact revisions from `Cargo.toml`/`Cargo.lock`. Locate matching dependency source with
  `cargo metadata --format-version 1 --locked`; do not guess Cargo checkout hashes or revisions.
- Consult the pinned source for API details. The Spanner client is the googleapis preview client;
  docs.rs/latest and older yoshidan-style examples describe a different API.
- Keep `adbc_core`, `adbc_ffi`, `adbc_driver_manager` and CMake's `ARROW_ADBC_TAG` on one revision;
  `src/ffi/abi.rs` transcribes that revision's `c/include/arrow-adbc/adbc.h`.
- Keep the eight dependencies from `google-cloud-rust`, including `spanner-grpc-mock`, on one rev.
- Tracking arrow-adbc `main` is deliberate; do not revert that pin merely to use a release.
  The Google Cloud pin supplies `Type::struct_type()` for native STRUCT mapping.
- Each git dependency family independently blocks crates.io publishing.

### Revert checklist

When intentionally moving a family to crates.io, update every affected location together:

- All dependency and dev-dependency entries in `Cargo.toml`; downstream `adbc_core` must match.
- `ARROW_ADBC_TAG` in `adbc-validation/CMakeLists.txt` for arrow-adbc changes.
- `deny.toml` allow-git entries once a repository has no remaining git dependencies.
- The type-mapping note in `README.md`, [CONTRIBUTING.md](CONTRIBUTING.md) and this section.
- Keep release `publish = false` until both families permit publishing. `spanner-grpc-mock` is
  unpublished upstream: verify whether its git dev-dependency is acceptable before enabling it.
  Revisit the Arrow version ranges when replacing git `adbc_core`.

## Releasing

Use `cargo-release`; never manually bump versions or create release commits/tags.
`cargo release patch` is a dry run; `cargo release patch --execute` commits, tags and pushes,
triggering shared-library and Python-wheel publishing. Follow [CONTRIBUTING.md](CONTRIBUTING.md).

## Read when relevant

- [README.md](README.md): supported functionality, type mapping, authentication and quirks.
- [docs/options.md](docs/options.md): option levels, grammar, defaults and getters.
- [docs/transactions.md](docs/transactions.md): transaction behavior, isolation and RPC paths.
- [docs/adbc.md](docs/adbc.md): ADBC concepts; `src/ffi/mod.rs` rustdoc: C ABI design.
- [docs/testing.md](docs/testing.md): suites, targets, fault injection and validation harnesses.
- [python/README.md](python/README.md): Python usage and publishing; [docs/dbt.md](docs/dbt.md): dbt.

Keep this file at most 100 lines; link to detailed docs instead of duplicating them.
