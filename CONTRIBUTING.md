# Contributing

Repository conventions are in [CLAUDE.md](CLAUDE.md); test setup and CI coverage are in
[docs/testing.md](docs/testing.md).

## Building and testing

Use the toolchain pinned in `rust-toolchain.toml`. From the repository root:

```sh
cargo build
cargo test
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all --check
scripts/with-emulator.sh cargo test  # requires Docker
```

Tests that need a Spanner target self-skip when none is configured. The emulator command runs
them against a disposable database; Toxiproxy fault tests and external validation harnesses
have separate commands in the testing guide. Rust integration tests can also use
`SPANNER_GCP_DATABASE=project.instance.database` with Application Default Credentials.

Before pushing code changes, run formatting, clippy, tests and emulator tests. CI also checks
rustdoc, the build without default features, Python integration, workflows and dependencies.
Keep pull requests focused and explain non-obvious constraints in nearby comments.

## Versioning

Use `MAJOR.MINOR.PATCH`: major for incompatible changes, minor for compatible functionality,
and patch for compatible fixes. Before 1.0, incompatible API changes may occur in minor releases.

## Releasing

Use [`cargo-release`](https://github.com/crate-ci/cargo-release), configured in `Cargo.toml`;
do not bump versions or create release commits/tags manually. Install it with
`cargo install cargo-release`; releases require push access to `main`.

```sh
cargo release patch            # dry run
cargo release patch --execute  # version, commit, tag and push
```

The pre-release hook runs fmt, clippy and `cargo test`. Cargo-release also updates the Python
fallback version. Its `publish = false` setting disables crates.io publishing while git pins remain.

A `vX.Y.Z` tag triggers `.github/workflows/libraries.yml`. Publishing requires a matching crate
version and successful CI for the tagged commit. The workflow attaches shared libraries to the
GitHub Release, then publishes Python wheels to PyPI through trusted publishing.

The PyPI project is `spanner-adbc`; the Python import package is `spanner_adbc`.
Its PyPI trusted publisher uses owner `fornwall`, repository `spanner-adbc`, workflow
`libraries.yml`, and GitHub environment `pypi`.

## Dependency pins

`Cargo.toml` pins the Google Cloud family and `adbc_core`/`adbc_ffi`/`adbc_driver_manager` to git.
The pinned APIs provide native STRUCT inspection and support for unknown ADBC info codes.
Either family independently prevents crates.io publishing.

Read revisions from `Cargo.toml`/`Cargo.lock` and follow the
[revert checklist](CLAUDE.md#revert-checklist) when switching to registry releases. Keep each
family on one revision, including the ADBC C++ validation header; downstream Rust users must
use the same `adbc_core` source. Check published API availability before removing a pin.
