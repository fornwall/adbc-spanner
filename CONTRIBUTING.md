# Contributing

Thanks for your interest in `adbc-spanner`, a Rust [ADBC](https://arrow.apache.org/adbc/) driver for
Google Cloud Spanner. This document covers the local checks, the release process, and the versioning
policy. `CLAUDE.md` holds the deeper architecture notes and is the source of truth for the
temporary dependency pins (see [Dependency pins](#dependency-pins) below).

## Building and testing

```sh
cargo build                 # builds the rlib and the cdylib (libadbc_spanner.so/.dylib/.dll)
cargo test                  # unit tests + doctests; the emulator integration test self-skips
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all --check     # CI enforces formatting
```

Plain `cargo test` is green everywhere: the emulator-gated integration and resilience suites
self-skip when no target is configured. To run everything, including the Spanner emulator
integration tests:

```sh
scripts/with-emulator.sh cargo test
```

`scripts/with-emulator.sh` runs the emulator in Docker, exports `SPANNER_EMULATOR_HOST`, runs the
command, and tears the emulator down. The suite can also run against a real Cloud Spanner database
via `SPANNER_GCP_DATABASE` (`project.instance.database`, using Application Default Credentials); see
the "Testing against the emulator" section of `CLAUDE.md`.

CI enforces all of the above — `cargo fmt --all --check`, `cargo clippy --all-targets --all-features
-- -D warnings`, the unit tests + doctests, and the emulator integration test — so run them before
pushing.

## Pull requests

- Match the surrounding style; keep `fmt` and `clippy` clean (CI fails otherwise).
- Add a note to the `## [Unreleased]` section of [CHANGELOG.md](CHANGELOG.md) for any user-visible
  change (a new option, behavior change, or bug fix).
- Keep changes truthful and focused; the existing code favors "why" comments at every non-obvious
  constraint — follow that convention.

## Versioning

This project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html): given `MAJOR.MINOR.PATCH`,

- `MAJOR` for incompatible API / behavior changes,
- `MINOR` for backwards-compatible functionality (e.g. a new option or supported type), and
- `PATCH` for backwards-compatible bug fixes.

While the crate is pre-1.0, the public surface may still shift between minor versions.

## Releasing

Always cut releases with [`cargo-release`](https://github.com/crate-ci/cargo-release) (configured
under `[package.metadata.release]` in `Cargo.toml`) — never bump the version, commit, or tag by
hand. Hand-rolling a release risks a malformed tag or a version that disagrees with `Cargo.toml`
(which the `python-wheels` CI job rejects), and cargo-release does the exact same steps
deterministically.

```sh
cargo release patch            # dry run (default) — preview only
cargo release patch --execute  # bump + commit "Release X.Y.Z" + tag vX.Y.Z + push
```

A `pre-release-hook` runs `cargo fmt --all --check`, clippy, and `cargo test` before the tag is
minted, so a release refuses to proceed if the local checks fail.

crates.io publishing is currently off (`publish = false`), because of the temporary git-pinned
dependencies (see below), so `cargo release --execute` only versions, commits, tags, and pushes — it
does not touch crates.io. Pushing the `vX.Y.Z` tag triggers CI (`libraries.yml`) to build and attach
the platform shared libraries to the GitHub Release and to build and publish the Python wheel
(`adbc-driver-spanner`) to PyPI via trusted publishing. A `version-gate` job fails the release if the
tag disagrees with the crate version, so crate / tag / wheel cannot drift.

## Dependency pins

`Cargo.toml` temporarily pins two dependency families to git revisions, each of which independently
blocks `cargo publish`:

1. the `google-cloud-*` family (to a `google-cloud-rust` revision), and
2. `adbc_core` / `adbc_ffi` / `adbc_driver_manager` (to an `apache/arrow-adbc` `main` revision).

Do not edit these pins ad hoc. The **Revert checklist** in `CLAUDE.md` ("Temporary git pins") is the
single source of truth: it lists both current revision SHAs and every location (`Cargo.toml`,
`deny.toml`, `README.md`, the docs, and the `publish` flag) that must change in lockstep when a
family is reverted to a crates.io release.
