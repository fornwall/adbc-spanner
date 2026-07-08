//! Resolves the version of the `arrow-array` crate this build links against, so `get_info` can
//! report the ADBC `DriverArrowVersion` info code. The arrow crates export no version constant,
//! so the version is read out of `Cargo.lock` and embedded via `ADBC_SPANNER_ARROW_VERSION`.

use std::env;
use std::fs;
use std::path::Path;

fn main() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("cargo sets CARGO_MANIFEST_DIR");
    let lockfile = Path::new(&manifest_dir).join("Cargo.lock");
    println!("cargo:rerun-if-changed={}", lockfile.display());

    // `Cargo.lock` is `exclude`d from the published package (see Cargo.toml), so a build from the
    // crates.io/sdist tarball has no lockfile at all. Fall back gracefully instead of panicking:
    // `src/info.rs` reads this value via `env!`, so it must always be set for the crate to compile.
    let version = match fs::read_to_string(&lockfile) {
        // A present-but-surprising lockfile (missing, duplicate or empty `arrow-array` version) is
        // still a hard error — better to fail the build than embed a wrong or empty string.
        Ok(lock) => arrow_array_version(&lock).unwrap_or_else(|| {
            panic!(
                "could not resolve a unique `arrow-array` version from {}",
                lockfile.display()
            )
        }),
        // No lockfile (e.g. a source build from the published package): warn and carry on with a
        // placeholder, so `get_info`'s `DriverArrowVersion` reports "vunknown" rather than the
        // build failing outright.
        Err(e) => {
            println!(
                "cargo:warning=could not read {} ({e}); reporting arrow-array version as \
                 \"unknown\" (expected for a source build from the published package, which \
                 excludes Cargo.lock)",
                lockfile.display()
            );
            "unknown".to_string()
        }
    };
    println!("cargo:rustc-env=ADBC_SPANNER_ARROW_VERSION={version}");
}

/// The `version` of the `arrow-array` package in the lockfile — `None` unless exactly one
/// (non-empty) version is present, so a surprising lockfile fails the build instead of silently
/// embedding a wrong or empty string.
fn arrow_array_version(lock: &str) -> Option<String> {
    let mut versions: Vec<&str> = Vec::new();
    let mut in_arrow_array = false;
    for line in lock.lines().map(str::trim) {
        if line == "[[package]]" {
            in_arrow_array = false;
        } else if line == "name = \"arrow-array\"" {
            in_arrow_array = true;
        } else if in_arrow_array
            && let Some(v) = line
                .strip_prefix("version = \"")
                .and_then(|rest| rest.strip_suffix('"'))
        {
            versions.push(v);
        }
    }
    match versions.as_slice() {
        [v] if !v.is_empty() => Some(v.to_string()),
        _ => None,
    }
}
