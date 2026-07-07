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

    let lock = fs::read_to_string(&lockfile)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", lockfile.display()));
    let version = arrow_array_version(&lock).unwrap_or_else(|| {
        panic!(
            "could not resolve a unique `arrow-array` version from {}",
            lockfile.display()
        )
    });
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
        } else if in_arrow_array {
            if let Some(v) = line
                .strip_prefix("version = \"")
                .and_then(|rest| rest.strip_suffix('"'))
            {
                versions.push(v);
            }
        }
    }
    match versions.as_slice() {
        [v] if !v.is_empty() => Some(v.to_string()),
        _ => None,
    }
}
