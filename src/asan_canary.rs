//! Test-only AddressSanitizer **tripwire**, compiled ONLY under `--cfg asan_canary`.
//!
//! This module does not exist in any normal build — not `cargo build`, not
//! `cargo build --release`, not `cargo clippy --all-targets --all-features` (a bare `--cfg` is
//! deliberately NOT set by `--all-features`, which is exactly why this is a `--cfg` gate and not a
//! Cargo feature), and not `cargo test`. It is switched on solely by the ADBC validation suite's
//! `rust-asan` leg (`scripts/run-adbc-validation.sh` appends `--cfg asan_canary` to the sanitizer
//! `RUSTFLAGS`), so the intentionally-out-of-bounds symbol below can NEVER leak into a shipped
//! cdylib.
//!
//! ## Why it exists
//!
//! A *passing* `rust-asan` leg on its own is no proof the Rust instrumentation is actually armed:
//! if the `-Zsanitizer=address` / `-Zbuild-std` wiring silently regressed (a flag typo, a toolchain
//! skew, an ABI mismatch that disarms rather than aborts), the suite would still go green and give
//! false confidence. This canary is the positive control: the validation script calls it from a
//! clang `-fsanitize=address` C++ program against C++-allocated heap memory and asserts ASan fires.
//! If ASan does NOT report the overflow, the leg fails loudly — a disarmed leg goes red instead of
//! silently passing.
//!
//! ## What it proves (the cross-boundary shape)
//!
//! [`adbc_spanner_asan_canary`] takes a pointer + length to a buffer the **C++ side allocated**
//! (with `new`/`malloc`, so the ASan redzone/poison lives on the C++ allocation) and writes one
//! byte past the end from *inside the instrumented Rust cdylib*. Because both sides share one
//! compiler-rt ASan runtime and shadow memory, the instrumented Rust store is checked against the
//! poison the C++ allocator placed, and ASan reports a `heap-buffer-overflow` attributed to the
//! Rust frame. That is precisely the cross-boundary composition — instrumented Rust writing out of
//! bounds on C++-allocated memory — a pure-Rust tripwire could not demonstrate.

/// Test-only ASan tripwire — **intentionally writes one byte out of bounds** and must NEVER be
/// enabled in a shipped build.
///
/// Compiled only under `--cfg asan_canary` (the validation suite's `rust-asan` leg). Given a
/// `ptr`/`len` describing a C++-allocated buffer, it writes to `ptr[len]` — one byte past the end —
/// so that AddressSanitizer, sharing its runtime with the calling clang `-fsanitize=address` C++
/// program, reports a `heap-buffer-overflow` attributed to this Rust frame. Called by
/// `adbc-validation/asan_canary.cc`; see this module's docs for the rationale.
///
/// # Safety
///
/// `ptr` must point to an allocation of at least `len` bytes. The function then deliberately
/// performs an out-of-bounds write at offset `len`, which is undefined behaviour by construction —
/// that is the whole point of the tripwire, and why it is quarantined behind `--cfg asan_canary`.
#[unsafe(no_mangle)]
#[allow(unsafe_code)]
// Deliberate out-of-bounds write; the crate-wide deny is overridden here only.
// Exported as a C symbol via `#[no_mangle]`, so `pub` is required even though the module is private
// and the lint cannot see the FFI export.
#[allow(unreachable_pub)]
pub extern "C" fn adbc_spanner_asan_canary(ptr: *mut u8, len: usize) {
    // Compute the one-past-the-end address and write to it. `write_volatile` keeps the compiler
    // from eliding the store (nothing reads it back), so ASan is guaranteed to see the access.
    // SAFETY: intentionally out of bounds — see the function's Safety section.
    unsafe {
        let out_of_bounds = ptr.add(len);
        out_of_bounds.write_volatile(0xAA);
    }
}
