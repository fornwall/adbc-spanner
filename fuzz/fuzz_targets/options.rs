#![no_main]

use adbc_spanner::fuzzing::{ensure_scheme, exercise_database_options, OptValue};
use libfuzzer_sys::fuzz_target;

// Fuzz the driver's option-handling boundary — the surface a C ABI driver manager pushes untrusted
// option keys/values through before any connection is made. Feeds arbitrary (key, value) pairs to
// `set_option` / `get_option_string` and normalizes arbitrary endpoint strings with `ensure_scheme`.
// No network I/O: this exercises the string/bool/int coercions and the unknown-key error path only,
// and must never panic.
fuzz_target!(|input: (Vec<(String, OptValue)>, String)| {
    let (ops, endpoint) = input;
    let _ = ensure_scheme(&endpoint);
    exercise_database_options(ops);
});
