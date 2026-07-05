#![no_main]

use libfuzzer_sys::fuzz_target;

// Fuzz the LIKE matcher used by get_objects filtering with an arbitrary (pattern, value) pair.
// It must never panic and must terminate quickly even for adversarial `%`-heavy patterns (the
// matcher is iterative, so no exponential blowup / stack overflow).
fuzz_target!(|input: (String, String)| {
    let (pattern, value) = input;
    let _ = adbc_spanner::fuzzing::like_match(&pattern, &value);
});
