#![no_main]

use libfuzzer_sys::fuzz_target;

// Fuzz the Spanner value-string parsers (DATE / TIMESTAMP / NUMERIC). They decode wire-format
// strings and must never panic on malformed input (only return None).
fuzz_target!(|s: String| {
    let _ = adbc_spanner::fuzzing::parse_date_days(&s);
    let _ = adbc_spanner::fuzzing::parse_timestamp_micros(&s);
    let _ = adbc_spanner::fuzzing::parse_numeric_i128(&s);
});
