#![no_main]

use libfuzzer_sys::fuzz_target;

// Fuzz the `spanner:` connection-URI boundary with an arbitrary `uri` option string: scheme
// detection, the `//authority/path?query#fragment` split, database-path validation,
// percent-decoding, and the eager expansion of query parameters into database options. This surface
// is reached by setting the standard `uri` option and is unreachable from the `options` target
// (which only feeds `Other(key)`). No network I/O — it stops well before `connect()` — and must
// never panic.
fuzz_target!(|uri: &str| {
    adbc_spanner::fuzzing::expand_connection_uri(uri);
});
