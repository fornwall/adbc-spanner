#![no_main]

use libfuzzer_sys::fuzz_target;

// Fuzz the `spanner.directed_read` grammar parser — the driver's most complex hand-written parser
// (`<mode>[:<sel>,...][;auto_failover_disabled]`, each `<sel>` a `<location>[:<type>]`). It parses
// an untrusted option string, so beyond the documented rejections it must never panic.
fuzz_target!(|value: &str| {
    adbc_spanner::fuzzing::parse_directed_read(value);
});
