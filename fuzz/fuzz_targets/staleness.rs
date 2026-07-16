#![no_main]

use libfuzzer_sys::fuzz_target;

// Fuzz the `spanner.read.staleness` parser with an arbitrary option string: the four prefixed
// forms (`exact:`/`max:`/`min:`/`read:`), a bare RFC 3339 timestamp, the shared duration grammar
// (`parse_duration`, also used by `spanner.commit.max_delay`), and the client `TimestampBound`
// mapping. This is the parser whose missing coverage let COR-1's panic through — it decodes an
// untrusted option value, so it must never panic (only return a clean error).
fuzz_target!(|value: &str| {
    adbc_spanner::fuzzing::parse_read_staleness(value);
    adbc_spanner::fuzzing::parse_duration(value);
});
