#![no_main]

use libfuzzer_sys::fuzz_target;

// Fuzz partition-descriptor decoding with arbitrary bytes — `read_partition`'s input is
// caller-supplied opaque data, so it must never panic. The oracles live in the wrapper (where the
// client's `Partition` type is in scope): a rejected descriptor is a clean `InvalidArguments`
// error, and an accepted one round-trips through the driver's encoder with the versioned envelope
// as the fixed point (decode → encode → decode → encode reproduces the bytes).
fuzz_target!(|descriptor: &[u8]| {
    let _ = adbc_spanner::fuzzing::decode_partition(descriptor);
});
