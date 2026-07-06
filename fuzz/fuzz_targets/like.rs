#![no_main]

use libfuzzer_sys::fuzz_target;
use regex::Regex;

// Fuzz the LIKE matcher used by get_objects filtering with an arbitrary (pattern, value) pair.
//
// Two things are checked:
//   1. It must never panic and must terminate quickly even for adversarial `%`-heavy patterns (the
//      matcher is iterative, so no exponential blowup / stack overflow).
//   2. Differential oracle: an independent regex translation of the same pattern must agree on the
//      match result. The ADBC LIKE grammar has no escape character — only `%` (any run) and `_`
//      (exactly one char) are special; every other char (including `\`) is a literal.
fuzz_target!(|input: (String, String)| {
    let (pattern, value) = input;
    let got = adbc_spanner::fuzzing::like_match(&pattern, &value);

    // Build an equivalent anchored regex: `%` -> `.*`, `_` -> `.`, everything else escaped.
    // `(?s)` makes `.` match newlines too, matching LIKE's "any character" semantics, and
    // `\A`/`\z` anchor the whole value (LIKE is a full match).
    let mut re = String::from(r"(?s)\A");
    for ch in pattern.chars() {
        match ch {
            '%' => re.push_str(".*"),
            '_' => re.push('.'),
            other => re.push_str(&regex::escape(&other.to_string())),
        }
    }
    re.push_str(r"\z");

    // The escaping above only ever produces a valid regex, so compilation cannot fail.
    let expected = Regex::new(&re).unwrap().is_match(&value);
    assert_eq!(
        got, expected,
        "like_match disagreed with regex oracle for pattern {pattern:?} value {value:?}"
    );
});
