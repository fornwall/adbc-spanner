#![no_main]

use libfuzzer_sys::fuzz_target;

// Fuzz the SQL statement splitter and DDL detector with arbitrary text. These parse untrusted
// query strings (byte/char handling, quotes, comments), so they must never panic and must uphold
// their invariants.
fuzz_target!(|sql: String| {
    let statements = adbc_spanner::fuzzing::split_statements(&sql);

    // Preservation invariant: every statement is non-empty, trimmed, and appears verbatim in the
    // input at non-decreasing positions. Each is a contiguous slice of the input, so it must be
    // findable in order — this catches any bug that drops, reorders, duplicates, or mangles bytes,
    // not just outright panics.
    let mut cursor = 0;
    for statement in &statements {
        assert!(!statement.is_empty());
        assert_eq!(statement.as_str(), statement.trim());
        match sql[cursor..].find(statement.as_str()) {
            Some(pos) => cursor += pos + statement.len(),
            None => panic!("statement {statement:?} is not an in-order substring of {sql:?}"),
        }
    }

    // DDL detection must not panic on the whole batch or any individual statement.
    let _ = adbc_spanner::fuzzing::is_ddl(&sql);
    for statement in &statements {
        let _ = adbc_spanner::fuzzing::is_ddl(statement);
    }
});
