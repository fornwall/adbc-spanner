#![no_main]

use libfuzzer_sys::fuzz_target;

// Fuzz the SQL statement splitter and DDL detector with arbitrary text. These parse untrusted
// query strings (byte/char handling, quotes, comments), so they must never panic and must uphold
// basic invariants.
fuzz_target!(|sql: String| {
    let statements = adbc_spanner::fuzzing::split_statements(&sql);

    for statement in &statements {
        // Split results are always non-empty and trimmed.
        assert!(!statement.is_empty());
        assert_eq!(statement.as_str(), statement.trim());
    }

    // DDL detection must not panic on the whole batch or any individual statement.
    let _ = adbc_spanner::fuzzing::is_ddl(&sql);
    for statement in &statements {
        let _ = adbc_spanner::fuzzing::is_ddl(statement);
    }
});
