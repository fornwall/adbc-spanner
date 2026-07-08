#![no_main]

use adbc_spanner::fuzzing::{
    first_keyword, is_ddl, is_dml, split_statements, strip_trailing_terminators,
};
use libfuzzer_sys::fuzz_target;

// Fuzz the statement classifier (`first_keyword` — whose statement-hint skipping had a real bug)
// and the trailing-terminator strip. Both lex arbitrary untrusted SQL, so beyond "never panics"
// every result is checked against independent restatements of the documented contract.
fuzz_target!(|sql: String| {
    let keyword = first_keyword(&sql);

    if let Some(kw) = &keyword {
        // Shape: a non-empty, all-uppercase ASCII-alphabetic word...
        assert!(!kw.is_empty(), "empty keyword for {sql:?}");
        assert!(
            kw.chars().all(|c| c.is_ascii_uppercase()),
            "keyword {kw:?} is not uppercase ASCII"
        );
        // ...lifted verbatim (case-insensitively) from the input: the keyword is a slice of the
        // input after hint/comment skipping, never synthesized.
        assert!(
            sql.to_ascii_uppercase().contains(kw.as_str()),
            "keyword {kw:?} does not occur in {sql:?}"
        );
    }

    // Independent classification oracle: `is_ddl` / `is_dml` are exactly "the first keyword is in
    // the DDL/DML list". The lists here are a deliberate second copy of the driver's.
    const DDL: &[&str] = &[
        "CREATE", "DROP", "ALTER", "RENAME", "GRANT", "REVOKE", "ANALYZE",
    ];
    const DML: &[&str] = &["INSERT", "UPDATE", "DELETE"];
    let kw = keyword.as_deref();
    assert_eq!(is_ddl(&sql), kw.is_some_and(|k| DDL.contains(&k)));
    assert_eq!(is_dml(&sql), kw.is_some_and(|k| DML.contains(&k)));

    // Metamorphic oracle: leading whitespace, each comment form, and a well-formed statement hint
    // are transparent to classification — the exact surface where the real bug lived (hinted DML
    // misread as "no keyword" and routed to the wrong execution path).
    for prefix in [
        " \t\n",
        "-- line comment\n",
        "# hash comment\n",
        "/* block comment */",
        "@{USE_ADDITIONAL_PARALLELISM=TRUE} ",
        "@{HINT='}'}", // a `}` inside a hint's string literal must not close the hint
    ] {
        assert_eq!(
            first_keyword(&format!("{prefix}{sql}")),
            keyword,
            "prefix {prefix:?} changed the keyword of {sql:?}"
        );
    }

    // strip_trailing_terminators: the result is a verbatim substring of the input (never
    // synthesized or reordered), stripping is idempotent, and it never changes what the
    // GoogleSQL-aware splitter sees — so `SELECT ';'` keeps its literal and a genuine
    // multi-statement batch is returned unchanged.
    let stripped = strip_trailing_terminators(&sql);
    assert!(
        sql.contains(&stripped),
        "{stripped:?} is not a substring of {sql:?}"
    );
    assert_eq!(
        strip_trailing_terminators(&stripped),
        stripped,
        "stripping is not idempotent for {sql:?}"
    );
    assert_eq!(
        split_statements(&stripped),
        split_statements(&sql),
        "stripping changed the statements of {sql:?}"
    );
});
