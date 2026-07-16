#![no_main]

use adbc_spanner::fuzzing::{
    named_parameters, quote_ident, resolve_parameter_names, split_statements,
};
use libfuzzer_sys::fuzz_target;

// Fuzz the bind-side SQL surfaces with an arbitrary (query, bound column names) pair: `@name`
// parameter extraction, the column→parameter pairing, and identifier quoting. Each is checked
// against an independent restatement of its contract, not just "doesn't panic".
fuzz_target!(|input: (String, Vec<String>, bool)| {
    let (sql, column_names, bind_by_name) = input;

    // named_parameters: every extracted name is distinct, is a well-formed parameter identifier,
    // and occurs verbatim (with its `@`) in the SQL — never synthesized or mangled.
    let params = named_parameters(&sql);
    for (i, name) in params.iter().enumerate() {
        assert!(!params[..i].contains(name), "duplicate parameter {name:?}");
        let mut chars = name.chars();
        let first = chars.next().expect("parameter names are non-empty");
        assert!(
            first == '_' || first.is_ascii_alphabetic(),
            "bad first char in parameter {name:?}"
        );
        assert!(
            chars.all(|c| c == '_' || c.is_ascii_alphanumeric()),
            "bad char in parameter {name:?}"
        );
        assert!(
            sql.contains(&format!("@{name}")),
            "@{name} does not occur in {sql:?}"
        );
    }

    // resolve_parameter_names against a batch with these column names must follow the documented
    // contract exactly for the chosen mode (restated here):
    // - by-name (bind_by_name = true): the columns themselves iff every column names a parameter,
    //   else a clean InvalidArguments rejection (asserted inside the wrapper);
    // - positional (bind_by_name = false): the query's parameters iff the counts match, else that
    //   same clean rejection.
    let all_named = column_names.iter().all(|c| params.contains(c));
    match resolve_parameter_names(&sql, &column_names, bind_by_name) {
        Some(resolved) if bind_by_name => {
            assert!(
                all_named,
                "by-name accepted an unmatched column: {column_names:?}"
            );
            assert_eq!(resolved, column_names);
        }
        Some(resolved) => assert_eq!(resolved, params),
        None if bind_by_name => assert!(
            !all_named,
            "by-name rejected a fully-matching pairing: {sql:?} / {column_names:?}"
        ),
        None => assert!(
            params.len() != column_names.len(),
            "positional rejected a count-matching pairing: {sql:?} / {column_names:?}"
        ),
    }

    // quote_ident: backtick-delimited, every backtick/backslash in the body is escaped — so
    // unquoting recovers the input exactly — and, checked against the crate's own GoogleSQL
    // lexer, the quoted identifier embeds into surrounding SQL as one opaque token (the
    // identifier-injection vector the function exists to close).
    for ident in column_names
        .iter()
        .map(String::as_str)
        .chain([sql.as_str()])
    {
        let quoted = quote_ident(ident);
        assert!(
            quoted.len() >= 2 && quoted.starts_with('`') && quoted.ends_with('`'),
            "not backtick-delimited: {quoted:?}"
        );
        let body = &quoted[1..quoted.len() - 1];
        let mut unquoted = String::new();
        let mut chars = body.chars();
        while let Some(c) = chars.next() {
            match c {
                '\\' => unquoted.push(chars.next().expect("dangling escape")),
                '`' => panic!("unescaped backtick in {quoted:?}"),
                other => unquoted.push(other),
            }
        }
        assert_eq!(
            unquoted, ident,
            "unquoting {quoted:?} did not recover input"
        );

        let embedded = format!("SELECT {quoted} FROM t; SELECT 1");
        assert_eq!(
            split_statements(&embedded),
            vec![format!("SELECT {quoted} FROM t"), "SELECT 1".to_string()],
            "quoted identifier {quoted:?} leaked into the surrounding SQL"
        );
    }
});
