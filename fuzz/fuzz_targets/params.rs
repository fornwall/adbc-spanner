#![no_main]

use libfuzzer_sys::fuzz_target;
use spanner_adbc::fuzzing::{
    named_parameters, quote_ident, resolve_parameter_names, split_statements,
};

// Fuzz the bind-side SQL surfaces with an arbitrary (query, bound column names) pair: `@name`
// parameter extraction, the column→parameter pairing, and identifier quoting. Each is checked
// against an independent restatement of its contract, not just "doesn't panic".
fuzz_target!(|input: (String, Vec<String>, bool)| {
    let (sql, column_names, bind_by_name) = input;

    // named_parameters: every extracted name is distinct *case-insensitively* (Spanner resolves
    // parameters case-insensitively and rejects two spellings of one name), is non-empty, carries
    // neither delimiter nor escape, and occurs verbatim in the SQL — never synthesized or mangled.
    // The name is not required to sit adjacent to its `@`: `@ p`, `@/*c*/p` and ``@`p` `` are all
    // references to `p`.
    let params = named_parameters(&sql);
    for (i, name) in params.iter().enumerate() {
        assert!(
            !params[..i]
                .iter()
                .any(|p| p.eq_ignore_ascii_case(name.as_str())),
            "duplicate parameter {name:?}"
        );
        assert!(!name.is_empty(), "empty parameter name");
        assert!(
            !name.contains(['`', '\\']),
            "parameter {name:?} carries a delimiter or escape"
        );
        assert!(
            sql.contains(name.as_str()),
            "{name} does not occur in {sql:?}"
        );
    }

    // resolve_parameter_names against a batch with these column names must follow the documented
    // contract exactly for the chosen mode (restated here):
    // - by-name (bind_by_name = true): each column's matching parameter iff every column names one
    //   (case-insensitively) and no two columns name the same parameter, else a clean
    //   InvalidArguments rejection (asserted inside the wrapper);
    // - positional (bind_by_name = false): the query's parameters iff the counts match, else that
    //   same clean rejection.
    let matching = |column: &String| {
        params
            .iter()
            .find(|p| p.eq_ignore_ascii_case(column.as_str()))
    };
    let all_named = column_names.iter().all(|c| matching(c).is_some());
    let all_distinct = !column_names.iter().enumerate().any(|(i, c)| {
        column_names[..i]
            .iter()
            .any(|earlier| earlier.eq_ignore_ascii_case(c.as_str()))
    });
    match resolve_parameter_names(&sql, &column_names, bind_by_name) {
        Some(resolved) if bind_by_name => {
            assert!(
                all_named && all_distinct,
                "by-name accepted an unmatched or duplicated column: {column_names:?}"
            );
            // Each column binds under the query's own spelling of the parameter it names.
            let expected: Vec<String> = column_names
                .iter()
                .map(|c| matching(c).expect("checked above").clone())
                .collect();
            assert_eq!(resolved, expected);
        }
        Some(resolved) => assert_eq!(resolved, params),
        None if bind_by_name => assert!(
            !all_named || !all_distinct,
            "by-name rejected a fully-matching pairing: {sql:?} / {column_names:?}"
        ),
        None => assert!(
            params.len() != column_names.len(),
            "positional rejected a count-matching pairing: {sql:?} / {column_names:?}"
        ),
    }

    // quote_ident: either the identifier is rejected, or the result is backtick-delimited and its
    // body carries no backtick and no backslash at all.
    //
    // That second property is deliberately stated without reference to this crate's lexer. Spanner
    // has two parsers that disagree about escaping — the query parser honours `` \` `` inside
    // backticks, the DDL parser does not and ends the identifier at the first backtick — so an
    // oracle built on the driver's own model of GoogleSQL can only confirm the driver is
    // self-consistent, which is exactly how an escaping bug on the DDL path stayed invisible here.
    // "No delimiter and no escape in the body" is grammar-independent: it holds under both.
    for ident in column_names
        .iter()
        .map(String::as_str)
        .chain([sql.as_str()])
    {
        let Some(quoted) = quote_ident(ident) else {
            continue;
        };
        assert!(
            quoted.len() >= 2 && quoted.starts_with('`') && quoted.ends_with('`'),
            "not backtick-delimited: {quoted:?}"
        );
        let body = &quoted[1..quoted.len() - 1];
        assert!(
            !body.contains(['`', '\\']),
            "accepted identifier {quoted:?} is not one opaque token"
        );
        assert_eq!(body, ident, "quoting {ident:?} altered it");

        let embedded = format!("SELECT {quoted} FROM t; SELECT 1");
        assert_eq!(
            split_statements(&embedded),
            vec![format!("SELECT {quoted} FROM t"), "SELECT 1".to_string()],
            "quoted identifier {quoted:?} leaked into the surrounding SQL"
        );
    }
});
