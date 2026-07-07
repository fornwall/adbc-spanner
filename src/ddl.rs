//! Detecting and splitting DDL statements.
//!
//! Spanner does not accept DDL (`CREATE`/`ALTER`/`DROP`/…) over the data-plane SQL API — DDL is
//! applied through the Database Admin `UpdateDatabaseDdl` long-running operation, which takes a
//! *list* of statements and applies them as one schema change. The driver therefore detects DDL and
//! routes it there (see [`SpannerStatement`](crate::SpannerStatement)).
//!
//! Because `UpdateDatabaseDdl` accepts multiple statements at once, a single `execute` of a
//! `;`-separated batch is submitted as one call. That is how dbt's "build an intermediate table,
//! then rename it over the target" swap can be made near-atomic:
//!
//! ```sql
//! CREATE TABLE my_model__tmp (id INT64, ...) PRIMARY KEY (id);
//! DROP TABLE my_model;
//! RENAME TABLE my_model__tmp TO my_model
//! ```

/// Leading keywords that identify a Spanner DDL statement.
const DDL_KEYWORDS: &[&str] = &[
    "CREATE", "DROP", "ALTER", "RENAME", "GRANT", "REVOKE", "ANALYZE",
];

/// Leading keywords that identify a Spanner DML statement (data modification).
const DML_KEYWORDS: &[&str] = &["INSERT", "UPDATE", "DELETE"];

/// Return `true` if `sql` begins with a DDL statement (ignoring leading whitespace and comments).
pub(crate) fn is_ddl(sql: &str) -> bool {
    first_keyword(sql).is_some_and(|kw| DDL_KEYWORDS.contains(&kw.as_str()))
}

/// Return `true` if `sql` begins with a DML statement (`INSERT`/`UPDATE`/`DELETE`).
///
/// Used to route DML that arrives through the query entry point (`execute`) — as every ADBC client
/// does, since the C ABI exposes only `ExecuteQuery` — onto the read/write transaction path instead
/// of a read-only single-use one, which Spanner rejects for DML.
pub(crate) fn is_dml(sql: &str) -> bool {
    first_keyword(sql).is_some_and(|kw| DML_KEYWORDS.contains(&kw.as_str()))
}

/// Return `true` if `sql` contains a top-level `THEN RETURN` clause — DML that returns rows
/// (`INSERT`/`UPDATE`/`DELETE ... THEN RETURN <columns>`).
///
/// Scans word tokens outside string/identifier literals and comments (the same lexical rules as
/// [`split_statements`]) for the keyword `THEN` immediately followed by `RETURN`,
/// case-insensitively — but only at `CASE` expression nesting depth zero. GoogleSQL's `THEN
/// RETURN` clause appears only at the top level, at the end of a DML statement; a `THEN` inside a
/// `CASE WHEN … THEN … END` expression belongs to the CASE, so a branch expression that is a
/// column literally named `return` (`RETURN` is not a reserved keyword) must not match. `CASE` and
/// `END` are tracked as a nesting depth (`END` only closes `CASE` in GoogleSQL expressions);
/// quoted identifiers and literals never affect the depth, and an unbalanced `END` (invalid SQL)
/// saturates at zero rather than underflowing.
pub(crate) fn is_dml_returning(sql: &str) -> bool {
    let mut previous_was_then = false;
    let mut case_depth = 0usize;
    let mut word = String::new();
    let mut chars = sql.chars().peekable();
    let check_word =
        |word: &mut String, previous_was_then: &mut bool, case_depth: &mut usize| -> bool {
            if word.is_empty() {
                return false;
            }
            let matched =
                *previous_was_then && *case_depth == 0 && word.eq_ignore_ascii_case("RETURN");
            *previous_was_then = word.eq_ignore_ascii_case("THEN");
            if word.eq_ignore_ascii_case("CASE") {
                *case_depth += 1;
            } else if word.eq_ignore_ascii_case("END") {
                *case_depth = case_depth.saturating_sub(1);
            }
            word.clear();
            matched
        };
    while let Some(c) = chars.next() {
        match c {
            '\'' | '"' | '`' => {
                // The raw-prefix check must read `word` before `check_word` clears it.
                let raw = c != '`' && is_raw_prefix(&word);
                if check_word(&mut word, &mut previous_was_then, &mut case_depth) {
                    return true;
                }
                // A quoted literal/identifier resets the keyword window.
                previous_was_then = false;
                consume_quoted(&mut chars, c, raw, |_| {});
            }
            '-' if chars.peek() == Some(&'-') => {
                if check_word(&mut word, &mut previous_was_then, &mut case_depth) {
                    return true;
                }
                for ch in chars.by_ref() {
                    if ch == '\n' {
                        break;
                    }
                }
            }
            '#' => {
                if check_word(&mut word, &mut previous_was_then, &mut case_depth) {
                    return true;
                }
                for ch in chars.by_ref() {
                    if ch == '\n' {
                        break;
                    }
                }
            }
            '/' if chars.peek() == Some(&'*') => {
                if check_word(&mut word, &mut previous_was_then, &mut case_depth) {
                    return true;
                }
                chars.next(); // '*'
                let mut prev = '\0';
                for ch in chars.by_ref() {
                    if prev == '*' && ch == '/' {
                        break;
                    }
                    prev = ch;
                }
            }
            _ if c == '_' || c.is_ascii_alphanumeric() => word.push(c),
            _ => {
                if check_word(&mut word, &mut previous_was_then, &mut case_depth) {
                    return true;
                }
            }
        }
    }
    check_word(&mut word, &mut previous_was_then, &mut case_depth)
}

/// Whether `word` — the identifier run immediately (adjacently) before a quote — is a GoogleSQL
/// literal prefix marking a **raw** literal (`r`, `rb` or `br`, any case), in which backslash is
/// an ordinary character rather than an escape. A plain bytes prefix (`b`) keeps backslash
/// escapes, so it needs no special handling.
pub(crate) fn is_raw_prefix(word: &str) -> bool {
    matches!(word.to_ascii_lowercase().as_str(), "r" | "rb" | "br")
}

/// Consume a string/bytes literal or quoted identifier whose opening `quote` has just been read,
/// feeding every consumed character (excluding the already-read opening quote) to `sink`.
///
/// Handles the GoogleSQL lexical structure:
/// - **triple-quoted** strings (`'''…'''` / `"""…"""`), which may contain unescaped quotes and
///   newlines and close only on three consecutive quote characters;
/// - **raw** literals (`raw` = true, from an `r`/`rb`/`br` prefix — see [`is_raw_prefix`]), in
///   which `\` does not escape anything;
/// - backslash escapes everywhere else, including quoted identifiers (`` ` ``, never triple/raw).
pub(crate) fn consume_quoted(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    quote: char,
    raw: bool,
    mut sink: impl FnMut(char),
) {
    let triple = quote != '`' && chars.peek() == Some(&quote) && {
        // Consume the second quote. If a third follows this is a triple-quoted string;
        // otherwise the literal was empty (`''` / `""`) and is already closed.
        sink(chars.next().unwrap());
        if chars.peek() == Some(&quote) {
            sink(chars.next().unwrap());
            true
        } else {
            return;
        }
    };
    let mut closing_run = 0usize;
    while let Some(ch) = chars.next() {
        sink(ch);
        if !raw && ch == '\\' {
            if let Some(escaped) = chars.next() {
                sink(escaped);
            }
            closing_run = 0;
        } else if ch == quote {
            if !triple {
                return;
            }
            closing_run += 1;
            if closing_run == 3 {
                return;
            }
        } else {
            closing_run = 0;
        }
    }
}

/// Split a (possibly multi-statement) SQL string into individual, trimmed, non-empty statements.
///
/// Splits on top-level `;`, ignoring semicolons inside string/bytes literals and quoted
/// identifiers — including triple-quoted (`'''…'''`/`"""…"""`) and raw (`r'…'`, `rb'…'`, …) forms,
/// see [`consume_quoted`] — and comments (`-- …`, `# …`, `/* … */`). This is shared by DDL
/// batching (`UpdateDatabaseDdl`) and multi-statement DML batching (`ExecuteBatchDml`).
pub(crate) fn split_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    // The trailing identifier run, tracked to recognise raw-literal prefixes (`r'…'`).
    let mut word = String::new();
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' | '"' | '`' => {
                let raw = c != '`' && is_raw_prefix(&word);
                word.clear();
                // A quoted literal/identifier: copy through the matching close quote.
                current.push(c);
                consume_quoted(&mut chars, c, raw, |ch| current.push(ch));
            }
            '-' if chars.peek() == Some(&'-') => {
                word.clear();
                copy_line_comment(&mut current, c, &mut chars)
            }
            '#' => {
                word.clear();
                copy_line_comment(&mut current, c, &mut chars)
            }
            '/' if chars.peek() == Some(&'*') => {
                word.clear();
                current.push(c);
                current.push(chars.next().unwrap()); // '*'
                let mut prev = '\0';
                for ch in chars.by_ref() {
                    current.push(ch);
                    if prev == '*' && ch == '/' {
                        break;
                    }
                    prev = ch;
                }
            }
            ';' => {
                word.clear();
                push_statement(&mut statements, &mut current)
            }
            _ => {
                if c == '_' || c.is_ascii_alphanumeric() {
                    word.push(c);
                } else {
                    word.clear();
                }
                current.push(c);
            }
        }
    }
    push_statement(&mut statements, &mut current);
    statements
}

/// Strip trailing statement terminators — one or more top-level `;`, plus surrounding whitespace —
/// from a **single** query, so `SELECT current_date;;;` becomes `SELECT current_date`.
///
/// Spanner's single-use query API rejects a trailing `;` ("Expected end of input but got `;`"), yet
/// many clients and conformance suites append one. This reuses the GoogleSQL-aware
/// [`split_statements`] scanner, so a `;` inside a string literal, quoted identifier or comment is
/// preserved (`SELECT ';'` is unchanged) rather than being mistaken for a terminator.
///
/// Only a single statement is stripped: if the SQL parses to more than one top-level statement it is
/// returned unchanged, so a genuine multi-statement query keeps reaching Spanner (which rejects it)
/// rather than being silently reduced to its first statement.
pub(crate) fn strip_trailing_terminators(sql: &str) -> String {
    let mut statements = split_statements(sql);
    if statements.len() == 1 {
        statements.pop().unwrap()
    } else {
        sql.to_string()
    }
}

fn copy_line_comment(
    out: &mut String,
    first: char,
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
) {
    out.push(first);
    for ch in chars.by_ref() {
        out.push(ch);
        if ch == '\n' {
            break;
        }
    }
}

fn push_statement(statements: &mut Vec<String>, current: &mut String) {
    let trimmed = current.trim();
    // Drop segments that carry no statement — pure whitespace, or only comments (e.g. a trailing
    // `-- cleanup` after the last `;`). Emitting those as statements makes Spanner reject the whole
    // batch with `INVALID_ARGUMENT` (silently buffered until commit in manual mode), and would make
    // `strip_trailing_terminators` see two statements for `SELECT 1; -- done`. A segment with real
    // SQL followed by a comment (`SELECT 1 -- done`) still has a leading keyword, so it is kept.
    if !skip_leading_whitespace_and_comments(trimmed).is_empty() {
        statements.push(trimmed.to_string());
    }
    current.clear();
}

/// Return `sql` with any leading whitespace and `--`/`#`/`/* … */` comments removed, repeatedly,
/// until the first character is neither whitespace nor the start of a comment (or the input is
/// exhausted). Shared by [`first_keyword`] and [`push_statement`]; an unterminated comment consumes
/// the rest of the input, mirroring the lexer in [`split_statements`].
fn skip_leading_whitespace_and_comments(sql: &str) -> &str {
    let mut rest = sql.trim_start();
    loop {
        if let Some(after) = rest.strip_prefix("--").or_else(|| rest.strip_prefix('#')) {
            rest = after
                .find('\n')
                .map_or("", |i| &after[i + 1..])
                .trim_start();
        } else if let Some(after) = rest.strip_prefix("/*") {
            rest = after
                .find("*/")
                .map_or("", |i| &after[i + 2..])
                .trim_start();
        } else {
            break;
        }
    }
    rest
}

/// The first SQL keyword, uppercased, skipping leading whitespace, `--`/`#`/`/* */` comments, and
/// [statement hints](https://cloud.google.com/spanner/docs/reference/standard-sql/query-syntax#statement_hints)
/// (`@{HINT=value, …}`), which GoogleSQL allows before the statement proper — so hinted DML/DDL is
/// classified by its real leading keyword, not misread as "no keyword" and routed to the wrong
/// execution path.
fn first_keyword(sql: &str) -> Option<String> {
    let mut rest = skip_leading_whitespace_and_comments(sql);
    while let Some(after) = rest.strip_prefix("@{") {
        rest = skip_leading_whitespace_and_comments(skip_hint_body(after));
    }
    let word: String = rest.chars().take_while(char::is_ascii_alphabetic).collect();
    (!word.is_empty()).then(|| word.to_ascii_uppercase())
}

/// Skip the body of a statement hint whose opening `@{` has already been consumed, returning the
/// remainder after the matching `}`. A `}` inside a string literal within the hint (hint values
/// may be literals) does not close it — literals are consumed via [`consume_quoted`]. An
/// unterminated hint consumes the rest of the input, mirroring how unterminated comments are
/// handled above.
fn skip_hint_body(after: &str) -> &str {
    let mut chars = after.chars().peekable();
    let mut consumed = 0usize;
    while let Some(c) = chars.next() {
        consumed += c.len_utf8();
        match c {
            '}' => return &after[consumed..],
            '\'' | '"' | '`' => {
                consume_quoted(&mut chars, c, false, |ch| consumed += ch.len_utf8());
            }
            _ => {}
        }
    }
    ""
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_ddl() {
        for sql in [
            "CREATE TABLE t (id INT64) PRIMARY KEY (id)",
            "  drop table t",
            "ALTER TABLE t ADD COLUMN c STRING(MAX)",
            "RENAME TABLE a TO b",
            "-- a comment\nCREATE INDEX idx ON t(c)",
            "/* header */ create table t (id int64) primary key (id)",
        ] {
            assert!(is_ddl(sql), "should be DDL: {sql}");
        }
    }

    #[test]
    fn detects_non_ddl() {
        for sql in [
            "SELECT 1",
            "INSERT INTO t (id) VALUES (1)",
            "UPDATE t SET c = 1 WHERE id = 1",
            "DELETE FROM t WHERE true",
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "",
        ] {
            assert!(!is_ddl(sql), "should not be DDL: {sql}");
        }
    }

    #[test]
    fn detects_dml() {
        for sql in [
            "INSERT INTO t (id) VALUES (1)",
            "  update t SET c = 1 WHERE id = 1",
            "DELETE FROM t WHERE true",
            "/* c */ insert into t (id) values (1)",
        ] {
            assert!(is_dml(sql), "should be DML: {sql}");
        }
        for sql in [
            "SELECT 1",
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "CREATE TABLE t (id INT64) PRIMARY KEY (id)",
            "",
        ] {
            assert!(!is_dml(sql), "should not be DML: {sql}");
        }
    }

    #[test]
    fn classifies_statements_behind_statement_hints() {
        // A leading `@{…}` statement hint does not hide the real keyword: hinted DML must route to
        // the read/write path (a read-only transaction rejects it), hinted DDL to the admin API.
        for sql in [
            "@{USE_ADDITIONAL_PARALLELISM=TRUE} UPDATE t SET c = 1 WHERE true",
            "@{PDML_MAX_PARALLELISM=8} delete from t where true",
            "/* c */ @{A=1} INSERT INTO t (id) VALUES (1)",
            "@{A=1} -- c\n UPDATE t SET c = 1 WHERE true",
            // A `}` inside a string-literal hint value does not close the hint early.
            "@{A='}'} DELETE FROM t WHERE true",
        ] {
            assert!(is_dml(sql), "should classify as DML: {sql}");
            assert!(!is_ddl(sql), "should not classify as DDL: {sql}");
        }
        assert!(is_ddl("@{A=1} CREATE TABLE t (id INT64) PRIMARY KEY (id)"));
        // A hinted query is neither.
        let hinted_query = "@{JOIN_METHOD=HASH_JOIN} SELECT * FROM t";
        assert!(!is_dml(hinted_query) && !is_ddl(hinted_query));
        // An unterminated hint swallows the rest — no keyword, like an unterminated comment.
        assert!(!is_dml("@{oops UPDATE t SET c = 1"));
    }

    #[test]
    fn detects_then_return() {
        for sql in [
            "INSERT INTO t (id) VALUES (1) THEN RETURN id",
            "insert into t (id) values (1) then return *",
            "UPDATE t SET c = 1 WHERE id = 1 THEN\n  RETURN c",
            "DELETE FROM t WHERE id = 1 then /* keep */ return id",
            "DELETE FROM t WHERE id = 1 THEN -- comment\n RETURN id",
        ] {
            assert!(is_dml_returning(sql), "should detect THEN RETURN: {sql}");
        }
        // Raw strings end at their closing quote (`\` is not an escape), so a clause after one is
        // still found.
        assert!(is_dml_returning(
            r"UPDATE t SET s = r'x\' WHERE true THEN RETURN Id"
        ));
        for sql in [
            "INSERT INTO t (id) VALUES (1)",
            "SELECT CASE WHEN a THEN b ELSE c END FROM t",
            // `THEN RETURN` inside a literal or a quoted identifier is not a clause — including
            // the triple-quoted and raw literal forms.
            "INSERT INTO t (s) VALUES ('THEN RETURN')",
            "INSERT INTO t (s) VALUES ('''THEN RETURN''')",
            r"INSERT INTO t (s) VALUES (r'THEN RETURN\')",
            "UPDATE t SET `then return` = 1 WHERE true",
            // `THEN RETURN` inside a comment is not a clause.
            "UPDATE t SET a = 1 /* THEN RETURN */ WHERE true",
            "UPDATE t SET a = 1 WHERE true -- THEN RETURN",
            "UPDATE t SET a = 1 WHERE true # THEN RETURN",
            // Adjacent words, not the two keywords.
            "UPDATE t SET a = thenreturn WHERE true",
            "UPDATE t SET a = x_then WHERE return_value = 1",
            // A literal between the words breaks the clause.
            "UPDATE t SET a = CASE WHEN b THEN 'x' ELSE returns END WHERE true",
            "",
        ] {
            assert!(!is_dml_returning(sql), "should not detect: {sql}");
        }
    }

    #[test]
    fn then_return_ignores_case_expression_branches() {
        // `RETURN` is not a reserved GoogleSQL keyword, so a CASE branch expression can be a
        // column literally named `return`. The `THEN` there belongs to the CASE, not a top-level
        // `THEN RETURN` clause — misdetecting it hard-errors valid DML in manual transaction mode.
        for sql in [
            "UPDATE t SET x = CASE WHEN c THEN return ELSE 0 END WHERE true",
            "update t set x = case when c then RETURN else 0 end where true",
            // Searched and multi-branch forms.
            "UPDATE t SET x = CASE y WHEN 1 THEN return END WHERE true",
            "UPDATE t SET x = CASE WHEN a THEN return WHEN b THEN return ELSE return END \
             WHERE true",
            // Nested CASE: the inner branch is still inside the outer CASE.
            "UPDATE t SET x = CASE WHEN a THEN CASE WHEN b THEN return END END WHERE true",
            // A comment between the CASE's THEN and the branch expression changes nothing.
            "UPDATE t SET x = CASE WHEN c THEN /* pick */ return ELSE 0 END WHERE true",
        ] {
            assert!(!is_dml_returning(sql), "CASE branch, not a clause: {sql}");
        }
        // A genuine top-level `THEN RETURN` *after* a CASE expression in the same statement must
        // still be detected — the depth is back to zero once the CASE closes.
        for sql in [
            "UPDATE t SET x = CASE WHEN c THEN 1 ELSE 0 END WHERE true THEN RETURN x",
            "UPDATE t SET x = CASE WHEN c THEN return ELSE 0 END WHERE true THEN RETURN x",
            "UPDATE t SET x = CASE WHEN a THEN CASE WHEN b THEN 2 END ELSE 0 END WHERE true \
             then return *",
            // `CASE` inside a literal or quoted identifier must not open a depth level (which
            // would suppress the real clause); `END`/`case` likewise must not close/open one.
            "UPDATE t SET s = 'CASE' WHERE true THEN RETURN s",
            "UPDATE t SET `case` = 1 WHERE true THEN RETURN `case`",
        ] {
            assert!(is_dml_returning(sql), "top-level clause after CASE: {sql}");
        }
        // An unbalanced `END` (invalid SQL) saturates at depth zero instead of underflowing, so a
        // following top-level clause is still seen.
        assert!(is_dml_returning(
            "UPDATE t SET a = b END WHERE true THEN RETURN a"
        ));
    }

    #[test]
    fn splits_statements() {
        let batch = "CREATE TABLE tmp (id INT64) PRIMARY KEY (id);\n\
                     DROP TABLE target;\n\
                     RENAME TABLE tmp TO target;";
        assert_eq!(
            split_statements(batch),
            vec![
                "CREATE TABLE tmp (id INT64) PRIMARY KEY (id)",
                "DROP TABLE target",
                "RENAME TABLE tmp TO target",
            ]
        );
        assert_eq!(split_statements("   ;  ; "), Vec::<String>::new());
    }

    #[test]
    fn split_drops_comment_only_segments() {
        // A trailing comment-only segment must not be emitted as a statement — otherwise Spanner
        // rejects the whole batch with INVALID_ARGUMENT (silently buffered until commit in manual
        // mode). Both DML and DDL batches go through here.
        assert_eq!(
            split_statements("DELETE FROM t1; DELETE FROM t2; -- cleanup"),
            vec!["DELETE FROM t1", "DELETE FROM t2"]
        );
        // A whitespace-only segment is dropped and a trailing block-comment-only segment is dropped;
        // a leading comment on a real statement is kept (Spanner accepts it, and `first_keyword`
        // skips it for classification), so those segments survive verbatim.
        assert_eq!(
            split_statements("SELECT 1;\n  \n-- a\nSELECT 2; # b\nSELECT 3; /* c */"),
            vec!["SELECT 1", "-- a\nSELECT 2", "# b\nSELECT 3"]
        );
        // A statement followed by an inline comment keeps its leading keyword, so it survives.
        assert_eq!(
            split_statements("SELECT 1 -- done\n; SELECT 2 /* tail */"),
            vec!["SELECT 1 -- done", "SELECT 2 /* tail */"]
        );
        // Purely comments/whitespace splits to nothing at all.
        assert_eq!(
            split_statements("-- just a comment\n/* and a block */  ; # trailing"),
            Vec::<String>::new()
        );
        // The single-query terminator strip must treat "SELECT 1; -- done" as one statement, so the
        // trailing `;` is removed just as it is for "SELECT 1;".
        assert_eq!(strip_trailing_terminators("SELECT 1; -- done"), "SELECT 1");
    }

    #[test]
    fn split_drops_interleaved_comment_only_segment_in_ddl_batch() {
        // The dbt "swap" DDL batch is submitted as one `UpdateDatabaseDdl` call, so a stray
        // comment-only segment *between* two real DDL statements (not just trailing) must also be
        // dropped — otherwise the empty `/* swap */` segment is sent as a DDL statement and Spanner
        // rejects the whole schema change with INVALID_ARGUMENT. Here the block-comment-only segment
        // sits between DROP and RENAME, and a trailing `# done` segment closes the batch; both
        // vanish while the three real statements survive verbatim.
        let batch = "CREATE TABLE tmp (id INT64) PRIMARY KEY (id);\n\
                     DROP TABLE target;\n\
                     /* swap in the rebuilt table */;\n\
                     RENAME TABLE tmp TO target;\n\
                     # done";
        assert_eq!(
            split_statements(batch),
            vec![
                "CREATE TABLE tmp (id INT64) PRIMARY KEY (id)",
                "DROP TABLE target",
                "RENAME TABLE tmp TO target",
            ]
        );
    }

    #[test]
    fn split_respects_raw_strings() {
        // In a raw string the backslash is an ordinary character, not an escape: `r'C:\'` ends at
        // the quote, so the `;` after it is a real separator. (The old lexer consumed the closing
        // quote as escaped and shipped the whole thing as one malformed statement.)
        assert_eq!(
            split_statements(r"UPDATE t SET path = r'C:\'; DELETE FROM u WHERE stale"),
            vec![r"UPDATE t SET path = r'C:\'", "DELETE FROM u WHERE stale"]
        );
        // All prefix spellings: rb / br / uppercase.
        assert_eq!(
            split_statements(r"INSERT INTO t (b) VALUES (rb'\'); SELECT 1"),
            vec![r"INSERT INTO t (b) VALUES (rb'\')", "SELECT 1"]
        );
        assert_eq!(
            split_statements(r#"SELECT BR"\"; SELECT R'\'"#),
            vec![r#"SELECT BR"\""#, r"SELECT R'\'"]
        );
        // A plain bytes prefix (no r) keeps backslash escapes: `b'\';x'` is one literal.
        assert_eq!(
            split_statements(r"SELECT b'\';x'; SELECT 2"),
            vec![r"SELECT b'\';x'", "SELECT 2"]
        );
        // A prefix only counts when adjacent: `r 'x'` and `xr'y'` are not raw strings — but both
        // are still ordinary (escaped) literals, so the split is unchanged either way here.
        assert_eq!(
            split_statements("SELECT r ';'; SELECT xr';'"),
            vec!["SELECT r ';'", "SELECT xr';'"]
        );
    }

    #[test]
    fn split_respects_triple_quoted_strings() {
        // A triple-quoted string may contain unescaped quotes and semicolons.
        assert_eq!(
            split_statements("INSERT INTO t (s) VALUES ('''don't; stop'''); DELETE FROM t"),
            vec![
                "INSERT INTO t (s) VALUES ('''don't; stop''')",
                "DELETE FROM t",
            ]
        );
        assert_eq!(
            split_statements(r#"SELECT """a;b"""; SELECT 2"#),
            vec![r#"SELECT """a;b""""#, "SELECT 2"]
        );
        // Runs of quotes inside the literal don't close it early unless three long.
        assert_eq!(
            split_statements("SELECT '''a''b'''; SELECT 2"),
            vec!["SELECT '''a''b'''", "SELECT 2"]
        );
        // Raw + triple combined: the backslash does not escape the closing quotes.
        assert_eq!(
            split_statements(r"SELECT r'''a\'''; SELECT 2"),
            vec![r"SELECT r'''a\'''", "SELECT 2"]
        );
        // An empty literal ('' / "") is not the start of a triple-quoted string.
        assert_eq!(
            split_statements(r#"SELECT ''; SELECT """#),
            vec!["SELECT ''", r#"SELECT """#]
        );
    }

    #[test]
    fn split_respects_literals_and_comments() {
        // A semicolon inside a string literal is not a separator.
        assert_eq!(
            split_statements("INSERT INTO t (s) VALUES ('a;b'); DELETE FROM t WHERE true"),
            vec![
                "INSERT INTO t (s) VALUES ('a;b')",
                "DELETE FROM t WHERE true",
            ]
        );
        // Backslash-escaped quote inside a string.
        assert_eq!(
            split_statements(r"UPDATE t SET s = 'x\';y' WHERE id = 1"),
            vec![r"UPDATE t SET s = 'x\';y' WHERE id = 1"]
        );
        // Semicolon inside a line comment.
        assert_eq!(
            split_statements("SELECT 1 -- a; b\n; SELECT 2"),
            vec!["SELECT 1 -- a; b", "SELECT 2"]
        );
    }

    #[test]
    fn strips_trailing_query_terminators() {
        // A single trailing `;`, a run of them, and trailing whitespace around them are all removed.
        assert_eq!(strip_trailing_terminators("SELECT 1;"), "SELECT 1");
        assert_eq!(strip_trailing_terminators("SELECT 1;;;"), "SELECT 1");
        assert_eq!(strip_trailing_terminators("SELECT 1 ;  "), "SELECT 1");
        assert_eq!(strip_trailing_terminators("  SELECT 1  "), "SELECT 1");
        // The conformance case verbatim.
        assert_eq!(
            strip_trailing_terminators("SELECT current_date;;;"),
            "SELECT current_date"
        );
        // A bare query is unchanged.
        assert_eq!(strip_trailing_terminators("SELECT 1"), "SELECT 1");
        // A `;` inside a string literal is not a terminator and is preserved.
        assert_eq!(strip_trailing_terminators("SELECT ';'"), "SELECT ';'");
        assert_eq!(strip_trailing_terminators("SELECT ';';"), "SELECT ';'");
        // A genuine multi-statement query is returned unchanged (left for Spanner to reject) rather
        // than reduced to its first statement.
        assert_eq!(
            strip_trailing_terminators("SELECT 1; SELECT 2"),
            "SELECT 1; SELECT 2"
        );
    }
}
