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

/// Return `true` if `sql` begins with a DDL statement (ignoring leading whitespace and comments).
pub(crate) fn is_ddl(sql: &str) -> bool {
    first_keyword(sql).is_some_and(|kw| DDL_KEYWORDS.contains(&kw.as_str()))
}

/// Split a (possibly multi-statement) SQL string into individual, trimmed, non-empty statements.
///
/// Splits on top-level `;`, ignoring semicolons inside string/identifier literals (`'…'`, `"…"`,
/// `` `…` `` with backslash escapes) and comments (`-- …`, `# …`, `/* … */`). This is shared by DDL
/// batching (`UpdateDatabaseDdl`) and multi-statement DML batching (`ExecuteBatchDml`).
pub(crate) fn split_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' | '"' | '`' => {
                // A quoted literal/identifier: copy through the matching close quote.
                current.push(c);
                while let Some(ch) = chars.next() {
                    current.push(ch);
                    match ch {
                        '\\' => {
                            if let Some(escaped) = chars.next() {
                                current.push(escaped);
                            }
                        }
                        _ if ch == c => break,
                        _ => {}
                    }
                }
            }
            '-' if chars.peek() == Some(&'-') => copy_line_comment(&mut current, c, &mut chars),
            '#' => copy_line_comment(&mut current, c, &mut chars),
            '/' if chars.peek() == Some(&'*') => {
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
            ';' => push_statement(&mut statements, &mut current),
            _ => current.push(c),
        }
    }
    push_statement(&mut statements, &mut current);
    statements
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
    if !trimmed.is_empty() {
        statements.push(trimmed.to_string());
    }
    current.clear();
}

/// The first SQL keyword, uppercased, skipping leading whitespace and `--`/`#`/`/* */` comments.
fn first_keyword(sql: &str) -> Option<String> {
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
    let word: String = rest.chars().take_while(char::is_ascii_alphabetic).collect();
    (!word.is_empty()).then(|| word.to_ascii_uppercase())
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
}
