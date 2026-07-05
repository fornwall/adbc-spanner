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
/// The split is on `;` and is intentionally simple — Spanner DDL statements do not normally contain
/// semicolons inside string literals, and `UpdateDatabaseDdl` wants the trailing `;` stripped.
pub(crate) fn split_statements(sql: &str) -> Vec<String> {
    sql.split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
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
}
