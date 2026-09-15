use super::*;

#[test]
fn raw_prefix_detection() {
    // Any case of `r` / `rb` / `br` is a raw-literal prefix; nothing else is.
    for word in ["r", "R", "rb", "Rb", "rB", "RB", "br", "bR", "Br", "BR"] {
        assert!(is_raw_prefix(word), "should be a raw prefix: {word}");
    }
    for word in ["", "b", "B", "rr", "bb", "rbr", "raw", "x", "ré"] {
        assert!(!is_raw_prefix(word), "should not be a raw prefix: {word}");
    }
}

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
fn first_keyword_edge_cases() {
    // The keyword is borrowed from the input in its original case (callers compare
    // case-insensitively), after skipping whitespace, all comment forms, and hints.
    assert_eq!(first_keyword("select 1"), Some("select"));
    assert_eq!(first_keyword("-- c\n# h\n/* b */ Update t"), Some("Update"));
    assert_eq!(
        first_keyword("@{A=1} @{B='}'} DELETE FROM t"),
        Some("DELETE")
    );
    // Empty / whitespace / comment-only / unterminated-comment input has no keyword.
    for sql in ["", "  \n\t", "-- only\n /* comments */", "/* unterminated"] {
        assert_eq!(first_keyword(sql), None, "no keyword in {sql:?}");
    }
    // A leading quoted identifier or literal is not a keyword.
    assert_eq!(first_keyword("`select` 1"), None);
    assert_eq!(first_keyword("'select'"), None);
    // Only the leading ASCII-alphabetic run counts: a digit or `_` ends it, and a word that
    // does not *start* with an ASCII letter carries no keyword at all.
    assert_eq!(first_keyword("select_into x"), Some("select"));
    assert_eq!(first_keyword("_select 1"), None);
    assert_eq!(first_keyword("9select 1"), None);
    // A bare `@param` / `@@var` (not a `@{…}` hint) is not a keyword, and a space between
    // `@` and `{` is not a hint either.
    assert_eq!(first_keyword("@p"), None);
    assert_eq!(first_keyword("@@seed UPDATE t"), None);
    assert_eq!(first_keyword("@ {A=1} SELECT 1"), None);
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
fn lexer_partitions_input_byte_for_byte() {
    // The shared lexer's core guarantee (relied on by `split_statements`' verbatim rebuild):
    // concatenating every lexeme's source reproduces the input exactly, across all four comment
    // forms, a raw+triple-quoted literal, a quoted identifier and a statement hint.
    for sql in [
        r#"SELECT r'''a\''' /* c */ FROM `t` -- x
               WHERE s = "y;z" # h
               @{HINT=1} AND @p = @@v"#,
        "",
        "-- only a comment, unterminated",
        "/* unterminated block",
        "r'unterminated raw",
    ] {
        let rebuilt: String = lex(sql)
            .map(|lexeme| match lexeme {
                Lexeme::Word(s) | Lexeme::Quoted(s) | Lexeme::Comment(s) => s.to_string(),
                Lexeme::Other(c) => c.to_string(),
            })
            .collect();
        assert_eq!(rebuilt, sql, "lexer did not partition {sql:?} exactly");
    }
    // Classification of a small, representative token stream. A raw prefix (`r`) is a `Word`
    // adjacent to the following literal, which the lexer must treat as raw (so the escaped
    // closing quote does not end it early).
    assert_eq!(
        lex(r"a 'b' -- c").collect::<Vec<_>>(),
        vec![
            Lexeme::Word("a"),
            Lexeme::Other(' '),
            Lexeme::Quoted("'b'"),
            Lexeme::Other(' '),
            Lexeme::Comment("-- c"),
        ]
    );
    assert_eq!(
        lex(r"r'x\' y").collect::<Vec<_>>(),
        vec![
            Lexeme::Word("r"),
            Lexeme::Quoted(r"'x\'"),
            Lexeme::Other(' '),
            Lexeme::Word("y"),
        ]
    );
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
    // dropped — otherwise the empty segment is sent as a DDL statement and Spanner rejects the
    // whole schema change with INVALID_ARGUMENT.
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
    // the quote, so the `;` after it is a real separator. Treating `\'` as an escape here
    // swallows the separator and ships the whole batch as one malformed statement.
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

#[test]
fn extracts_named_parameters() {
    // Basic references, in order, with a later reuse deduped.
    assert_eq!(
        named_parameters("SELECT @a, @b FROM t WHERE @a > 0"),
        vec!["a", "b"]
    );
    // No parameters.
    assert_eq!(named_parameters("SELECT 1"), Vec::<String>::new());
    // `@` inside string literals and comments is not a parameter.
    assert_eq!(named_parameters("SELECT '@x', @y -- @z\n"), vec!["y"]);
    assert_eq!(named_parameters("SELECT @y /* @z */, @w"), vec!["y", "w"]);
    assert_eq!(named_parameters("SELECT `@col`, @p"), vec!["p"]);
    // Statement hints (`@{…}`) and system variables (`@@var`) are not parameters.
    assert_eq!(
        named_parameters("SELECT @{JOIN_METHOD=HASH_JOIN} * FROM t WHERE id = @id"),
        vec!["id"]
    );
    assert_eq!(named_parameters("SELECT @@rows"), Vec::<String>::new());
    // First-seen order is preserved across repeats.
    assert_eq!(named_parameters("@b @a @a @b @c"), vec!["b", "a", "c"]);
}

#[test]
fn named_parameters_skip_raw_and_triple_quoted_strings() {
    // In a raw string the backslash is not an escape: the literal ends at the first quote and
    // scanning resumes after it. Treating `\'` as escaped keeps the lexer in string mode and
    // swallows the parameters that follow.
    assert_eq!(named_parameters(r"SELECT r'\', @p"), vec!["p"]);
    assert_eq!(named_parameters(r"SELECT rb'@x\', @p"), vec!["p"]);
    // `@name` inside a triple-quoted string is not a parameter; one after it is.
    assert_eq!(named_parameters("SELECT '''@x''', @y"), vec!["y"]);
    assert_eq!(named_parameters(r#"SELECT """it's @x""", @y"#), vec!["y"]);
    // An empty literal is not the start of a triple-quoted string.
    assert_eq!(named_parameters("SELECT '', @z"), vec!["z"]);
    // A non-adjacent or non-prefix word does not make the literal raw: `\'` stays an escape,
    // so the literal runs to the *third* quote and @a is inside it.
    assert_eq!(named_parameters(r"SELECT xr'\' @a ', @b"), vec!["b"]);
}

#[test]
fn qualifies_table_names() {
    assert_eq!(qualified_table(None, "Users"), "`Users`");
    assert_eq!(qualified_table(Some(""), "Users"), "`Users`");
    assert_eq!(qualified_table(Some("app"), "Users"), "`app`.`Users`");
    // Caller-supplied names are escaped, so a backtick cannot leak into the surrounding SQL.
    assert_eq!(qualified_table(None, "a`b"), r"`a\`b`");
    assert_eq!(qualified_table(Some("s`x"), r"t\y"), r"`s\`x`.`t\\y`");
}

#[test]
fn quotes_identifiers_with_googlesql_escapes() {
    assert_eq!(quote_ident("plain"), "`plain`");
    assert_eq!(quote_ident("create"), "`create`");
    assert_eq!(quote_ident("a`b"), r"`a\`b`");
    assert_eq!(quote_ident(r"a\b"), r"`a\\b`");
    assert_eq!(quote_ident(r"a\`b"), r"`a\\\`b`");
    assert_eq!(quote_ident("spaced name"), "`spaced name`");
}
