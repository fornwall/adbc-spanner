use super::*;
use adbc_core::error::Status;

#[test]
fn backslash_binds_the_next_char_in_every_literal_including_raw() {
    // A literal prefix changes how Spanner *interprets* the bytes, never where the literal
    // *ends*, and this lexer only ever looks for the end — so `\` binds the following character
    // in a raw literal exactly as in a plain one. GoogleSQL: "a raw string cannot end with an odd
    // number of backslashes". Verified against the Spanner emulator, which parses each of these
    // as a single `SELECT` of one string:
    //     SELECT r'x\'; SELECT 2'          ->  one row, value  x\'; SELECT 2
    //     SELECT r'''a\'''; SELECT 2'''    ->  one row, value  a\'''; SELECT 2
    // Reading `\'` as a closing quote instead would hand Spanner a statement boundary it never
    // saw, turning one harmless SELECT into a batch containing a live DELETE.
    assert_eq!(
        split_statements(r"SELECT r'x\'; DELETE FROM t WHERE true; SELECT 1'"),
        vec![r"SELECT r'x\'; DELETE FROM t WHERE true; SELECT 1'"]
    );
    assert_eq!(
        split_statements(r"SELECT r'''a\'''; DELETE FROM t WHERE true'''"),
        vec![r"SELECT r'''a\'''; DELETE FROM t WHERE true'''"]
    );
    // Every prefix spelling behaves the same, as does an unprefixed literal.
    for prefix in ["r", "R", "rb", "RB", "br", "Br", "b", "B", ""] {
        let sql = format!(r"SELECT {prefix}'x\'; DELETE FROM t'");
        assert_eq!(
            split_statements(&sql),
            vec![sql.clone()],
            "prefix {prefix:?}"
        );
    }
    // An even number of backslashes does close the literal, so the `;` after it really separates.
    assert_eq!(
        split_statements(r"SELECT r'a\\'; DELETE FROM t WHERE stale"),
        vec![r"SELECT r'a\\'", "DELETE FROM t WHERE stale"]
    );
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
    // A bare `@param` / `@@var` (not a `@{…}` hint) is not a keyword.
    assert_eq!(first_keyword("@p"), None);
    assert_eq!(first_keyword("@ p"), None);
    assert_eq!(first_keyword("@@seed UPDATE t"), None);
    // GoogleSQL lexes `@` and `{` as separate tokens, so whitespace or a comment may sit between
    // them and the result is still a statement hint. Spanner executes all three of these (the
    // emulator returns a row for each), so the statement after the hint must still be classified
    // — otherwise hinted DML is misread as "no keyword" and routed to a read-only single-use
    // transaction, which Spanner rejects with "DML statements can only be performed in a
    // read-write or partitioned-dml transaction".
    assert_eq!(first_keyword("@ {A=1} SELECT 1"), Some("SELECT"));
    assert_eq!(first_keyword("@\n{A=1} SELECT 1"), Some("SELECT"));
    assert_eq!(first_keyword("@/* c */{A=1} SELECT 1"), Some("SELECT"));
    assert!(is_dml(
        "@ {PDML_MAX_PARALLELISM=8} UPDATE t SET n = 1 WHERE true"
    ));
    assert!(is_ddl("@ {A=1} CREATE TABLE t (id INT64) PRIMARY KEY (id)"));
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
    // `\'` does not close a raw literal, so the words after it are still inside the string and
    // are not a clause; an even backslash run does close it, and then the clause is found.
    assert!(!is_dml_returning(
        r"UPDATE t SET s = r'x\' WHERE true THEN RETURN Id"
    ));
    assert!(is_dml_returning(
        r"UPDATE t SET s = r'x\\' WHERE true THEN RETURN Id"
    ));
    for sql in [
        "INSERT INTO t (id) VALUES (1)",
        "SELECT CASE WHEN a THEN b ELSE c END FROM t",
        // `THEN RETURN` inside a literal or a quoted identifier is not a clause — including
        // the triple-quoted and raw literal forms.
        "INSERT INTO t (s) VALUES ('THEN RETURN')",
        "INSERT INTO t (s) VALUES ('''THEN RETURN''')",
        r"INSERT INTO t (s) VALUES (r'THEN RETURN\\')",
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
    // `\'` is an escaped quote even behind a raw prefix, so the literal swallows the rest of the
    // input instead of ending at that quote; doubling the backslash closes it.
    assert_eq!(
        lex(r"r'x\' y").collect::<Vec<_>>(),
        vec![Lexeme::Word("r"), Lexeme::Quoted(r"'x\' y")]
    );
    assert_eq!(
        lex(r"r'x\\' y").collect::<Vec<_>>(),
        vec![
            Lexeme::Word("r"),
            Lexeme::Quoted(r"'x\\'"),
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
    // A trailing backslash does NOT end a raw literal — it escapes the quote, exactly as in a
    // plain literal — so `r'C:\'` is unterminated and swallows the rest of the input. This is
    // the path-shaped value that makes the distinction matter: were `\'` read as a closing
    // quote, the `DELETE` below would become a statement of its own and be executed.
    assert_eq!(
        split_statements(r"UPDATE t SET path = r'C:\'; DELETE FROM u WHERE stale"),
        vec![r"UPDATE t SET path = r'C:\'; DELETE FROM u WHERE stale"]
    );
    // All prefix spellings behave alike, raw or not.
    assert_eq!(
        split_statements(r"INSERT INTO t (b) VALUES (rb'\'); SELECT 1"),
        vec![r"INSERT INTO t (b) VALUES (rb'\'); SELECT 1"]
    );
    assert_eq!(
        split_statements(r#"SELECT BR"\"; SELECT R'\'"#),
        vec![r#"SELECT BR"\"; SELECT R'\'"#]
    );
    assert_eq!(
        split_statements(r"SELECT b'\';x'; SELECT 2"),
        vec![r"SELECT b'\';x'", "SELECT 2"]
    );
    // Properly closed literals still separate normally, whatever the prefix.
    assert_eq!(
        split_statements(r"UPDATE t SET path = r'C:\\'; DELETE FROM u WHERE stale"),
        vec![r"UPDATE t SET path = r'C:\\'", "DELETE FROM u WHERE stale"]
    );
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
    // Raw + triple combined: the backslash escapes the first closing quote here too, so the
    // literal is still open and the `;` is inside it (emulator-verified, see
    // `backslash_binds_the_next_char_in_every_literal_including_raw`).
    assert_eq!(
        split_statements(r"SELECT r'''a\'''; SELECT 2"),
        vec![r"SELECT r'''a\'''; SELECT 2"]
    );
    assert_eq!(
        split_statements(r"SELECT r'''a\\'''; SELECT 2"),
        vec![r"SELECT r'''a\\'''", "SELECT 2"]
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
fn strip_trailing_terminators_handles_statementless_and_leading_semicolons() {
    // The statement is recovered from the splitter, not trimmed off the end, so a leading or
    // interior `;` goes the same way as a trailing one.
    assert_eq!(strip_trailing_terminators("; SELECT 1"), "SELECT 1");
    assert_eq!(strip_trailing_terminators(" ; SELECT 1 ; "), "SELECT 1");
    assert_eq!(strip_trailing_terminators(";;SELECT 1"), "SELECT 1");
    // A trailing comment-only segment is dropped with the terminator; a comment that belongs to
    // the statement is part of it and survives.
    assert_eq!(
        strip_trailing_terminators("SELECT 1;\n-- tail\n;"),
        "SELECT 1"
    );
    assert_eq!(
        strip_trailing_terminators("SELECT 1 -- tail"),
        "SELECT 1 -- tail"
    );
    // Input that parses to no statement at all is returned untouched (the `len() != 1` branch),
    // leaving Spanner to produce the error rather than inventing one here.
    for sql in [
        "",
        ";",
        ";;",
        "  \n",
        "-- only a comment",
        "/* just a block */",
    ] {
        assert_eq!(strip_trailing_terminators(sql), sql, "for {sql:?}");
    }
}

#[test]
fn split_drops_only_comment_and_whitespace_segments() {
    // Comment-only segments are dropped even when separated by interior whitespace — the
    // whitespace arm of `push_statement`'s check is what makes this work, since `trim` only
    // strips the ends and cannot remove the space *between* two comments.
    assert_eq!(split_statements("/* a */ /* b */"), Vec::<String>::new());
    assert_eq!(split_statements("-- a\n /* b */ # c"), Vec::<String>::new());
    // Punctuation is not a comment: it is user-written SQL text, so it is kept and left for
    // Spanner to reject rather than silently swallowed along with a typo.
    assert_eq!(split_statements("SELECT 1; ()"), vec!["SELECT 1", "()"]);
}

#[test]
fn split_statements_returns_verbatim_in_order_slices() {
    // The invariant `fuzz/fuzz_targets/keyword.rs` leans on: `split_statements` rebuilds each
    // statement character by character, so this pins that the rebuild never alters or reorders
    // the text. Exercised over the shapes most likely to break it.
    for sql in [
        r"SELECT r'x\'; DELETE FROM t'",
        "SELECT '''a;b''' ; SELECT `c;d` ; -- t\n SELECT 2",
        "/* unterminated ; SELECT 1",
        "SELECT 'unterminated ; SELECT 1",
        "SELECT 1;;;SELECT 2",
        "SELECT '\u{2028}\u{0}é😀'; SELECT 2",
        "",
    ] {
        let mut cursor = 0;
        for statement in split_statements(sql) {
            assert_eq!(statement, statement.trim());
            assert!(!statement.is_empty());
            let found = sql[cursor..]
                .find(statement.as_str())
                .unwrap_or_else(|| panic!("{statement:?} is not an in-order slice of {sql:?}"));
            cursor += found + statement.len();
        }
    }
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
    // In a raw string the backslash still escapes, so `r'\'` is unterminated and the `@p` after
    // it is inside the literal, not a parameter. Closing the literal exposes it again.
    assert_eq!(named_parameters(r"SELECT r'\', @p"), Vec::<String>::new());
    assert_eq!(
        named_parameters(r"SELECT rb'@x\', @p"),
        Vec::<String>::new()
    );
    assert_eq!(named_parameters(r"SELECT r'\\', @p"), vec!["p"]);
    assert_eq!(named_parameters(r"SELECT rb'@x\\', @p"), vec!["p"]);
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
fn named_parameters_accept_every_spelling_spanner_does() {
    // GoogleSQL lexes `@` and the name as separate tokens, and the name may itself be a quoted
    // identifier. Spanner treats all of these as a reference to `p` (emulator: each returns a row
    // when `p` is bound, and reports "Incomplete query parameters p" when it is not). Missing them
    // makes valid SQL unbindable: `resolve_parameter_names` sees zero parameters for a one-column
    // batch and fails with a parameter-count mismatch.
    for sql in [
        "SELECT @p AS a",
        "SELECT @ p AS a",
        "SELECT @/* c */p AS a",
        "SELECT @`p` AS a",
        "SELECT @\n  p AS a",
    ] {
        assert_eq!(named_parameters(sql), vec!["p"], "for {sql:?}");
    }
    // A backtick-quoted name may contain characters a bare identifier cannot.
    assert_eq!(named_parameters("SELECT @`a b` AS x"), vec!["a b"]);
    // An escaped or empty quoted name is skipped rather than decoded incorrectly, and an
    // unterminated one has no closing delimiter to strip (its last byte may even sit
    // mid-character, so this must not be a byte slice).
    assert_eq!(named_parameters(r"SELECT @`a\`b`"), Vec::<String>::new());
    assert_eq!(named_parameters("SELECT @``"), Vec::<String>::new());
    assert_eq!(named_parameters("SELECT @`"), Vec::<String>::new());
    assert_eq!(named_parameters("SELECT @`θ"), Vec::<String>::new());
    assert_eq!(
        named_parameters("SELECT @`unterminated"),
        Vec::<String>::new()
    );
    // A hint or system variable is still not a parameter, with or without a separator.
    assert_eq!(
        named_parameters("@ {JOIN_METHOD=HASH_JOIN} SELECT * FROM t WHERE id = @id"),
        vec!["id"]
    );
    assert_eq!(named_parameters("SELECT @@rows"), Vec::<String>::new());
    // A quoted identifier that is not a parameter name is untouched.
    assert_eq!(named_parameters("SELECT `@col`, @p"), vec!["p"]);
}

#[test]
fn named_parameters_dedupe_case_insensitively() {
    // Spanner resolves parameter names case-insensitively (`@p` binds from `P`) and rejects a
    // request carrying two spellings of one name with "Duplicate parameter name p". Reporting
    // both spellings would make the driver build exactly that rejected request — and demand two
    // bound columns for one logical parameter. The first spelling wins, because that is the one
    // the query planner reports back.
    assert_eq!(
        named_parameters("UPDATE t SET s = @v WHERE id = @V"),
        vec!["v"]
    );
    assert_eq!(named_parameters("SELECT @Name, @name, @NAME"), vec!["Name"]);
    // Distinct names are still distinct, and first-appearance order is preserved.
    assert_eq!(
        named_parameters("SELECT @b, @a, @B, @c, @A"),
        vec!["b", "a", "c"]
    );
}

#[test]
fn named_parameters_dedupe_scales_to_large_statements() {
    // The dedupe is a hash lookup, not a linear scan of everything seen so far: a wide generated
    // statement (still well under Spanner's 1 MB request limit) must not cost quadratic time.
    // This runs on the calling thread before any RPC, in `get_parameter_schema` and once per
    // (sql, batch) in `resolve_parameter_names`.
    let sql = format!(
        "SELECT {}",
        (0..20_000)
            .map(|i| format!("@p{i}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let params = named_parameters(&sql);
    assert_eq!(params.len(), 20_000);
    assert_eq!(params[0], "p0");
    assert_eq!(params[19_999], "p19999");
}

#[test]
fn qualifies_table_names() {
    assert_eq!(qualified_table(None, "Users").unwrap(), "`Users`");
    assert_eq!(qualified_table(Some(""), "Users").unwrap(), "`Users`");
    assert_eq!(
        qualified_table(Some("app"), "Users").unwrap(),
        "`app`.`Users`"
    );
    // A schema is one identifier, not a path: a dot inside it cannot add a qualification level.
    assert_eq!(qualified_table(Some("a.b"), "t").unwrap(), "`a.b`.`t`");
    // Either half being unquotable rejects the whole name.
    for (schema, table) in [
        (None, "a`b"),
        (Some("s`x"), "t"),
        (Some("s"), r"t\y"),
        (None, ""),
        (Some("s"), ""),
    ] {
        assert_eq!(
            qualified_table(schema, table).unwrap_err().status,
            Status::InvalidArguments,
            "should be rejected: {schema:?}.{table:?}"
        );
    }
}

#[test]
fn quote_ident_accepts_every_name_spanner_can_represent() {
    // Backticks make reserved words, spaces and non-ASCII letters usable as identifiers; all of
    // these are accepted by Spanner as column names (emulator-verified).
    assert_eq!(quote_ident("plain").unwrap(), "`plain`");
    assert_eq!(quote_ident("create").unwrap(), "`create`");
    assert_eq!(quote_ident("spaced name").unwrap(), "`spaced name`");
    assert_eq!(quote_ident("é").unwrap(), "`é`");
    assert_eq!(quote_ident("a.b").unwrap(), "`a.b`");
}

#[test]
fn quote_ident_rejects_names_backticks_cannot_contain() {
    // The identifier-injection boundary. Spanner's two parsers disagree about escaping — the
    // query parser honours `` \` ``, the DDL parser ends the identifier at the first backtick and
    // treats `\` as an ordinary character — so no escaping scheme is safe on both paths.
    // Rejecting is: what survives contains neither a delimiter nor an escape, making it one
    // opaque token under either grammar.
    //
    // The payload below is the real one: as an ingest column name it previously produced
    // `CREATE TABLE `t` (`k\` INT64, Id INT64 NOT NULL) PRIMARY KEY (Id), INTERLEAVE IN PARENT
    // parent ON DELETE CASCADE -- ` INT64)`, which the emulator executed — creating a table
    // interleaved into an unrelated parent with cascading delete. DDL is not transactional, so
    // the injected schema change could not be rolled back.
    let payload = "k` INT64, Id INT64 NOT NULL) PRIMARY KEY (Id), \
                   INTERLEAVE IN PARENT parent ON DELETE CASCADE -- ";
    for ident in [
        payload, "a`b", r"a\b", r"a\`b", "a\nb", "a\rb", "a\tb", "a\0b", "",
    ] {
        let err = quote_ident(ident).unwrap_err();
        assert_eq!(
            err.status,
            Status::InvalidArguments,
            "should be rejected: {ident:?}"
        );
    }
    // Whatever a name does contain, an accepted one never carries a backtick or backslash
    // beyond its two delimiters — the property that holds under both Spanner grammars.
    for ident in ["plain", "spaced name", "é", "a.b", "créate", "x-y", "1"] {
        let quoted = quote_ident(ident).unwrap();
        assert!(quoted.starts_with('`') && quoted.ends_with('`'));
        assert!(
            !quoted[1..quoted.len() - 1].contains(['`', '\\']),
            "{quoted:?} is not one opaque token"
        );
    }
}
