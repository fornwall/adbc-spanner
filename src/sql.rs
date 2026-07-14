//! GoogleSQL text helpers: lexing, statement classification/splitting, identifier quoting, and
//! bind-parameter extraction.
//!
//! This is the single home for the driver's SQL-text concerns — everything that inspects or
//! rewrites raw SQL strings under GoogleSQL's lexical rules lives here, so the lexer
//! ([`lex`]/[`Lexer`]/[`Lexeme`]) and its consumers ([`split_statements`], [`is_dml_returning`],
//! [`named_parameters`], …) plus identifier quoting ([`quote_ident`]/[`qualified_table`]) sit next
//! to each other rather than being duplicated across modules.
//!
//! **DDL routing.** Spanner does not accept DDL (`CREATE`/`ALTER`/`DROP`/…) over the data-plane SQL
//! API — DDL is applied through the Database Admin `UpdateDatabaseDdl` long-running operation, which
//! takes a *list* of statements and applies them as one schema change. The driver therefore detects
//! DDL ([`is_ddl`]) and routes it there (see [`SpannerStatement`](crate::SpannerStatement)).
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
/// Scans word tokens outside string/identifier literals and comments — via the shared
/// [`Lexer`] — for the keyword `THEN` immediately followed by `RETURN`,
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
    for lexeme in lex(sql) {
        match lexeme {
            Lexeme::Word(word) => {
                if previous_was_then && case_depth == 0 && word.eq_ignore_ascii_case("RETURN") {
                    return true;
                }
                previous_was_then = word.eq_ignore_ascii_case("THEN");
                if word.eq_ignore_ascii_case("CASE") {
                    case_depth += 1;
                } else if word.eq_ignore_ascii_case("END") {
                    case_depth = case_depth.saturating_sub(1);
                }
            }
            // A quoted literal/identifier resets the keyword window, so `THEN 'x' RETURN` is not a
            // clause. Comments and punctuation between the two keywords are transparent (they carry
            // no word), matching `THEN /* keep */ RETURN` and `THEN\n  RETURN`.
            Lexeme::Quoted(_) => previous_was_then = false,
            Lexeme::Comment(_) | Lexeme::Other(_) => {}
        }
    }
    false
}

/// Whether `word` — the identifier run immediately (adjacently) before a quote — is a GoogleSQL
/// literal prefix marking a **raw** literal (`r`, `rb` or `br`, any case), in which backslash is
/// an ordinary character rather than an escape. A plain bytes prefix (`b`) keeps backslash
/// escapes, so it needs no special handling.
pub(crate) fn is_raw_prefix(word: &str) -> bool {
    // `eq_ignore_ascii_case` keeps this allocation-free — this runs once per word lexeme of every
    // lexed SQL string, so a `to_ascii_lowercase()` here would allocate per word.
    word.eq_ignore_ascii_case("r")
        || word.eq_ignore_ascii_case("rb")
        || word.eq_ignore_ascii_case("br")
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

/// A single GoogleSQL lexeme produced by [`lex`]. The pieces partition the input with no gaps or
/// overlaps: concatenating `Word`/`Quoted`/`Comment` slices and `Other` chars in order reproduces
/// the source byte-for-byte. That lets a *copying* consumer ([`split_statements`]) rebuild the text
/// while *skipping* consumers ([`is_dml_returning`], [`named_parameters`])
/// ignore the lexeme kinds they do not care about — all three sharing one lexer instead of a
/// hand-rolled comment/quote walker each.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Lexeme<'a> {
    /// A maximal run of identifier characters (`[A-Za-z0-9_]`): a keyword, identifier or number.
    Word(&'a str),
    /// A string/bytes literal or quoted identifier, delimiters included — triple-quoted, raw
    /// (`r'…'`/`rb'…'`/`br'…'`) and backslash-escaped forms handled per [`consume_quoted`], so an
    /// embedded quote / `;` / comment marker never ends it early.
    Quoted(&'a str),
    /// A `--`/`#` line comment (through its terminating newline, or end of input) or a `/* … */`
    /// block comment, delimiters included. An unterminated comment runs to end of input.
    Comment(&'a str),
    /// Any other single character — whitespace, punctuation, an operator, `;`, `@`, ….
    Other(char),
}

/// Tokenize `sql` into [`Lexeme`]s under GoogleSQL's lexical rules (the string/comment structure
/// shared by DDL/DML batch splitting, `THEN RETURN` detection and `@name` extraction). The lexer
/// tracks the trailing identifier run itself so it can recognise raw-literal prefixes (`r'…'`) —
/// callers never need to; see [`consume_quoted`].
pub(crate) fn lex(sql: &str) -> Lexer<'_> {
    Lexer {
        input: sql,
        chars: sql.chars().peekable(),
        pos: 0,
        prev_raw_prefix: false,
    }
}

/// The iterator behind [`lex`]. See [`Lexeme`] for the guarantees each item carries.
pub(crate) struct Lexer<'a> {
    input: &'a str,
    chars: std::iter::Peekable<std::str::Chars<'a>>,
    /// Byte offset of the next character `chars` will yield — used to slice `input`.
    pos: usize,
    /// Whether the immediately-preceding lexeme was an identifier run that is a raw-literal prefix,
    /// so a quote following it (with no intervening character) opens a raw literal.
    prev_raw_prefix: bool,
}

impl Lexer<'_> {
    fn bump(&mut self) -> Option<char> {
        let c = self.chars.next()?;
        self.pos += c.len_utf8();
        Some(c)
    }

    /// Consume a `--`/`#` line comment through its terminating newline (inclusive) or end of input.
    /// The introducer (`#`, or the first `-`) has already been consumed.
    fn consume_line_comment(&mut self) {
        while let Some(ch) = self.bump() {
            if ch == '\n' {
                break;
            }
        }
    }
}

fn is_ident_char(c: char) -> bool {
    c == '_' || c.is_ascii_alphanumeric()
}

impl<'a> Iterator for Lexer<'a> {
    type Item = Lexeme<'a>;

    fn next(&mut self) -> Option<Lexeme<'a>> {
        let start = self.pos;
        let c = self.bump()?;
        let lexeme = match c {
            '\'' | '"' | '`' => {
                let raw = c != '`' && self.prev_raw_prefix;
                // The opening quote is already consumed; advance `pos` past the rest of the literal.
                let mut consumed = 0usize;
                consume_quoted(&mut self.chars, c, raw, |ch| consumed += ch.len_utf8());
                self.pos += consumed;
                Lexeme::Quoted(&self.input[start..self.pos])
            }
            '-' if self.chars.peek() == Some(&'-') => {
                self.consume_line_comment();
                Lexeme::Comment(&self.input[start..self.pos])
            }
            '#' => {
                self.consume_line_comment();
                Lexeme::Comment(&self.input[start..self.pos])
            }
            '/' if self.chars.peek() == Some(&'*') => {
                self.bump(); // '*'
                let mut prev = '\0';
                while let Some(ch) = self.bump() {
                    if prev == '*' && ch == '/' {
                        break;
                    }
                    prev = ch;
                }
                Lexeme::Comment(&self.input[start..self.pos])
            }
            _ if is_ident_char(c) => {
                while self.chars.peek().copied().is_some_and(is_ident_char) {
                    self.bump();
                }
                Lexeme::Word(&self.input[start..self.pos])
            }
            _ => Lexeme::Other(c),
        };
        // A raw prefix must sit immediately before the quote, so only an identifier run adjacent to
        // the next quote keeps the flag set; every other lexeme (including whitespace/punctuation)
        // clears it.
        self.prev_raw_prefix = matches!(lexeme, Lexeme::Word(w) if is_raw_prefix(w));
        Some(lexeme)
    }
}

/// Split a (possibly multi-statement) SQL string into individual, trimmed, non-empty statements.
///
/// Splits on top-level `;`, ignoring semicolons inside string/bytes literals and quoted
/// identifiers — including triple-quoted (`'''…'''`/`"""…"""`) and raw (`r'…'`, `rb'…'`, …) forms,
/// see [`consume_quoted`] — and comments (`-- …`, `# …`, `/* … */`). Both are handled by the
/// shared [`lex`] tokenizer, which reproduces every non-`;` lexeme verbatim. This is shared by DDL
/// batching (`UpdateDatabaseDdl`) and multi-statement DML batching (`ExecuteBatchDml`).
pub(crate) fn split_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    for lexeme in lex(sql) {
        match lexeme {
            // A top-level `;` ends the current statement (and is itself dropped).
            Lexeme::Other(';') => push_statement(&mut statements, &mut current),
            // Every other lexeme is copied through verbatim, so literals/comments survive intact.
            Lexeme::Word(s) | Lexeme::Quoted(s) | Lexeme::Comment(s) => current.push_str(s),
            Lexeme::Other(c) => current.push(c),
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
pub(crate) fn first_keyword(sql: &str) -> Option<String> {
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

/// Extract the distinct named parameters (`@name`) referenced by `sql`, in order of first
/// appearance.
///
/// Skips `@name` occurrences inside string / bytes literals and quoted identifiers — including
/// triple-quoted (`'''…'''`/`"""…"""`) and raw (`r'…'`, `rb'…'`, …) forms — and comments
/// (`-- …`, `# …`, `/* … */`), and does not treat statement hints (`@{…}`) or system variables
/// (`@@var`) as parameters. The lexical scan is delegated to the shared [`lex`] tokenizer (the same
/// rules as [`split_statements`]), so a `@name` marker is only a parameter when the `@` character
/// stands as its own token — never inside a literal, comment or hint body. Used by
/// `get_parameter_schema` when no parameter data has been bound yet.
pub(crate) fn named_parameters(sql: &str) -> Vec<String> {
    let mut params: Vec<String> = Vec::new();
    let mut lexemes = lex(sql).peekable();
    while let Some(lexeme) = lexemes.next() {
        // Only a bare `@` token can introduce a parameter; `@` inside a literal/comment/hint body
        // is folded into that lexeme and never reaches here.
        if lexeme != Lexeme::Other('@') {
            continue;
        }
        // `.copied()` releases the peek borrow so the matched arm can advance `lexemes`.
        match lexemes.peek().copied() {
            // `@@var` (system variable) or `@{…}` (statement hint): not a bind parameter — consume
            // the second marker character and keep scanning (a hint body is lexed normally).
            Some(Lexeme::Other('@')) | Some(Lexeme::Other('{')) => {
                lexemes.next();
            }
            // `@name`: a bind parameter. The name is the adjacent identifier run, which must start
            // with a letter or underscore — a word run may also start with a digit (`@1` is not a
            // parameter).
            Some(Lexeme::Word(name))
                if name.starts_with(|ch: char| ch == '_' || ch.is_ascii_alphabetic()) =>
            {
                lexemes.next();
                if !params.iter().any(|p| p == name) {
                    params.push(name.to_string());
                }
            }
            _ => {}
        }
    }
    params
}

/// Backtick-quote a Spanner identifier. Keeps reserved words (`create`, `index`, …) and
/// otherwise-unsafe names valid, and closes the identifier-injection vector when a caller's
/// table/column names reach the generated SQL.
///
/// GoogleSQL quoted identifiers use **backslash escape sequences** (`\\``, `\\\\`) — not the
/// backtick doubling of other dialects, which Spanner would reject as a syntax error. (Real
/// Spanner object names cannot contain backticks or backslashes, so an escaped name will fail
/// server-side with a clear "invalid name" error rather than mangling the surrounding SQL.)
pub(crate) fn quote_ident(ident: &str) -> String {
    let mut out = String::with_capacity(ident.len() + 2);
    out.push('`');
    for c in ident.chars() {
        if c == '`' || c == '\\' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('`');
    out
}

/// Backtick-quote a table name, optionally qualified by a (named) schema, with proper GoogleSQL
/// identifier escaping (see [`quote_ident`]) so a hostile or mistyped name cannot leak into the
/// surrounding SQL. An empty schema (Spanner's default, unnamed schema) qualifies to the bare table.
pub(crate) fn qualified_table(db_schema: Option<&str>, table_name: &str) -> String {
    match db_schema.filter(|s| !s.is_empty()) {
        Some(schema) => format!("{}.{}", quote_ident(schema), quote_ident(table_name)),
        None => quote_ident(table_name),
    }
}

#[cfg(test)]
mod tests {
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
        // scanning resumes correctly after it. (The old lexer consumed `\'` as escaped, stayed in
        // string mode, and swallowed the parameters that followed.)
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
}
