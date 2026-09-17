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

use crate::error::invalid_argument;
use adbc_core::error::Result;

/// Leading keywords that identify a Spanner DDL statement.
const DDL_KEYWORDS: &[&str] = &[
    "CREATE", "DROP", "ALTER", "RENAME", "GRANT", "REVOKE", "ANALYZE",
];

/// Leading keywords that identify a Spanner DML statement (data modification).
const DML_KEYWORDS: &[&str] = &["INSERT", "UPDATE", "DELETE"];

/// Return `true` if `sql` begins with a DDL statement (ignoring leading whitespace and comments).
pub(crate) fn is_ddl(sql: &str) -> bool {
    first_keyword(sql).is_some_and(|kw| DDL_KEYWORDS.iter().any(|k| kw.eq_ignore_ascii_case(k)))
}

/// Return `true` if `sql` begins with a DML statement (`INSERT`/`UPDATE`/`DELETE`).
///
/// Used to route DML that arrives through the query entry point (`execute`) — as every ADBC client
/// does, since the C ABI exposes only `ExecuteQuery` — onto the read/write transaction path instead
/// of a read-only single-use one, which Spanner rejects for DML.
pub(crate) fn is_dml(sql: &str) -> bool {
    first_keyword(sql).is_some_and(|kw| DML_KEYWORDS.iter().any(|k| kw.eq_ignore_ascii_case(k)))
}

/// Return `true` if `sql` contains a top-level `THEN RETURN` clause — DML that returns rows
/// (`INSERT`/`UPDATE`/`DELETE ... THEN RETURN <columns>`).
///
/// Scans word tokens outside literals and comments — via the shared [`Lexer`] — for `THEN`
/// immediately followed by `RETURN`, case-insensitively, but only at `CASE` nesting depth zero.
/// GoogleSQL's `THEN RETURN` clause appears only at the top level, at the end of a DML statement; a
/// `THEN` inside `CASE WHEN … THEN … END` belongs to the CASE, so a branch expression that is a
/// column literally named `return` (`RETURN` is not a reserved keyword) must not match. `CASE`/`END`
/// are tracked as a nesting depth (`END` only closes `CASE` in GoogleSQL expressions); quoted
/// identifiers and literals never affect it, and an unbalanced `END` (invalid SQL) saturates at zero
/// rather than underflowing.
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

/// Consume a string/bytes literal or quoted identifier whose opening `quote` has just been read,
/// feeding every consumed character (excluding the already-read opening quote) to `sink`.
///
/// Handles the GoogleSQL lexical structure:
/// - **triple-quoted** strings (`'''…'''` / `"""…"""`), which may contain unescaped quotes and
///   newlines and close only on three consecutive quote characters;
/// - backslash escapes, both in ordinary literals and inside quoted identifiers
///   (`` ` ``, which are never triple-quoted).
///
/// A backslash binds the following character in **every** literal, raw ones (`r'…'`, `rb'…'`,
/// `br'…'`) included: the `r` prefix changes how the bytes are *interpreted*, not where the
/// literal *ends*, and this lexer only ever looks for the end. GoogleSQL agrees — "a raw string
/// cannot end with an odd number of backslashes" — so `r'x\'` is unterminated rather than
/// complete, and `r'x\'; DROP TABLE t'` is one string, not a statement followed by a `DROP`.
/// Treating `\` as inert here would let a value ending in a backslash (a Windows path, a regex)
/// close its literal early and smuggle statement boundaries past [`split_statements`].
///
/// One deliberate divergence: GoogleSQL ends a non-triple literal at an unescaped newline, while
/// this scan runs on to end of input. Such SQL is invalid either way and Spanner rejects it; the
/// lenient scan merely errs toward *fewer* statement boundaries, never more, which is the safe
/// direction for a splitter that feeds `UpdateDatabaseDdl`/`ExecuteBatchDml`.
fn consume_quoted(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    quote: char,
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
        if ch == '\\' {
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
/// while *skipping* consumers ([`is_dml_returning`], [`named_parameters`], [`first_keyword`])
/// ignore the lexeme kinds they do not care about — all of them sharing this one lexer instead of
/// a hand-rolled comment/quote walker each.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lexeme<'a> {
    /// A maximal run of identifier characters (`[A-Za-z0-9_]`): a keyword, identifier or number.
    Word(&'a str),
    /// A string/bytes literal or quoted identifier, delimiters included — triple-quoted and
    /// backslash-escaped forms handled per [`consume_quoted`], so an embedded quote / `;` /
    /// comment marker never ends it early.
    Quoted(&'a str),
    /// A `--`/`#` line comment (through its terminating newline, or end of input) or a `/* … */`
    /// block comment, delimiters included. An unterminated comment runs to end of input.
    Comment(&'a str),
    /// Any other single character — whitespace, punctuation, an operator, `;`, `@`, ….
    Other(char),
}

/// Tokenize `sql` into [`Lexeme`]s under GoogleSQL's lexical rules (the string/comment structure
/// shared by DDL/DML batch splitting, statement classification, `THEN RETURN` detection and
/// `@name` extraction). A literal's prefix (`r`/`rb`/`br`/`b`) lexes as an ordinary [`Lexeme::Word`]
/// because it does not change where the literal ends; see [`consume_quoted`].
fn lex(sql: &str) -> Lexer<'_> {
    Lexer {
        input: sql,
        chars: sql.chars().peekable(),
        pos: 0,
    }
}

/// The iterator behind [`lex`]. See [`Lexeme`] for the guarantees each item carries.
struct Lexer<'a> {
    input: &'a str,
    chars: std::iter::Peekable<std::str::Chars<'a>>,
    /// Byte offset of the next character `chars` will yield — used to slice `input`.
    pos: usize,
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
                // The opening quote is already consumed; advance `pos` past the rest of the literal.
                let mut consumed = 0usize;
                consume_quoted(&mut self.chars, c, |ch| consumed += ch.len_utf8());
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
        Some(lexeme)
    }
}

/// The lexemes of `sql` that can carry meaning for a classifier, with comments and whitespace
/// dropped and one lexeme of lookahead.
///
/// GoogleSQL lexes `@`, `{` and a parameter's name as separate tokens, so whitespace and comments
/// are legal *between* them: `@ {HINT=1} UPDATE …` and `@/*c*/p` are both accepted by Spanner.
/// Filtering the transparent lexemes out up front lets [`first_keyword`] and [`named_parameters`]
/// match those two-token shapes without each re-implementing the skipping.
fn significant_lexemes(sql: &str) -> std::iter::Peekable<impl Iterator<Item = Lexeme<'_>>> {
    lex(sql)
        .filter(|lexeme| {
            !matches!(lexeme, Lexeme::Comment(_))
                && !matches!(lexeme, Lexeme::Other(c) if c.is_whitespace())
        })
        .peekable()
}

/// Split a (possibly multi-statement) SQL string into individual, trimmed, non-empty statements.
///
/// Segments carrying no statement — pure whitespace, or only comments — are dropped rather than
/// emitted as empty statements (see [`push_statement`]). Callers depend on the resulting *count*:
/// it decides the all-DML batch check, the partitioned-DML and `THEN RETURN` single-statement
/// guards, and whether [`strip_trailing_terminators`] treats the input as one query.
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

/// Strip top-level `;` terminators, plus surrounding whitespace, from a **single** query, so
/// `SELECT current_date;;;` becomes `SELECT current_date`.
///
/// Leading and interior terminators go the same way (`"; SELECT 1"` becomes `SELECT 1`), because
/// the statement is recovered from the splitter rather than trimmed off the end; a trailing
/// comment-only segment is dropped with them (`"SELECT 1;\n-- tail\n;"` becomes `SELECT 1`), while
/// a comment attached to the statement itself is kept (`"SELECT 1 -- tail"` is unchanged).
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
    let has_statement = lex(trimmed).any(|lexeme| match lexeme {
        Lexeme::Comment(_) => false,
        Lexeme::Other(c) => !c.is_whitespace(),
        Lexeme::Word(_) | Lexeme::Quoted(_) => true,
    });
    if has_statement {
        statements.push(trimmed.to_string());
    }
    current.clear();
}

/// The first SQL keyword — the leading ASCII-alphabetic run of the first word, borrowed from
/// `sql` in its original case (callers compare case-insensitively, so classification allocates
/// nothing) — skipping leading whitespace, `--`/`#`/`/* */` comments, and
/// [statement hints](https://cloud.google.com/spanner/docs/reference/standard-sql/query-syntax#statement_hints)
/// (`@{HINT=value, …}`), which GoogleSQL allows before the statement proper — so hinted DML/DDL is
/// classified by its real leading keyword, not misread as "no keyword" and routed to the wrong
/// execution path.
///
/// The scan is delegated to the shared [`lex`] tokenizer: a hint body is lexed normally, so a `}`
/// inside a string literal in the hint does not close it, and an unterminated hint or comment
/// consumes the rest of the input — no keyword. Anything else before the first word — a quoted
/// identifier/literal, punctuation, a bare `@param`/`@@var` — means the input does not start with a
/// keyword, as does a first word that does not start with an ASCII letter.
///
/// Only the leading ASCII-alphabetic run of the first word is returned, so an identifier-shaped
/// word is classified by its prefix (`CREATE2` routes as `CREATE`). No valid Spanner statement
/// begins with such a word, so this only decides which error the user sees.
pub(crate) fn first_keyword(sql: &str) -> Option<&str> {
    let mut lexemes = significant_lexemes(sql);
    while let Some(lexeme) = lexemes.next() {
        match lexeme {
            // Already filtered out, but skipping keeps this correct on its own terms.
            Lexeme::Comment(_) => {}
            // A statement hint `@{…}`: skip through its closing `}`. GoogleSQL lexes `@` and `{`
            // as separate tokens, so `@ {…}` and `@/*c*/{…}` are hints too (Spanner accepts both)
            // — `significant_lexemes` has already dropped whatever sat between them.
            Lexeme::Other('@') if lexemes.peek() == Some(&Lexeme::Other('{')) => {
                for hint_lexeme in lexemes.by_ref() {
                    if hint_lexeme == Lexeme::Other('}') {
                        break;
                    }
                }
            }
            Lexeme::Word(word) => {
                let end = word
                    .find(|c: char| !c.is_ascii_alphabetic())
                    .unwrap_or(word.len());
                return (end > 0).then(|| &word[..end]);
            }
            Lexeme::Quoted(_) | Lexeme::Other(_) => return None,
        }
    }
    None
}

/// Extract the distinct named parameters (`@name`) referenced by `sql`, in order of first
/// appearance.
///
/// Skips `@name` occurrences inside string / bytes literals and quoted identifiers — including
/// triple-quoted (`'''…'''`/`"""…"""`) and raw (`r'…'`, `rb'…'`, …) forms — and comments
/// (`-- …`, `# …`, `/* … */`), and does not treat statement hints (`@{…}`) or system variables
/// (`@@var`) as parameters: the shared [`lex`] tokenizer folds those into their own lexeme, so a
/// `@name` is only a parameter when the `@` stands as its own token. Used by
/// `get_parameter_schema` when no parameter data has been bound yet.
///
/// The name may be a bare identifier run or a backtick-quoted identifier, and whitespace or a
/// comment may sit between the `@` and it — Spanner accepts `` @`p` ``, `@ p` and `@/*c*/p` as
/// references to the same parameter `p`. A backtick-quoted name containing a backslash escape is
/// skipped rather than guessed at: decoding it wrongly would name a parameter that does not exist,
/// which is worse than the omission (such names do not occur in practice).
///
/// Names are deduplicated case-insensitively, keeping the first spelling seen. Spanner resolves
/// parameters case-insensitively — `@p` binds from `P` — and rejects a request that supplies two
/// spellings of one name with "Duplicate parameter name", so reporting `@v` and `@V` separately
/// would make the driver build exactly that rejected request.
pub(crate) fn named_parameters(sql: &str) -> Vec<String> {
    let mut params: Vec<String> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut lexemes = significant_lexemes(sql);
    while let Some(lexeme) = lexemes.next() {
        if lexeme != Lexeme::Other('@') {
            continue;
        }
        // `.copied()` releases the peek borrow so the matched arm can advance `lexemes`.
        let name = match lexemes.peek().copied() {
            // `@@var` (system variable) or `@{…}` (statement hint): not a bind parameter — consume
            // the second marker character and keep scanning (a hint body is lexed normally).
            Some(Lexeme::Other('@')) | Some(Lexeme::Other('{')) => {
                lexemes.next();
                continue;
            }
            // `@name`: the name is an identifier run, which must start with a letter or underscore
            // — a word run may also start with a digit (`@1` is not a parameter).
            Some(Lexeme::Word(name))
                if name.starts_with(|ch: char| ch == '_' || ch.is_ascii_alphabetic()) =>
            {
                lexemes.next();
                name
            }
            // ``@`name` ``: a backtick-quoted identifier, delimiters stripped. `strip_suffix`
            // rather than a byte slice: an *unterminated* `` ` `` literal runs to end of input and
            // has no closing delimiter to remove, and its last byte may sit mid-character.
            Some(Lexeme::Quoted(quoted)) => {
                lexemes.next();
                let Some(inner) = quoted
                    .strip_prefix('`')
                    .and_then(|rest| rest.strip_suffix('`'))
                else {
                    continue;
                };
                if inner.is_empty() || inner.contains('\\') {
                    continue;
                }
                inner
            }
            _ => continue,
        };
        if seen.insert(name.to_ascii_lowercase()) {
            params.push(name.to_string());
        }
    }
    params
}

/// Backtick-quote a Spanner identifier, rejecting any name that backticks cannot safely contain.
///
/// This is the identifier-injection boundary: caller-supplied table, schema and column names reach
/// generated SQL through here, so the emitted token must be inert under **both** of Spanner's
/// parsers. They do not agree on escaping:
///
/// - the **query** parser (`ExecuteSql`) honours backslash escapes inside backticks, so
///   `` `a\`b` `` is one identifier named ``a`b``;
/// - the **Database Admin DDL** parser (`UpdateDatabaseDdl`) does **not** — it ends the identifier
///   at the first backtick, escaped or not, and treats the backslash as an ordinary character.
///
/// So no escaping scheme can make a backtick safe everywhere: on the DDL path a name containing one
/// closes the identifier early and the rest of the name is parsed as SQL. Spanner accepts a *column*
/// named `` c\ ``, so the truncated remainder need not even be rejected downstream — an ingest
/// column name could append columns, a `PRIMARY KEY`, or `INTERLEAVE IN PARENT … ON DELETE CASCADE`
/// to a generated `CREATE TABLE`, and DDL is not transactional, so the change would stick.
///
/// Validating instead of escaping sidesteps the disagreement: reject the characters Spanner cannot
/// represent in an identifier anyway — a backtick, a backslash, or a control character — plus the
/// empty name (`` `` `` is "Syntax error: Invalid empty identifier"). What remains is guaranteed to
/// contain no delimiter and no escape, so it is one opaque token under either grammar. Everything
/// Spanner genuinely supports still quotes: spaces, non-ASCII letters and reserved words
/// (`create`, `index`, …) all round-trip.
pub(crate) fn quote_ident(ident: &str) -> Result<String> {
    if ident.is_empty() {
        return Err(invalid_argument(
            "invalid SQL identifier: the name is empty, which Spanner rejects as an invalid empty \
             identifier",
        ));
    }
    if let Some(bad) = ident
        .chars()
        .find(|&c| c == '`' || c == '\\' || c.is_control())
    {
        return Err(invalid_argument(format!(
            "invalid SQL identifier {ident:?}: a name cannot contain {bad:?}, because Spanner's \
             DDL parser does not honour backslash escapes inside backticks and would read the \
             remainder of the name as SQL"
        )));
    }
    let mut out = String::with_capacity(ident.len() + 2);
    out.push('`');
    out.push_str(ident);
    out.push('`');
    Ok(out)
}

/// Backtick-quote a table name, optionally qualified by a (named) schema, validating both parts
/// through [`quote_ident`] so a hostile or mistyped name cannot leak into the surrounding SQL. An
/// empty schema (Spanner's default, unnamed schema) qualifies to the bare table.
///
/// A schema is one identifier, not a path: `Some("a.b")` quotes to `` `a.b` `` — a single name
/// containing a dot — because Spanner's named schemas are single-level. That is also what keeps a
/// dotted schema from injecting an extra qualification level.
pub(crate) fn qualified_table(db_schema: Option<&str>, table_name: &str) -> Result<String> {
    match db_schema.filter(|s| !s.is_empty()) {
        Some(schema) => Ok(format!(
            "{}.{}",
            quote_ident(schema)?,
            quote_ident(table_name)?
        )),
        None => quote_ident(table_name),
    }
}

#[cfg(test)]
mod tests;
