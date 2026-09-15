//! Offline unit tests for the bulk-ingest chunk budget, the commit-failure annotation and the
//! ingest option parsers.

use super::*;
use adbc_core::error::Status;

/// Simulate the [`SpannerStatement::run_ingest_mutations`] chunk loop for `rows` uniform rows
/// of `columns` columns and ~`row_bytes` bytes each, returning the chunk boundaries as
/// per-chunk row counts.
fn chunk_lengths(rows: usize, columns: usize, row_bytes: usize) -> Vec<usize> {
    let mut lengths = Vec::new();
    let mut current = 0_usize;
    let mut budget = IngestChunkBudget::default();
    for _ in 0..rows {
        if !budget.fits(columns, row_bytes) {
            lengths.push(current);
            current = 0;
            budget = IngestChunkBudget::default();
        }
        current += 1;
        budget.add(columns, row_bytes);
    }
    if current > 0 {
        lengths.push(current);
    }
    lengths
}

#[test]
fn ingest_chunks_cut_at_the_mutation_limit() {
    // 10-column rows of negligible byte size: the mutation budget binds, so the boundary falls
    // at LIMIT / columns rows per chunk (2,000 with the current 20,000 budget).
    let per_chunk = (INGEST_CHUNK_MUTATION_LIMIT / 10) as usize;
    assert_eq!(chunk_lengths(per_chunk, 10, 1), vec![per_chunk]);
    assert_eq!(chunk_lengths(per_chunk + 1, 10, 1), vec![per_chunk, 1]);
    assert_eq!(
        chunk_lengths(3 * per_chunk - 1, 10, 1),
        vec![per_chunk, per_chunk, per_chunk - 1]
    );
    // A column count that doesn't divide the limit still stays under it: 20,000 / 3 = 6,666.
    assert_eq!(chunk_lengths(6_667, 3, 1), vec![6_666, 1]);
}

#[test]
fn ingest_chunks_cut_at_the_byte_budget() {
    // 1 MiB rows: the byte budget binds long before the mutation budget does.
    let mib = 1024 * 1024;
    let per_chunk = (INGEST_CHUNK_BYTE_BUDGET / mib as u64) as usize;
    assert_eq!(chunk_lengths(per_chunk, 2, mib), vec![per_chunk]);
    assert_eq!(
        chunk_lengths(2 * per_chunk + 1, 2, mib),
        vec![per_chunk, per_chunk, 1]
    );
}

#[test]
fn ingest_chunks_never_starve_on_an_oversized_row() {
    // A single row larger than the whole budget (in both dimensions, with saturating cost
    // arithmetic) still forms its own one-row chunk instead of an empty chunk / infinite loop.
    assert_eq!(chunk_lengths(3, usize::MAX, usize::MAX), vec![1, 1, 1]);
    // Zero-cost rows never cut: everything fits in one chunk.
    assert_eq!(chunk_lengths(100_000, 0, 0), vec![100_000]);
}

#[test]
fn ingest_of_zero_rows_emits_no_chunks() {
    // Bound batches holding no rows at all (e.g. a stream of zero-row batches) must produce no
    // commit chunk — the trailing `write_mutation_chunk` guards the empty case, so nothing is
    // sent to Spanner.
    assert_eq!(chunk_lengths(0, 10, 100), Vec::<usize>::new());
}

#[test]
fn mid_ingest_failure_notes_committed_rows() {
    let source = || {
        let mut e = err("Spanner error: row already exists", Status::AlreadyExists);
        e.vendor_code = 6; // gRPC ALREADY_EXISTS
        e.details = Some(vec![(
            "google.rpc.errorinfo".to_string(),
            br#"{"reason":"DUPLICATE_KEY"}"#.to_vec(),
        )]);
        e
    };
    // First-chunk failure: nothing was committed, the error passes through untouched.
    let untouched = note_rows_already_committed(source(), 0);
    assert_eq!(untouched.message, "Spanner error: row already exists");
    assert_eq!(untouched.status, Status::AlreadyExists);
    // Later-chunk failure: the exact committed row count is reported, and the status,
    // vendor_code and forwarded details survive so callers still branch on — and diagnose —
    // the underlying failure.
    let annotated = note_rows_already_committed(source(), 4_000);
    assert!(
        annotated
            .message
            .contains("4000 row(s) from this bulk ingest were already committed"),
        "{}",
        annotated.message
    );
    assert!(
        annotated.message.contains("row already exists"),
        "the original failure must stay in the message: {}",
        annotated.message
    );
    assert_eq!(annotated.status, Status::AlreadyExists);
    assert_eq!(annotated.vendor_code, 6);
    assert_eq!(annotated.details, source().details);
}

#[test]
fn timed_out_or_cancelled_chunk_reports_unknown_outcome() {
    // CON-5: a cancel/timeout drops the in-flight `Commit` future, which may still land
    // server-side, so the failing chunk's own outcome is unknown — the annotation must flag
    // the ambiguity (and the duplicate-row risk) rather than implying exact accounting.
    for status in [Status::Timeout, Status::Cancelled] {
        // Even a first-chunk failure (nothing counted as committed) must warn, because the
        // failing chunk itself may have landed.
        let first = note_rows_already_committed(err("commit interrupted", status), 0);
        assert_eq!(first.status, status);
        assert!(
            first.message.contains("outcome is unknown")
                && first.message.contains("duplicate rows"),
            "an interrupted first chunk must flag the ambiguous outcome: {}",
            first.message
        );

        // A later-chunk failure keeps the exact earlier-chunk count *and* flags the failing
        // chunk's unknown outcome.
        let later = note_rows_already_committed(err("commit interrupted", status), 4_000);
        assert!(
            later
                .message
                .contains("4000 row(s) from this bulk ingest were already committed"),
            "the exact earlier-chunk count must survive: {}",
            later.message
        );
        assert!(
            later.message.contains("outcome is unknown"),
            "the failing chunk's ambiguity must still be flagged: {}",
            later.message
        );
    }
}

#[test]
fn mutation_limit_predicate_matches_only_the_too_many_mutations_error() {
    // The real Spanner rejection: INVALID_ARGUMENT with the stable "too many mutations" phrase.
    // `from_spanner` prefixes "Spanner error: ", which the substring match sees through.
    let mut over_limit = err(
        "Spanner error: The transaction contains too many mutations. Insert and update \
         operations count with the multiplicity of the number of columns they affect. …Please \
         reduce the number of writes, or use fewer indexes. (Maximum number: 80000)",
        Status::InvalidArguments,
    );
    over_limit.vendor_code = 3; // INVALID_ARGUMENT
    assert!(is_mutation_limit_exceeded(&over_limit));

    // Right phrase, wrong status: only an INVALID_ARGUMENT is the mutation-limit rejection.
    let mut wrong_status = over_limit.clone();
    wrong_status.status = Status::Internal;
    assert!(!is_mutation_limit_exceeded(&wrong_status));

    // Other INVALID_ARGUMENTs must NOT bisect — they have to propagate so the append/create
    // remaps and the committed-rows annotation still fire.
    for message in [
        "Spanner error: Invalid value for column Foo: expected INT64",
        "Spanner error: Syntax error: Unexpected token",
        "Spanner error: The commit request is too large",
        "Spanner error: Table not found: Nope",
    ] {
        let other = err(message, Status::InvalidArguments);
        assert!(
            !is_mutation_limit_exceeded(&other),
            "must not match: {message}"
        );
    }

    // A duplicate primary key (AlreadyExists) — the most important non-match — never bisects.
    let mut dup = err("Spanner error: row already exists", Status::AlreadyExists);
    dup.vendor_code = 6;
    assert!(!is_mutation_limit_exceeded(&dup));
}

#[test]
fn accepts_only_the_empty_ingest_catalog() {
    // Spanner's single, unnamed catalog is accepted and preserved for round-tripping.
    assert_eq!(check_target_catalog(String::new()).unwrap(), "");
    // Any named catalog is rejected as unsupported.
    let error = check_target_catalog("main".to_string()).unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);
}

#[test]
fn ingest_mode_parses_both_spellings_and_rejects_unknown() {
    let key = OptionStatement::IngestMode;
    // Both the spec's canonical `adbc.ingest.mode.*` spelling and the bare short form parse to
    // the same mode, and the mode reports back (`get_option`) in canonical form.
    for (canonical, short, mode) in [
        ("adbc.ingest.mode.append", "append", IngestMode::Append),
        ("adbc.ingest.mode.create", "create", IngestMode::Create),
        (
            "adbc.ingest.mode.create_append",
            "create_append",
            IngestMode::CreateAppend,
        ),
        ("adbc.ingest.mode.replace", "replace", IngestMode::Replace),
    ] {
        for spelling in [canonical, short] {
            assert_eq!(
                ingest_mode_option(&key, OptionValue::String(spelling.into())).unwrap(),
                mode,
                "spelling {spelling:?}"
            );
        }
        assert_eq!(String::from(mode), canonical);
    }
    // Unknown modes are rejected at set_option time, as unimplemented.
    let error = ingest_mode_option(&key, OptionValue::String("upsert".into())).unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);
    assert!(error.message.contains("ingest mode \"upsert\""), "{error}");
    // Non-string values fail string coercion, naming the option's full key (IDIO-7).
    let error = ingest_mode_option(&key, OptionValue::Int(1)).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("adbc.ingest.mode"), "{error}");
}

#[test]
fn ingest_batch_write_option_coerces_and_unsets_on_empty() {
    // The accepted boolean spellings coerce: exactly the strings "true"/"false".
    assert!(ingest_batch_write_option(OptionValue::String("true".into())).unwrap());
    assert!(!ingest_batch_write_option(OptionValue::String("false".into())).unwrap());
    // Empty / whitespace unsets it, back to the default (false) — never an error.
    for empty in ["", "   "] {
        assert!(!ingest_batch_write_option(OptionValue::String(empty.into())).unwrap());
    }
    // A non-bool string — including the formerly-accepted lenient spellings (COR-7) — and an
    // int-typed set (COR-4) are rejected with InvalidArguments (the shared boolean coercion).
    for bad in ["maybe", "TRUE", "1", "yes", "FALSE", "0", "no"] {
        let error = ingest_batch_write_option(OptionValue::String(bad.into())).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{bad}");
    }
    let error = ingest_batch_write_option(OptionValue::Int(1)).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
}
