//! Offline unit tests for the statement execution guards and option parsers.

use super::*;
use adbc_core::error::Status;

#[test]
fn ingest_temporary_accepts_false_and_rejects_true() {
    let check_ingest_temporary = |value| {
        check_unsupported_true(
            value,
            "option adbc.ingest.temporary",
            "setting adbc.ingest.temporary to true: Spanner has no temporary tables; leave it \
             unset or false",
        )
    };
    // The spec default (`false`, as the exact string) is a no-op.
    check_ingest_temporary(OptionValue::String("false".into())).unwrap();
    // Spanner has no temporary tables: a truthy value is rejected as unimplemented.
    let error = check_ingest_temporary(OptionValue::String("true".into())).unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);
    // Malformed values fail boolean coercion, not the temporary-table check — the
    // formerly-accepted lenient spellings (COR-7) and int-typed sets (COR-4) alike.
    for bad in ["maybe", "FALSE", "0", "no", "TRUE", "1", "yes"] {
        let error = check_ingest_temporary(OptionValue::String(bad.into())).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{bad}");
    }
    for bad in [OptionValue::Int(0), OptionValue::Int(1)] {
        let error = check_ingest_temporary(bad).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
    }
}

#[test]
fn exec_incremental_accepts_false_and_rejects_true() {
    let check_exec_incremental = |value| {
        check_unsupported_true(
            value,
            "option adbc.statement.exec.incremental",
            "setting adbc.statement.exec.incremental to true: incremental execute_partitions is \
             not implemented; leave it unset or false",
        )
    };
    // The spec default (`false`, as the exact string) is a no-op.
    check_exec_incremental(OptionValue::String("false".into())).unwrap();
    // Incremental execution is not implemented: a truthy value is rejected as such.
    let error = check_exec_incremental(OptionValue::String("true".into())).unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);
    // Malformed values fail boolean coercion, not the incremental check — the
    // formerly-accepted lenient spellings (COR-7) and int-typed sets (COR-4) alike.
    for bad in ["maybe", "FALSE", "0", "no", "TRUE", "1", "yes"] {
        let error = check_exec_incremental(OptionValue::String(bad.into())).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{bad}");
    }
    for bad in [OptionValue::Int(0), OptionValue::Int(1)] {
        let error = check_exec_incremental(bad).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
    }
}

#[test]
fn dml_partitioned_option_coerces_and_unsets_on_empty() {
    // Exactly the strings "true"/"false", with empty/whitespace unsetting it (default false).
    assert!(dml_partitioned_option(OptionValue::String("true".into())).unwrap());
    assert!(!dml_partitioned_option(OptionValue::String("false".into())).unwrap());
    for empty in ["", "   "] {
        assert!(!dml_partitioned_option(OptionValue::String(empty.into())).unwrap());
    }
    for bad in ["maybe", "TRUE", "1", "yes", "FALSE", "0", "no"] {
        let error = dml_partitioned_option(OptionValue::String(bad.into())).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{bad}");
    }
    let error = dml_partitioned_option(OptionValue::Int(1)).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
}

#[test]
fn partitioned_dml_accepts_a_single_statement() {
    for sql in [
        "UPDATE Singers SET Active = true WHERE Active = false",
        "DELETE FROM Singers WHERE SingerId > 100",
        // A trailing terminator still splits to one statement.
        "DELETE FROM Singers WHERE SingerId > 100;",
    ] {
        assert!(check_partitioned_dml(sql, true, 0).is_ok(), "{sql}");
    }
    // One bound parameter row is fine — it binds the single statement.
    assert!(check_partitioned_dml("DELETE FROM Singers WHERE SingerId = @id", true, 1).is_ok());
}

#[test]
fn partitioned_dml_rejects_what_it_cannot_express() {
    let sql = "UPDATE Singers SET Active = true WHERE Active = false";
    // Manual transaction mode: partitioned DML cannot join a buffer-and-commit transaction.
    let error = check_partitioned_dml(sql, false, 0).unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    assert!(
        error.message.contains("spanner.dml.partitioned")
            && error.message.contains("manual transaction"),
        "{}",
        error.message
    );
    // A `;`-separated batch: partitioned DML runs exactly one statement.
    let error = check_partitioned_dml(&format!("{sql}; {sql}"), true, 0).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(
        error.message.contains("spanner.dml.partitioned")
            && error.message.contains("batch of 2 statements"),
        "{}",
        error.message
    );
    // THEN RETURN: partitioned DML returns no rows.
    let error = check_partitioned_dml(&format!("{sql} THEN RETURN SingerId"), true, 0).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(
        error.message.contains("spanner.dml.partitioned") && error.message.contains("THEN RETURN"),
        "{}",
        error.message
    );
    // Several bound parameter rows: again, only one statement can run.
    let error = check_partitioned_dml(sql, true, 3).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(
        error.message.contains("spanner.dml.partitioned")
            && error.message.contains("3 parameter rows"),
        "{}",
        error.message
    );
}

#[test]
fn execute_schema_guard_rejects_ddl_and_dml() {
    // Queries — plain, CTE, parenthesised, statement-hinted — pass through to the PLAN probe.
    for sql in [
        "SELECT 1",
        "WITH cte AS (SELECT 1 AS a) SELECT a FROM cte",
        "(SELECT 1)",
        "@{USE_ADDITIONAL_PARALLELISM=true} SELECT 1",
        "GRAPH g MATCH (n) RETURN n.id",
    ] {
        check_schema_query(sql).unwrap_or_else(|e| panic!("query should pass: {sql}: {e}"));
    }
    // DDL is rejected up front with the same `InvalidArguments` as DML — both are the "not a
    // query" class (SPEC-6).
    let error = check_schema_query("CREATE TABLE t (id INT64) PRIMARY KEY (id)").unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    // DML — in any spelling, hinted, or with THEN RETURN — gets a clear `InvalidArguments`
    // instead of Spanner's raw read-only-transaction error from the PLAN probe.
    for sql in [
        "INSERT INTO t (id) VALUES (1)",
        "update t set c = 1 where true",
        "Delete From t Where true",
        "/* comment */ INSERT INTO t (id) VALUES (1)",
        "@{PDML_MAX_PARALLELISM=1} DELETE FROM t WHERE true",
        "INSERT INTO t (id) VALUES (1) THEN RETURN id",
    ] {
        let error = check_schema_query(sql).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{sql}");
        assert!(
            error.message.contains("only supports queries"),
            "unexpected message for {sql}: {}",
            error.message
        );
    }
}

#[test]
fn execute_partitions_guard_rejects_ddl_and_dml() {
    // Queries — plain, CTE, parenthesised, statement-hinted — pass through to partitioning.
    for sql in [
        "SELECT 1",
        "WITH cte AS (SELECT 1 AS a) SELECT a FROM cte",
        "(SELECT 1)",
        "@{USE_ADDITIONAL_PARALLELISM=true} SELECT 1",
        "GRAPH g MATCH (n) RETURN n.id",
    ] {
        check_partition_query(sql).unwrap_or_else(|e| panic!("query should pass: {sql}: {e}"));
    }
    // DDL is rejected up front with the same `InvalidArguments` as DML — both are the "not a
    // query" class (SPEC-6).
    let error = check_partition_query("CREATE TABLE t (id INT64) PRIMARY KEY (id)").unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(
        error.message.contains("execute_partitions"),
        "unexpected message: {}",
        error.message
    );
    // DML — in any spelling, hinted, or with THEN RETURN — gets a clear `InvalidArguments`
    // instead of Spanner's raw read-only-transaction error from `partition_query` (COR-11).
    for sql in [
        "INSERT INTO t (id) VALUES (1)",
        "update t set c = 1 where true",
        "Delete From t Where true",
        "/* comment */ INSERT INTO t (id) VALUES (1)",
        "@{PDML_MAX_PARALLELISM=1} DELETE FROM t WHERE true",
        "INSERT INTO t (id) VALUES (1) THEN RETURN id",
    ] {
        let error = check_partition_query(sql).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{sql}");
        assert!(
            error
                .message
                .contains("execute_partitions only supports queries"),
            "unexpected message for {sql}: {}",
            error.message
        );
    }
}

#[test]
fn all_dml_batch_guard_rejects_mixed_batches_and_passes_single_statements() {
    let split = |sql: &str| crate::sql::split_statements(sql);
    // Single statements always pass — classification of a lone statement is the caller's
    // concern — as do genuine all-DML batches and empty text.
    for sql in [
        "SELECT 1",
        "INSERT INTO t (id) VALUES (1)",
        "DELETE FROM t WHERE true; INSERT INTO t (id) VALUES (1)",
        "@{PDML_MAX_PARALLELISM=1} DELETE FROM t WHERE true; update t set c = 1 where true",
        "",
    ] {
        check_all_dml_batch(&split(sql)).unwrap_or_else(|e| panic!("should pass: {sql}: {e}"));
    }
    // A multi-statement batch mixing DML with a query or DDL is rejected with
    // InvalidArguments, naming the offending statement — whichever side comes first.
    for (sql, offending) in [
        ("DELETE FROM t WHERE true; SELECT 1", "SELECT 1"),
        ("SELECT 1; DELETE FROM t WHERE true", "SELECT 1"),
        ("DELETE FROM t WHERE true; DROP TABLE t", "DROP TABLE t"),
        ("SELECT 1; SELECT 2", "SELECT 1"),
    ] {
        let error = check_all_dml_batch(&split(sql)).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{sql}");
        assert!(
            error.message.contains("all-DML") && error.message.contains(offending),
            "unexpected message for {sql}: {}",
            error.message
        );
    }
    // A `;` inside a literal is not a separator, so this is a single statement and passes.
    check_all_dml_batch(&split("SELECT 'a;b'")).unwrap();
}

#[test]
fn string_option_requires_a_string_value() {
    let key = OptionStatement::TargetTable;
    assert_eq!(
        string_option(&key, OptionValue::String("hi".into())).unwrap(),
        "hi"
    );
    // A non-string value kind is rejected as an invalid argument, and the error names the
    // offending option's full key rather than a generic "statement option" (IDIO-7).
    for value in [OptionValue::Int(1), OptionValue::Double(1.0)] {
        let error = string_option(&key, value).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(
            error.message.contains("adbc.ingest.target_table"),
            "{}",
            error.message
        );
    }
}

#[test]
fn bool_option_parses_exact_true_false() {
    // Only the exact lowercase ADBC canonical spellings are accepted (matching adbc_core's
    // own `TryFrom<OptionValue> for bool` and the reference C++ drivers).
    assert!(bool_option(OptionValue::String("true".into()), "option o").unwrap());
    assert!(!bool_option(OptionValue::String("false".into()), "option o").unwrap());
}

#[test]
fn bool_option_rejects_non_bool_values() {
    // A string that is not exactly "true"/"false" — including the formerly-accepted lenient
    // spellings (case variants, 1/0, yes/no), dropped for ADBC-ecosystem parity (COR-7).
    for bad in [
        "maybe", "", "2", "t", "on", "TRUE", "True", "FALSE", "False", "1", "0", "yes", "no",
        "YES", "NO",
    ] {
        let error = bool_option(OptionValue::String(bad.into()), "option o").unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
    }
    // Non-string value kinds — including int-typed sets (COR-4) — are rejected outright.
    for bad in [
        OptionValue::Int(0),
        OptionValue::Int(1),
        OptionValue::Double(1.0),
    ] {
        let error = bool_option(bad, "option o").unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
    }
}

#[test]
fn bind_by_name_option_parses_as_a_boolean_naming_the_option() {
    // The option is a plain boolean parsed by the shared `bool_option`; an invalid value is
    // rejected with `InvalidArguments`, and the error names the option.
    let what = "option adbc.statement.bind_by_name";
    assert!(crate::options::bool_option(OptionValue::String("true".into()), what).unwrap());
    assert!(!crate::options::bool_option(OptionValue::String("false".into()), what).unwrap());
    for bad in [
        OptionValue::String("maybe".into()),
        // An int-typed set is rejected like any other non-string value (COR-4).
        OptionValue::Int(1),
    ] {
        let error = crate::options::bool_option(bad, what).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(
            error.message.contains("adbc.statement.bind_by_name"),
            "{}",
            error.message
        );
    }
}

#[test]
fn rows_per_batch_option_accepts_positive_ints_and_strings() {
    assert_eq!(rows_per_batch_option(OptionValue::Int(1)).unwrap(), 1);
    assert_eq!(
        rows_per_batch_option(OptionValue::String("8192".into())).unwrap(),
        8192
    );
}

#[test]
fn rows_per_batch_option_rejects_zero_negative_and_malformed() {
    // Zero is explicitly invalid (a batch must hold at least one row).
    let error = rows_per_batch_option(OptionValue::Int(0)).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    // Negatives fail the `usize::try_from` / positivity filter.
    let error = rows_per_batch_option(OptionValue::Int(-8192)).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    // Strings must parse to a positive integer.
    for bad in ["0", "-1", "abc", "1.5", ""] {
        let error = rows_per_batch_option(OptionValue::String(bad.into())).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
    }
    // A non-int, non-string value kind is rejected.
    let error = rows_per_batch_option(OptionValue::Double(3.0)).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
}
