"""End-to-end tests exercising the whole Python package against the emulator.

This drives the installed wheel exactly as a user would: open a DBAPI connection,
run DDL/DML, pull results back as Arrow and pandas, bulk-ingest an Arrow table,
and verify the manual buffer-and-commit transaction model. It self-skips without
``SPANNER_EMULATOR_HOST`` (see conftest).
"""

import pytest

pa = pytest.importorskip("pyarrow")

import adbc_driver_spanner
import adbc_driver_spanner.dbapi as spanner
from adbc_driver_manager import ProgrammingError
from adbc_driver_spanner import DatabaseOptions, StatementOptions


def _connect(database, *, autocommit):
    return spanner.connect(
        db_kwargs={
            DatabaseOptions.URI.value: f"spanner:///{database}",
            DatabaseOptions.EMULATOR.value: "true",
        },
        autocommit=autocommit,
    )


def test_driver_library_is_bundled():
    # The wheel under test must ship the native library; a bare source tree wouldn't.
    assert adbc_driver_spanner._driver_path()


def test_ddl_dml_query_roundtrip(emulator_database):
    conn = _connect(emulator_database, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS Singers")
            cur.execute(
                "CREATE TABLE Singers ("
                "  SingerId INT64 NOT NULL,"
                "  FirstName STRING(1024),"
                "  Active BOOL,"
                ") PRIMARY KEY (SingerId)"
            )
            cur.execute(
                "INSERT INTO Singers (SingerId, FirstName, Active) VALUES "
                "(1, 'Alice', true), (2, 'Bob', false)"
            )

            cur.execute("SELECT SingerId, FirstName, Active FROM Singers ORDER BY SingerId")
            table = cur.fetch_arrow_table()

        assert table.num_rows == 2
        assert table.column("SingerId").to_pylist() == [1, 2]
        assert table.column("FirstName").to_pylist() == ["Alice", "Bob"]
        assert table.column("Active").to_pylist() == [True, False]
    finally:
        conn.close()


def test_fetch_dataframe(emulator_database):
    pytest.importorskip("pandas")
    conn = _connect(emulator_database, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS AdbcPyDf")
            cur.execute(
                "CREATE TABLE AdbcPyDf ("
                "  Id INT64 NOT NULL,"
                "  Name STRING(MAX),"
                ") PRIMARY KEY (Id)"
            )
            cur.execute("INSERT INTO AdbcPyDf (Id, Name) VALUES (1, 'Alice'), (2, 'Bob')")

            cur.execute("SELECT Id, Name FROM AdbcPyDf ORDER BY Id")
            df = cur.fetch_df()
        assert df["Id"].tolist() == [1, 2]
        assert list(df["Name"]) == ["Alice", "Bob"]
    finally:
        conn.close()


def test_bulk_ingest_append(emulator_database):
    conn = _connect(emulator_database, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS AdbcPyIngest")
            cur.execute(
                "CREATE TABLE AdbcPyIngest ("
                "  Id INT64 NOT NULL,"
                "  Name STRING(MAX),"
                ") PRIMARY KEY (Id)"
            )

            data = pa.table(
                {
                    "Id": pa.array([10, 20, 30], type=pa.int64()),
                    "Name": pa.array(["x", "y", "z"], type=pa.string()),
                }
            )
            # Only append mode is supported by the driver; the table exists already.
            count = cur.adbc_ingest("AdbcPyIngest", data, mode="append")
            assert count == 3

            cur.execute("SELECT Id, Name FROM AdbcPyIngest ORDER BY Id")
            out = cur.fetch_arrow_table()
        assert out.column("Id").to_pylist() == [10, 20, 30]
        assert out.column("Name").to_pylist() == ["x", "y", "z"]
    finally:
        conn.close()


def test_execute_partitions_round_trip(emulator_database):
    # Partitioned execution: adbc_execute_partitions splits a scan into opaque
    # descriptors and adbc_read_partition reads each back; their union must be the
    # full result set, each row once. Data Boost + max_partitions are statement
    # options set on the underlying ADBC statement (the emulator ignores Data
    # Boost but still accepts the flag, exercising the plumbing).
    conn = _connect(emulator_database, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS AdbcPyPartition")
            cur.execute("CREATE TABLE AdbcPyPartition (Id INT64 NOT NULL) PRIMARY KEY (Id)")
            cur.execute(
                "INSERT INTO AdbcPyPartition (Id) "
                "SELECT n FROM UNNEST(GENERATE_ARRAY(1, 200)) AS n"
            )

        with conn.cursor() as cur:
            cur.adbc_statement.set_options(
                **{
                    StatementOptions.DATA_BOOST.value: "true",
                    StatementOptions.MAX_PARTITIONS.value: "4",
                }
            )
            # A single-table scan is partitionable (no ORDER BY: not partitionable).
            partitions, schema = cur.adbc_execute_partitions(
                "SELECT Id FROM AdbcPyPartition"
            )
        assert len(partitions) >= 1
        assert schema is not None
        assert schema.names == ["Id"]

        # Read every partition back — a fresh cursor per descriptor — and union ids.
        seen: set[int] = set()
        for token in partitions:
            with conn.cursor() as cur:
                cur.adbc_read_partition(token)
                table = cur.fetch_arrow_table()
            assert table.schema.names == ["Id"]
            ids = table.column("Id").to_pylist()
            # No id may appear in more than one partition.
            assert seen.isdisjoint(ids)
            seen.update(ids)

        assert seen == set(range(1, 201))
    finally:
        conn.close()


def test_manual_commit_and_rollback(emulator_database):
    # autocommit off => statements group into manual transactions, each one kind of work
    # (queries or DML) fixed by its first statement. DDL is not transaction-aware: it
    # applies immediately, regardless of the transaction.
    conn = _connect(emulator_database, autocommit=False)
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS AdbcPyTxn")  # DDL runs immediately
            cur.execute("CREATE TABLE AdbcPyTxn (Id INT64 NOT NULL) PRIMARY KEY (Id)")

        with conn.cursor() as cur:
            cur.execute("INSERT INTO AdbcPyTxn (Id) VALUES (1)")
            # Buffered, not yet committed. A query issued while DML is buffered would
            # not observe the pending write (no read-your-writes), so rather than
            # silently returning a stale snapshot the driver rejects it with
            # InvalidState (surfaced as DBAPI ProgrammingError). The buffered INSERT is
            # left intact and still applies on the commit below.
            with pytest.raises(ProgrammingError):
                cur.execute("SELECT COUNT(*) AS n FROM AdbcPyTxn")

        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM AdbcPyTxn")
            assert cur.fetch_arrow_table().column("n").to_pylist() == [1]
        # The count opened a query transaction (a pinned read-only snapshot); end it so the
        # write below starts a fresh transaction.
        conn.rollback()

        # A rolled-back insert must leave the table unchanged.
        with conn.cursor() as cur:
            cur.execute("INSERT INTO AdbcPyTxn (Id) VALUES (2)")
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM AdbcPyTxn")
            assert cur.fetch_arrow_table().column("n").to_pylist() == [1]
    finally:
        conn.close()
