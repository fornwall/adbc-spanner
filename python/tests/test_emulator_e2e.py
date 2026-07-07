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


def _connect(database, *, autocommit):
    return spanner.connect(database=database, emulator=True, autocommit=autocommit)


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


def test_manual_commit_and_rollback(emulator_database):
    # autocommit off => the driver buffers DML and applies it atomically on commit.
    conn = _connect(emulator_database, autocommit=False)
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS AdbcPyTxn")  # DDL runs immediately
            cur.execute("CREATE TABLE AdbcPyTxn (Id INT64 NOT NULL) PRIMARY KEY (Id)")
        conn.commit()

        with conn.cursor() as cur:
            cur.execute("INSERT INTO AdbcPyTxn (Id) VALUES (1)")
            # Buffered, not yet committed: a fresh single-use read must not see it.
            cur.execute("SELECT COUNT(*) AS n FROM AdbcPyTxn")
            assert cur.fetch_arrow_table().column("n").to_pylist() == [0]

        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM AdbcPyTxn")
            assert cur.fetch_arrow_table().column("n").to_pylist() == [1]

        # A rolled-back insert must leave the table unchanged.
        with conn.cursor() as cur:
            cur.execute("INSERT INTO AdbcPyTxn (Id) VALUES (2)")
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM AdbcPyTxn")
            assert cur.fetch_arrow_table().column("n").to_pylist() == [1]
    finally:
        conn.close()
