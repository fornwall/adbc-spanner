import adbc_driver_manager.dbapi
import adbc_drivers_validation.tests.connection
from adbc_drivers_validation import model

from . import spanner

# create-mode bulk ingest injects this synthetic UUID primary key (Spanner
# mandates one; the INSERTs omit it so its DEFAULT (GENERATE_UUID()) fills it —
# see src/statement.rs). get_objects faithfully lists it, so the two suite tests
# that assert the *exact* ingested column list must drop it first.
SYNTHETIC_INGEST_COLUMN = "adbc_ingest_key"


def pytest_generate_tests(metafunc) -> None:
    quirks = [spanner.get_quirks(metafunc.config.getoption("vendor_version"))]
    return adbc_drivers_validation.tests.connection.generate_tests(quirks, metafunc)


class TestConnection(adbc_drivers_validation.tests.connection.TestConnection):
    """Spanner adaptation of the shared connection suite.

    Everything is inherited unchanged except the two ``get_objects`` column-filter
    tests that assert the column list *exactly* equals the ingested columns. The
    ``get_objects_table`` fixture creates its table with ``mode="create"`` ingest,
    which on Spanner adds a synthetic primary-key column (see
    ``SYNTHETIC_INGEST_COLUMN``); we filter it out before the strict assertions.
    Membership-based filter tests (``_catalog`` / ``_schema`` / ``_column_name``)
    already pass and are inherited as-is.

    This keeps the Spanner-only quirk in the driver repo rather than the shared
    suite — see the discussion on adbc-drivers/validation#250.
    """

    @staticmethod
    def _columns(objects: list) -> list:
        columns = [
            (
                obj["catalog_name"],
                schema["db_schema_name"],
                table["table_name"],
                column["column_name"],
            )
            for obj in objects
            for schema in obj["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
            for column in table["table_columns"]
        ]
        return [c for c in columns if c[-1] != SYNTHETIC_INGEST_COLUMN]

    def test_get_objects_column_filter_table_name(
        self,
        conn: adbc_driver_manager.dbapi.Connection,
        driver: model.DriverQuirks,
        get_objects_table: tuple[str | None, str | None, str],
    ) -> None:
        table_id = get_objects_table
        objects = (
            conn.adbc_get_objects(depth="columns", table_name_filter=table_id[-1])
            .read_all()
            .to_pylist()
        )
        columns = self._columns(objects)
        assert list(sorted(set(columns))) == list(sorted(columns))
        assert (*table_id, "ints") in columns
        assert (*table_id, "strs") in columns
        assert len(columns) == 2

    def test_get_objects_column_filter_table(
        self,
        conn: adbc_driver_manager.dbapi.Connection,
        driver: model.DriverQuirks,
        get_objects_table: tuple[str | None, str | None, str],
    ) -> None:
        table_id = get_objects_table
        objects = (
            conn.adbc_get_objects(
                depth="columns",
                catalog_filter=driver.features.current_catalog,
                db_schema_filter=driver.features.current_schema,
                table_name_filter=table_id[-1],
            )
            .read_all()
            .to_pylist()
        )
        columns = self._columns(objects)
        assert list(sorted(set(columns))) == list(sorted(columns))
        assert columns == [(*table_id, "ints"), (*table_id, "strs")]
