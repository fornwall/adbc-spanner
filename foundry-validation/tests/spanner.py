# Driver quirks describing the adbc-spanner driver to the ADBC Driver Foundry
# validation suite (https://github.com/adbc-drivers/validation).
#
# The suite is driver-agnostic: it drives our cdylib through the ADBC driver
# manager and runs a corpus of declarative type/feature cases, so nothing here
# depends on how the driver is implemented (it is not related to driverbase-rs).
import os
from pathlib import Path

from adbc_drivers_validation import model, quirks


class SpannerQuirks(model.DriverQuirks):
    name = "spanner"
    driver = "adbc_spanner"
    # These must match what the driver reports via get_info (see src/lib.rs, src/info.rs).
    driver_name = "adbc-spanner"
    vendor_name = "Google Cloud Spanner"
    vendor_version = ""  # the driver reports no Spanner server version
    short_version = "emulator"

    features = model.DriverFeatures(
        connection_get_table_schema=True,
        connection_get_statistics=True,
        connection_transactions=True,
        get_objects=True,
        # The constraint-setup DDL hook (sample_ddl_constraints below) is implemented
        # and the driver reports the constraints faithfully. _primary now passes (the
        # driver reports constraint_column_usage as NULL for non-FK constraints); only
        # _foreign stays gated off:
        # - _foreign: Spanner mandates a primary key on every table (even
        #   `PRIMARY KEY ()` still yields a PK_<table> row in
        #   INFORMATION_SCHEMA.TABLE_CONSTRAINTS), so the FK tables report the PK
        #   constraint alongside the FK where the suite asserts exactly one
        #   constraint per table.
        # Everything else the tests assert matches what the driver reports: the FK
        # shapes are exact, and declared key order is preserved (PRIMARY KEY (b, a)
        # reports ["b", "a"], FOREIGN KEY (c, b) reports ["c", "b"]), so the
        # quirk_get_objects_constraints_*_normalized defaults (False) are correct.
        get_objects_constraints_foreign=False,
        get_objects_constraints_primary=True,
        statement_bind=True,
        statement_bulk_ingest=True,
        statement_execute_schema=True,
        statement_get_parameter_schema=True,
        statement_prepare=True,
        statement_rows_affected=True,
        # NB: create-mode ingest adds a synthetic UUID primary key (Spanner requires
        # one), which get_objects faithfully lists. The two strict column-list
        # assertions that would trip over it are overridden to filter it out in
        # test_connection.py (TestConnection subclass) rather than via a shared-suite
        # feature flag — see adbc-drivers/validation#250.
        supported_xdbc_fields=[],
        # Spanner's default catalog and schema are both the empty string (GoogleSQL
        # INFORMATION_SCHEMA), which is what get_objects reports.
        current_catalog="",
        current_schema="",
    )

    # database options are filled in by get_quirks() (emulator vs real target).
    setup = model.DriverSetup(
        database={"uri": model.FromEnv("ADBC_SPANNER_URI")},
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return (Path(__file__).parent.parent / "queries" / "spanner",)

    def bind_parameter(self, index: int) -> str:
        # Spanner uses named parameters (@name); the driver binds a batch column
        # to the @<column-name> of the same name. The suite substitutes $N via
        # this hook, so we emit @pN and pair it with a pN-named bind column.
        return f"@p{index}"

    def query_override(self, context: str, default: str) -> str:
        # The suite's sample table uses portable DDL (INT/VARCHAR, no key); Spanner needs a
        # primary key and native type names.
        if context == "TestStatement.sample_table":
            return "CREATE TABLE `sample_table` (id INT64, value STRING(MAX)) PRIMARY KEY (id)"
        if context == "TestStatement.test_rows_affected.create_table":
            # The suite's default is `CREATE TABLE <quoted_name> (id INT)`; Spanner needs
            # a native type and a mandatory primary key. `id` can't be the key: the test
            # runs `UPDATE ... SET id = id + 1`, which Spanner rejects on a key column, so
            # add a synthetic UUID key (defaulted, so the test's `INSERT (id)` still works).
            return default.replace(
                "(id INT)",
                "(id INT64, adbc_pk STRING(36) DEFAULT (GENERATE_UUID()))"
                " PRIMARY KEY (adbc_pk)",
            )
        return super().query_override(context, default)

    def quote_one_identifier(self, identifier: str) -> str:
        return "`" + identifier.replace("`", "``") + "`"

    @property
    def sample_ddl_constraints(self) -> list[str]:
        # Spanner DDL: INT64, PRIMARY KEY clause after the column list, table-level
        # FOREIGN KEY constraints. Parents are created before children. Only the
        # tables needed by the enabled tests (primary/foreign) are created; Spanner
        # has no UNIQUE table constraint (only unique indexes), and the check
        # feature is off.
        return [
            "CREATE TABLE constraint_primary (a INT64, b INT64) PRIMARY KEY (a)",
            "CREATE TABLE constraint_primary_multi (a INT64, b INT64) PRIMARY KEY (b, a)",
            "CREATE TABLE constraint_primary_multi2 (a INT64, b INT64) PRIMARY KEY (a, b)",
            "CREATE TABLE constraint_foreign ("
            " a INT64, b INT64,"
            " CONSTRAINT fk_constraint_foreign FOREIGN KEY (b)"
            " REFERENCES constraint_primary (a)"
            ") PRIMARY KEY (a)",
            "CREATE TABLE constraint_foreign_multi ("
            " a INT64, b INT64, c INT64,"
            " CONSTRAINT fk_constraint_foreign_multi FOREIGN KEY (c, b)"
            " REFERENCES constraint_primary_multi2 (a, b)"
            ") PRIMARY KEY (a)",
        ]

    def split_statement(self, statement: str) -> list[str]:
        return quirks.split_statement(statement)

    def is_table_not_found(self, table_name, error: Exception) -> bool:
        text = str(error).lower()
        if "table not found" in text or "not found" in text:
            return table_name is None or table_name.lower() in text
        return False


def get_quirks(vendor_version: str | None = None) -> SpannerQuirks:
    q = SpannerQuirks()
    database = {"uri": model.FromEnv("ADBC_SPANNER_URI")}
    if os.environ.get("SPANNER_EMULATOR_HOST"):
        database["spanner.emulator"] = "true"
    q.setup = model.DriverSetup(database=database)
    return q
