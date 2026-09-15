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
    vendor_version = None  # the driver reports VendorVersion as a null value (no Spanner server version)
    short_version = "emulator"

    features = model.DriverFeatures(
        connection_get_table_schema=True,
        connection_get_statistics=True,
        connection_transactions=True,
        get_objects=True,
        # The constraint-setup DDL hook (sample_ddl_constraints below) is implemented
        # and the driver reports the constraints faithfully, so both enabled cases
        # pass: _primary because the driver reports constraint_column_usage as NULL
        # for non-FK constraints, and _foreign because the FK tables are created with
        # no PRIMARY KEY clause (Spanner keys them on a hidden `rowid`, whose implicit
        # PK/NOT NULL constraints get_objects omits along with the column itself), so
        # each reports exactly the one FK constraint the suite asserts.
        # Everything else the tests assert matches what the driver reports: the FK
        # shapes are exact, and declared key order is preserved (PRIMARY KEY (b, a)
        # reports ["b", "a"], FOREIGN KEY (c, b) reports ["c", "b"]), so the
        # quirk_get_objects_constraints_*_normalized defaults (False) are correct.
        get_objects_constraints_foreign=True,
        get_objects_constraints_primary=True,
        statement_bind=True,
        statement_bulk_ingest=True,
        statement_execute_schema=True,
        statement_get_parameter_schema=True,
        statement_prepare=True,
        statement_rows_affected=True,
        supported_xdbc_fields=[],
        # The driver reports the database id as the ADBC catalog; the default schema is
        # the unnamed "" of GoogleSQL INFORMATION_SCHEMA.
        current_catalog="adbc-test",
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
            # The suite's default is `CREATE TABLE <quoted_name> (id INT)`; Spanner just
            # needs the native type name. No PRIMARY KEY clause: the test runs
            # `UPDATE ... SET id = id + 1`, which Spanner rejects on a key column, and a
            # keyless table is keyed on a hidden `rowid` instead.
            return default.replace("(id INT)", "(id INT64)")
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
        #
        # The two FK children declare no primary key of their own — the suite asserts
        # each reports exactly one constraint, and a declared key would be a second.
        # Spanner keys them on a hidden `rowid`, which get_objects omits (along with
        # its implicit PK and NOT NULL check constraints).
        return [
            "CREATE TABLE constraint_primary (a INT64, b INT64) PRIMARY KEY (a)",
            "CREATE TABLE constraint_primary_multi (a INT64, b INT64) PRIMARY KEY (b, a)",
            "CREATE TABLE constraint_primary_multi2 (a INT64, b INT64) PRIMARY KEY (a, b)",
            "CREATE TABLE constraint_foreign ("
            " a INT64, b INT64,"
            " CONSTRAINT fk_constraint_foreign FOREIGN KEY (b)"
            " REFERENCES constraint_primary (a)"
            ")",
            "CREATE TABLE constraint_foreign_multi ("
            " a INT64, b INT64, c INT64,"
            " CONSTRAINT fk_constraint_foreign_multi FOREIGN KEY (c, b)"
            " REFERENCES constraint_primary_multi2 (a, b)"
            ")",
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
