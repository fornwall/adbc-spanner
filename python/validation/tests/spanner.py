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
    driver_name = "ADBC Spanner Driver"
    vendor_name = "Google Cloud Spanner"
    vendor_version = "emulator"
    short_version = "emulator"

    features = model.DriverFeatures(
        connection_get_table_schema=True,
        connection_get_statistics=True,
        connection_transactions=True,
        get_objects=True,
        get_objects_constraints_foreign=True,
        get_objects_constraints_primary=True,
        statement_bind=True,
        statement_bulk_ingest=True,
        statement_execute_schema=True,
        statement_get_parameter_schema=True,
        statement_prepare=True,
        statement_rows_affected=True,
        supported_xdbc_fields=[],
    )

    # database options are filled in by get_quirks() (emulator vs real target).
    setup = model.DriverSetup(
        database={"spanner.database": model.FromEnv("ADBC_SPANNER_DATABASE")},
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return (Path(__file__).parent.parent / "queries" / "spanner",)

    def bind_parameter(self, index: int) -> str:
        # Spanner uses named parameters (@name); the driver binds a batch column
        # to the @<column-name> of the same name. The suite substitutes $N via
        # this hook, so we emit @pN and pair it with a pN-named bind column.
        return f"@p{index}"

    def quote_one_identifier(self, identifier: str) -> str:
        return "`" + identifier.replace("`", "``") + "`"

    def split_statement(self, statement: str) -> list[str]:
        return quirks.split_statement(statement)

    def is_table_not_found(self, table_name, error: Exception) -> bool:
        text = str(error).lower()
        if "table not found" in text or "not found" in text:
            return table_name is None or table_name.lower() in text
        return False


def get_quirks(vendor_version: str | None = None) -> SpannerQuirks:
    q = SpannerQuirks()
    database = {"spanner.database": model.FromEnv("ADBC_SPANNER_DATABASE")}
    if os.environ.get("SPANNER_EMULATOR_HOST"):
        database["spanner.emulator"] = "true"
    q.setup = model.DriverSetup(database=database)
    return q
