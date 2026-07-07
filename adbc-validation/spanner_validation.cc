// Runs the canonical Apache Arrow ADBC C++ validation suite against the
// adbc-spanner driver, loaded as a shared library through the ADBC driver
// manager. The driver library path and target Spanner database come from the
// environment (set by scripts/run-adbc-validation.sh):
//
//   ADBC_SPANNER_LIBRARY   path to the built cdylib (libadbc_spanner.so)
//   ADBC_SPANNER_DATABASE  projects/<p>/instances/<i>/databases/<d>
//   SPANNER_EMULATOR_HOST  (read by the driver itself) selects the emulator
//
// `SpannerQuirks` describes Spanner's capabilities to the suite so tests that do
// not apply to Spanner's model (temp tables, views, float16 ingest, ...)
// self-skip rather than fail.

#include <cstdlib>
#include <cstring>
#include <string>

#include <arrow-adbc/adbc.h>
#include <gtest/gtest.h>

#include "validation/adbc_validation.h"
#include "validation/adbc_validation_util.h"

namespace {

std::string EnvOr(const char* name, const char* fallback) {
  const char* value = std::getenv(name);
  return value ? std::string(value) : std::string(fallback);
}

class SpannerQuirks : public adbc_validation::DriverQuirks {
 public:
  AdbcStatusCode SetupDatabase(struct AdbcDatabase* database,
                               struct AdbcError* error) const override {
    // Load our cdylib through the driver manager and point it at the target
    // database. The driver auto-detects SPANNER_EMULATOR_HOST from the env.
    RAISE_ADBC(AdbcDatabaseSetOption(database, "driver",
                                     EnvOr("ADBC_SPANNER_LIBRARY", "").c_str(), error));
    RAISE_ADBC(
        AdbcDatabaseSetOption(database, "entrypoint", "AdbcSpannerInit", error));
    RAISE_ADBC(AdbcDatabaseSetOption(database, "uri",
                                     EnvOr("ADBC_SPANNER_DATABASE", "").c_str(), error));
    return ADBC_STATUS_OK;
  }

  // Spanner DDL runs through the admin API; the driver detects and routes it, so
  // an ordinary ExecuteQuery of a DROP/CREATE works here.
  AdbcStatusCode DropTable(struct AdbcConnection* connection, const std::string& name,
                           struct AdbcError* error) const override {
    return RunIgnoringResult(connection, "DROP TABLE IF EXISTS " + Qualified(name, ""),
                             error);
  }

  AdbcStatusCode DropTable(struct AdbcConnection* connection, const std::string& name,
                           const std::string& db_schema,
                           struct AdbcError* error) const override {
    return RunIgnoringResult(
        connection, "DROP TABLE IF EXISTS " + Qualified(name, db_schema), error);
  }

  // Spanner has named schemas (CREATE SCHEMA), so db-schema-scoped metadata can be tested.
  AdbcStatusCode EnsureDbSchema(struct AdbcConnection* connection, const std::string& name,
                                struct AdbcError* error) const override {
    return RunIgnoringResult(connection, "CREATE SCHEMA IF NOT EXISTS `" + name + "`",
                             error);
  }

  // The suite's fixed two-column sample table (int64s INT64, strings STRING).
  // Spanner requires a primary key (and permits NULL key values), so key on
  // int64s and seed the canonical {42, -42, NULL} / {"foo", NULL, ""} rows.
  AdbcStatusCode CreateSampleTable(struct AdbcConnection* connection,
                                   const std::string& name,
                                   struct AdbcError* error) const override {
    return CreateSampleTable(connection, name, "", error);
  }

  AdbcStatusCode CreateSampleTable(struct AdbcConnection* connection,
                                   const std::string& name, const std::string& schema,
                                   struct AdbcError* error) const override {
    const std::string table = Qualified(name, schema);
    RAISE_ADBC(RunIgnoringResult(
        connection,
        "CREATE TABLE " + table + " (int64s INT64, strings STRING(MAX)) PRIMARY KEY (int64s)",
        error));
    return RunIgnoringResult(connection,
                             "INSERT INTO " + table +
                                 " (int64s, strings) VALUES (42, 'foo'), (-42, NULL), (NULL, '')",
                             error);
  }

  std::optional<std::string> PrimaryKeyTableDdl(std::string_view name) const override {
    return "CREATE TABLE `" + std::string(name) + "` (id INT64) PRIMARY KEY (id)";
  }

  std::optional<std::string> PrimaryKeyIngestTableDdl(
      std::string_view name) const override {
    return "CREATE TABLE `" + std::string(name) +
           "` (id INT64, value INT64) PRIMARY KEY (id)";
  }

  std::optional<std::string> CompositePrimaryKeyTableDdl(
      std::string_view name) const override {
    return "CREATE TABLE `" + std::string(name) +
           "` (id_primary_col1 INT64, id_primary_col2 INT64) "
           "PRIMARY KEY (id_primary_col1, id_primary_col2)";
  }

  // A child table with a single-column FK (id_child_col3 -> parent_1.id) and a composite FK
  // ((id_child_col1, id_child_col2) -> parent_2.(id_primary_col1, id_primary_col2)); Spanner
  // supports table-level FOREIGN KEY constraints.
  std::optional<std::string> ForeignKeyChildTableDdl(
      std::string_view child_name, std::string_view parent_name_1,
      std::string_view parent_name_2) const override {
    return "CREATE TABLE `" + std::string(child_name) +
           "` (id_child_col1 INT64, id_child_col2 INT64, id_child_col3 INT64, "
           "CONSTRAINT fk_single FOREIGN KEY (id_child_col3) REFERENCES `" +
           std::string(parent_name_1) +
           "` (id), "
           "CONSTRAINT fk_composite FOREIGN KEY (id_child_col1, id_child_col2) REFERENCES `" +
           std::string(parent_name_2) +
           "` (id_primary_col1, id_primary_col2)) PRIMARY KEY (id_child_col1)";
  }

  // Spanner uses named query parameters (@name); the driver binds a column named
  // `pN` to @pN.
  std::string BindParameter(int index) const override {
    return "@p" + std::to_string(index);
  }

  // The driver supports all four ingest modes; for the create modes it builds
  // the table from the ingest data's Arrow schema with a synthetic
  // `adbc_ingest_key` UUID primary key (Spanner mandates a primary key).
  bool supports_bulk_ingest(const char* mode) const override {
    return std::strcmp(mode, "adbc.ingest.mode.append") == 0 ||
           std::strcmp(mode, "adbc.ingest.mode.create") == 0 ||
           std::strcmp(mode, "adbc.ingest.mode.create_append") == 0 ||
           std::strcmp(mode, "adbc.ingest.mode.replace") == 0;
  }

  bool supports_execute_schema() const override { return true; }
  bool supports_get_sql_info() const override { return true; }
  bool supports_get_objects() const override { return true; }
  bool supports_partitioned_data() const override { return true; }
  bool supports_statistics() const override { return true; }
  bool supports_cancel() const override { return true; }
  bool supports_dynamic_parameter_binding() const override { return true; }
  bool supports_error_on_incompatible_schema() const override { return false; }
  bool supports_concurrent_statements() const override { return true; }
  bool supports_transactions() const override { return true; }
  // Spanner has a single, unnamed catalog and default schema (both "", which base
  // catalog()/db_schema() return), so the connection reports them rather than NOT_FOUND.
  bool supports_metadata_current_catalog() const override { return true; }
  bool supports_metadata_current_db_schema() const override { return true; }
  bool supports_ingest_view_types() const override { return false; }
  bool supports_ingest_float16() const override { return false; }

  std::string catalog() const override { return ""; }
  std::string db_schema() const override { return ""; }

 private:
  // Backtick-quote a table name, optionally qualified by a named schema.
  static std::string Qualified(const std::string& name, const std::string& schema) {
    if (schema.empty()) return "`" + name + "`";
    return "`" + schema + "`.`" + name + "`";
  }

  // Execute a statement purely for its side effect (DDL/DML), discarding any result.
  static AdbcStatusCode RunIgnoringResult(struct AdbcConnection* connection,
                                          const std::string& sql,
                                          struct AdbcError* error) {
    adbc_validation::Handle<struct AdbcStatement> statement;
    RAISE_ADBC(AdbcStatementNew(connection, &statement.value, error));
    RAISE_ADBC(AdbcStatementSetSqlQuery(&statement.value, sql.c_str(), error));
    RAISE_ADBC(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, error));
    return AdbcStatementRelease(&statement.value, error);
  }
};

class SpannerDatabaseTest : public ::testing::Test, public adbc_validation::DatabaseTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

 protected:
  SpannerQuirks quirks_;
};
ADBCV_TEST_DATABASE(SpannerDatabaseTest)

class SpannerConnectionTest : public ::testing::Test,
                              public adbc_validation::ConnectionTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

 protected:
  SpannerQuirks quirks_;
};
ADBCV_TEST_CONNECTION(SpannerConnectionTest)

class SpannerStatementTest : public ::testing::Test,
                             public adbc_validation::StatementTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

 protected:
  SpannerQuirks quirks_;
};
ADBCV_TEST_STATEMENT(SpannerStatementTest)

}  // namespace
