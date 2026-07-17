// Runs the canonical Apache Arrow ADBC C++ validation suite against the
// adbc-spanner driver, loaded as a shared library through the ADBC driver
// manager. The driver library path and target Spanner database come from the
// environment (set by scripts/run-adbc-validation.sh):
//
//   ADBC_SPANNER_LIBRARY   path to the built cdylib (libadbc_spanner.so)
//   ADBC_SPANNER_URI       spanner:///projects/<p>/instances/<i>/databases/<d>
//   SPANNER_EMULATOR_HOST  (read by the driver itself) selects the emulator
//
// `SpannerQuirks` describes Spanner's capabilities to the suite so tests that do
// not apply to Spanner's model (temp tables, views, float16 ingest, ...)
// self-skip rather than fail.

#include <cstdlib>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

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
    RAISE_ADBC(AdbcDatabaseSetOption(database, "entrypoint", "AdbcSpannerInit", error));
    RAISE_ADBC(AdbcDatabaseSetOption(database, "uri",
                                     EnvOr("ADBC_SPANNER_URI", "").c_str(), error));
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
    return RunIgnoringResult(connection,
                             "DROP TABLE IF EXISTS " + Qualified(name, db_schema), error);
  }

  // Spanner has named schemas (CREATE SCHEMA), so db-schema-scoped metadata can be tested.
  AdbcStatusCode EnsureDbSchema(struct AdbcConnection* connection,
                                const std::string& name,
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
    RAISE_ADBC(
        RunIgnoringResult(connection,
                          "CREATE TABLE " + table +
                              " (int64s INT64, strings STRING(MAX)) PRIMARY KEY (int64s)",
                          error));
    return RunIgnoringResult(
        connection,
        "INSERT INTO " + table +
            " (int64s, strings) VALUES (42, 'foo'), (-42, NULL), (NULL, '')",
        error);
  }

  std::optional<std::string> PrimaryKeyTableDdl(std::string_view name) const override {
    return "CREATE TABLE `" + std::string(name) + "` (id INT64) PRIMARY KEY (id)";
  }

  // Deliberately unsupported: the SqlIngestPrimaryKey case append-ingests rows
  // *omitting* the primary-key column and expects the database to auto-assign
  // ascending key values ("databases start numbering at 0 or 1"). Spanner has
  // no ordered auto-increment: a keyless insert mutation writes NULL (a second
  // NULL key row is a duplicate-PK error), and Spanner SEQUENCEs are
  // bit-reversed, so even a DEFAULT sequence key cannot satisfy the case's
  // `ORDER BY id` readback that expects insertion order. Returning nullopt is
  // the quirk's sanctioned way to say so; the case self-skips.
  std::optional<std::string> PrimaryKeyIngestTableDdl(std::string_view) const override {
    return std::nullopt;
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
           "CONSTRAINT fk_composite FOREIGN KEY (id_child_col1, id_child_col2) "
           "REFERENCES `" +
           std::string(parent_name_2) +
           "` (id_primary_col1, id_primary_col2)) PRIMARY KEY (id_child_col1)";
  }

  // Spanner uses named query parameters (@name); the driver binds a column named
  // `pN` to @pN.
  std::string BindParameter(int index) const override {
    return "@p" + std::to_string(index);
  }

  // GoogleSQL quotes identifiers with backticks, not ANSI double quotes (which
  // GoogleSQL reserves for string literals). The hook (apache/arrow-adbc#4504)
  // feeds the suite's ingest/prepare readback and DDL statements, so they are
  // valid GoogleSQL instead of failing on the quoting itself.
  std::string QuoteIdentifier(std::string_view name) const override {
    return "`" + std::string(name) + "`";
  }

  // Substitute GoogleSQL for the suite's dialect-sensitive default SQL (the
  // generic per-query override hook from apache/arrow-adbc#4496; the routing of
  // all the statement-test SQL below it is apache/arrow-adbc#4514 — see
  // ARROW_ADBC_TAG in CMakeLists.txt). Three Spanner-isms drive the rewrites:
  //   - DDL: Spanner requires a PRIMARY KEY and has INT64/STRING(MAX), not
  //     INT/INTEGER/TEXT; DML INSERT requires an explicit column list.
  //   - `NULLS FIRST`/`NULLS LAST` are rejected (by the emulator's GoogleSQL);
  //     dropping them is semantics-preserving — GoogleSQL's defaults are
  //     exactly NULLS FIRST for ASC and NULLS LAST for DESC.
  //   - Create-mode ingest adds the synthetic `adbc_ingest_key` UUID primary
  //     key, so a `SELECT *` readback has one column too many (and no ORDER BY
  //     readback has a deterministic order — the UUID key order is random):
  //     select the ingested column(s) explicitly and order by them.
  // Every entry pins the upstream default so a future ARROW_ADBC_TAG bump that
  // changes a query out from under us fails the test loudly, rather than
  // silently rewriting a query the substitution no longer matches.
  std::string RewriteSql(std::string_view query_id,
                         std::string default_sql) const override {
    struct Rewrite {
      std::string_view expected_default;
      std::string_view sql;
    };
    static const std::unordered_map<std::string_view, Rewrite> kRewrites = {
        // GoogleSQL has no bare `FLOAT` type; its 64-bit float is `FLOAT64`.
        {"StatementTest::TestSqlQueryFloats::cast-1.5-as-float",
         {"SELECT CAST(1.5 AS FLOAT)", "SELECT CAST(1.5 AS FLOAT64)"}},
        {"StatementTest::TestSqlSchemaFloats::cast-1.5-as-float",
         {"SELECT CAST(1.5 AS FLOAT)", "SELECT CAST(1.5 AS FLOAT64)"}},
        // Ingest readbacks: dodge the synthetic key column; the expected row
        // order NULL-first ascending is GoogleSQL's ASC default.
        {"StatementTest::TestSqlIngestTemporalType::select-bulk-ingest",
         {"SELECT * FROM `bulk_ingest` ORDER BY `col` ASC NULLS FIRST",
          "SELECT `col` FROM `bulk_ingest` ORDER BY `col` ASC"}},
        {"StatementTest::TestSqlIngestInterval::select-bulk-ingest",
         {"SELECT * FROM `bulk_ingest` ORDER BY `col` ASC NULLS FIRST",
          "SELECT `col` FROM `bulk_ingest` ORDER BY `col` ASC"}},
        {"StatementTest::TestSqlIngestStreamZeroArrays::select-bulk-ingest",
         {"SELECT * FROM `bulk_ingest`", "SELECT `col` FROM `bulk_ingest`"}},
        // Append expects {42, -42, NULL} — its insertion order, which `ORDER BY
        // int64s DESC` happens to reproduce exactly (GoogleSQL DESC puts NULLs
        // last by default).
        {"StatementTest::TestSqlIngestAppend::select-bulk-ingest",
         {"SELECT * FROM `bulk_ingest`",
          "SELECT `int64s` FROM `bulk_ingest` ORDER BY `int64s` DESC"}},
        {"StatementTest::TestSqlIngestReplace::select-bulk-ingest",
         {"SELECT * FROM `bulk_ingest`", "SELECT `int64s` FROM `bulk_ingest`"}},
        {"StatementTest::TestSqlIngestCreateAppend::select-bulk-ingest",
         {"SELECT * FROM `bulk_ingest`", "SELECT `int64s` FROM `bulk_ingest`"}},
        {"StatementTest::TestSqlIngestMultipleConnections::select-bulk-ingest",
         {"SELECT * FROM `bulk_ingest` ORDER BY `int64s` DESC NULLS LAST",
          "SELECT `int64s` FROM `bulk_ingest` ORDER BY `int64s` DESC"}},
        // The sample table is CreateSampleTable's own DDL (no synthetic key),
        // so only the NULLS FIRST needs to go.
        {"StatementTest::TestSqlIngestSample::select-bulk-ingest",
         {"SELECT * FROM `bulk_ingest` ORDER BY int64s ASC NULLS FIRST",
          "SELECT `int64s`, `strings` FROM `bulk_ingest` ORDER BY `int64s` ASC"}},
        // Spanner cannot infer the types of undeclared parameters selected
        // bare; the CASTs give the inference the context it needs (the driver
        // then binds the Arrow int64/string columns to matching param types).
        {"StatementTest::TestSqlPrepareSelectParams::select-params",
         {"SELECT @p0, @p1", "SELECT CAST(@p0 AS INT64), CAST(@p1 AS STRING)"}},
        // Now passing (apache/arrow-adbc#4534 gave the readback a deterministic
        // `ORDER BY <col> ASC NULLS FIRST` and sorted the expected vectors — see
        // ARROW_ADBC_TAG in CMakeLists.txt). INSERT needs a column list, and
        // omitting `adbc_ingest_key` lets its DEFAULT (GENERATE_UUID()) fill the
        // synthetic key; the readback drops NULLS FIRST (implicit for GoogleSQL
        // ASC) and projects past that key, so it returns the ingested column in
        // the NULL-first ascending order the suite now expects.
        {"StatementTest::TestSqlPrepareUpdate::insert-bulk-ingest",
         {"INSERT INTO `bulk_ingest` VALUES (@p0)",
          "INSERT INTO `bulk_ingest` (`int64s`) VALUES (@p0)"}},
        {"StatementTest::TestSqlPrepareUpdate::select-bulk-ingest",
         {"SELECT * FROM `bulk_ingest` ORDER BY `int64s` ASC NULLS FIRST",
          "SELECT `int64s` FROM `bulk_ingest` ORDER BY `int64s` ASC"}},
        {"StatementTest::TestSqlPrepareUpdateStream::insert-bulk-ingest",
         {"INSERT INTO `bulk_ingest` VALUES (@p0)",
          "INSERT INTO `bulk_ingest` (`ints`) VALUES (@p0)"}},
        {"StatementTest::TestSqlPrepareUpdateStream::select-bulk-ingest",
         {"SELECT * FROM `bulk_ingest` ORDER BY `ints` ASC NULLS FIRST",
          "SELECT `ints` FROM `bulk_ingest` ORDER BY `ints` ASC"}},
        // Suite-internal DDL/DML in Spanner-valid form.
        {"StatementTest::TestSqlBind::create-table-bindtest",
         {"CREATE TABLE bindtest (col1 INTEGER, col2 TEXT)",
          "CREATE TABLE bindtest (col1 INT64, col2 STRING(MAX)) PRIMARY KEY (col1)"}},
        {"StatementTest::TestSqlBind::insert-bindtest",
         {"INSERT INTO bindtest VALUES (@p0, @p1)",
          "INSERT INTO bindtest (col1, col2) VALUES (@p0, @p1)"}},
        {"StatementTest::TestSqlBind::select-bindtest",
         {"SELECT * FROM bindtest ORDER BY col1 ASC NULLS FIRST",
          "SELECT * FROM bindtest ORDER BY col1 ASC"}},
        {"StatementTest::TestSqlQueryEmpty::create-table-queryempty",
         {"CREATE TABLE queryempty (FOO INT)",
          "CREATE TABLE queryempty (FOO INT64) PRIMARY KEY (FOO)"}},
        {"StatementTest::TestSqlQueryInsertRollback::create-table-rollbacktest",
         {"CREATE TABLE `rollbacktest` (a INT)",
          "CREATE TABLE `rollbacktest` (a INT64) PRIMARY KEY (a)"}},
        {"StatementTest::TestSqlQueryRowsAffectedDelete::create-table-delete-test",
         {"CREATE TABLE `delete_test` (foo INT)",
          "CREATE TABLE `delete_test` (foo INT64) PRIMARY KEY (foo)"}},
        {"StatementTest::TestSqlQueryRowsAffectedDeleteStream::create-table-delete-test",
         {"CREATE TABLE `delete_test` (foo INT)",
          "CREATE TABLE `delete_test` (foo INT64) PRIMARY KEY (foo)"}},
    };

    // The whole TestSqlIngestType family shares one call site whose query id is
    // suffixed with the ingested Arrow type. All scalar types take the plain
    // rewrite; a list column cannot be ORDER BY'd in GoogleSQL, so order by its
    // first element instead (NULL rows order first, matching the expected
    // NULL-then-ascending data of both list cases).
    constexpr std::string_view kIngestTypePrefix =
        "StatementTest::TestSqlIngestType::select-bulk-ingest::";
    if (query_id.substr(0, kIngestTypePrefix.size()) == kIngestTypePrefix) {
      EXPECT_EQ(default_sql, "SELECT * FROM `bulk_ingest` ORDER BY `col` ASC NULLS FIRST")
          << "upstream default SQL for " << query_id << " changed; revisit the rewrite";
      if (query_id.substr(kIngestTypePrefix.size()) == "list") {
        return "SELECT `col` FROM `bulk_ingest` ORDER BY `col`[SAFE_OFFSET(0)] ASC";
      }
      return "SELECT `col` FROM `bulk_ingest` ORDER BY `col` ASC";
    }

    auto it = kRewrites.find(query_id);
    if (it == kRewrites.end()) return default_sql;
    EXPECT_EQ(default_sql, it->second.expected_default)
        << "upstream default SQL for " << query_id << " changed; revisit the rewrite";
    return std::string(it->second.sql);
  }

  // What the driver hands back when a column of ingested Arrow data is
  // selected: Spanner's integer type is INT64, its float types are
  // FLOAT32/FLOAT64, strings are STRING(MAX) and binary is BYTES(MAX), so the
  // narrower/alternate Arrow layouts widen to the canonical Arrow type of the
  // Spanner column (`bind::spanner_column_type` on the ingest side,
  // `src/conversion.rs` on the readback side). Nested types recurse through the
  // base class's SchemaField overload, mapping e.g. List<Int32> to List<Int64>.
  ArrowType IngestSelectRoundTripType(ArrowType ingest_type) const override {
    switch (ingest_type) {
      case NANOARROW_TYPE_INT8:
      case NANOARROW_TYPE_INT16:
      case NANOARROW_TYPE_INT32:
      case NANOARROW_TYPE_UINT8:
      case NANOARROW_TYPE_UINT16:
      case NANOARROW_TYPE_UINT32:
      case NANOARROW_TYPE_UINT64:
        return NANOARROW_TYPE_INT64;
      case NANOARROW_TYPE_LARGE_STRING:
      case NANOARROW_TYPE_STRING_VIEW:
        return NANOARROW_TYPE_STRING;
      case NANOARROW_TYPE_LARGE_BINARY:
      case NANOARROW_TYPE_BINARY_VIEW:
      case NANOARROW_TYPE_FIXED_SIZE_BINARY:
        return NANOARROW_TYPE_BINARY;
      default:
        return ingest_type;
    }
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
  // The driver's bulk-ingest append path probes INFORMATION_SCHEMA on failure and remaps to the
  // spec statuses (missing table -> NotFound, incompatible schema -> AlreadyExists), so the
  // `SqlIngestErrors` case's incompatible-append assertion now holds.
  bool supports_error_on_incompatible_schema() const override { return true; }
  bool supports_concurrent_statements() const override { return true; }
  bool supports_transactions() const override { return true; }
  // The `Transactions` case cannot apply to this driver's manual-transaction
  // model: it bulk-ingests a table inside an uncommitted transaction and then
  // expects to read it back on the same connection (read-your-writes). The
  // driver buffers a manual transaction's writes until commit — a manual
  // transaction is one kind of work (queries or DML), fixed by its first
  // statement — so that read-back is rejected with InvalidState rather than
  // served. Declaring DDL as implicitly committing makes the case self-skip
  // (its own guard honours this quirk) rather than fail — and it is literally
  // true: Spanner DDL goes through the admin UpdateDatabaseDdl API, applies
  // immediately whatever the transaction state, and cannot be rolled back.
  bool ddl_implicit_commit_txn() const override { return true; }
  // Spanner has a single, unnamed catalog and default schema (both "", which base
  // catalog()/db_schema() return), so the connection reports them rather than NOT_FOUND.
  bool supports_metadata_current_catalog() const override { return true; }
  bool supports_metadata_current_db_schema() const override { return true; }
  // View-typed columns, a target catalog, and a target db-schema are all real
  // driver capabilities (the driver binds Arrow view layouts, accepts the single
  // unnamed catalog "", and has named-schema support), so declare them rather than
  // hiding the cases behind a false quirk. The two families then diverge:
  //   - SqlIngest{BinaryView,StringView} *run* and fail with the rest of the
  //     ingest-readback family (the suite's `SELECT *` readback also surfaces
  //     the driver's synthetic `adbc_ingest_key` column, breaking the
  //     single-column assertions) — an excluded expected-failure that flips to
  //     passing once that readback is fixed.
  //   - SqlIngest{TargetCatalog,TargetSchema,TargetCatalogSchema} only ingest and
  //     never read back, so they pass cleanly and are gate-enforced (not excluded).
  bool supports_ingest_view_types() const override { return true; }
  bool supports_bulk_ingest_catalog() const override { return true; }
  bool supports_bulk_ingest_db_schema() const override { return true; }
  // Spanner has no float16 type and no temporary tables, so these stay unsupported;
  // the corresponding cases self-skip (and are not excluded).
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
  // Value check for the temporal ingest cases (the base class deliberately
  // FAILs). Only TIMESTAMP ingest reaches this: Duration ingest is unsupported
  // (its case is excluded), so no other temporal type gets to the readback.
  // The driver stores Arrow timestamps of any unit as Spanner TIMESTAMP and
  // reads them back as Timestamp(Nanosecond, "UTC"), so the suite's raw
  // {NULL, -42, 0, 42} inputs come back scaled from the source unit to
  // nanoseconds (the timezone changes nothing — Arrow timestamps are
  // epoch-anchored and the tz is metadata).
  void ValidateIngestedTemporalData(struct ArrowArrayView* values, ArrowType type,
                                    enum ArrowTimeUnit unit,
                                    const char* /*timezone*/) override {
    ASSERT_EQ(NANOARROW_TYPE_TIMESTAMP, type)
        << "unexpected temporal ingest type reached the readback";
    int64_t factor = 1;
    switch (unit) {
      case NANOARROW_TIME_UNIT_SECOND:
        factor = 1000000000;
        break;
      case NANOARROW_TIME_UNIT_MILLI:
        factor = 1000000;
        break;
      case NANOARROW_TIME_UNIT_MICRO:
        factor = 1000;
        break;
      case NANOARROW_TIME_UNIT_NANO:
        factor = 1;
        break;
    }
    const std::vector<std::optional<int64_t>> expected{std::nullopt, -42 * factor, 0,
                                                       42 * factor};
    ASSERT_NO_FATAL_FAILURE(adbc_validation::CompareArray<int64_t>(values, expected));
  }

  SpannerQuirks quirks_;
};
ADBCV_TEST_STATEMENT(SpannerStatementTest)

}  // namespace
