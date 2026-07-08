//! Building the nested result of [`Connection::get_objects`](adbc_core::Connection::get_objects).
//!
//! The ADBC `get_objects` result is a deeply nested structure:
//! `catalog → list<db_schema → list<table → list<column>>>`. We populate it from Spanner's
//! `INFORMATION_SCHEMA` (a Spanner database is a single, unnamed catalog). Levels strictly below
//! the requested [`ObjectDepth`] are left null via [`new_null_array`]; a list at or above the
//! requested depth is always a valid (possibly **empty**) list — per the ADBC spec, a filter that
//! matches nothing must still yield the parent skeleton with empty lists, never NULL.

use std::collections::HashMap;
use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::ObjectDepth;
use adbc_core::schemas::GET_OBJECTS_SCHEMA;
use arrow_array::{
    new_null_array, Array, ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray, StructArray,
};
use arrow_buffer::NullBuffer;
use arrow_schema::Fields;
use google_cloud_spanner::client::DatabaseClient;

use crate::connection::{like_match, query_batch, str_col};
use crate::error::err;
use crate::nested::{arrow_err, field, list_item, list_of, list_of_nullable, struct_fields};
use crate::runtime::{block_on_cancellable, CancelSignal, SharedRuntime};

/// A column of a table, as returned by `get_objects`.
pub(crate) struct Column {
    pub name: String,
    pub ordinal: i32,
    pub nullable: bool,
    /// The Spanner-native type name (`INFORMATION_SCHEMA.COLUMNS.SPANNER_TYPE`), e.g.
    /// `STRING(MAX)` or `ARRAY<INT64>`; reported as `xdbc_type_name`.
    pub type_name: String,
}

/// A column referenced by a foreign-key constraint (the parent side).
pub(crate) struct Usage {
    pub db_schema: String,
    pub table: String,
    pub column: String,
}

/// A constraint on a table (primary key, foreign key, unique, check).
pub(crate) struct Constraint {
    pub name: Option<String>,
    /// ADBC constraint type string, e.g. `"PRIMARY KEY"`, `"FOREIGN KEY"`.
    pub constraint_type: String,
    /// The constraint's key columns, in key order (empty for e.g. check constraints).
    pub columns: Vec<String>,
    /// For a foreign key, the referenced (parent) columns, in the same order as `columns`.
    /// Empty for non-foreign-key constraints.
    pub usages: Vec<Usage>,
}

/// A table with its columns and constraints.
pub(crate) struct Table {
    pub name: String,
    pub table_type: String,
    pub columns: Vec<Column>,
    pub constraints: Vec<Constraint>,
}

/// A database schema with its tables.
pub(crate) struct DbSchema {
    pub name: String,
    pub tables: Vec<Table>,
}

/// Query `INFORMATION_SCHEMA` and assemble the schema→table→column hierarchy for `get_objects`,
/// applying the ADBC `LIKE`/type filters and the requested depth.
#[allow(clippy::too_many_arguments)]
pub(crate) fn collect_objects(
    runtime: &SharedRuntime,
    client: &DatabaseClient,
    cancel: &CancelSignal,
    depth: ObjectDepth,
    db_schema: Option<&str>,
    table_name: Option<&str>,
    table_type: &Option<Vec<&str>>,
    column_name: Option<&str>,
) -> Result<Vec<DbSchema>> {
    // At catalog depth the result is just the single unnamed catalog with a null db_schemas
    // list — `build` ignores the collected schemas entirely — so skip INFORMATION_SCHEMA
    // (and any RPC) altogether. The remaining depths each fetch only what they populate:
    // Schemas queries SCHEMATA only; Tables adds TABLES; All/Columns add COLUMNS and the
    // constraint tables.
    if depth == ObjectDepth::Catalogs {
        return Ok(Vec::new());
    }
    let populate_tables = matches!(
        depth,
        ObjectDepth::All | ObjectDepth::Tables | ObjectDepth::Columns
    );
    let populate_columns = matches!(depth, ObjectDepth::All | ObjectDepth::Columns);
    let client = client.clone();

    let (
        schema_batch,
        table_batch,
        column_batch,
        constraint_batch,
        key_column_batch,
        referential_batch,
    ) = block_on_cancellable(runtime, cancel, async move {
        let schemas = query_batch(
            &client,
            "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA",
        )
        .await?;
        let tables = if populate_tables {
            Some(
                query_batch(
                    &client,
                    "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES",
                )
                .await?,
            )
        } else {
            None
        };
        let columns = if populate_columns {
            Some(
                query_batch(
                    &client,
                    "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, \
                     IS_NULLABLE, SPANNER_TYPE \
                     FROM INFORMATION_SCHEMA.COLUMNS \
                     ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION",
                )
                .await?,
            )
        } else {
            None
        };
        // Constraints (primary/foreign/unique/check) and their key columns, populated at the
        // same depth as columns. KEY_COLUMN_USAGE covers the key-based constraints; its rows are
        // ordered so each constraint's columns come out in key order. For foreign keys,
        // REFERENTIAL_CONSTRAINTS maps the FK to the referenced unique/primary-key constraint,
        // whose own KEY_COLUMN_USAGE rows give the referenced (parent) columns in order — the
        // ordering CONSTRAINT_COLUMN_USAGE does not preserve.
        let (constraints, key_columns, referential) = if populate_columns {
            (
                Some(
                    query_batch(
                        &client,
                        "SELECT TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE \
                         FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS",
                    )
                    .await?,
                ),
                Some(
                    query_batch(
                        &client,
                        "SELECT CONSTRAINT_SCHEMA, CONSTRAINT_NAME, TABLE_SCHEMA, TABLE_NAME, \
                         COLUMN_NAME, CAST(ORDINAL_POSITION AS STRING), \
                         CAST(POSITION_IN_UNIQUE_CONSTRAINT AS STRING) \
                         FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE \
                         ORDER BY CONSTRAINT_SCHEMA, CONSTRAINT_NAME, ORDINAL_POSITION",
                    )
                    .await?,
                ),
                Some(
                    query_batch(
                        &client,
                        "SELECT CONSTRAINT_SCHEMA, CONSTRAINT_NAME, UNIQUE_CONSTRAINT_SCHEMA, \
                         UNIQUE_CONSTRAINT_NAME \
                         FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS",
                    )
                    .await?,
                ),
            )
        } else {
            (None, None, None)
        };
        Ok::<_, Error>((
            schemas,
            tables,
            columns,
            constraints,
            key_columns,
            referential,
        ))
    })?;

    let schema_names = str_col(&schema_batch, 0)?;

    // Group each INFORMATION_SCHEMA batch ONCE into lookup maps keyed by (schema, table) — and
    // the key/referential batches by (constraint_schema, constraint_name) — so the assembly
    // below is a series of hash lookups rather than a full rescan of each batch per table (which
    // was quadratic for large schemas). This mirrors `collect_statistics`. The per-group `Vec`s
    // keep batch (i.e. `ORDER BY`) order, so column and key-column ordering is preserved exactly.
    let tables_by_schema = match &table_batch {
        Some(batch) => group_tables(batch)?,
        None => HashMap::new(),
    };
    let columns_by_table = match &column_batch {
        Some(batch) => group_columns(batch)?,
        None => HashMap::new(),
    };
    let constraints_by_table = match &constraint_batch {
        Some(batch) => group_constraints(batch)?,
        None => HashMap::new(),
    };
    let key_columns_by_constraint = match &key_column_batch {
        Some(batch) => group_key_columns(batch)?,
        None => HashMap::new(),
    };
    let referential_by_constraint = match &referential_batch {
        Some(batch) => group_referential(batch)?,
        None => HashMap::new(),
    };

    let mut result = Vec::new();
    for i in 0..schema_batch.num_rows() {
        let schema_name = schema_names.value(i);
        if db_schema.is_some_and(|p| !like_match(p, schema_name)) {
            continue;
        }
        let mut tables = Vec::new();
        // `tables_by_schema` is empty unless tables were populated, so this yields no tables at
        // schema-only depth — matching the previous `if let Some(&table_batch)` guard.
        for table in tables_by_schema.get(schema_name).into_iter().flatten() {
            let name = table.name;
            if table_name.is_some_and(|p| !like_match(p, name)) {
                continue;
            }
            let ttype = table.table_type.to_string();
            if table_type
                .as_ref()
                .is_some_and(|types| !types.iter().any(|t| *t == ttype))
            {
                continue;
            }
            // Empty maps (columns/constraints not populated at this depth) yield empty lists,
            // matching the previous depth-gated `match` arms.
            let columns = collect_columns(&columns_by_table, schema_name, name, column_name);
            let constraints = collect_constraints(
                &constraints_by_table,
                &key_columns_by_constraint,
                &referential_by_constraint,
                schema_name,
                name,
            );
            tables.push(Table {
                name: name.to_string(),
                table_type: ttype,
                columns,
                constraints,
            });
        }
        result.push(DbSchema {
            name: schema_name.to_string(),
            tables,
        });
    }
    Ok(result)
}

/// One grouped `INFORMATION_SCHEMA.TABLES` row (per schema group).
struct TableRow<'a> {
    name: &'a str,
    table_type: &'a str,
}

/// Maps each foreign key's (constraint_schema, constraint_name) to the referenced constraint's
/// (unique_schema, unique_name), grouped once from `REFERENTIAL_CONSTRAINTS`.
type ReferentialMap<'a> = HashMap<(&'a str, &'a str), (&'a str, &'a str)>;

/// One grouped `INFORMATION_SCHEMA.COLUMNS` row (per (schema, table) group, in ordinal order).
struct ColumnRow<'a> {
    name: &'a str,
    ordinal: i32,
    nullable: bool,
    type_name: &'a str,
}

/// One grouped `INFORMATION_SCHEMA.TABLE_CONSTRAINTS` row (per (schema, table) group).
struct ConstraintRow<'a> {
    name: &'a str,
    constraint_type: &'a str,
}

/// One grouped `INFORMATION_SCHEMA.KEY_COLUMN_USAGE` row (per (constraint_schema, constraint_name)
/// group, in `ORDINAL_POSITION` order). `position` is `POSITION_IN_UNIQUE_CONSTRAINT`, null except
/// for foreign-key columns.
struct KeyColumnRow<'a> {
    column: &'a str,
    table_schema: &'a str,
    table: &'a str,
    ordinal: &'a str,
    position: Option<&'a str>,
}

/// Group `INFORMATION_SCHEMA.TABLES` rows by `TABLE_SCHEMA`, preserving batch order within a schema.
fn group_tables(batch: &RecordBatch) -> Result<HashMap<&str, Vec<TableRow<'_>>>> {
    let (ts, tn, tt) = (str_col(batch, 0)?, str_col(batch, 1)?, str_col(batch, 2)?);
    let mut map: HashMap<&str, Vec<TableRow>> = HashMap::new();
    for r in 0..batch.num_rows() {
        map.entry(ts.value(r)).or_default().push(TableRow {
            name: tn.value(r),
            table_type: tt.value(r),
        });
    }
    Ok(map)
}

/// Group `INFORMATION_SCHEMA.COLUMNS` rows by (schema, table); each group keeps ordinal order (the
/// query's `ORDER BY`).
fn group_columns(batch: &RecordBatch) -> Result<HashMap<(&str, &str), Vec<ColumnRow<'_>>>> {
    let (ts, tn, cn, nul, typ) = (
        str_col(batch, 0)?,
        str_col(batch, 1)?,
        str_col(batch, 2)?,
        str_col(batch, 4)?,
        str_col(batch, 5)?,
    );
    let ordinal = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| err("ORDINAL_POSITION is not an integer", Status::Internal))?;
    let mut map: HashMap<(&str, &str), Vec<ColumnRow>> = HashMap::new();
    for r in 0..batch.num_rows() {
        map.entry((ts.value(r), tn.value(r)))
            .or_default()
            .push(ColumnRow {
                name: cn.value(r),
                ordinal: ordinal.value(r) as i32,
                nullable: nul.value(r).eq_ignore_ascii_case("YES"),
                type_name: typ.value(r),
            });
    }
    Ok(map)
}

/// Group `INFORMATION_SCHEMA.TABLE_CONSTRAINTS` rows by (schema, table).
fn group_constraints(batch: &RecordBatch) -> Result<HashMap<(&str, &str), Vec<ConstraintRow<'_>>>> {
    let (ts, tn, cn, ct) = (
        str_col(batch, 0)?,
        str_col(batch, 1)?,
        str_col(batch, 2)?,
        str_col(batch, 3)?,
    );
    let mut map: HashMap<(&str, &str), Vec<ConstraintRow>> = HashMap::new();
    for r in 0..batch.num_rows() {
        map.entry((ts.value(r), tn.value(r)))
            .or_default()
            .push(ConstraintRow {
                name: cn.value(r),
                constraint_type: ct.value(r),
            });
    }
    Ok(map)
}

/// Group `INFORMATION_SCHEMA.KEY_COLUMN_USAGE` rows by (constraint_schema, constraint_name); each
/// group keeps `ORDINAL_POSITION` order (the query's `ORDER BY`).
fn group_key_columns(batch: &RecordBatch) -> Result<HashMap<(&str, &str), Vec<KeyColumnRow<'_>>>> {
    // 0 CONSTRAINT_SCHEMA, 1 CONSTRAINT_NAME, 2 TABLE_SCHEMA, 3 TABLE_NAME, 4 COLUMN_NAME,
    // 5 ORDINAL_POSITION, 6 POSITION_IN_UNIQUE_CONSTRAINT.
    let (kcs, kcn, kts, ktn, kcol, kord, kpos) = (
        str_col(batch, 0)?,
        str_col(batch, 1)?,
        str_col(batch, 2)?,
        str_col(batch, 3)?,
        str_col(batch, 4)?,
        str_col(batch, 5)?,
        str_col(batch, 6)?,
    );
    let mut map: HashMap<(&str, &str), Vec<KeyColumnRow>> = HashMap::new();
    for r in 0..batch.num_rows() {
        map.entry((kcs.value(r), kcn.value(r)))
            .or_default()
            .push(KeyColumnRow {
                column: kcol.value(r),
                table_schema: kts.value(r),
                table: ktn.value(r),
                ordinal: kord.value(r),
                position: if kpos.is_null(r) {
                    None
                } else {
                    Some(kpos.value(r))
                },
            });
    }
    Ok(map)
}

/// Group `INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS` by (constraint_schema, constraint_name),
/// mapping each foreign key to its referenced (unique_schema, unique_name). Keeps the first row per
/// key, matching the previous `find`.
fn group_referential(batch: &RecordBatch) -> Result<ReferentialMap<'_>> {
    // 0 CONSTRAINT_SCHEMA, 1 CONSTRAINT_NAME, 2 UNIQUE_CONSTRAINT_SCHEMA, 3 UNIQUE_CONSTRAINT_NAME.
    let (rcs, rcn, rus, run) = (
        str_col(batch, 0)?,
        str_col(batch, 1)?,
        str_col(batch, 2)?,
        str_col(batch, 3)?,
    );
    let mut map: ReferentialMap = HashMap::new();
    for r in 0..batch.num_rows() {
        map.entry((rcs.value(r), rcn.value(r)))
            .or_insert_with(|| (rus.value(r), run.value(r)));
    }
    Ok(map)
}

/// Collect the columns of one table from the pre-grouped `COLUMNS` map, applying the `LIKE` filter.
fn collect_columns<'a>(
    columns_by_table: &HashMap<(&'a str, &'a str), Vec<ColumnRow<'a>>>,
    schema: &'a str,
    table: &'a str,
    filter: Option<&str>,
) -> Vec<Column> {
    let mut columns = Vec::new();
    for column in columns_by_table.get(&(schema, table)).into_iter().flatten() {
        if filter.is_some_and(|p| !like_match(p, column.name)) {
            continue;
        }
        columns.push(Column {
            name: column.name.to_string(),
            ordinal: column.ordinal,
            nullable: column.nullable,
            type_name: column.type_name.to_string(),
        });
    }
    columns
}

/// Assemble the constraints for one table from the pre-grouped `TABLE_CONSTRAINTS`,
/// `KEY_COLUMN_USAGE` and `REFERENTIAL_CONSTRAINTS` maps. Each constraint's key columns come out in
/// key order (the `KEY_COLUMN_USAGE` group keeps `ORDINAL_POSITION` order); check/... constraints
/// have no key columns and get an empty list.
fn collect_constraints<'a>(
    constraints_by_table: &HashMap<(&'a str, &'a str), Vec<ConstraintRow<'a>>>,
    key_columns_by_constraint: &HashMap<(&'a str, &'a str), Vec<KeyColumnRow<'a>>>,
    referential_by_constraint: &ReferentialMap<'a>,
    schema: &'a str,
    table: &'a str,
) -> Vec<Constraint> {
    let mut out = Vec::new();
    for constraint in constraints_by_table
        .get(&(schema, table))
        .into_iter()
        .flatten()
    {
        let name = constraint.name;
        // Columns for this constraint, in key order (the group keeps ORDINAL_POSITION order);
        // check/... constraints have no key columns and get an empty list.
        let columns = key_columns_by_constraint
            .get(&(schema, name))
            .into_iter()
            .flatten()
            .map(|k| k.column.to_string())
            .collect();
        let constraint_type = constraint.constraint_type.to_string();
        let usages = if constraint_type == "FOREIGN KEY" {
            foreign_key_usages(
                key_columns_by_constraint,
                referential_by_constraint,
                schema,
                name,
            )
        } else {
            Vec::new()
        };
        out.push(Constraint {
            name: Some(name.to_string()),
            constraint_type,
            columns,
            usages,
        });
    }
    out
}

/// The referenced (parent) columns of one foreign key, in the same order as its own key columns.
///
/// `CONSTRAINT_COLUMN_USAGE` lists the referenced columns but does not preserve order, so instead:
/// find the referenced unique constraint via `REFERENTIAL_CONSTRAINTS`, index its key columns by
/// ordinal, then walk the FK's own columns (ordered) mapping each through
/// `POSITION_IN_UNIQUE_CONSTRAINT` to the referenced column at that ordinal.
fn foreign_key_usages<'a>(
    key_columns_by_constraint: &HashMap<(&'a str, &'a str), Vec<KeyColumnRow<'a>>>,
    referential_by_constraint: &ReferentialMap<'a>,
    schema: &'a str,
    fk_name: &'a str,
) -> Vec<Usage> {
    // The referenced unique/primary-key constraint.
    let Some(&(unique_schema, unique_name)) = referential_by_constraint.get(&(schema, fk_name))
    else {
        return Vec::new();
    };

    // Index the referenced constraint's key columns by ordinal position.
    let referenced: HashMap<i64, &KeyColumnRow> = key_columns_by_constraint
        .get(&(unique_schema, unique_name))
        .into_iter()
        .flatten()
        .filter_map(|k| Some((k.ordinal.parse::<i64>().ok()?, k)))
        .collect();

    // Walk the FK's own columns in key order, mapping each to its referenced column.
    let mut fk_columns: Vec<(i64, i64)> = key_columns_by_constraint
        .get(&(schema, fk_name))
        .into_iter()
        .flatten()
        .filter_map(|k| Some((k.ordinal.parse().ok()?, k.position?.parse().ok()?)))
        .collect();
    fk_columns.sort_by_key(|&(ordinal, _)| ordinal);

    fk_columns
        .into_iter()
        .filter_map(|(_, position)| {
            referenced.get(&position).map(|k| Usage {
                db_schema: k.table_schema.to_string(),
                table: k.table.to_string(),
                column: k.column.to_string(),
            })
        })
        .collect()
}

/// Build the single-catalog `get_objects` record batch.
pub(crate) fn build(depth: ObjectDepth, schemas: Vec<DbSchema>) -> Result<RecordBatch> {
    let out_schema = GET_OBJECTS_SCHEMA.clone();
    let top_fields = out_schema.fields();

    let populate_schemas = depth != ObjectDepth::Catalogs;
    let populate_tables = matches!(
        depth,
        ObjectDepth::All | ObjectDepth::Tables | ObjectDepth::Columns
    );
    let populate_columns = matches!(depth, ObjectDepth::All | ObjectDepth::Columns);

    // catalog_db_schemas: list<db_schema> — one entry (the single catalog).
    let db_schemas_field = field(top_fields, "catalog_db_schemas")?;
    let db_schema_item = list_item(&db_schemas_field)?;
    let db_schema_fields = struct_fields(&db_schema_item)?;

    let catalog_db_schemas: ArrayRef = if !populate_schemas {
        new_null_array(db_schemas_field.data_type(), 1)
    } else {
        // Build the db_schema struct across all schemas.
        let schema_names: ArrayRef = Arc::new(StringArray::from_iter(
            schemas.iter().map(|s| Some(s.name.clone())),
        ));

        let tables_field = field(&db_schema_fields, "db_schema_tables")?;
        let db_schema_tables: ArrayRef = if !populate_tables {
            new_null_array(tables_field.data_type(), schemas.len())
        } else {
            // Flatten tables across schemas; build the table struct.
            let tables: Vec<&Table> = schemas.iter().flat_map(|s| s.tables.iter()).collect();
            let table_item = list_item(&tables_field)?;
            let table_fields = struct_fields(&table_item)?;

            let table_name: ArrayRef = Arc::new(StringArray::from_iter(
                tables.iter().map(|t| Some(t.name.clone())),
            ));
            let table_type: ArrayRef = Arc::new(StringArray::from_iter(
                tables.iter().map(|t| Some(t.table_type.clone())),
            ));

            let cols_field = field(&table_fields, "table_columns")?;
            let table_columns: ArrayRef = if !populate_columns {
                new_null_array(cols_field.data_type(), tables.len())
            } else {
                let col_item = list_item(&cols_field)?;
                let column_fields = struct_fields(&col_item)?;
                let columns: Vec<&Column> = tables.iter().flat_map(|t| t.columns.iter()).collect();
                let column_struct = build_column_struct(&column_fields, &columns)?;
                let lengths: Vec<usize> = tables.iter().map(|t| t.columns.len()).collect();
                list_of(col_item, &lengths, column_struct)?
            };
            // table_constraints: a non-null (possibly empty) list per table at column depth,
            // otherwise a null list per table (like table_columns).
            let cons_field = field(&table_fields, "table_constraints")?;
            let table_constraints: ArrayRef = if !populate_columns {
                new_null_array(cons_field.data_type(), tables.len())
            } else {
                let cons_item = list_item(&cons_field)?;
                let constraint_fields = struct_fields(&cons_item)?;
                let constraints: Vec<&Constraint> =
                    tables.iter().flat_map(|t| t.constraints.iter()).collect();
                let constraint_struct = build_constraint_struct(&constraint_fields, &constraints)?;
                let lengths: Vec<usize> = tables.iter().map(|t| t.constraints.len()).collect();
                list_of(cons_item, &lengths, constraint_struct)?
            };

            let table_struct: ArrayRef = Arc::new(
                StructArray::try_new(
                    table_fields.clone(),
                    vec![table_name, table_type, table_columns, table_constraints],
                    None,
                )
                .map_err(arrow_err)?,
            );
            let lengths: Vec<usize> = schemas.iter().map(|s| s.tables.len()).collect();
            list_of(table_item, &lengths, table_struct)?
        };

        let db_schema_struct: ArrayRef = Arc::new(
            StructArray::try_new(
                db_schema_fields.clone(),
                vec![schema_names, db_schema_tables],
                None,
            )
            .map_err(arrow_err)?,
        );
        list_of(db_schema_item, &[schemas.len()], db_schema_struct)?
    };

    let catalog_name: ArrayRef = Arc::new(StringArray::from(vec![""]));
    RecordBatch::try_new(out_schema, vec![catalog_name, catalog_db_schemas]).map_err(arrow_err)
}

/// Build the column struct array, populating the fields we know and leaving the rest of the
/// `xdbc_*` metadata null (all nullable in the ADBC schema).
fn build_column_struct(column_fields: &Fields, columns: &[&Column]) -> Result<ArrayRef> {
    let n = columns.len();
    let arrays: Vec<ArrayRef> = column_fields
        .iter()
        .map(|f| match f.name().as_str() {
            "column_name" => Arc::new(StringArray::from_iter(
                columns.iter().map(|c| Some(c.name.clone())),
            )) as ArrayRef,
            "ordinal_position" => Arc::new(Int32Array::from_iter(
                columns.iter().map(|c| Some(c.ordinal)),
            )) as ArrayRef,
            "xdbc_type_name" => Arc::new(StringArray::from_iter(
                columns.iter().map(|c| Some(c.type_name.clone())),
            )) as ArrayRef,
            "xdbc_is_nullable" => Arc::new(StringArray::from_iter(
                columns
                    .iter()
                    .map(|c| Some(if c.nullable { "YES" } else { "NO" })),
            )) as ArrayRef,
            _ => new_null_array(f.data_type(), n),
        })
        .collect();
    Ok(Arc::new(
        StructArray::try_new(column_fields.clone(), arrays, None).map_err(arrow_err)?,
    ))
}

/// Build the constraint struct array: `constraint_name` / `constraint_type` / the key-column list
/// (`constraint_column_names`) / and, for foreign keys, the referenced columns
/// (`constraint_column_usage`).
fn build_constraint_struct(
    constraint_fields: &Fields,
    constraints: &[&Constraint],
) -> Result<ArrayRef> {
    let n = constraints.len();

    // constraint_column_names: one non-null list<utf8> per constraint, its key columns in order.
    let names_field = field(constraint_fields, "constraint_column_names")?;
    let name_item = list_item(&names_field)?;
    let flat_columns: Vec<&str> = constraints
        .iter()
        .flat_map(|c| c.columns.iter().map(String::as_str))
        .collect();
    let column_child: ArrayRef =
        Arc::new(StringArray::from_iter(flat_columns.into_iter().map(Some)));
    let column_lengths: Vec<usize> = constraints.iter().map(|c| c.columns.len()).collect();
    let constraint_column_names = list_of(name_item, &column_lengths, column_child)?;

    // constraint_column_usage: a list<struct> of the referenced (parent) columns for a FOREIGN KEY,
    // and SQL NULL (not an empty list) for every other constraint type — matching the ADBC spec and
    // what the driver validation suite expects for PRIMARY KEY / CHECK / UNIQUE constraints.
    let usage_field = field(constraint_fields, "constraint_column_usage")?;
    let usage_item = list_item(&usage_field)?;
    let usage_fields = struct_fields(&usage_item)?;
    let flat_usages: Vec<&Usage> = constraints.iter().flat_map(|c| c.usages.iter()).collect();
    let usage_struct = build_usage_struct(&usage_fields, &flat_usages)?;
    let usage_lengths: Vec<usize> = constraints.iter().map(|c| c.usages.len()).collect();
    // Only FOREIGN KEY constraints carry a usage list; the rest are NULL.
    let usage_valid: Vec<bool> = constraints
        .iter()
        .map(|c| c.constraint_type == "FOREIGN KEY")
        .collect();
    let constraint_column_usage = list_of_nullable(
        usage_item,
        &usage_lengths,
        usage_struct,
        Some(NullBuffer::from(usage_valid)),
    )?;

    let arrays: Vec<ArrayRef> = constraint_fields
        .iter()
        .map(|f| match f.name().as_str() {
            "constraint_name" => Arc::new(StringArray::from_iter(
                constraints.iter().map(|c| c.name.clone()),
            )) as ArrayRef,
            "constraint_type" => Arc::new(StringArray::from_iter(
                constraints.iter().map(|c| Some(c.constraint_type.clone())),
            )) as ArrayRef,
            "constraint_column_names" => constraint_column_names.clone(),
            "constraint_column_usage" => constraint_column_usage.clone(),
            _ => new_null_array(f.data_type(), n),
        })
        .collect();
    Ok(Arc::new(
        StructArray::try_new(constraint_fields.clone(), arrays, None).map_err(arrow_err)?,
    ))
}

/// Build the foreign-key `constraint_column_usage` struct array (one entry per referenced column):
/// `fk_table` / `fk_column_name` from the parent side, `fk_db_schema` the parent schema, and an
/// empty `fk_catalog` (Spanner has a single unnamed catalog).
fn build_usage_struct(usage_fields: &Fields, usages: &[&Usage]) -> Result<ArrayRef> {
    let n = usages.len();
    let arrays: Vec<ArrayRef> = usage_fields
        .iter()
        .map(|f| match f.name().as_str() {
            "fk_catalog" => {
                Arc::new(StringArray::from_iter(usages.iter().map(|_| Some("")))) as ArrayRef
            }
            "fk_db_schema" => Arc::new(StringArray::from_iter(
                usages.iter().map(|u| Some(u.db_schema.clone())),
            )) as ArrayRef,
            "fk_table" => Arc::new(StringArray::from_iter(
                usages.iter().map(|u| Some(u.table.clone())),
            )) as ArrayRef,
            "fk_column_name" => Arc::new(StringArray::from_iter(
                usages.iter().map(|u| Some(u.column.clone())),
            )) as ArrayRef,
            _ => new_null_array(f.data_type(), n),
        })
        .collect();
    Ok(Arc::new(
        StructArray::try_new(usage_fields.clone(), arrays, None).map_err(arrow_err)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, ListArray};

    fn sample() -> Vec<DbSchema> {
        vec![DbSchema {
            name: "".to_string(),
            tables: vec![Table {
                name: "Users".to_string(),
                table_type: "BASE TABLE".to_string(),
                columns: vec![
                    Column {
                        name: "Id".into(),
                        ordinal: 1,
                        nullable: false,
                        type_name: "INT64".into(),
                    },
                    Column {
                        name: "Name".into(),
                        ordinal: 2,
                        nullable: true,
                        type_name: "STRING(MAX)".into(),
                    },
                ],
                constraints: vec![
                    Constraint {
                        name: Some("PK_Users".into()),
                        constraint_type: "PRIMARY KEY".into(),
                        columns: vec!["Id".into()],
                        usages: Vec::new(),
                    },
                    Constraint {
                        name: Some("FK_Users_Org".into()),
                        constraint_type: "FOREIGN KEY".into(),
                        columns: vec!["OrgId".into()],
                        usages: vec![Usage {
                            db_schema: String::new(),
                            table: "Orgs".into(),
                            column: "Id".into(),
                        }],
                    },
                ],
            }],
        }]
    }

    #[test]
    fn build_full_depth_matches_schema() {
        let batch = build(ObjectDepth::All, sample()).unwrap();
        assert_eq!(batch.schema(), GET_OBJECTS_SCHEMA.clone());
        assert_eq!(batch.num_rows(), 1);
        let schemas = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        // One catalog with a non-null list of db schemas.
        assert!(schemas.is_valid(0));
        assert_eq!(schemas.value(0).len(), 1);
    }

    #[test]
    fn columns_carry_xdbc_type_name_and_nullability() {
        let batch = build(ObjectDepth::All, sample()).unwrap();
        let list = |a: &dyn Array| a.as_any().downcast_ref::<ListArray>().unwrap().value(0);
        let strukt = |a: ArrayRef| a.as_any().downcast_ref::<StructArray>().unwrap().clone();
        let child = |s: &StructArray, name: &str| s.column_by_name(name).unwrap().clone();

        let schemas = strukt(list(batch.column(1).as_ref()));
        let tables = strukt(list(child(&schemas, "db_schema_tables").as_ref()));
        let columns = strukt(list(child(&tables, "table_columns").as_ref()));
        let string_child = |name: &str| {
            child(&columns, name)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone()
        };

        let type_name = string_child("xdbc_type_name");
        assert_eq!(type_name.value(0), "INT64");
        assert_eq!(type_name.value(1), "STRING(MAX)");
        let is_nullable = string_child("xdbc_is_nullable");
        assert_eq!(is_nullable.value(0), "NO");
        assert_eq!(is_nullable.value(1), "YES");
    }

    /// The `catalog_db_schemas` list column of the (single-row) result batch.
    fn schemas_list(batch: &RecordBatch) -> ListArray {
        batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .clone()
    }

    /// The `db_schema_tables` list of the catalog's (valid) `catalog_db_schemas` entry.
    fn tables_list(schemas: &ListArray) -> ListArray {
        let schemas = schemas.value(0);
        let schemas = schemas.as_any().downcast_ref::<StructArray>().unwrap();
        schemas
            .column_by_name("db_schema_tables")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .clone()
    }

    /// The named list child (`table_columns` / `table_constraints`) of the first schema's
    /// (valid) `db_schema_tables` entry.
    fn table_child_list(schemas: &ListArray, name: &str) -> ListArray {
        let tables = tables_list(schemas).value(0);
        let tables = tables.as_any().downcast_ref::<StructArray>().unwrap();
        tables
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .clone()
    }

    #[test]
    fn build_catalogs_depth_leaves_schemas_null() {
        let batch = build(ObjectDepth::Catalogs, sample()).unwrap();
        let schemas = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(schemas.is_null(0));
    }

    #[test]
    fn build_catalogs_depth_ignores_collected_schemas() {
        // `collect_objects` short-circuits at catalog depth and hands `build` an empty Vec
        // without ever querying INFORMATION_SCHEMA. That is only sound if the output is
        // identical to what building from real collected schemas would produce.
        let from_empty = build(ObjectDepth::Catalogs, Vec::new()).unwrap();
        let from_sample = build(ObjectDepth::Catalogs, sample()).unwrap();
        assert_eq!(from_empty, from_sample);
        assert_eq!(from_empty.num_rows(), 1, "single unnamed catalog");
        let name = from_empty
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name.value(0), "");
    }

    /// ADBC depth rule: NULL is reserved for list levels strictly BELOW the requested depth. A
    /// filter matching nothing must still yield the parent skeleton with EMPTY — valid,
    /// zero-length — lists at (and above) the requested depth. DuckDB shipped this wrong (its
    /// `get_objects` returned NULL where the spec requires an empty list) and the adbc-drivers
    /// validation suite caught it (duckdb/duckdb PR #21018); these tests pin our behaviour at
    /// every depth boundary.
    #[test]
    fn schema_filter_matching_nothing_yields_empty_schemas_list_not_null() {
        // A db_schema filter that excludes every schema: `collect_objects` hands `build` an
        // empty Vec. At every depth that requests schemas, the single catalog row must carry an
        // empty catalog_db_schemas list, not NULL.
        for depth in [
            ObjectDepth::Schemas,
            ObjectDepth::Tables,
            ObjectDepth::Columns,
            ObjectDepth::All,
        ] {
            let batch = build(depth, Vec::new()).unwrap();
            assert_eq!(batch.num_rows(), 1, "single catalog at {depth:?}");
            let schemas = schemas_list(&batch);
            assert!(
                schemas.is_valid(0),
                "catalog_db_schemas must not be NULL at {depth:?}"
            );
            assert_eq!(
                schemas.value(0).len(),
                0,
                "catalog_db_schemas must be empty at {depth:?}"
            );
        }
    }

    #[test]
    fn table_filter_matching_nothing_keeps_schema_skeleton_with_empty_table_lists() {
        // The DuckDB corner: a table_name/table_type filter that excludes every table must still
        // return the catalog + db_schema skeleton, with each schema's db_schema_tables an EMPTY
        // list (tables are at/above the requested depth), never NULL.
        for depth in [ObjectDepth::Tables, ObjectDepth::Columns, ObjectDepth::All] {
            let schemas = vec![DbSchema {
                name: "".to_string(),
                tables: Vec::new(),
            }];
            let batch = build(depth, schemas).unwrap();
            let schemas = schemas_list(&batch);
            assert!(schemas.is_valid(0));
            assert_eq!(
                schemas.value(0).len(),
                1,
                "schema skeleton kept at {depth:?}"
            );
            let tables = tables_list(&schemas);
            assert!(
                tables.is_valid(0),
                "db_schema_tables must not be NULL at {depth:?}"
            );
            assert_eq!(
                tables.value(0).len(),
                0,
                "db_schema_tables must be empty at {depth:?}"
            );
        }
    }

    #[test]
    fn column_filter_matching_nothing_yields_empty_column_and_constraint_lists() {
        // A column_name filter that excludes every column (constraints are populated at the same
        // depth and can likewise come out empty): the table row must carry EMPTY table_columns /
        // table_constraints lists at Columns/All depth, never NULL.
        for depth in [ObjectDepth::Columns, ObjectDepth::All] {
            let schemas = vec![DbSchema {
                name: "".to_string(),
                tables: vec![Table {
                    name: "Users".to_string(),
                    table_type: "BASE TABLE".to_string(),
                    columns: Vec::new(),
                    constraints: Vec::new(),
                }],
            }];
            let batch = build(depth, schemas).unwrap();
            let schemas = schemas_list(&batch);
            for name in ["table_columns", "table_constraints"] {
                let list = table_child_list(&schemas, name);
                assert!(list.is_valid(0), "{name} must not be NULL at {depth:?}");
                assert_eq!(list.value(0).len(), 0, "{name} must be empty at {depth:?}");
            }
        }
    }

    #[test]
    fn schemas_depth_leaves_tables_null() {
        // Strictly below the requested depth: at Schemas depth each schema's db_schema_tables is
        // NULL (not an empty list) — the level was not requested.
        let batch = build(ObjectDepth::Schemas, sample()).unwrap();
        let schemas = schemas_list(&batch);
        assert!(schemas.is_valid(0));
        let tables = tables_list(&schemas);
        assert!(
            tables.is_null(0),
            "db_schema_tables must be NULL at Schemas depth"
        );
    }

    #[test]
    fn tables_depth_leaves_columns_and_constraints_null() {
        // Strictly below the requested depth: at Tables depth each table's table_columns and
        // table_constraints are NULL (not empty lists) — the column level was not requested.
        let batch = build(ObjectDepth::Tables, sample()).unwrap();
        let schemas = schemas_list(&batch);
        for name in ["table_columns", "table_constraints"] {
            let list = table_child_list(&schemas, name);
            assert!(list.is_null(0), "{name} must be NULL at Tables depth");
        }
    }

    #[test]
    fn constraint_column_usage_is_null_for_non_foreign_keys() {
        // The sample has one PRIMARY KEY (no usages) and one FOREIGN KEY (one usage). The usage
        // list must be NULL for the primary key and a non-null single-element list for the FK.
        let batch = build(ObjectDepth::All, sample()).unwrap();
        let list = |a: &dyn Array| a.as_any().downcast_ref::<ListArray>().unwrap().value(0);
        let strukt = |a: ArrayRef| a.as_any().downcast_ref::<StructArray>().unwrap().clone();
        let child = |s: &StructArray, name: &str| s.column_by_name(name).unwrap().clone();

        let schemas = strukt(list(batch.column(1).as_ref()));
        let tables = strukt(list(child(&schemas, "db_schema_tables").as_ref()));
        let constraints_list = child(&tables, "table_constraints");
        let constraints = constraints_list
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(0);
        let constraints = constraints.as_any().downcast_ref::<StructArray>().unwrap();

        let ctype = constraints
            .column_by_name("constraint_type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let usage = constraints
            .column_by_name("constraint_column_usage")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        assert_eq!(ctype.value(0), "PRIMARY KEY");
        assert!(usage.is_null(0), "PRIMARY KEY usage must be NULL, not []");
        assert_eq!(ctype.value(1), "FOREIGN KEY");
        assert!(usage.is_valid(1));
        assert_eq!(usage.value(1).len(), 1);
    }
}
