//! Building the nested result of [`Connection::get_objects`](adbc_core::Connection::get_objects).
//!
//! The ADBC `get_objects` result is a deeply nested structure:
//! `catalog → list<db_schema → list<table → list<column>>>`. We populate it from Spanner's
//! `INFORMATION_SCHEMA` (a Spanner database is a single, unnamed catalog). Levels below the
//! requested [`ObjectDepth`] are left null via [`new_null_array`].

use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::ObjectDepth;
use adbc_core::schemas::GET_OBJECTS_SCHEMA;
use arrow_array::{
    new_null_array, ArrayRef, Int32Array, ListArray, RecordBatch, StringArray, StructArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, FieldRef, Fields};

use crate::error::err;

/// A column of a table, as returned by `get_objects`.
pub(crate) struct Column {
    pub name: String,
    pub ordinal: i32,
    pub nullable: bool,
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

fn arrow_err(e: arrow_schema::ArrowError) -> Error {
    err(
        format!("failed to build get_objects batch: {e}"),
        Status::Internal,
    )
}

/// Extract the `Fields` of a named list-of-struct field from a struct's fields.
fn list_struct_fields(fields: &Fields, name: &str) -> Fields {
    match fields.find(name).map(|(_, f)| f.data_type()) {
        Some(DataType::List(inner)) => match inner.data_type() {
            DataType::Struct(fs) => fs.clone(),
            _ => Fields::empty(),
        },
        _ => Fields::empty(),
    }
}

/// The `Field` for a named field within `fields`.
fn field(fields: &Fields, name: &str) -> FieldRef {
    fields
        .find(name)
        .map(|(_, f)| f.clone())
        .expect("adbc_core get_objects schema field")
}

/// Wrap `child` (one entry per element) into a `ListArray` grouping elements by `lengths`, one list
/// per parent. All lists are non-null.
fn list_of(item: FieldRef, lengths: &[usize], child: ArrayRef) -> Result<ArrayRef> {
    list_of_nullable(item, lengths, child, None)
}

/// Like [`list_of`], but marks selected list entries null via `nulls` (a validity mask, one bool
/// per entry — `false` = SQL NULL). A null entry still has a zero-length slice, so its `lengths`
/// value must be 0.
fn list_of_nullable(
    item: FieldRef,
    lengths: &[usize],
    child: ArrayRef,
    nulls: Option<NullBuffer>,
) -> Result<ArrayRef> {
    let mut offsets = Vec::with_capacity(lengths.len() + 1);
    offsets.push(0i32);
    let mut acc = 0i32;
    for len in lengths {
        acc += *len as i32;
        offsets.push(acc);
    }
    let list = ListArray::try_new(
        item,
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        child,
        nulls,
    )
    .map_err(arrow_err)?;
    Ok(Arc::new(list))
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
    let db_schemas_field = field(top_fields, "catalog_db_schemas");
    let db_schema_item = match db_schemas_field.data_type() {
        DataType::List(item) => item.clone(),
        _ => unreachable!("catalog_db_schemas is a list"),
    };
    let db_schema_fields = match db_schema_item.data_type() {
        DataType::Struct(fs) => fs.clone(),
        _ => unreachable!("db_schema is a struct"),
    };

    let catalog_db_schemas: ArrayRef = if !populate_schemas {
        new_null_array(db_schemas_field.data_type(), 1)
    } else {
        // Build the db_schema struct across all schemas.
        let schema_names: ArrayRef = Arc::new(StringArray::from_iter(
            schemas.iter().map(|s| Some(s.name.clone())),
        ));

        let db_schema_tables: ArrayRef = if !populate_tables {
            new_null_array(
                field(&db_schema_fields, "db_schema_tables").data_type(),
                schemas.len(),
            )
        } else {
            // Flatten tables across schemas; build the table struct.
            let tables: Vec<&Table> = schemas.iter().flat_map(|s| s.tables.iter()).collect();
            let tables_field = field(&db_schema_fields, "db_schema_tables");
            let table_item = match tables_field.data_type() {
                DataType::List(item) => item.clone(),
                _ => unreachable!(),
            };
            let table_fields = list_struct_fields(&db_schema_fields, "db_schema_tables");

            let table_name: ArrayRef = Arc::new(StringArray::from_iter(
                tables.iter().map(|t| Some(t.name.clone())),
            ));
            let table_type: ArrayRef = Arc::new(StringArray::from_iter(
                tables.iter().map(|t| Some(t.table_type.clone())),
            ));

            let table_columns: ArrayRef = if !populate_columns {
                new_null_array(
                    field(&table_fields, "table_columns").data_type(),
                    tables.len(),
                )
            } else {
                let cols_field = field(&table_fields, "table_columns");
                let col_item = match cols_field.data_type() {
                    DataType::List(item) => item.clone(),
                    _ => unreachable!(),
                };
                let column_fields = list_struct_fields(&table_fields, "table_columns");
                let columns: Vec<&Column> = tables.iter().flat_map(|t| t.columns.iter()).collect();
                let column_struct = build_column_struct(&column_fields, &columns);
                let lengths: Vec<usize> = tables.iter().map(|t| t.columns.len()).collect();
                list_of(col_item, &lengths, column_struct)?
            };
            // table_constraints: a non-null (possibly empty) list per table at column depth,
            // otherwise a null list per table (like table_columns).
            let table_constraints: ArrayRef = if !populate_columns {
                new_null_array(
                    field(&table_fields, "table_constraints").data_type(),
                    tables.len(),
                )
            } else {
                let cons_field = field(&table_fields, "table_constraints");
                let cons_item = match cons_field.data_type() {
                    DataType::List(item) => item.clone(),
                    _ => unreachable!(),
                };
                let constraint_fields = list_struct_fields(&table_fields, "table_constraints");
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

/// Build the column struct array, populating the fields we know and leaving the `xdbc_*` metadata
/// null (all nullable in the ADBC schema).
fn build_column_struct(column_fields: &Fields, columns: &[&Column]) -> ArrayRef {
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
            "xdbc_is_nullable" => Arc::new(StringArray::from_iter(
                columns
                    .iter()
                    .map(|c| Some(if c.nullable { "YES" } else { "NO" })),
            )) as ArrayRef,
            _ => new_null_array(f.data_type(), n),
        })
        .collect();
    Arc::new(StructArray::new(column_fields.clone(), arrays, None))
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
    let names_field = field(constraint_fields, "constraint_column_names");
    let name_item = match names_field.data_type() {
        DataType::List(item) => item.clone(),
        _ => unreachable!("constraint_column_names is a list"),
    };
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
    let usage_field = field(constraint_fields, "constraint_column_usage");
    let usage_item = match usage_field.data_type() {
        DataType::List(item) => item.clone(),
        _ => unreachable!("constraint_column_usage is a list"),
    };
    let usage_fields = match usage_item.data_type() {
        DataType::Struct(fs) => fs.clone(),
        _ => unreachable!("constraint_column_usage item is a struct"),
    };
    let flat_usages: Vec<&Usage> = constraints.iter().flat_map(|c| c.usages.iter()).collect();
    let usage_struct = build_usage_struct(&usage_fields, &flat_usages);
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
fn build_usage_struct(usage_fields: &Fields, usages: &[&Usage]) -> ArrayRef {
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
    Arc::new(StructArray::new(usage_fields.clone(), arrays, None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;

    fn sample() -> Vec<DbSchema> {
        vec![DbSchema {
            name: "".to_string(),
            tables: vec![Table {
                name: "Users".to_string(),
                table_type: "TABLE".to_string(),
                columns: vec![
                    Column {
                        name: "Id".into(),
                        ordinal: 1,
                        nullable: false,
                    },
                    Column {
                        name: "Name".into(),
                        ordinal: 2,
                        nullable: true,
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
