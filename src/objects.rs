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
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, FieldRef, Fields};

use crate::error::err;

/// A column of a table, as returned by `get_objects`.
pub(crate) struct Column {
    pub name: String,
    pub ordinal: i32,
    pub nullable: bool,
}

/// A table with its columns.
pub(crate) struct Table {
    pub name: String,
    pub table_type: String,
    pub columns: Vec<Column>,
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
        None,
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
            // table_constraints: not populated → null list per table.
            let table_constraints = new_null_array(
                field(&table_fields, "table_constraints").data_type(),
                tables.len(),
            );

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
}
