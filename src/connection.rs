//! The [`SpannerConnection`] — an ADBC connection backed by a Spanner [`DatabaseClient`].
//!
//! This driver operates in autocommit mode: every statement runs in its own Spanner transaction
//! (a single-use read-only transaction for queries, a read/write transaction for DML). Manual
//! multi-statement transactions are not supported yet, so [`Connection::commit`] and
//! [`Connection::rollback`] return an error, and disabling autocommit is rejected.

use std::collections::HashSet;
use std::sync::Arc;

use adbc_core::error::{Result, Status};
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionValue};
use adbc_core::{Connection, Optionable};
use arrow_array::{ArrayRef, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray};
use arrow_schema::{DataType, Field, Schema};
use google_cloud_spanner::client::DatabaseClient;

use crate::error::{err, invalid_argument, invalid_state, not_implemented};
use crate::runtime::SharedRuntime;
use crate::statement::SpannerStatement;

/// An ADBC connection to a Spanner database.
pub struct SpannerConnection {
    runtime: SharedRuntime,
    client: DatabaseClient,
    read_only: bool,
}

impl SpannerConnection {
    pub(crate) fn new(runtime: SharedRuntime, client: DatabaseClient) -> Self {
        Self {
            runtime,
            client,
            read_only: false,
        }
    }
}

impl Optionable for SpannerConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match &key {
            OptionConnection::AutoCommit => {
                if !parse_bool(value)? {
                    return Err(not_implemented("disabling autocommit"));
                }
            }
            OptionConnection::ReadOnly => self.read_only = parse_bool(value)?,
            other => {
                return Err(invalid_argument(format!(
                    "unsupported Spanner connection option: {}",
                    connection_option_name(other)
                )))
            }
        }
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match &key {
            OptionConnection::AutoCommit => Ok(true.to_string()),
            OptionConnection::ReadOnly => Ok(self.read_only.to_string()),
            other => Err(err(
                format!("option {} is not set", connection_option_name(other)),
                Status::NotFound,
            )),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        Ok(self.get_option_string(key)?.into_bytes())
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        Err(err(
            format!("option {} is not an integer", connection_option_name(&key)),
            Status::NotFound,
        ))
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        Err(err(
            format!("option {} is not a double", connection_option_name(&key)),
            Status::NotFound,
        ))
    }
}

impl Connection for SpannerConnection {
    type StatementType = SpannerStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        Ok(SpannerStatement::new(
            self.runtime.clone(),
            self.client.clone(),
            self.read_only,
        ))
    }

    fn cancel(&mut self) -> Result<()> {
        Err(not_implemented("Connection::cancel"))
    }

    fn get_info(
        &self,
        _codes: Option<HashSet<InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(not_implemented("Connection::get_info"))
    }

    fn get_objects(
        &self,
        _depth: ObjectDepth,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _table_type: Option<Vec<&str>>,
        _column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(not_implemented("Connection::get_objects"))
    }

    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: &str,
    ) -> Result<Schema> {
        Err(not_implemented("Connection::get_table_schema"))
    }

    /// Return the table types supported by Spanner as a single-column (`table_type: utf8`) batch,
    /// per the ADBC specification.
    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "table_type",
            DataType::Utf8,
            false,
        )]));
        let array = Arc::new(StringArray::from(vec!["TABLE", "VIEW"])) as ArrayRef;
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).map_err(|e| {
            err(
                format!("failed to build table types batch: {e}"),
                Status::Internal,
            )
        })?;
        Ok(Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema)))
    }

    fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(not_implemented("Connection::get_statistic_names"))
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(not_implemented("Connection::get_statistics"))
    }

    fn commit(&mut self) -> Result<()> {
        Err(invalid_state(
            "connection is in autocommit mode; explicit commit is not supported",
        ))
    }

    fn rollback(&mut self) -> Result<()> {
        Err(invalid_state(
            "connection is in autocommit mode; explicit rollback is not supported",
        ))
    }

    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(not_implemented("Connection::read_partition"))
    }
}

fn parse_bool(value: OptionValue) -> Result<bool> {
    match value {
        OptionValue::String(s) => match s.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Ok(true),
            "false" | "0" | "no" => Ok(false),
            other => Err(invalid_argument(format!(
                "expected a boolean, got {other:?}"
            ))),
        },
        OptionValue::Int(i) => Ok(i != 0),
        _ => Err(invalid_argument("expected a boolean option value")),
    }
}

fn connection_option_name(key: &OptionConnection) -> String {
    key.as_ref().to_string()
}
