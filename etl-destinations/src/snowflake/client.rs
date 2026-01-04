//! Snowflake client for database operations.
//!
//! Provides methods for table management and query execution against Snowflake
//! with authentication and error handling.

use arrow::array::Int64Array;
use etl::error::{ErrorKind, EtlError, EtlResult};
use etl::etl_error;
use etl::types::{ColumnSchema, Type, is_array_type};
use snowflake_api::{QueryResult, SnowflakeApi};
use std::fmt;
use tracing::info;

/// Snowflake account identifier.
pub type SnowflakeAccountId = String;
/// Snowflake database identifier.
pub type SnowflakeDatabaseId = String;
/// Snowflake schema identifier.
pub type SnowflakeSchemaId = String;
/// Snowflake table identifier.
pub type SnowflakeTableId = String;
/// Snowflake warehouse identifier.
pub type SnowflakeWarehouseId = String;

/// Client for interacting with Snowflake.
///
/// Provides methods for table management and query execution
/// against Snowflake databases with authentication and error handling.
pub struct SnowflakeClient {
    account_id: SnowflakeAccountId,
    database_id: SnowflakeDatabaseId,
    schema_id: SnowflakeSchemaId,
    api: SnowflakeApi,
}

impl SnowflakeClient {
    /// Creates a new [`SnowflakeClient`] with password authentication.
    ///
    /// Authenticates with Snowflake using username and password credentials.
    pub fn new_with_password(
        account_id: SnowflakeAccountId,
        warehouse_id: Option<SnowflakeWarehouseId>,
        database_id: SnowflakeDatabaseId,
        schema_id: SnowflakeSchemaId,
        username: &str,
        role: Option<&str>,
        password: &str,
    ) -> EtlResult<Self> {
        let api = SnowflakeApi::with_password_auth(
            &account_id,
            warehouse_id.as_deref(),
            Some(&database_id),
            Some(&schema_id),
            username,
            role,
            password,
        )
        .map_err(snowflake_error_to_etl_error)?;

        Ok(Self {
            account_id,
            database_id,
            schema_id,
            api,
        })
    }

    /// Returns the fully qualified Snowflake table name.
    ///
    /// Formats the table name as `database.schema.table` with proper quoting.
    pub fn full_table_name(&self, table_id: &SnowflakeTableId) -> EtlResult<String> {
        let database_id = Self::sanitize_identifier(&self.database_id, "Snowflake database id")?;
        let schema_id = Self::sanitize_identifier(&self.schema_id, "Snowflake schema id")?;
        let table_id = Self::sanitize_identifier(table_id, "Snowflake table id")?;

        Ok(format!("\"{database_id}\".\"{schema_id}\".\"{table_id}\""))
    }

    /// Checks whether a table exists in the Snowflake schema.
    ///
    /// Returns `true` if the table exists, `false` otherwise.
    pub async fn table_exists(&mut self, table_id: &SnowflakeTableId) -> EtlResult<bool> {
        let database_id = Self::sanitize_identifier(&self.database_id, "Snowflake database id")?;
        let schema_id = Self::sanitize_identifier(&self.schema_id, "Snowflake schema id")?;
        let table_id_sanitized = Self::sanitize_identifier(table_id, "Snowflake table id")?;

        let query = format!(
            "SELECT COUNT(*) FROM \"{database_id}\".INFORMATION_SCHEMA.TABLES \
             WHERE TABLE_SCHEMA = '{schema_id}' AND TABLE_NAME = '{table_id_sanitized}'"
        );

        let result = self.execute_query(&query).await?;

        match result {
            QueryResult::Arrow(batches) => {
                if batches.is_empty() {
                    return Ok(false);
                }
                let batch = &batches[0];
                if batch.num_rows() == 0 {
                    return Ok(false);
                }
                // The count column should be the first column.
                let column = batch.column(0);
                if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
                    Ok(arr.value(0) > 0)
                } else {
                    // Fallback: assume table doesn't exist if we can't parse.
                    Ok(false)
                }
            }
            QueryResult::Json(_) | QueryResult::Empty => Ok(false),
        }
    }

    /// Creates a table in Snowflake if it doesn't already exist.
    ///
    /// Returns `true` if the table was created, `false` if it already existed.
    pub async fn create_table_if_missing(
        &mut self,
        table_id: &SnowflakeTableId,
        column_schemas: &[ColumnSchema],
    ) -> EtlResult<bool> {
        if self.table_exists(table_id).await? {
            return Ok(false);
        }

        self.create_table(table_id, column_schemas).await?;
        Ok(true)
    }

    /// Creates a new table in Snowflake.
    ///
    /// Builds and executes a CREATE TABLE statement with the provided column schemas.
    pub async fn create_table(
        &mut self,
        table_id: &SnowflakeTableId,
        column_schemas: &[ColumnSchema],
    ) -> EtlResult<()> {
        let full_table_name = self.full_table_name(table_id)?;
        let columns_spec = Self::create_columns_spec(column_schemas)?;

        info!(%full_table_name, "creating table in snowflake");

        let query = format!("CREATE TABLE {full_table_name} {columns_spec}");
        let _ = self.execute_query(&query).await?;

        Ok(())
    }

    /// Creates or replaces a table in Snowflake.
    ///
    /// Uses CREATE OR REPLACE TABLE statement which atomically replaces the table.
    pub async fn create_or_replace_table(
        &mut self,
        table_id: &SnowflakeTableId,
        column_schemas: &[ColumnSchema],
    ) -> EtlResult<()> {
        let full_table_name = self.full_table_name(table_id)?;
        let columns_spec = Self::create_columns_spec(column_schemas)?;

        info!(%full_table_name, "creating or replacing table in snowflake");

        let query = format!("CREATE OR REPLACE TABLE {full_table_name} {columns_spec}");
        let _ = self.execute_query(&query).await?;

        Ok(())
    }

    /// Truncates all data from a Snowflake table.
    ///
    /// Executes a TRUNCATE TABLE statement to remove all rows while preserving the table structure.
    pub async fn truncate_table(&mut self, table_id: &SnowflakeTableId) -> EtlResult<()> {
        let full_table_name = self.full_table_name(table_id)?;

        info!(%full_table_name, "truncating table in snowflake");

        let query = format!("TRUNCATE TABLE {full_table_name}");
        let _ = self.execute_query(&query).await?;

        Ok(())
    }

    /// Drops a table from Snowflake.
    ///
    /// Executes a DROP TABLE IF EXISTS statement to remove the table.
    pub async fn drop_table(&mut self, table_id: &SnowflakeTableId) -> EtlResult<()> {
        let full_table_name = self.full_table_name(table_id)?;

        info!(%full_table_name, "dropping table from snowflake");

        let query = format!("DROP TABLE IF EXISTS {full_table_name}");
        let _ = self.execute_query(&query).await?;

        Ok(())
    }

    /// Executes a SQL query against Snowflake and returns the result.
    pub async fn execute_query(&mut self, query: &str) -> EtlResult<QueryResult> {
        self.api
            .exec(query)
            .await
            .map_err(snowflake_error_to_etl_error)
    }

    /// Sanitizes a Snowflake identifier for safe double-quote quoting.
    ///
    /// Rejects empty identifiers and identifiers containing control characters. Internal
    /// double quotes are escaped by doubling them.
    fn sanitize_identifier(identifier: &str, context: &str) -> EtlResult<String> {
        if identifier.is_empty() {
            return Err(etl_error!(
                ErrorKind::DestinationTableNameInvalid,
                "Invalid Snowflake identifier",
                format!("{context} cannot be empty")
            ));
        }

        if identifier.chars().any(char::is_control) {
            return Err(etl_error!(
                ErrorKind::DestinationTableNameInvalid,
                "Invalid Snowflake identifier",
                format!("{context} contains control characters")
            ));
        }

        // Escape double quotes by doubling them.
        let escaped = identifier.replace('"', "\"\"");
        Ok(escaped)
    }

    /// Generates SQL column specification for CREATE TABLE statements.
    fn column_spec(column_schema: &ColumnSchema) -> EtlResult<String> {
        let column_name = Self::sanitize_identifier(&column_schema.name, "Snowflake column name")?;

        let mut column_spec = format!(
            "\"{}\" {}",
            column_name,
            Self::postgres_to_snowflake_type(&column_schema.typ)
        );

        if !column_schema.nullable && !is_array_type(&column_schema.typ) {
            column_spec.push_str(" NOT NULL");
        };

        Ok(column_spec)
    }

    /// Builds complete column specifications for CREATE TABLE statements.
    fn create_columns_spec(column_schemas: &[ColumnSchema]) -> EtlResult<String> {
        let specs = column_schemas
            .iter()
            .map(Self::column_spec)
            .collect::<EtlResult<Vec<_>>>()?
            .join(", ");

        Ok(format!("({specs})"))
    }

    /// Converts Postgres data types to Snowflake equivalent types.
    ///
    /// Maps Postgres types to their closest Snowflake equivalents while preserving
    /// data fidelity where possible.
    pub fn postgres_to_snowflake_type(typ: &Type) -> String {
        // Handle array types first.
        if is_array_type(typ) {
            return "ARRAY".to_string();
        }

        match typ {
            // Boolean.
            &Type::BOOL => "BOOLEAN",

            // Character types.
            &Type::CHAR | &Type::BPCHAR | &Type::VARCHAR | &Type::NAME | &Type::TEXT => "VARCHAR",

            // Integer types.
            &Type::INT2 | &Type::INT4 => "INTEGER",
            &Type::INT8 => "BIGINT",

            // Floating point types.
            &Type::FLOAT4 => "FLOAT",
            &Type::FLOAT8 => "DOUBLE",

            // Numeric/decimal - Snowflake NUMBER supports up to 38 digits.
            &Type::NUMERIC => "NUMBER(38, 9)",

            // Date and time types.
            &Type::DATE => "DATE",
            &Type::TIME => "TIME",
            &Type::TIMESTAMP => "TIMESTAMP_NTZ",
            &Type::TIMESTAMPTZ => "TIMESTAMP_TZ",

            // UUID - stored as fixed-length string.
            &Type::UUID => "VARCHAR(36)",

            // JSON types - Snowflake VARIANT is ideal for semi-structured data.
            &Type::JSON | &Type::JSONB => "VARIANT",

            // Binary data.
            &Type::BYTEA => "BINARY",

            // OID - object identifier, stored as integer.
            &Type::OID => "INTEGER",

            // Default fallback for unknown types.
            _ => "VARCHAR",
        }
        .to_string()
    }
}

impl fmt::Debug for SnowflakeClient {
    /// Formats the client for debugging, excluding sensitive client details.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnowflakeClient")
            .field("account_id", &self.account_id)
            .field("database_id", &self.database_id)
            .field("schema_id", &self.schema_id)
            .finish()
    }
}

/// Converts Snowflake API errors to ETL errors with appropriate classification.
fn snowflake_error_to_etl_error(err: snowflake_api::SnowflakeApiError) -> EtlError {
    use snowflake_api::SnowflakeApiError;

    let (kind, description) = match &err {
        SnowflakeApiError::AuthError(_) => {
            (ErrorKind::AuthenticationError, "Snowflake authentication error")
        }
        SnowflakeApiError::RequestError(_) => {
            (ErrorKind::DestinationIoError, "Snowflake request failed")
        }
        SnowflakeApiError::ResponseDeserializationError(_) => {
            (ErrorKind::InvalidData, "Snowflake response deserialization error")
        }
        SnowflakeApiError::ObjectStoreError(_) => {
            (ErrorKind::DestinationIoError, "Snowflake object store error")
        }
        SnowflakeApiError::ArrowError(_) => {
            (ErrorKind::ConversionError, "Snowflake Arrow conversion error")
        }
        _ => (ErrorKind::DestinationError, "Snowflake API error"),
    };

    etl_error!(kind, description, err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgres_to_snowflake_type_boolean() {
        assert_eq!(SnowflakeClient::postgres_to_snowflake_type(&Type::BOOL), "BOOLEAN");
    }

    #[test]
    fn test_postgres_to_snowflake_type_character_types() {
        assert_eq!(SnowflakeClient::postgres_to_snowflake_type(&Type::TEXT), "VARCHAR");
        assert_eq!(SnowflakeClient::postgres_to_snowflake_type(&Type::VARCHAR), "VARCHAR");
        assert_eq!(SnowflakeClient::postgres_to_snowflake_type(&Type::CHAR), "VARCHAR");
        assert_eq!(SnowflakeClient::postgres_to_snowflake_type(&Type::BPCHAR), "VARCHAR");
        assert_eq!(SnowflakeClient::postgres_to_snowflake_type(&Type::NAME), "VARCHAR");
    }

    #[test]
    fn test_postgres_to_snowflake_type_integer_types() {
        assert_eq!(SnowflakeClient::postgres_to_snowflake_type(&Type::INT2), "INTEGER");
        assert_eq!(SnowflakeClient::postgres_to_snowflake_type(&Type::INT4), "INTEGER");
        assert_eq!(SnowflakeClient::postgres_to_snowflake_type(&Type::INT8), "BIGINT");
        assert_eq!(SnowflakeClient::postgres_to_snowflake_type(&Type::OID), "INTEGER");
    }

    #[test]
    fn test_postgres_to_snowflake_type_floating_point() {
        assert_eq!(SnowflakeClient::postgres_to_snowflake_type(&Type::FLOAT4), "FLOAT");
        assert_eq!(SnowflakeClient::postgres_to_snowflake_type(&Type::FLOAT8), "DOUBLE");
    }

    #[test]
    fn test_postgres_to_snowflake_type_numeric() {
        assert_eq!(SnowflakeClient::postgres_to_snowflake_type(&Type::NUMERIC), "NUMBER(38, 9)");
    }

    #[test]
    fn test_postgres_to_snowflake_type_temporal() {
        assert_eq!(SnowflakeClient::postgres_to_snowflake_type(&Type::DATE), "DATE");
        assert_eq!(SnowflakeClient::postgres_to_snowflake_type(&Type::TIME), "TIME");
        assert_eq!(SnowflakeClient::postgres_to_snowflake_type(&Type::TIMESTAMP), "TIMESTAMP_NTZ");
        assert_eq!(SnowflakeClient::postgres_to_snowflake_type(&Type::TIMESTAMPTZ), "TIMESTAMP_TZ");
    }

    #[test]
    fn test_postgres_to_snowflake_type_uuid() {
        assert_eq!(SnowflakeClient::postgres_to_snowflake_type(&Type::UUID), "VARCHAR(36)");
    }

    #[test]
    fn test_postgres_to_snowflake_type_json() {
        assert_eq!(SnowflakeClient::postgres_to_snowflake_type(&Type::JSON), "VARIANT");
        assert_eq!(SnowflakeClient::postgres_to_snowflake_type(&Type::JSONB), "VARIANT");
    }

    #[test]
    fn test_postgres_to_snowflake_type_binary() {
        assert_eq!(SnowflakeClient::postgres_to_snowflake_type(&Type::BYTEA), "BINARY");
    }

    #[test]
    fn test_postgres_to_snowflake_type_arrays() {
        assert_eq!(SnowflakeClient::postgres_to_snowflake_type(&Type::BOOL_ARRAY), "ARRAY");
        assert_eq!(SnowflakeClient::postgres_to_snowflake_type(&Type::TEXT_ARRAY), "ARRAY");
        assert_eq!(SnowflakeClient::postgres_to_snowflake_type(&Type::INT4_ARRAY), "ARRAY");
        assert_eq!(SnowflakeClient::postgres_to_snowflake_type(&Type::FLOAT8_ARRAY), "ARRAY");
        assert_eq!(SnowflakeClient::postgres_to_snowflake_type(&Type::TIMESTAMP_ARRAY), "ARRAY");
    }

    #[test]
    fn test_column_spec_nullable() {
        let column = ColumnSchema::new("test_col".to_string(), Type::TEXT, -1, true, false);
        let spec = SnowflakeClient::column_spec(&column).expect("column spec generation");
        assert_eq!(spec, "\"test_col\" VARCHAR");
    }

    #[test]
    fn test_column_spec_not_null() {
        let column = ColumnSchema::new("id".to_string(), Type::INT4, -1, false, true);
        let spec = SnowflakeClient::column_spec(&column).expect("not null column spec");
        assert_eq!(spec, "\"id\" INTEGER NOT NULL");
    }

    #[test]
    fn test_column_spec_array_always_nullable() {
        // Arrays in Snowflake don't support NOT NULL constraint in the same way.
        let column = ColumnSchema::new("tags".to_string(), Type::TEXT_ARRAY, -1, false, false);
        let spec = SnowflakeClient::column_spec(&column).expect("array column spec");
        assert_eq!(spec, "\"tags\" ARRAY");
    }

    #[test]
    fn test_column_spec_escapes_quotes() {
        let column = ColumnSchema::new("weird\"name".to_string(), Type::TEXT, -1, true, false);
        let spec = SnowflakeClient::column_spec(&column).expect("escaped column spec");
        assert_eq!(spec, "\"weird\"\"name\" VARCHAR");
    }

    #[test]
    fn test_sanitize_identifier_rejects_empty() {
        let result = SnowflakeClient::sanitize_identifier("", "column");
        assert!(matches!(
            result,
            Err(err) if err.kind() == ErrorKind::DestinationTableNameInvalid
        ));
    }

    #[test]
    fn test_sanitize_identifier_rejects_control_chars() {
        let result = SnowflakeClient::sanitize_identifier("bad\nname", "column");
        assert!(matches!(
            result,
            Err(err) if err.kind() == ErrorKind::DestinationTableNameInvalid
        ));
    }

    #[test]
    fn test_sanitize_identifier_escapes_quotes() {
        let result = SnowflakeClient::sanitize_identifier("name\"with\"quotes", "column");
        assert_eq!(result.unwrap(), "name\"\"with\"\"quotes");
    }

    #[test]
    fn test_create_columns_spec() {
        let columns = vec![
            ColumnSchema::new("id".to_string(), Type::INT4, -1, false, true),
            ColumnSchema::new("name".to_string(), Type::TEXT, -1, true, false),
            ColumnSchema::new("active".to_string(), Type::BOOL, -1, false, false),
        ];
        let spec = SnowflakeClient::create_columns_spec(&columns).expect("columns spec");
        assert_eq!(
            spec,
            "(\"id\" INTEGER NOT NULL, \"name\" VARCHAR, \"active\" BOOLEAN NOT NULL)"
        );
    }

    #[test]
    fn test_create_table_query_generation() {
        let columns = vec![
            ColumnSchema::new("id".to_string(), Type::INT8, -1, false, true),
            ColumnSchema::new("data".to_string(), Type::JSONB, -1, true, false),
            ColumnSchema::new("created_at".to_string(), Type::TIMESTAMPTZ, -1, false, false),
        ];
        let columns_spec = SnowflakeClient::create_columns_spec(&columns).unwrap();

        // Verify the structure is correct for a CREATE TABLE statement.
        assert!(columns_spec.starts_with('('));
        assert!(columns_spec.ends_with(')'));
        assert!(columns_spec.contains("\"id\" BIGINT NOT NULL"));
        assert!(columns_spec.contains("\"data\" VARIANT"));
        assert!(columns_spec.contains("\"created_at\" TIMESTAMP_TZ NOT NULL"));
    }
}
