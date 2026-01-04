//! Snowflake data encoding utilities.
//!
//! Provides formatting for Postgres cell values to Snowflake SQL literals.

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use etl::types::{ArrayCell, Cell, TableRow};

/// Wrapper for a table row that can be encoded for Snowflake insertion.
#[derive(Debug)]
pub struct SnowflakeTableRow {
    cells: Vec<Cell>,
}

impl SnowflakeTableRow {
    /// Creates a new [`SnowflakeTableRow`] from a [`TableRow`].
    pub fn new(table_row: TableRow) -> Self {
        Self {
            cells: table_row.values,
        }
    }

    /// Returns the cells in this row.
    pub fn cells(&self) -> &[Cell] {
        &self.cells
    }

    /// Formats all cells as SQL value literals for an INSERT statement.
    ///
    /// Returns a comma-separated list of SQL literals.
    pub fn to_sql_values(&self) -> EtlResult<String> {
        let formatted: Result<Vec<_>, _> = self.cells.iter().map(format_cell_as_sql).collect();
        Ok(formatted?.join(", "))
    }
}

impl TryFrom<TableRow> for SnowflakeTableRow {
    type Error = etl::error::EtlError;

    fn try_from(table_row: TableRow) -> Result<Self, Self::Error> {
        // Validate cells during conversion.
        for (i, cell) in table_row.values.iter().enumerate() {
            validate_cell(cell, i)?;
        }
        Ok(Self::new(table_row))
    }
}

/// Validates a cell value for Snowflake compatibility.
fn validate_cell(cell: &Cell, index: usize) -> EtlResult<()> {
    match cell {
        Cell::F32(v) if v.is_nan() || v.is_infinite() => Err(etl_error!(
            ErrorKind::InvalidData,
            "Snowflake does not support NaN or Infinity",
            format!("Cell at index {index} contains invalid float value: {v}")
        )),
        Cell::F64(v) if v.is_nan() || v.is_infinite() => Err(etl_error!(
            ErrorKind::InvalidData,
            "Snowflake does not support NaN or Infinity",
            format!("Cell at index {index} contains invalid double value: {v}")
        )),
        Cell::Array(array_cell) => validate_array_cell(array_cell, index),
        _ => Ok(()),
    }
}

/// Validates array cell values for Snowflake compatibility.
fn validate_array_cell(array_cell: &ArrayCell, index: usize) -> EtlResult<()> {
    match array_cell {
        ArrayCell::F32(values) => {
            for (j, opt_v) in values.iter().enumerate() {
                if let Some(v) = opt_v {
                    if v.is_nan() || v.is_infinite() {
                        return Err(etl_error!(
                            ErrorKind::InvalidData,
                            "Snowflake does not support NaN or Infinity in arrays",
                            format!(
                                "Array at index {index}, element {j} contains invalid float: {v}"
                            )
                        ));
                    }
                }
            }
            Ok(())
        }
        ArrayCell::F64(values) => {
            for (j, opt_v) in values.iter().enumerate() {
                if let Some(v) = opt_v {
                    if v.is_nan() || v.is_infinite() {
                        return Err(etl_error!(
                            ErrorKind::InvalidData,
                            "Snowflake does not support NaN or Infinity in arrays",
                            format!(
                                "Array at index {index}, element {j} contains invalid double: {v}"
                            )
                        ));
                    }
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Formats a cell value as a Snowflake SQL literal.
fn format_cell_as_sql(cell: &Cell) -> EtlResult<String> {
    match cell {
        Cell::Null => Ok("NULL".to_string()),

        Cell::Bool(v) => Ok(if *v { "TRUE" } else { "FALSE" }.to_string()),

        Cell::I16(v) => Ok(v.to_string()),
        Cell::I32(v) => Ok(v.to_string()),
        Cell::U32(v) => Ok(v.to_string()),
        Cell::I64(v) => Ok(v.to_string()),

        Cell::F32(v) => Ok(v.to_string()),
        Cell::F64(v) => Ok(v.to_string()),

        Cell::Numeric(v) => Ok(v.to_string()),

        Cell::String(v) => Ok(format!("'{}'", escape_sql_string(v))),

        Cell::Bytes(v) => {
            // Snowflake uses hex encoding for binary data.
            let hex = v.iter().map(|b| format!("{b:02X}")).collect::<String>();
            Ok(format!("X'{hex}'"))
        }

        Cell::Date(v) => Ok(format!("'{}'", format_date(v))),

        Cell::Time(v) => Ok(format!("'{}'", format_time(v))),

        Cell::Timestamp(v) => Ok(format!("'{}'", format_timestamp(v))),

        Cell::TimestampTz(v) => Ok(format!("'{}'", format_timestamptz(v))),

        Cell::Uuid(v) => Ok(format!("'{v}'")),

        Cell::Json(v) => {
            // PARSE_JSON function for Snowflake VARIANT type.
            let json_str = serde_json::to_string(v).map_err(|e| {
                etl_error!(
                    ErrorKind::SerializationError,
                    "Failed to serialize JSON",
                    e.to_string()
                )
            })?;
            Ok(format!("PARSE_JSON('{}')", escape_sql_string(&json_str)))
        }

        Cell::Array(array_cell) => format_array_as_sql(array_cell),
    }
}

/// Formats an array cell as a Snowflake ARRAY_CONSTRUCT expression.
fn format_array_as_sql(array_cell: &ArrayCell) -> EtlResult<String> {
    let elements = match array_cell {
        ArrayCell::Bool(values) => format_optional_values(values, |v| {
            Ok(if *v { "TRUE" } else { "FALSE" }.to_string())
        })?,
        ArrayCell::String(values) => {
            format_optional_values(values, |v| Ok(format!("'{}'", escape_sql_string(v))))?
        }
        ArrayCell::I16(values) => format_optional_values(values, |v| Ok(v.to_string()))?,
        ArrayCell::I32(values) => format_optional_values(values, |v| Ok(v.to_string()))?,
        ArrayCell::U32(values) => format_optional_values(values, |v| Ok(v.to_string()))?,
        ArrayCell::I64(values) => format_optional_values(values, |v| Ok(v.to_string()))?,
        ArrayCell::F32(values) => format_optional_values(values, |v| Ok(v.to_string()))?,
        ArrayCell::F64(values) => format_optional_values(values, |v| Ok(v.to_string()))?,
        ArrayCell::Numeric(values) => format_optional_values(values, |v| Ok(v.to_string()))?,
        ArrayCell::Date(values) => {
            format_optional_values(values, |v| Ok(format!("'{}'", format_date(v))))?
        }
        ArrayCell::Time(values) => {
            format_optional_values(values, |v| Ok(format!("'{}'", format_time(v))))?
        }
        ArrayCell::Timestamp(values) => {
            format_optional_values(values, |v| Ok(format!("'{}'", format_timestamp(v))))?
        }
        ArrayCell::TimestampTz(values) => {
            format_optional_values(values, |v| Ok(format!("'{}'", format_timestamptz(v))))?
        }
        ArrayCell::Uuid(values) => {
            format_optional_values(values, |v| Ok(format!("'{v}'")))?
        }
        ArrayCell::Json(values) => format_optional_values(values, |v| {
            let json_str = serde_json::to_string(v).map_err(|e| {
                etl_error!(
                    ErrorKind::SerializationError,
                    "Failed to serialize JSON in array",
                    e.to_string()
                )
            })?;
            Ok(format!("PARSE_JSON('{}')", escape_sql_string(&json_str)))
        })?,
        ArrayCell::Bytes(values) => format_optional_values(values, |v| {
            let hex = v.iter().map(|b| format!("{b:02X}")).collect::<String>();
            Ok(format!("X'{hex}'"))
        })?,
    };

    Ok(format!("ARRAY_CONSTRUCT({})", elements.join(", ")))
}

/// Formats a vector of optional values using the provided formatter.
fn format_optional_values<T, F>(values: &[Option<T>], formatter: F) -> EtlResult<Vec<String>>
where
    F: Fn(&T) -> EtlResult<String>,
{
    values
        .iter()
        .map(|opt| match opt {
            Some(v) => formatter(v),
            None => Ok("NULL".to_string()),
        })
        .collect()
}

/// Escapes a string for use in a Snowflake SQL string literal.
///
/// Single quotes are escaped by doubling them.
fn escape_sql_string(s: &str) -> String {
    s.replace('\'', "''")
}

/// Formats a NaiveDate for Snowflake.
fn format_date(date: &NaiveDate) -> String {
    date.format("%Y-%m-%d").to_string()
}

/// Formats a NaiveTime for Snowflake.
fn format_time(time: &NaiveTime) -> String {
    time.format("%H:%M:%S%.f").to_string()
}

/// Formats a NaiveDateTime for Snowflake.
fn format_timestamp(timestamp: &NaiveDateTime) -> String {
    timestamp.format("%Y-%m-%d %H:%M:%S%.f").to_string()
}

/// Formats a DateTime<Utc> for Snowflake.
fn format_timestamptz(timestamp: &DateTime<Utc>) -> String {
    timestamp.format("%Y-%m-%d %H:%M:%S%.f %:z").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_format_null() {
        assert_eq!(format_cell_as_sql(&Cell::Null).unwrap(), "NULL");
    }

    #[test]
    fn test_format_bool() {
        assert_eq!(format_cell_as_sql(&Cell::Bool(true)).unwrap(), "TRUE");
        assert_eq!(format_cell_as_sql(&Cell::Bool(false)).unwrap(), "FALSE");
    }

    #[test]
    fn test_format_integers() {
        assert_eq!(format_cell_as_sql(&Cell::I16(42)).unwrap(), "42");
        assert_eq!(format_cell_as_sql(&Cell::I32(-100)).unwrap(), "-100");
        assert_eq!(
            format_cell_as_sql(&Cell::I64(9223372036854775807)).unwrap(),
            "9223372036854775807"
        );
        assert_eq!(format_cell_as_sql(&Cell::U32(42)).unwrap(), "42");
    }

    #[test]
    fn test_format_floats() {
        assert_eq!(format_cell_as_sql(&Cell::F32(3.14)).unwrap(), "3.14");
        assert_eq!(
            format_cell_as_sql(&Cell::F64(2.718281828)).unwrap(),
            "2.718281828"
        );
    }

    #[test]
    fn test_format_string() {
        assert_eq!(
            format_cell_as_sql(&Cell::String("hello".to_string())).unwrap(),
            "'hello'"
        );
    }

    #[test]
    fn test_format_string_with_quotes() {
        assert_eq!(
            format_cell_as_sql(&Cell::String("it's a test".to_string())).unwrap(),
            "'it''s a test'"
        );
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(
            format_cell_as_sql(&Cell::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF])).unwrap(),
            "X'DEADBEEF'"
        );
    }

    #[test]
    fn test_format_uuid() {
        let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(
            format_cell_as_sql(&Cell::Uuid(uuid)).unwrap(),
            "'550e8400-e29b-41d4-a716-446655440000'"
        );
    }

    #[test]
    fn test_format_json() {
        let json = serde_json::json!({"key": "value"});
        let result = format_cell_as_sql(&Cell::Json(json)).unwrap();
        assert!(result.starts_with("PARSE_JSON('"));
        assert!(result.ends_with("')"));
    }

    #[test]
    fn test_format_date() {
        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        assert_eq!(format_cell_as_sql(&Cell::Date(date)).unwrap(), "'2024-01-15'");
    }

    #[test]
    fn test_format_time() {
        let time = NaiveTime::from_hms_micro_opt(14, 30, 45, 123456).unwrap();
        let result = format_cell_as_sql(&Cell::Time(time)).unwrap();
        assert!(result.starts_with("'14:30:45"));
    }

    #[test]
    fn test_format_timestamp() {
        let timestamp = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            NaiveTime::from_hms_opt(14, 30, 45).unwrap(),
        );
        let result = format_cell_as_sql(&Cell::Timestamp(timestamp)).unwrap();
        assert!(result.starts_with("'2024-01-15 14:30:45"));
    }

    #[test]
    fn test_format_timestamptz() {
        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 14, 30, 45).unwrap();
        let result = format_cell_as_sql(&Cell::TimestampTz(timestamp)).unwrap();
        assert!(result.contains("2024-01-15"));
        assert!(result.contains("+00:00"));
    }

    #[test]
    fn test_format_array_integers() {
        let array = Cell::Array(ArrayCell::I32(vec![Some(1), Some(2), None, Some(4)]));
        assert_eq!(
            format_cell_as_sql(&array).unwrap(),
            "ARRAY_CONSTRUCT(1, 2, NULL, 4)"
        );
    }

    #[test]
    fn test_format_array_strings() {
        let array = Cell::Array(ArrayCell::String(vec![
            Some("a".to_string()),
            Some("b".to_string()),
        ]));
        assert_eq!(
            format_cell_as_sql(&array).unwrap(),
            "ARRAY_CONSTRUCT('a', 'b')"
        );
    }

    #[test]
    fn test_validate_nan_f32() {
        let result = validate_cell(&Cell::F32(f32::NAN), 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_infinity_f64() {
        let result = validate_cell(&Cell::F64(f64::INFINITY), 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_neg_infinity_f64() {
        let result = validate_cell(&Cell::F64(f64::NEG_INFINITY), 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_array_with_nan() {
        let array = Cell::Array(ArrayCell::F32(vec![Some(1.0), Some(f32::NAN)]));
        let result = validate_cell(&array, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_escape_sql_string() {
        assert_eq!(escape_sql_string("hello"), "hello");
        assert_eq!(escape_sql_string("it's"), "it''s");
        assert_eq!(escape_sql_string("'quoted'"), "''quoted''");
    }

    #[test]
    fn test_snowflake_table_row_to_sql_values() {
        let row = TableRow {
            values: vec![
                Cell::I32(1),
                Cell::String("test".to_string()),
                Cell::Bool(true),
            ],
        };
        let sf_row = SnowflakeTableRow::new(row);
        assert_eq!(sf_row.to_sql_values().unwrap(), "1, 'test', TRUE");
    }
}
