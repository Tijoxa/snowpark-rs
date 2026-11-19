use std::{collections::HashMap, sync::Arc};

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, RecordBatch,
    StringArray, Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef, TimeUnit};
use chrono::{DateTime, Timelike, Utc};
use snowflake_connector_rs::{CellValue, ColumnType, DynamicRow, ResultTable};

use super::ExecuteError;

pub(super) fn schema(source: &snowflake_connector_rs::Schema) -> Result<SchemaRef, ArrowError> {
    let fields = source
        .columns()
        .iter()
        .map(|column| {
            let ty = column.ty();
            let data_type = match ty {
                ColumnType::Fixed { .. } => {
                    let precision = ty.precision().unwrap_or(38);
                    let scale = ty.scale().unwrap_or(0);
                    if !(1..=38).contains(&precision) || scale > precision {
                        return Err(ArrowError::SchemaError(format!(
                            "Invalid Snowflake precision or scale for column {}",
                            column.name()
                        )));
                    }
                    DataType::Decimal128(precision, scale as i8)
                }
                ColumnType::Real => DataType::Float64,
                ColumnType::Boolean => DataType::Boolean,
                ColumnType::Date => DataType::Date32,
                ColumnType::Time { .. } => DataType::Time64(TimeUnit::Nanosecond),
                ColumnType::TimestampNtz { .. } => DataType::Timestamp(timestamp_unit(ty), None),
                ColumnType::TimestampLtz { .. } => {
                    DataType::Timestamp(timestamp_unit(ty), Some("UTC".into()))
                }
                ColumnType::TimestampTz { .. } => {
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
                }
                ColumnType::Binary { .. } => DataType::Binary,
                _ => DataType::Utf8,
            };
            let mut metadata =
                HashMap::from([("snowflake:type".to_owned(), ty.as_str().to_owned())]);
            if let Some(scale) = ty.scale() {
                metadata.insert("snowflake:scale".into(), scale.to_string());
            }
            Ok(Field::new(column.name(), data_type, column.is_nullable()).with_metadata(metadata))
        })
        .collect::<Result<Vec<_>, ArrowError>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn timestamp_unit(ty: &ColumnType) -> TimeUnit {
    match ty.scale().unwrap_or(9) {
        0 => TimeUnit::Second,
        1..=3 => TimeUnit::Millisecond,
        4..=6 => TimeUnit::Microsecond,
        _ => TimeUnit::Nanosecond,
    }
}

pub(super) fn batch(table: &ResultTable, schema: SchemaRef) -> Result<RecordBatch, ExecuteError> {
    let read_error = |source| ExecuteError::Read {
        query_id: table.query_id().to_owned(),
        source,
    };
    let arrow_error = |source| ExecuteError::Arrow {
        query_id: table.query_id().to_owned(),
        source,
    };
    let rows: Vec<_> = table
        .dynamic_rows()
        .map_err(read_error)?
        .collect::<Result<_, _>>()
        .map_err(read_error)?;
    let arrays = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| array(&rows, index, field))
        .collect::<Result<Vec<_>, _>>()
        .map_err(arrow_error)?;
    RecordBatch::try_new(schema, arrays).map_err(arrow_error)
}

fn values<'a, T>(
    rows: &'a [DynamicRow],
    index: usize,
    field: &Field,
    convert: impl Fn(&'a CellValue) -> Option<T>,
) -> Result<Vec<Option<T>>, ArrowError> {
    rows.iter()
        .map(|row| match &row.values()[index] {
            CellValue::Null => Ok(None),
            value => convert(value).map(Some).ok_or_else(|| {
                ArrowError::CastError(format!(
                    "Cannot represent column {} as {}",
                    field.name(),
                    field.data_type()
                ))
            }),
        })
        .collect()
}

fn array(rows: &[DynamicRow], index: usize, field: &Field) -> Result<ArrayRef, ArrowError> {
    let array: ArrayRef =
        match field.data_type() {
            DataType::Boolean => Arc::new(BooleanArray::from(values(
                rows,
                index,
                field,
                |value| match value {
                    CellValue::Boolean(value) => Some(*value),
                    _ => None,
                },
            )?)),
            DataType::Float64 => Arc::new(Float64Array::from(values(
                rows,
                index,
                field,
                |value| match value {
                    CellValue::Float(value) => Some(*value),
                    _ => None,
                },
            )?)),
            DataType::Decimal128(precision, scale) => {
                let limit = 10_i128.pow(u32::from(*precision));
                let numbers = values(rows, index, field, |value| {
                    let scaled = match value {
                        CellValue::Integer(number) => {
                            number.checked_mul(10_i128.pow(*scale as u32))
                        }
                        CellValue::Decimal(number) => scaled_decimal(number.raw(), *scale as usize),
                        _ => None,
                    }?;
                    (scaled > -limit && scaled < limit).then_some(scaled)
                })?;
                Arc::new(
                    Decimal128Array::from(numbers).with_precision_and_scale(*precision, *scale)?,
                )
            }
            DataType::Date32 => {
                Arc::new(Date32Array::from(values(
                    rows,
                    index,
                    field,
                    |value| match value {
                        CellValue::Date(value) => i32::try_from(
                            value
                                .signed_duration_since(DateTime::UNIX_EPOCH.date_naive())
                                .num_days(),
                        )
                        .ok(),
                        _ => None,
                    },
                )?))
            }
            DataType::Time64(TimeUnit::Nanosecond) => Arc::new(Time64NanosecondArray::from(
                values(rows, index, field, |value| match value {
                    CellValue::Time(value) => Some(
                        i64::from(value.num_seconds_from_midnight()) * 1_000_000_000
                            + i64::from(value.nanosecond()),
                    ),
                    _ => None,
                })?,
            )),
            DataType::Timestamp(unit, zone) => {
                let timestamps = values(rows, index, field, |value| timestamp_cell(value, unit))?;
                timestamp_array(unit, zone.clone(), timestamps)
            }
            DataType::Binary => {
                let bytes = values(rows, index, field, |value| match value {
                    CellValue::Binary(value) => Some(value.as_bytes()),
                    _ => None,
                })?;
                Arc::new(BinaryArray::from(bytes))
            }
            DataType::Utf8 => {
                let text = values(rows, index, field, |value| match value {
                    CellValue::String(value) => Some(value.clone()),
                    CellValue::Json(value) => Some(value.to_string()),
                    CellValue::Vector(_) => Some(value.clone().into_json_value().to_string()),
                    _ => None,
                })?;
                Arc::new(StringArray::from(text))
            }
            _ => unreachable!("Arrow data types are defined by schema()"),
        };
    Ok(array)
}

fn timestamp_cell(value: &CellValue, unit: &TimeUnit) -> Option<i64> {
    let datetime = match value {
        CellValue::TimestampNtz(value) => value.and_utc(),
        CellValue::TimestampLtz(value) => *value,
        CellValue::TimestampTz(value) => value.to_utc(),
        _ => return None,
    };
    timestamp_value(datetime, unit)
}

fn timestamp_value(timestamp: DateTime<Utc>, unit: &TimeUnit) -> Option<i64> {
    match unit {
        TimeUnit::Second => Some(timestamp.timestamp()),
        TimeUnit::Millisecond => Some(timestamp.timestamp_millis()),
        TimeUnit::Microsecond => Some(timestamp.timestamp_micros()),
        TimeUnit::Nanosecond => timestamp.timestamp_nanos_opt(),
    }
}

fn timestamp_array(unit: &TimeUnit, zone: Option<Arc<str>>, values: Vec<Option<i64>>) -> ArrayRef {
    match unit {
        TimeUnit::Second => Arc::new(TimestampSecondArray::from(values).with_timezone_opt(zone)),
        TimeUnit::Millisecond => {
            Arc::new(TimestampMillisecondArray::from(values).with_timezone_opt(zone))
        }
        TimeUnit::Microsecond => {
            Arc::new(TimestampMicrosecondArray::from(values).with_timezone_opt(zone))
        }
        TimeUnit::Nanosecond => {
            Arc::new(TimestampNanosecondArray::from(values).with_timezone_opt(zone))
        }
    }
}

fn scaled_decimal(raw: &str, scale: usize) -> Option<i128> {
    let negative = raw.starts_with('-');
    let unsigned = raw.strip_prefix(['-', '+']).unwrap_or(raw);
    let (whole, fractional) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let fractional = fractional.trim_end_matches('0');
    if fractional.len() > scale
        || !whole
            .bytes()
            .chain(fractional.bytes())
            .all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let whole = whole.parse::<i128>().ok()?;
    let fraction = if fractional.is_empty() {
        0
    } else {
        fractional.parse::<i128>().ok()? * 10_i128.pow((scale - fractional.len()) as u32)
    };
    let value = whole
        .checked_mul(10_i128.pow(scale as u32))?
        .checked_add(fraction)?;
    Some(if negative { -value } else { value })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_tz_preserves_the_instant_in_utc_nanoseconds() {
        let datetime = DateTime::parse_from_rfc3339("1970-01-01T01:00:00.123456789+01:00").unwrap();
        assert_eq!(
            timestamp_cell(&CellValue::TimestampTz(datetime), &TimeUnit::Nanosecond),
            Some(123_456_789)
        );
    }
}
