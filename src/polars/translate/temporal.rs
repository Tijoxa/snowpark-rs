use polars_defs::time::duration::Duration;
use snowflake_ast::TemporalBucket;

use super::{
    DataType, Expr, Result, Shape, TimeUnit, TimeZone, Translator, Value, is_temporal,
    string_literal,
};

const TRUNCATE: &str = "Function.TemporalExpr.Truncate";
const DAY_NS: u128 = 86_400_000_000_000;

impl Translator<'_> {
    pub(super) fn truncate(&self, expr: &Expr, every: &Expr) -> Result<Value> {
        let mut input = self.visit(expr, false)?;
        if input.untyped_null || input.dtype.is_some() && !is_temporal(input.dtype) {
            return Err(self.error(TRUNCATE, "a date or timestamp is required"));
        }
        let quantum_ns = match (input.dtype, input.time_unit) {
            (Some(DataType::Date), _) | (_, Some(TimeUnit::Milliseconds)) => 1_000_000,
            (_, Some(TimeUnit::Microseconds)) => 1_000,
            _ => 1,
        };
        let naive = input.dtype == Some(DataType::TimestampNtz)
            || input.zone.as_ref() == Some(&TimeZone::UTC);
        input = input.in_expression_time_zone();

        if let Some(period) = string_literal(every) {
            let bucket = parse_bucket(period, quantum_ns, naive)
                .or_else(|reason| {
                    if input.dtype.is_none()
                        && let Ok(TemporalBucket::Nanoseconds(ns)) =
                            parse_bucket(period, quantum_ns, true)
                    {
                        return Ok(TemporalBucket::NaiveNanoseconds(ns));
                    }
                    Err(reason)
                })
                .map_err(|reason| self.error(TRUNCATE, reason))?;
            input.column = input.column.truncate_temporal(bucket);
        } else {
            let every = self.visit(every, false)?;
            if every.dtype.is_some_and(|dtype| dtype != DataType::Varchar) {
                return Err(self.error(TRUNCATE, "the duration expression must produce strings"));
            }
            input.shape = self.combine_shape(TRUNCATE, input.shape, every.shape)?;
            input.column = input.column.truncate_temporal_by(
                every.column,
                quantum_ns,
                every.shape != Shape::Row,
            );
        }
        Ok(input)
    }
}

fn parse_bucket(
    period: &str,
    quantum_ns: u64,
    naive: bool,
) -> std::result::Result<TemporalBucket, String> {
    // Polars' parser uses machine integers without checked arithmetic. Reject
    // overflowing components before giving it a duration to normalize.
    validate_duration_range(period)?;
    let duration = Duration::try_parse(period).map_err(|error| error.to_string())?;
    if duration.negative() {
        return Err("cannot truncate to a negative duration".into());
    }
    let (months, weeks, days, ns) = (
        duration.months() as u64,
        duration.weeks() as u64,
        duration.days() as u64,
        duration.nanoseconds() as u64,
    );
    let fixed = u128::from(ns - ns % quantum_ns);
    match (months, weeks, days, ns) {
        (0, 0, 1, 0) => Ok(TemporalBucket::Day),
        (0, 0, 0, 3_600_000_000_000) => Ok(TemporalBucket::Hour),
        (0, 0, 0, 900_000_000_000) => Ok(TemporalBucket::QuarterHour),
        (0, 0, 0, 0) if !naive => Err("duration cannot be zero".into()),
        (0, 0, 0, _) => Ok(TemporalBucket::Nanoseconds(fixed)),
        (0, 0, days, 0) => Ok(TemporalBucket::Days(days)),
        (0, 0, days, _) if naive => Ok(TemporalBucket::Nanoseconds(
            u128::from(days) * DAY_NS + fixed,
        )),
        (0, weeks, 0, 0) => Ok(TemporalBucket::Weeks(weeks)),
        (months, 0, 0, 0) => Ok(TemporalBucket::Months(months)),
        _ => Err("cannot mix month, week, day, and sub-daily units for this operation".into()),
    }
}

fn validate_duration_range(period: &str) -> std::result::Result<(), String> {
    let mut totals = [0u64; 4];
    let mut remaining = period.trim_start_matches(['+', '-']);
    while !remaining.is_empty() {
        let number_end = remaining.bytes().take_while(u8::is_ascii_digit).count();
        let (number, rest) = remaining.split_at(number_end);
        if number.is_empty() {
            return Ok(());
        }
        let number = number
            .parse::<i64>()
            .map_err(|_| "duration component is too large".to_owned())? as u64;
        let unit_end = rest.bytes().take_while(u8::is_ascii_alphabetic).count();
        let (unit, tail) = rest.split_at(unit_end);
        let (slot, factor) = match unit {
            "y" => (0, 12),
            "q" => (0, 3),
            "mo" => (0, 1),
            "w" => (1, 1),
            "d" => (2, 1),
            "i" | "ns" => (3, 1),
            "us" => (3, 1_000),
            "ms" => (3, 1_000_000),
            "s" => (3, 1_000_000_000),
            "m" => (3, 60_000_000_000),
            "h" => (3, 3_600_000_000_000),
            // The upstream parser supplies the syntax diagnostic.
            _ => return Ok(()),
        };
        totals[slot] = number
            .checked_mul(factor)
            .and_then(|n| n.checked_add(totals[slot]))
            .filter(|total| i64::try_from(*total).is_ok())
            .ok_or_else(|| "duration component is too large".to_owned())?;
        remaining = tail;
    }
    Ok(())
}
