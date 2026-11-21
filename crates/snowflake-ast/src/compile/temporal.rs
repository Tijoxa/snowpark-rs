use super::{BindValue, CompileError, CompiledQuery, DataType, Expr, Scope};
use crate::TemporalBucket;

const DAY_NS: u128 = 86_400_000_000_000;
const LOCAL: &str = "local_time";

impl Scope<'_> {
    pub(super) fn emit_temporal_truncate(
        &self,
        input: &Expr,
        bucket: TemporalBucket,
        query: &mut CompiledQuery,
        allow_aggregate: bool,
    ) -> Result<(), CompileError> {
        let dtype = self.expression_type(input)?.0;
        let named_zone = matches!(input, Expr::InTimeZone(..));
        let simple = match (bucket, dtype) {
            (
                TemporalBucket::Day | TemporalBucket::Hour | TemporalBucket::QuarterHour,
                Some(DataType::Date),
            ) => Some(("", "")),
            (TemporalBucket::Day, _) if !named_zone => Some(("DATE_TRUNC(DAY, ", ")")),
            (TemporalBucket::Hour, Some(DataType::TimestampNtz)) => {
                Some(("DATE_TRUNC(HOUR, ", ")"))
            }
            (TemporalBucket::QuarterHour, Some(DataType::TimestampNtz)) => {
                Some(("TIME_SLICE(", ", 15, 'MINUTE')"))
            }
            (TemporalBucket::Nanoseconds(0 | 1), _) => Some(("", "")),
            _ => None,
        };
        if let Some((prefix, suffix)) = simple {
            query.sql.push_str(prefix);
            self.emit_expression(input, query, false, allow_aggregate)?;
            query.sql.push_str(suffix);
            return Ok(());
        }

        let wall = match bucket {
            TemporalBucket::Day => format!("DATE_TRUNC(DAY, {LOCAL})"),
            TemporalBucket::Hour => clock_bucket("3600000000000"),
            TemporalBucket::QuarterHour => clock_bucket("900000000000"),
            TemporalBucket::Days(days) => day_bucket("1970-01-01", &days.max(1).to_string()),
            // Polars anchors multi-week buckets on Monday, January 5, 1970.
            TemporalBucket::Weeks(weeks) => {
                day_bucket("1970-01-05", &(7 * u128::from(weeks.max(1))).to_string())
            }
            TemporalBucket::Months(months) => month_bucket(&months.max(1).to_string()),
            TemporalBucket::Nanoseconds(ns) | TemporalBucket::NaiveNanoseconds(ns) => {
                clock_bucket(&ns.to_string())
            }
        };
        let require_naive = matches!(bucket, TemporalBucket::NaiveNanoseconds(_));
        self.emit_truncated_value(input, require_naive, query, allow_aggregate, |query| {
            query.sql.push_str("REDUCE(ARRAY_CONSTRUCT(0), CAST(");
            self.emit_expression(input, query, false, allow_aggregate)?;
            query
                .sql
                .push_str(" AS TIMESTAMP_NTZ), (local_time, unused) -> ");
            query.sql.push_str(&wall);
            query.sql.push(')');
            Ok(())
        })
    }

    pub(super) fn emit_temporal_truncate_by(
        &self,
        input: &Expr,
        every: &Expr,
        (quantum_ns, scalar): (u64, bool),
        query: &mut CompiledQuery,
        allow_aggregate: bool,
    ) -> Result<(), CompileError> {
        let dtype = self.expression_type(input)?.0;
        let naive = dtype == Some(DataType::TimestampNtz)
            || matches!(input, Expr::InTimeZone(_, zone) if zone == "UTC");
        let check_naive = scalar && dtype.is_none();
        let quantum_ns = quantum_ns.max(1);
        let fixed = format!("(units[3]::NUMBER - MOD(units[3]::NUMBER, {quantum_ns}))");
        let clock = clock_bucket(&fixed);
        let day = day_bucket("1970-01-01", "units[2]::NUMBER");
        let week = day_bucket("1970-01-05", "(7 * units[1]::NUMBER)");
        let month = month_bucket("units[0]::NUMBER");
        if !scalar {
            query.sql.push_str("CASE WHEN ");
            self.emit_expression(input, query, false, allow_aggregate)?;
            query.sql.push_str(" IS NULL THEN NULL ELSE ");
        }
        self.emit_truncated_value(input, false, query, allow_aggregate, |query| {
            query.sql.push_str("REDUCE(ARRAY_CONSTRUCT(");
            if check_naive {
                query.sql.push_str("ARRAY_APPEND(");
            }
            self.emit_duration(every, query, allow_aggregate)?;
            if check_naive {
                query.sql.push_str(", AS_TIMESTAMP_NTZ(TO_VARIANT(");
                self.emit_expression(input, query, false, allow_aggregate)?;
                query.sql.push_str(")) IS NOT NULL)");
            }
            query.sql.push_str("), CAST(");
            self.emit_expression(input, query, false, allow_aggregate)?;
            query.sql.push_str(" AS TIMESTAMP_NTZ), (local_time, units ARRAY) -> CASE WHEN units IS NULL THEN NULL ");
            if scalar && (naive || check_naive) {
                let combined = format!("({fixed} + units[2]::NUMBER * {DAY_NS})");
                query.sql.push_str("WHEN units[0]::NUMBER = 0 AND units[1]::NUMBER = 0 ");
                if check_naive {
                    query.sql.push_str("AND units[4]::BOOLEAN ");
                }
                query.sql.push_str(&format!("THEN {} ", clock_bucket(&combined)));
            }
            query.sql.push_str(&format!(concat!(
                "WHEN units[0]::NUMBER > 0 AND units[1]::NUMBER = 0 AND units[2]::NUMBER = 0 AND units[3]::NUMBER = 0 THEN {} ",
                "WHEN units[0]::NUMBER = 0 AND units[1]::NUMBER > 0 AND units[2]::NUMBER = 0 AND units[3]::NUMBER = 0 THEN {} ",
                "WHEN units[0]::NUMBER = 0 AND units[1]::NUMBER = 0 AND units[2]::NUMBER > 0 AND units[3]::NUMBER = 0 THEN {} ",
                "WHEN units[0]::NUMBER = 0 AND units[1]::NUMBER = 0 AND units[2]::NUMBER = 0 AND units[3]::NUMBER > 0 THEN {} ",
                "ELSE TO_TIMESTAMP_NTZ('invalid dt.truncate duration: ' || TO_VARCHAR(units)) END)"
            ), month, week, day, clock));
            Ok(())
        })?;
        if !scalar {
            query.sql.push_str(" END");
        }
        Ok(())
    }

    fn emit_duration(
        &self,
        every: &Expr,
        query: &mut CompiledQuery,
        allow_aggregate: bool,
    ) -> Result<(), CompileError> {
        const TOKEN: &str = "[0-9]+(ns|us|ms|mo|s|m|h|d|w|q|y|i)";
        query.sql.push_str("REDUCE(ARRAY_CONSTRUCT(COLLATE(CAST(");
        self.emit_expression(every, query, false, allow_aggregate)?;
        query.sql.push_str(&format!(concat!(
            " AS VARCHAR), '')), ARRAY_CONSTRUCT(0, 0, 0, 0), (ignored, period VARCHAR) -> CASE ",
            "WHEN period IS NULL THEN NULL ",
            "WHEN NOT REGEXP_LIKE(period, '[+]?({token})*') THEN ",
            "ARRAY_CONSTRUCT(TO_NUMBER('invalid dt.truncate duration: ' || period), 0, 0, 0) ",
            "ELSE REDUCE(REGEXP_SUBSTR_ALL(period, '{token}'), ARRAY_CONSTRUCT(0, 0, 0, 0), ",
            "(sizes, token VARCHAR) -> ARRAY_CONSTRUCT( ",
            "sizes[0]::NUMBER + {n} * CASE {unit} WHEN 'mo' THEN 1 WHEN 'q' THEN 3 WHEN 'y' THEN 12 ELSE 0 END, ",
            "sizes[1]::NUMBER + IFF({unit} = 'w', {n}, 0), ",
            "sizes[2]::NUMBER + IFF({unit} = 'd', {n}, 0), ",
            "sizes[3]::NUMBER + {n} * CASE {unit} WHEN 'i' THEN 1 WHEN 'ns' THEN 1 WHEN 'us' THEN 1000 ",
            "WHEN 'ms' THEN 1000000 WHEN 's' THEN 1000000000 WHEN 'm' THEN 60000000000 ",
            "WHEN 'h' THEN 3600000000000 ELSE 0 END)) END)"
        ), token = TOKEN, n = "REGEXP_SUBSTR(token, '^[0-9]+')::NUMBER", unit = "REGEXP_SUBSTR(token, '[a-z]+$')"));
        Ok(())
    }

    fn emit_truncated_value(
        &self,
        input: &Expr,
        require_naive: bool,
        query: &mut CompiledQuery,
        allow_aggregate: bool,
        emit_wall: impl FnOnce(&mut CompiledQuery) -> Result<(), CompileError>,
    ) -> Result<(), CompileError> {
        let dtype = self.expression_type(input)?.0;
        let zone = match input {
            Expr::InTimeZone(_, zone) => Some(zone),
            _ => None,
        };
        if dtype == Some(DataType::TimestampNtz) {
            return emit_wall(query);
        }
        if dtype == Some(DataType::Date) {
            // Polars divides the boundary's epoch milliseconds toward zero for Date.
            query.sql.push_str("REDUCE(ARRAY_CONSTRUCT(");
            emit_wall(query)?;
            query.sql.push_str(concat!(
                "), DATE '1970-01-01', (unused, wall TIMESTAMP_NTZ) -> ",
                "DATEADD(DAY, (DATE_PART(EPOCH_MILLISECOND, wall) - ",
                "MOD(DATE_PART(EPOCH_MILLISECOND, wall), 86400000)) / 86400000, unused))"
            ));
            return Ok(());
        }

        // First try the input's UTC offset. This retains either occurrence of a
        // repeated local time. When the target has a different offset, localize it.
        if let Some(zone) = zone {
            query.sql.push_str("CONVERT_TIMEZONE(?, ");
            query.binds.push(BindValue::Text(zone.clone()));
        }
        query.sql.push_str("REDUCE(ARRAY_CONSTRUCT(");
        emit_wall(query)?;
        query.sql.push_str("), ");
        self.emit_expression(input, query, false, allow_aggregate)?;
        query
            .sql
            .push_str(", (original, wall TIMESTAMP_NTZ) -> DATEADD(NANOSECOND, CASE ");
        if require_naive {
            query.sql.push_str(concat!(
                "WHEN AS_TIMESTAMP_NTZ(TO_VARIANT(original)) IS NULL THEN ",
                "TO_NUMBER('dt.truncate duration requires TIMESTAMP_NTZ: ' || TO_VARCHAR(original)) "
            ));
        }
        query.sql.push_str("WHEN ");
        let shift = "DATEDIFF(NANOSECOND, CAST(original AS TIMESTAMP_NTZ), wall)";
        let candidate = format!("DATEADD(NANOSECOND, {shift}, original)");
        if let Some(zone) = zone {
            query.sql.push_str(&format!(
                "CAST(CONVERT_TIMEZONE(?, {candidate}) AS TIMESTAMP_NTZ) <> wall THEN "
            ));
            query.binds.push(BindValue::Text(zone.clone()));
            query
                .sql
                .push_str("DATE_PART(EPOCH_NANOSECOND, CONVERT_TIMEZONE(?, 'UTC', wall)) ");
            query.binds.push(BindValue::Text(zone.clone()));
        } else {
            query.sql.push_str(&format!("AS_TIMESTAMP_LTZ(TO_VARIANT(original)) IS NOT NULL AND CAST({candidate} AS TIMESTAMP_NTZ) <> wall THEN "));
            query
                .sql
                .push_str("DATE_PART(EPOCH_NANOSECOND, CAST(wall AS TIMESTAMP_LTZ)) ");
        }
        query.sql.push_str(&format!(
            "- DATE_PART(EPOCH_NANOSECOND, original) ELSE {shift} END, original))"
        ));
        if zone.is_some() {
            query.sql.push(')');
        }
        Ok(())
    }
}

fn positive_remainder(value: &str, width: &str) -> String {
    format!("MOD(MOD({value}, {width}) + {width}, {width})")
}

fn clock_bucket(width: &str) -> String {
    let rem = positive_remainder(
        "DATE_PART(EPOCH_NANOSECOND, local_time)",
        &format!("NULLIF({width}, 0)"),
    );
    format!("DATEADD(NANOSECOND, -COALESCE({rem}, 0), {LOCAL})")
}

fn day_bucket(anchor: &str, width: &str) -> String {
    let rem = positive_remainder(&format!("DATEDIFF(DAY, DATE '{anchor}', {LOCAL})"), width);
    format!("DATEADD(DAY, -{rem}, DATE_TRUNC(DAY, {LOCAL}))")
}

fn month_bucket(width: &str) -> String {
    let rem = positive_remainder("DATEDIFF(MONTH, DATE '1970-01-01', local_time)", width);
    format!("DATEADD(MONTH, -{rem}, DATE_TRUNC(MONTH, {LOCAL}))")
}
