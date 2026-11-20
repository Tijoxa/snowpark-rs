use std::collections::HashSet;
use std::path::Path;

use chrono::{NaiveDate, TimeZone as _};
use polars_core::{
    chunked_array::cast::CastOptions,
    prelude::{AnyValue, DataType as PolarsType, Schema, TimeUnit, TimeZone},
};
use polars_plan::{
    dsl::{
        AggExpr, BooleanFunction, DataTypeExpr, Expr, FunctionExpr, Operator, PowFunction,
        Selector, TemporalFunction, WindowMapping,
    },
    prelude::{DynLiteralValue, LiteralValue},
};
use snowflake_ast::TimestampUnit;

use super::{AstFrame, Context, Result, unsupported};
use crate::{
    BindValue, Column, CompileError, DataType, Decimal, Field, Projection, col, count, count_all,
    lit, lit_null,
};

pub(super) const BROADCAST: &str = "broadcasting a reduction over input rows is unsupported";
pub(super) const DECLARE_SCHEMA: &str = "declare the source schema or select named columns first";
const NO_TRANSLATION: &str = "no translation to Snowflake SQL";
const ROW_COUNT: DataType = number(10, 0);
const SUBQUERY_PREFIX: &str = "snowpark_rs:membership:";

type SchemaColumns = Vec<(String, Option<DataType>)>;

pub(super) fn identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn field_name(name: &str) -> String {
    name.strip_prefix('"')
        .and_then(|name| name.strip_suffix('"'))
        .map_or_else(|| name.to_owned(), |name| name.replace("\"\"", "\""))
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Shape {
    Scalar,
    Row,
    Aggregate,
}

pub(super) struct Translation {
    pub(super) column: Column,
    pub(super) name: String,
    pub(super) reference: Option<String>,
    pub(super) shape: Shape,
    pub(super) dtype: Option<DataType>,
    pub(super) untyped_null: bool,
}

struct Value {
    column: Column,
    shape: Shape,
    dtype: Option<DataType>,
    zone: Option<TimeZone>,
    untyped_null: bool,
}

impl Value {
    fn new(column: Column, shape: Shape, dtype: Option<DataType>) -> Self {
        Self {
            column,
            shape,
            dtype,
            zone: None,
            untyped_null: false,
        }
    }

    fn scalar(column: Column, dtype: DataType) -> Self {
        Self::new(column, Shape::Scalar, Some(dtype))
    }

    fn timestamp(value: i64, unit: TimeUnit, zone: Option<TimeZone>) -> Self {
        let dtype = if zone.is_some() {
            DataType::TimestampTz
        } else {
            DataType::TimestampNtz
        };
        let unit = match unit {
            TimeUnit::Milliseconds => TimestampUnit::Milliseconds,
            TimeUnit::Microseconds => TimestampUnit::Microseconds,
            TimeUnit::Nanoseconds => TimestampUnit::Nanoseconds,
        };
        let mut result = Self::scalar(
            Column::timestamp_from_epoch(value, unit, zone.is_some()),
            dtype,
        );
        result.zone = zone;
        result
    }
}

pub(super) struct Translator<'a> {
    operation: &'static str,
    context: &'a Context<'a>,
    pub(super) schema: Option<SchemaColumns>,
    clause_source: Option<Option<SchemaColumns>>,
}

impl<'a> Translator<'a> {
    pub(super) fn new(
        input: &AstFrame,
        operation: &'static str,
        context: &'a Context<'a>,
    ) -> Result<Self> {
        Ok(Self {
            operation,
            context,
            schema: input.schema()?.map(schema_columns),
            clause_source: None,
        })
    }

    pub(super) fn set_clause_source(&mut self, source: Option<Vec<Field>>) {
        self.clause_source = Some(source.map(schema_columns));
    }

    pub(super) fn for_operation(mut self, operation: &'static str) -> Self {
        self.operation = operation;
        self
    }

    pub(super) fn error(&self, node: impl Into<String>, reason: impl Into<String>) -> CompileError {
        unsupported(self.operation, node, reason)
    }

    pub(super) fn expressions(&self, expressions: &[Expr]) -> Result<Vec<Translation>> {
        let mut output = Vec::new();
        for expr in expressions {
            for expr in self.expand(expr)? {
                let value = self.visit(
                    &expr,
                    matches!(self.operation, "filter" | "having" | "qualify"),
                )?;
                let reference = match unalias(&expr) {
                    Expr::Column(name) => Some(name.to_string()),
                    _ => None,
                };
                output.push(Translation {
                    column: value.column,
                    shape: value.shape,
                    dtype: value.dtype,
                    untyped_null: value.untyped_null,
                    name: output_name(&expr).to_owned(),
                    reference,
                });
            }
        }
        Ok(output)
    }

    fn expand(&self, expr: &Expr) -> Result<Vec<Expr>> {
        let mut selectors = expr.into_iter().filter_map(|node| match node {
            Expr::Selector(selector) => Some(selector),
            _ => None,
        });
        let Some(first) = selectors.next() else {
            return Ok(vec![expr.clone()]);
        };
        if selectors.any(|selector| selector != first) {
            return Err(self.error("Selector", "multiple different selectors in one expression"));
        }
        let names = match &self.schema {
            None => match first {
                Selector::ByName {
                    names,
                    strict: true,
                } => names.to_vec(),
                _ => return Err(self.error("Selector", DECLARE_SCHEMA)),
            },
            Some(fields) => {
                let schema = fields
                    .iter()
                    .map(|(name, dtype)| (name.as_str().into(), dtype.and_then(polars_type)))
                    .collect::<Vec<_>>();
                if dtype_selector(first) && schema.iter().any(|(_, dtype)| dtype.is_none()) {
                    return Err(self.error(
                        "Selector",
                        "a dtype selector requires a known Polars dtype for each field",
                    ));
                }
                let schema: Schema = schema
                    .into_iter()
                    .map(|(name, dtype)| (name, dtype.unwrap_or(PolarsType::Null)))
                    .collect();
                first
                    .into_columns(&schema, &Default::default())
                    .map_err(|error| self.error("Selector", error.to_string()))?
                    .into_iter()
                    .collect()
            }
        };
        Ok(names
            .into_iter()
            .map(|name| {
                expr.clone().map_expr(|node| match node {
                    Expr::Selector(_) => Expr::Column(name.clone()),
                    _ => node,
                })
            })
            .collect())
    }

    fn visit(&self, expr: &Expr, allow_subquery: bool) -> Result<Value> {
        match expr {
            Expr::Alias(inner, _) => self.visit(inner, allow_subquery),
            Expr::Column(name) => {
                let dtype = self.column_type(name)?;
                Ok(Value::new(col(identifier(name)), Shape::Row, dtype))
            }
            Expr::Literal(literal) => self.literal(literal),
            Expr::Cast {
                expr,
                dtype: DataTypeExpr::Literal(dtype),
                options,
            } => self.cast(expr, dtype, *options),
            Expr::BinaryExpr { left, op, right } => self.binary(*op, left, right, allow_subquery),
            Expr::Function { input, function } => self.function(function, input, allow_subquery),
            Expr::Agg(AggExpr::Sum(inner)) => self.aggregate(inner, true),
            Expr::Agg(AggExpr::Count {
                input,
                include_nulls: false,
            }) => self.aggregate(input, false),
            Expr::Agg(AggExpr::Count {
                input,
                include_nulls: true,
            }) => {
                if self.schema.is_none() || !matches!(input.as_ref(), Expr::Column(_)) {
                    return Err(self.error("Agg.Count", "Expr.len() requires a named column with a known schema; use pl.len() for the row count"));
                }
                self.visit(input, false)?;
                Ok(Value::new(count_all(), Shape::Aggregate, Some(ROW_COUNT)))
            }
            Expr::Len => Ok(Value::new(count_all(), Shape::Aggregate, Some(ROW_COUNT))),
            Expr::Over {
                function,
                partition_by,
                order_by: None,
                mapping: WindowMapping::GroupsToRows,
            } => self.window(function, partition_by),
            Expr::Over { .. } => Err(self.error(
                "Over",
                "supported windows use a single sum, count or len reduction and groups_to_rows without order_by",
            )),
            Expr::AnonymousFunction { .. } => Err(self.error("AnonymousFunction", NO_TRANSLATION)),
            Expr::Agg(_) => Err(self.error("Agg", format!("{NO_TRANSLATION} for {expr:?}"))),
            Expr::Cast { .. } => Err(self.error("Cast", "a concrete Polars dtype is required")),
            _ => Err(self.error(format!("{expr:?}"), NO_TRANSLATION)),
        }
    }

    fn column_type(&self, name: &str) -> Result<Option<DataType>> {
        let lookup = |fields: &[(String, Option<DataType>)]| {
            fields
                .iter()
                .find(|(field, _)| field == name)
                .map(|field| field.1)
        };
        if let Some(dtype) = self.schema.as_deref().and_then(lookup) {
            return Ok(dtype);
        }
        match self.clause_source.as_ref().unwrap_or(&self.schema) {
            None => Ok(None),
            Some(fields) => lookup(fields).ok_or_else(|| {
                self.error(
                    "Column",
                    format!("{name:?} is absent from the input schema"),
                )
            }),
        }
    }

    fn window(&self, function: &Expr, partition_by: &[Expr]) -> Result<Value> {
        let source = self.source_translator();
        let function = source.visit(function, false)?;
        if function.shape != Shape::Aggregate {
            return Err(self.error("Over", "a sum, count or len window reduction is required"));
        }
        let keys = source.expressions(partition_by)?;
        if keys
            .iter()
            .any(|key| key.shape == Shape::Aggregate || key.untyped_null)
        {
            return Err(self.error(
                "Over",
                "partition keys must be row expressions or typed literals",
            ));
        }
        Ok(Value::new(
            function.column.over(keys.into_iter().map(|key| key.column)),
            Shape::Row,
            function.dtype,
        ))
    }

    fn source_translator(&self) -> Self {
        Self {
            operation: self.operation,
            context: self.context,
            schema: self.clause_source.as_ref().unwrap_or(&self.schema).clone(),
            clause_source: None,
        }
    }

    fn literal(&self, value: &LiteralValue) -> Result<Value> {
        match value {
            LiteralValue::Dyn(DynLiteralValue::Int(value)) => {
                let value = i64::try_from(*value).map_err(|_| self.unsupported_literal())?;
                Ok(Value::scalar(lit(value), number(38, 0)))
            }
            LiteralValue::Dyn(DynLiteralValue::Float(value)) if value.is_finite() => Ok(
                Value::scalar(lit(BindValue::Float(*value)), DataType::Float),
            ),
            LiteralValue::Dyn(DynLiteralValue::Str(value)) => {
                Ok(Value::scalar(lit(value.as_str()), DataType::Varchar))
            }
            LiteralValue::Scalar(value) => self.scalar(value.value(), value.dtype()),
            _ => Err(self.unsupported_literal()),
        }
    }

    fn unsupported_literal(&self) -> CompileError {
        self.error(
            "Literal",
            "expected a scalar literal representable as a Snowflake bind",
        )
    }

    fn scalar(&self, value: &AnyValue<'_>, dtype: &PolarsType) -> Result<Value> {
        let result = match value {
            AnyValue::Null => {
                let dtype = sql_type(dtype).map_err(|reason| self.error("Literal", reason))?;
                let mut node = Value::new(
                    lit_null(dtype.unwrap_or(DataType::Varchar)),
                    Shape::Scalar,
                    dtype,
                );
                node.untyped_null = dtype.is_none();
                node
            }
            AnyValue::Date(days) => Value::scalar(
                Column::date_from_epoch_days(i64::from(*days)),
                DataType::Date,
            ),
            AnyValue::Datetime(value, unit, zone) => Value::timestamp(*value, *unit, zone.cloned()),
            AnyValue::DatetimeOwned(value, unit, zone) => {
                Value::timestamp(*value, *unit, zone.as_deref().cloned())
            }
            AnyValue::Decimal(coefficient, _, scale) => {
                let scale = u8::try_from(*scale).map_err(|_| self.unsupported_literal())?;
                let decimal =
                    Decimal::new(*coefficient, scale).map_err(|_| self.unsupported_literal())?;
                let precision = u8::try_from(coefficient.unsigned_abs().to_string().len())
                    .map_err(|_| self.unsupported_literal())?;
                Value::scalar(lit(decimal), number(precision.max(scale), scale))
            }
            AnyValue::String(value) => Value::scalar(lit(*value), DataType::Varchar),
            AnyValue::StringOwned(value) => Value::scalar(lit(value.as_str()), DataType::Varchar),
            AnyValue::Boolean(value) => Value::scalar(lit(*value), DataType::Boolean),
            AnyValue::Float64(value) if value.is_finite() => {
                Value::scalar(lit(BindValue::Float(*value)), DataType::Float)
            }
            _ => {
                let precision =
                    integer_precision(dtype).ok_or_else(|| self.unsupported_literal())?;
                let dtype = number(precision, 0);
                let value = value
                    .extract::<i64>()
                    .ok_or_else(|| self.unsupported_literal())?;
                Value::scalar(lit(value).cast(dtype), dtype)
            }
        };
        Ok(result)
    }

    fn cast(&self, expr: &Expr, dtype: &PolarsType, options: CastOptions) -> Result<Value> {
        if options != CastOptions::Strict {
            return Err(self.error(
                "Cast",
                "only strict casts to a supported SQL type are translated",
            ));
        }
        let target = sql_type(dtype)
            .map_err(|reason| self.error("Cast", reason))?
            .ok_or_else(|| {
                self.error(
                    "Cast",
                    "only strict casts to a supported SQL type are translated",
                )
            })?;
        if target == DataType::Date
            && let Expr::Literal(LiteralValue::Scalar(source)) = unalias(expr)
            && let AnyValue::Datetime(value, unit, None)
            | AnyValue::DatetimeOwned(value, unit, None) = source.value()
        {
            let per_day = match unit {
                TimeUnit::Milliseconds => 86_400_000,
                TimeUnit::Microseconds => 86_400_000_000,
                TimeUnit::Nanoseconds => 86_400_000_000_000,
            };
            return Ok(Value::scalar(
                Column::date_from_epoch_days(value.div_euclid(per_day)),
                DataType::Date,
            ));
        }
        let inner = self.visit(expr, false)?;
        if dtype.is_integer()
            && inner.dtype.is_some_and(|dtype| {
                matches!(dtype, DataType::Float | DataType::Number { scale: 1.., .. })
            })
        {
            return Err(self.error(
                "Cast",
                "fractional-to-integer casts require Polars truncation semantics",
            ));
        }
        let column = if target == DataType::Date && is_zoned(inner.dtype) {
            inner.column.utc_date()
        } else {
            inner.column.cast(target)
        };
        Ok(Value::new(column, inner.shape, Some(target)))
    }

    fn binary(
        &self,
        op: Operator,
        left: &Expr,
        right: &Expr,
        allow_subquery: bool,
    ) -> Result<Value> {
        use Operator::*;
        let node = format!(
            "BinaryExpr.{}",
            match op {
                Eq => "Eq",
                EqValidity => "EqValidity",
                NotEq => "NotEq",
                NotEqValidity => "NotEqValidity",
                Lt => "Lt",
                LtEq => "LtEq",
                Gt => "Gt",
                GtEq => "GtEq",
                Plus => "Plus",
                Minus => "Minus",
                Multiply => "Multiply",
                TrueDivide => "TrueDivide",
                FloorDivide => "FloorDivide",
                TruncDivide => "TruncDivide",
                RustDivide => "RustDivide",
                Modulus => "Modulus",
                And => "And",
                Or => "Or",
                Xor => "Xor",
                LogicalAnd => "LogicalAnd",
                LogicalOr => "LogicalOr",
            }
        );
        let logical = matches!(op, And | Or | LogicalAnd | LogicalOr);
        let comparison = op.is_comparison();
        if matches!(op, FloorDivide | TruncDivide | RustDivide | Xor) {
            return Err(self.error(node, NO_TRANSLATION));
        }
        let allow_child = allow_subquery && matches!(op, And | LogicalAnd);
        let mut lhs = self.visit(left, allow_child)?;
        let mut rhs = self.visit(right, allow_child)?;
        let shape = self.combine_shape(&node, lhs.shape, rhs.shape)?;
        let dtype = if logical {
            if lhs.dtype != Some(DataType::Boolean) || rhs.dtype != Some(DataType::Boolean) {
                return Err(self.error(node, "logical operations require known Boolean operands"));
            }
            Some(DataType::Boolean)
        } else if comparison {
            self.comparison_operands(&node, &mut lhs, &mut rhs)?;
            Some(DataType::Boolean)
        } else {
            self.require_numeric(&node, &[&lhs, &rhs])?;
            (op == TrueDivide || [lhs.dtype, rhs.dtype].contains(&Some(DataType::Float)))
                .then_some(DataType::Float)
        };

        let left = lhs.column;
        let right_column = rhs.column;
        let column = match op {
            Eq => left.eq(right_column),
            EqValidity => left.eq_missing(right_column),
            NotEq => left.ne(right_column),
            NotEqValidity => left.ne_missing(right_column),
            Lt => left.lt(right_column),
            LtEq => left.le(right_column),
            Gt => left.gt(right_column),
            GtEq => left.ge(right_column),
            And | LogicalAnd => left.and(right_column),
            Or | LogicalOr => left.or(right_column),
            Plus => left + right_column,
            Minus => left - right_column,
            Multiply => left * right_column,
            TrueDivide => {
                let nonzero = match right {
                    Expr::Literal(LiteralValue::Dyn(DynLiteralValue::Float(value))) => {
                        *value != 0.0
                    }
                    Expr::Literal(value) => value.extract_i64().is_ok_and(|value| value != 0),
                    _ => false,
                };
                if !nonzero {
                    return Err(self.error(node, "a nonzero numeric literal divisor is required"));
                }
                left.cast(DataType::Float) / right_column
            }
            Modulus => {
                if !matches!(right, Expr::Literal(value) if value.extract_i64().is_ok_and(|value| value != 0))
                    || lhs.dtype == Some(DataType::Float)
                {
                    return Err(self.error(
                        node,
                        "an integral dividend and nonzero integer literal divisor are required",
                    ));
                }
                let remainder = left % right_column.clone();
                return Ok(Value::new(
                    (remainder + right_column.clone()) % right_column,
                    shape,
                    lhs.dtype,
                ));
            }
            FloorDivide | TruncDivide | RustDivide | Xor => unreachable!(),
        };
        Ok(Value::new(column, shape, dtype))
    }

    fn combine_shape(&self, node: &str, left: Shape, right: Shape) -> Result<Shape> {
        match (left, right) {
            (Shape::Row, Shape::Aggregate) | (Shape::Aggregate, Shape::Row)
                if self.clause_source.is_some() =>
            {
                Ok(Shape::Aggregate)
            }
            (Shape::Row, Shape::Aggregate) | (Shape::Aggregate, Shape::Row) => {
                Err(self.error(node, BROADCAST))
            }
            (Shape::Scalar, shape) | (shape, Shape::Scalar) => Ok(shape),
            (shape, _) => Ok(shape),
        }
    }

    fn require_numeric(&self, node: &str, values: &[&Value]) -> Result<()> {
        if values
            .iter()
            .any(|value| value.dtype.is_some_and(|dtype| !numeric(Some(dtype))))
        {
            return Err(self.error(node, "arithmetic requires numeric operands"));
        }
        Ok(())
    }

    fn comparison_operands(&self, node: &str, left: &mut Value, right: &mut Value) -> Result<()> {
        if (left.dtype == Some(DataType::Varchar) && numeric(right.dtype))
            || (right.dtype == Some(DataType::Varchar) && numeric(left.dtype))
        {
            return Err(self.error(node, "Polars cannot compare string and numeric operands"));
        }
        if !is_temporal(left.dtype) && !is_temporal(right.dtype) {
            return Ok(());
        }
        if [left.dtype, right.dtype]
            .into_iter()
            .any(|dtype| dtype.is_some() && !is_temporal(dtype))
        {
            return Err(self.error(
                node,
                "temporal comparisons require compatible temporal operands",
            ));
        }
        let zone = |value: &Value| {
            value
                .zone
                .clone()
                .or_else(|| is_zoned(value.dtype).then_some(TimeZone::UTC))
        };
        if is_timestamp(left.dtype) && is_timestamp(right.dtype) && zone(left) != zone(right) {
            return Err(self.error(node, "Polars requires matching datetime time zones"));
        }
        if left.dtype != right.dtype {
            left.column = left.column.clone().temporal_epoch(left.dtype);
            right.column = right.column.clone().temporal_epoch(right.dtype);
        }
        Ok(())
    }

    fn function(
        &self,
        function: &FunctionExpr,
        input: &[Expr],
        allow_subquery: bool,
    ) -> Result<Value> {
        use BooleanFunction::{IsIn, IsNotNull, IsNull, Not};
        match (function, input) {
            (FunctionExpr::Boolean(IsIn { nulls_equal }), [left, right]) => {
                self.membership(left, right, *nulls_equal, allow_subquery)
            }
            (FunctionExpr::Boolean(op @ (IsNull | IsNotNull | Not)), [expr]) => {
                let inner = self.visit(expr, false)?;
                let column = match op {
                    IsNull => inner.column.is_null(),
                    IsNotNull => inner.column.is_not_null(),
                    Not if inner.dtype == Some(DataType::Boolean) => !inner.column,
                    _ => {
                        return Err(self.error(
                            "Function.Boolean.Not",
                            "a known Boolean operand is required",
                        ));
                    }
                };
                Ok(Value::new(column, inner.shape, Some(DataType::Boolean)))
            }
            (FunctionExpr::Negate, [expr]) => {
                let inner = self.visit(expr, false)?;
                self.require_numeric("Function.Negate", &[&inner])?;
                Ok(Value::new(-inner.column, inner.shape, inner.dtype))
            }
            (FunctionExpr::Pow(PowFunction::Generic), [left, right]) => {
                let lhs = self.visit(left, false)?;
                let rhs = self.visit(right, false)?;
                let shape = self.combine_shape("Function.Pow", lhs.shape, rhs.shape)?;
                self.require_numeric("Function.Pow", &[&lhs, &rhs])?;
                Ok(Value::new(
                    lhs.column.pow(rhs.column),
                    shape,
                    Some(DataType::Float),
                ))
            }
            (
                FunctionExpr::TemporalExpr(TemporalFunction::DatetimeFunction {
                    time_unit,
                    time_zone,
                }),
                args,
            ) => self.datetime_parts(args, *time_unit, time_zone.as_ref()),
            (
                FunctionExpr::FfiPlugin {
                    lib,
                    symbol,
                    kwargs,
                    ..
                },
                [expr],
            ) if symbol == "try_parse_json"
                && kwargs.is_empty()
                && self
                    .context
                    .plugin_path
                    .is_some_and(|path| path.is_absolute() && Path::new(lib.as_str()) == path) =>
            {
                let inner = self.visit(expr, false)?;
                Ok(Value::new(
                    inner.column.try_parse_json(),
                    inner.shape,
                    Some(DataType::Variant),
                ))
            }
            (FunctionExpr::FfiPlugin { .. }, _) => Err(self.error(
                "Function.FfiPlugin",
                "only registered Snowflake plugins are supported",
            )),
            _ => {
                let kind = format!("{function:?}");
                let end = kind
                    .find(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
                    .unwrap_or(kind.len());
                Err(self.error(format!("Function.{}", &kind[..end]), NO_TRANSLATION))
            }
        }
    }

    fn datetime_parts(
        &self,
        args: &[Expr],
        unit: TimeUnit,
        zone: Option<&TimeZone>,
    ) -> Result<Value> {
        let [parts @ .., Expr::Literal(ambiguity)] = args else {
            return Err(self.error("DatetimeFunction", "expected seven datetime components"));
        };
        if ambiguity.extract_str() != Some("raise") {
            return Err(self.error(
                "DatetimeFunction",
                "only the default ambiguity policy is supported",
            ));
        }
        let parts: Vec<_> = parts
            .iter()
            .map(|expr| match expr {
                Expr::Literal(value) => value.extract_i64().map_err(|_| {
                    self.error(
                        "DatetimeFunction",
                        "date and datetime components must be integer literals",
                    )
                }),
                _ => Err(self.error(
                    "DatetimeFunction",
                    "date and datetime components must be integer literals",
                )),
            })
            .collect::<Result<_>>()?;
        let [year, month, day, hour, minute, second, microsecond] = parts.as_slice() else {
            return Err(self.error("DatetimeFunction", "expected seven datetime components"));
        };
        let invalid = || self.error("DatetimeFunction", "invalid date or datetime components");
        let unsigned = |value: i64| u32::try_from(value).map_err(|_| invalid());
        let naive = NaiveDate::from_ymd_opt(
            i32::try_from(*year).map_err(|_| invalid())?,
            unsigned(*month)?,
            unsigned(*day)?,
        )
        .and_then(|date| {
            date.and_hms_micro_opt(
                u32::try_from(*hour).ok()?,
                u32::try_from(*minute).ok()?,
                u32::try_from(*second).ok()?,
                u32::try_from(*microsecond).ok()?,
            )
        })
        .ok_or_else(invalid)?;
        let utc = match zone {
            Some(zone) => zone
                .to_chrono()
                .map_err(|error| self.error("DatetimeFunction", error.to_string()))?
                .from_local_datetime(&naive)
                .single()
                .ok_or_else(|| {
                    self.error(
                        "DatetimeFunction",
                        "the local datetime is ambiguous or does not exist",
                    )
                })?
                .to_utc(),
            None => naive.and_utc(),
        };
        let value = match unit {
            TimeUnit::Milliseconds => utc.timestamp_millis(),
            TimeUnit::Microseconds => utc.timestamp_micros(),
            TimeUnit::Nanoseconds => utc.timestamp_nanos_opt().ok_or_else(invalid)?,
        };
        Ok(Value::timestamp(value, unit, zone.cloned()))
    }

    fn membership(
        &self,
        left: &Expr,
        right: &Expr,
        nulls_equal: bool,
        allow_subquery: bool,
    ) -> Result<Value> {
        let value = self.visit(left, false)?;
        let right = unalias(right);
        if let Expr::AnonymousFunction { input, .. } = right
            && let [Expr::Literal(literal)] = input.as_slice()
            && let Some(token) = literal
                .extract_str()
                .filter(|token| token.starts_with(SUBQUERY_PREFIX))
        {
            if !allow_subquery || nulls_equal {
                return Err(self.error("IsIn", "table membership requires a positive filter (optionally joined with AND) and nulls_equal=False"));
            }
            if value.shape == Shape::Aggregate {
                return Err(self.error(
                    "IsIn",
                    "a table membership predicate cannot contain an aggregate",
                ));
            }
            let frame = self
                .context
                .membership
                .map(|lookup| lookup(token))
                .transpose()?
                .flatten()
                .ok_or_else(|| {
                    self.error(
                        "IsIn",
                        "the referenced Snowflake query is no longer available",
                    )
                })?;
            if let Some(schema) = frame.schema()?
                && schema.len() != 1
            {
                return Err(self.error(
                    "IsIn",
                    format!(
                        "expected a single-column query, got {} columns",
                        schema.len()
                    ),
                ));
            }
            return Ok(Value::new(
                frame.membership(value.column),
                Shape::Row,
                Some(DataType::Boolean),
            ));
        }

        let right = match right {
            Expr::Agg(AggExpr::Implode { input, .. }) => unalias(input),
            _ => right,
        };
        let items = match right {
            Expr::Literal(LiteralValue::Series(series)) => (**series).clone(),
            Expr::Literal(LiteralValue::Scalar(value)) => match value.value() {
                AnyValue::List(series) => series.clone(),
                _ => return Err(self.error("IsIn", "the right operand must be a Series literal, a list, or a single-column Snowflake query")),
            },
            _ => return Err(self.error("IsIn", "the right operand must be a Series literal, a list, or a single-column Snowflake query")),
        };
        let items = if matches!(items.dtype(), PolarsType::List(_)) {
            match (items.len(), items.get(0)) {
                (1, Ok(AnyValue::List(items))) => items,
                _ => return Err(self.error("IsIn", "only a single imploded list is supported")),
            }
        } else {
            items
        };
        let mut values = Vec::new();
        for index in 0..items.len() {
            let item = items
                .get(index)
                .map_err(|error| self.error("IsIn", error.to_string()))?;
            if item.is_null() {
                continue;
            }
            let item = self.scalar(&item, items.dtype())?;
            if value.dtype.is_some()
                && value.dtype != item.dtype
                && !(numeric(value.dtype) && numeric(item.dtype))
            {
                return Err(self.error("IsIn", "membership operands must have compatible dtypes"));
            }
            values.push(item.column);
        }
        Ok(Value::new(
            value
                .column
                .is_in_list(values, nulls_equal, items.null_count() != 0),
            value.shape,
            Some(DataType::Boolean),
        ))
    }

    fn aggregate(&self, expr: &Expr, is_sum: bool) -> Result<Value> {
        let mut inner = if self.clause_source.is_some() {
            self.source_translator().visit(expr, false)?
        } else {
            self.visit(expr, false)?
        };
        let node = if is_sum { "Agg.Sum" } else { "Agg.Count" };
        if inner.shape == Shape::Aggregate {
            return Err(self.error(node, "nested aggregates are unsupported"));
        }
        if !is_sum {
            return Ok(if inner.shape == Shape::Scalar {
                Value::scalar(inner.column.is_not_null().cast(ROW_COUNT), ROW_COUNT)
            } else {
                Value::new(count(inner.column), Shape::Aggregate, Some(ROW_COUNT))
            });
        }
        if inner.dtype == Some(DataType::Boolean) {
            let dtype = number(1, 0);
            inner.column = inner.column.cast(dtype);
            inner.dtype = Some(dtype);
        } else if inner.dtype.is_some() && !numeric(inner.dtype) {
            return Err(self.error(node, "a numeric or Boolean operand is required"));
        }
        if inner.shape == Shape::Scalar {
            let dtype = inner
                .dtype
                .ok_or_else(|| self.error(node, "a scalar sum requires a known numeric dtype"))?;
            return Ok(Value::scalar(
                inner.column.coalesce(lit(0).cast(dtype)),
                dtype,
            ));
        }
        Ok(Value::new(
            inner.column.sum_or_zero(),
            Shape::Aggregate,
            inner.dtype,
        ))
    }
}

fn schema_columns(fields: Vec<Field>) -> SchemaColumns {
    fields
        .into_iter()
        .map(|field| (field_name(&field.name()), field.data_type))
        .collect()
}

pub(super) fn projections(
    translator: &Translator<'_>,
    values: &[Translation],
) -> Result<Vec<Projection>> {
    if values.is_empty() {
        return Err(translator.error("projection", "the selection has no output columns"));
    }
    let mut names = HashSet::with_capacity(values.len());
    values
        .iter()
        .map(|value| {
            if !names.insert(value.name.as_str()) {
                return Err(
                    translator.error("projection", "duplicate output column names; use aliases")
                );
            }
            Ok(if value.reference.as_deref() == Some(value.name.as_str()) {
                value.column.clone().into()
            } else {
                value.column.clone().alias(identifier(&value.name))
            })
        })
        .collect()
}

fn unalias(mut expr: &Expr) -> &Expr {
    while let Expr::Alias(inner, _) = expr {
        expr = inner;
    }
    expr
}

fn output_name(expr: &Expr) -> &str {
    match expr {
        Expr::Alias(_, name) | Expr::Column(name) => name,
        Expr::BinaryExpr { left, .. }
        | Expr::Cast { expr: left, .. }
        | Expr::Over { function: left, .. }
        | Expr::Agg(AggExpr::Sum(left) | AggExpr::Count { input: left, .. }) => output_name(left),
        Expr::Function { input, .. } if !input.is_empty() => output_name(&input[0]),
        Expr::Len => "len",
        _ => "literal",
    }
}

fn dtype_selector(selector: &Selector) -> bool {
    match selector {
        Selector::ByDType(_) => true,
        Selector::Union(lhs, rhs)
        | Selector::Difference(lhs, rhs)
        | Selector::ExclusiveOr(lhs, rhs)
        | Selector::Intersect(lhs, rhs) => dtype_selector(lhs) || dtype_selector(rhs),
        _ => false,
    }
}

const fn number(precision: u8, scale: u8) -> DataType {
    DataType::Number { precision, scale }
}

fn numeric(dtype: Option<DataType>) -> bool {
    matches!(dtype, Some(DataType::Float | DataType::Number { .. }))
}

fn is_zoned(dtype: Option<DataType>) -> bool {
    matches!(dtype, Some(DataType::TimestampLtz | DataType::TimestampTz))
}

fn is_timestamp(dtype: Option<DataType>) -> bool {
    dtype == Some(DataType::TimestampNtz) || is_zoned(dtype)
}

fn is_temporal(dtype: Option<DataType>) -> bool {
    dtype == Some(DataType::Date) || is_timestamp(dtype)
}

fn integer_precision(dtype: &PolarsType) -> Option<u8> {
    Some(match dtype {
        PolarsType::Int8 | PolarsType::UInt8 => 3,
        PolarsType::Int16 | PolarsType::UInt16 => 5,
        PolarsType::Int32 | PolarsType::UInt32 => 10,
        PolarsType::Int64 => 19,
        PolarsType::UInt64 => 20,
        _ => return None,
    })
}

fn polars_type(dtype: DataType) -> Option<PolarsType> {
    Some(match dtype {
        DataType::Number { precision, scale } => {
            PolarsType::Decimal(usize::from(precision), usize::from(scale))
        }
        DataType::Float => PolarsType::Float64,
        DataType::Varchar => PolarsType::String,
        DataType::Boolean => PolarsType::Boolean,
        DataType::Binary => PolarsType::Binary,
        DataType::Time => PolarsType::Time,
        DataType::Date => PolarsType::Date,
        DataType::TimestampNtz => PolarsType::Datetime(TimeUnit::Nanoseconds, None),
        DataType::TimestampLtz | DataType::TimestampTz => {
            PolarsType::Datetime(TimeUnit::Nanoseconds, Some(TimeZone::UTC))
        }
        _ => return None,
    })
}

fn sql_type(dtype: &PolarsType) -> std::result::Result<Option<DataType>, String> {
    let result = match dtype {
        PolarsType::Null => return Ok(None),
        PolarsType::String => DataType::Varchar,
        PolarsType::Boolean => DataType::Boolean,
        PolarsType::Float64 => DataType::Float,
        PolarsType::Binary => DataType::Binary,
        PolarsType::Time => DataType::Time,
        PolarsType::Date => DataType::Date,
        PolarsType::Datetime(_, None) => DataType::TimestampNtz,
        PolarsType::Datetime(_, Some(zone)) if *zone == TimeZone::UTC => DataType::TimestampTz,
        PolarsType::Decimal(precision, scale)
            if (1..=38).contains(precision) && *scale <= 37 && scale <= precision =>
        {
            number(*precision as u8, *scale as u8)
        }
        dtype => number(
            integer_precision(dtype)
                .ok_or_else(|| format!("unsupported Polars dtype {dtype:?}"))?,
            0,
        ),
    };
    Ok(Some(result))
}
