use std::{
    ops::{Add, Div, Mul, Neg, Not, Rem, Sub},
    sync::Arc,
};

use crate::{BindValue, DataType, Star, plan::LogicalPlan, star};

/// A reference, literal or predicate in a DataFrame expression.
#[derive(Clone)]
#[must_use]
pub struct Column(pub(crate) Expr);

/// An output expression with an optional SQL alias, or a star selection.
#[derive(Clone)]
#[must_use]
pub struct Projection(pub(crate) ProjectionKind);

#[derive(Clone)]
pub(crate) enum ProjectionKind {
    Expression { expr: Expr, alias: Option<String> },
    Star(Box<Star>),
}

/// Reference a column by an unquoted or double-quoted Snowflake identifier.
pub fn col(name: impl Into<String>) -> Column {
    Column(Expr::Reference {
        name: name.into(),
        relation: None,
    })
}

/// Bind a value as a SQL parameter.
pub fn lit(value: impl Into<BindValue>) -> Column {
    Column(Expr::Literal(value.into()))
}

/// Bind a SQL NULL with an explicit Snowflake type.
pub fn lit_null(data_type: DataType) -> Column {
    lit(BindValue::Null(data_type))
}

/// Count all rows, including rows whose columns are null.
pub fn count_all() -> Column {
    Column(Expr::Aggregate(Aggregate::CountAll))
}

/// Count the non-null values of an expression.
pub fn count(column: impl Into<Column>) -> Column {
    let column = column.into();
    if matches!(&column.0, Expr::Reference { name, relation: None } if name == "*") {
        return count_all();
    }
    Column(Expr::Aggregate(Aggregate::Count(Box::new(column.0))))
}

/// Sum the non-null numeric values of an expression.
pub fn sum(column: impl Into<Column>) -> Column {
    Column(Expr::Aggregate(Aggregate::Sum(Box::new(column.into().0))))
}

/// Number rows within each partition using explicit sort keys.
pub fn row_number(
    partition_by: impl IntoIterator<Item = Column>,
    order_by: impl IntoIterator<Item = SortKey>,
) -> Column {
    Column(Expr::Over {
        function: Box::new(Expr::RowNumber),
        partition_by: partition_by.into_iter().map(|column| column.0).collect(),
        order_by: order_by.into_iter().collect(),
    })
}

impl Column {
    /// Represent the instant in the specified time zone.
    pub fn in_time_zone(self, zone: impl Into<String>) -> Self {
        Self(Expr::InTimeZone(Box::new(self.0), zone.into()))
    }

    /// Truncate a temporal value to a supported calendar or clock bucket.
    pub fn truncate_temporal(self, bucket: TemporalBucket) -> Self {
        Self(Expr::TruncateTemporal(Box::new(self.0), bucket))
    }

    /// Extract the minute of a timestamp or time.
    pub fn minute(self) -> Self {
        Self(Expr::Minute(Box::new(self.0)))
    }

    /// Search for a literal substring using Snowflake's UTF-8 collation.
    pub fn contains_text(self, pattern: Self) -> Self {
        Self(Expr::ContainsText(Box::new(self.0), Box::new(pattern.0)))
    }

    /// Search for a substring matching a Snowflake regular expression.
    pub fn contains_regex(
        self,
        pattern: String,
        case_insensitive: bool,
        matches_empty: bool,
    ) -> Self {
        Self(Expr::ContainsRegex {
            subject: Box::new(self.0),
            pattern,
            case_insensitive,
            matches_empty,
        })
    }

    /// Compute a COUNT or SUM over every row in the specified partition.
    pub fn over(self, partition_by: impl IntoIterator<Item = Column>) -> Self {
        Self(Expr::Over {
            function: Box::new(self.0),
            partition_by: partition_by.into_iter().map(|column| column.0).collect(),
            order_by: Vec::new(),
        })
    }

    pub fn date_from_epoch_days(days: i64) -> Self {
        Self(Expr::DateFromEpochDays(days))
    }

    pub fn timestamp_from_epoch(value: i64, unit: TimestampUnit, with_timezone: bool) -> Self {
        Self(Expr::TimestampFromEpoch(value, unit, with_timezone))
    }

    pub fn temporal_epoch(self, data_type: Option<DataType>) -> Self {
        Self(Expr::TemporalEpoch(Box::new(self.0), data_type))
    }

    pub fn utc_date(self) -> Self {
        Self(Expr::UtcDate(Box::new(self.0)))
    }

    pub fn is_in_list(self, values: Vec<Self>, nulls_equal: bool, has_null: bool) -> Self {
        Self(Expr::InList {
            expr: Box::new(self.0),
            values: values.into_iter().map(|value| value.0).collect(),
            nulls_equal,
            has_null,
        })
    }

    pub fn coalesce(self, fallback: Self) -> Self {
        Self(Expr::Coalesce(Box::new(self.0), Box::new(fallback.0)))
    }

    pub fn try_parse_json(self) -> Self {
        Self(Expr::TryParseJson(Box::new(self.0)))
    }

    pub fn sum_or_zero(self) -> Self {
        Self(Expr::Aggregate(Aggregate::SumOrZero(Box::new(self.0))))
    }

    pub fn eq_missing(self, other: Self) -> Self {
        self.binary(BinaryOp::EqMissing, other)
    }

    pub fn ne_missing(self, other: Self) -> Self {
        self.binary(BinaryOp::NeMissing, other)
    }

    pub fn alias(self, name: impl Into<String>) -> Projection {
        Projection(ProjectionKind::Expression {
            expr: self.0,
            alias: Some(name.into()),
        })
    }

    pub fn eq(self, other: Self) -> Self {
        self.binary(BinaryOp::Eq, other)
    }

    pub fn ne(self, other: Self) -> Self {
        self.binary(BinaryOp::Ne, other)
    }

    pub fn lt(self, other: Self) -> Self {
        self.binary(BinaryOp::Lt, other)
    }

    pub fn le(self, other: Self) -> Self {
        self.binary(BinaryOp::Le, other)
    }

    pub fn gt(self, other: Self) -> Self {
        self.binary(BinaryOp::Gt, other)
    }

    pub fn ge(self, other: Self) -> Self {
        self.binary(BinaryOp::Ge, other)
    }

    pub fn and(self, other: Self) -> Self {
        self.binary(BinaryOp::And, other)
    }

    pub fn or(self, other: Self) -> Self {
        self.binary(BinaryOp::Or, other)
    }

    pub fn is_null(self) -> Self {
        Self(Expr::IsNull(Box::new(self.0)))
    }

    pub fn is_not_null(self) -> Self {
        Self(Expr::IsNotNull(Box::new(self.0)))
    }

    pub fn cast(self, data_type: DataType) -> Self {
        match self.0 {
            Expr::Literal(BindValue::Null(_)) => lit_null(data_type),
            expr => Self(Expr::Cast(Box::new(expr), data_type)),
        }
    }

    pub fn pow(self, exponent: Self) -> Self {
        Self(Expr::Power(Box::new(self.0), Box::new(exponent.0)))
    }

    pub fn asc(self) -> SortKey {
        self.asc_nulls_last()
    }

    pub fn desc(self) -> SortKey {
        self.desc_nulls_first()
    }

    pub fn asc_nulls_first(self) -> SortKey {
        SortKey::new(self, true, true)
    }

    pub fn asc_nulls_last(self) -> SortKey {
        SortKey::new(self, true, false)
    }

    pub fn desc_nulls_first(self) -> SortKey {
        SortKey::new(self, false, true)
    }

    pub fn desc_nulls_last(self) -> SortKey {
        SortKey::new(self, false, false)
    }

    fn binary(self, op: BinaryOp, other: Self) -> Self {
        Self(Expr::Binary(Box::new(self.0), op, Box::new(other.0)))
    }
}

impl Not for Column {
    type Output = Self;

    fn not(self) -> Self {
        Self(Expr::Not(Box::new(self.0)))
    }
}

impl Neg for Column {
    type Output = Self;

    fn neg(self) -> Self {
        Self(Expr::Negate(Box::new(self.0)))
    }
}

macro_rules! arithmetic {
    ($trait:ident, $method:ident, $op:ident) => {
        impl $trait for Column {
            type Output = Self;

            fn $method(self, other: Self) -> Self {
                self.binary(BinaryOp::$op, other)
            }
        }
    };
}

arithmetic!(Add, add, Add);
arithmetic!(Sub, sub, Subtract);
arithmetic!(Mul, mul, Multiply);
arithmetic!(Div, div, Divide);
arithmetic!(Rem, rem, Modulo);

impl From<&str> for Column {
    fn from(name: &str) -> Self {
        col(name)
    }
}

impl From<String> for Column {
    fn from(name: String) -> Self {
        col(name)
    }
}

/// A sort expression with an explicit direction and null placement.
#[derive(Clone)]
#[must_use]
pub struct SortKey {
    pub(crate) expr: Expr,
    pub(crate) ascending: bool,
    pub(crate) nulls_first: bool,
}

impl SortKey {
    fn new(column: Column, ascending: bool, nulls_first: bool) -> Self {
        Self {
            expr: column.0,
            ascending,
            nulls_first,
        }
    }

    /// Override the direction and apply the corresponding default null placement.
    pub fn with_direction(self, ascending: bool) -> Self {
        Self::new(Column(self.expr), ascending, !ascending)
    }
}

impl<T: Into<Column>> From<T> for SortKey {
    fn from(column: T) -> Self {
        column.into().asc()
    }
}

impl From<Column> for Projection {
    fn from(column: Column) -> Self {
        Self(ProjectionKind::Expression {
            expr: column.0,
            alias: None,
        })
    }
}

impl From<&str> for Projection {
    fn from(name: &str) -> Self {
        if name == "*" {
            star().into()
        } else {
            col(name).into()
        }
    }
}

impl From<String> for Projection {
    fn from(name: String) -> Self {
        if name == "*" {
            star().into()
        } else {
            col(name).into()
        }
    }
}

impl From<Star> for Projection {
    fn from(star: Star) -> Self {
        Self(ProjectionKind::Star(Box::new(star)))
    }
}

#[derive(Clone)]
pub(crate) enum Expr {
    Reference {
        name: String,
        relation: Option<Arc<LogicalPlan>>,
    },
    Aggregate(Aggregate),
    RowNumber,
    Over {
        function: Box<Self>,
        partition_by: Vec<Self>,
        order_by: Vec<SortKey>,
    },
    Literal(BindValue),
    DateFromEpochDays(i64),
    TimestampFromEpoch(i64, TimestampUnit, bool),
    TemporalEpoch(Box<Self>, Option<DataType>),
    UtcDate(Box<Self>),
    InTimeZone(Box<Self>, String),
    TruncateTemporal(Box<Self>, TemporalBucket),
    Minute(Box<Self>),
    ContainsText(Box<Self>, Box<Self>),
    ContainsRegex {
        subject: Box<Self>,
        pattern: String,
        case_insensitive: bool,
        matches_empty: bool,
    },
    InList {
        expr: Box<Self>,
        values: Vec<Self>,
        nulls_equal: bool,
        has_null: bool,
    },
    InQuery(Box<Self>, Arc<LogicalPlan>),
    Binary(Box<Self>, BinaryOp, Box<Self>),
    Cast(Box<Self>, DataType),
    Negate(Box<Self>),
    Power(Box<Self>, Box<Self>),
    Coalesce(Box<Self>, Box<Self>),
    TryParseJson(Box<Self>),
    Not(Box<Self>),
    IsNull(Box<Self>),
    IsNotNull(Box<Self>),
}

impl Expr {
    pub(crate) fn contains_aggregate(&self) -> bool {
        match self {
            Self::Aggregate(_) => true,
            Self::Reference { .. }
            | Self::RowNumber
            | Self::Over { .. }
            | Self::Literal(_)
            | Self::DateFromEpochDays(_)
            | Self::TimestampFromEpoch(..) => false,
            Self::InList { expr, values, .. } => {
                expr.contains_aggregate() || values.iter().any(Self::contains_aggregate)
            }
            Self::Binary(left, _, right)
            | Self::Power(left, right)
            | Self::Coalesce(left, right)
            | Self::ContainsText(left, right) => {
                left.contains_aggregate() || right.contains_aggregate()
            }
            Self::Cast(inner, _)
            | Self::TemporalEpoch(inner, _)
            | Self::UtcDate(inner)
            | Self::InTimeZone(inner, _)
            | Self::TruncateTemporal(inner, _)
            | Self::Minute(inner)
            | Self::ContainsRegex { subject: inner, .. }
            | Self::InQuery(inner, _)
            | Self::TryParseJson(inner)
            | Self::Negate(inner)
            | Self::Not(inner)
            | Self::IsNull(inner)
            | Self::IsNotNull(inner) => inner.contains_aggregate(),
        }
    }

    pub(crate) fn contains_ungrouped_reference(&self) -> bool {
        match self {
            Self::Reference { .. } | Self::Over { .. } => true,
            Self::Aggregate(_)
            | Self::RowNumber
            | Self::Literal(_)
            | Self::DateFromEpochDays(_)
            | Self::TimestampFromEpoch(..) => false,
            Self::InList { expr, values, .. } => {
                expr.contains_ungrouped_reference()
                    || values.iter().any(Self::contains_ungrouped_reference)
            }
            Self::Binary(left, _, right)
            | Self::Power(left, right)
            | Self::Coalesce(left, right)
            | Self::ContainsText(left, right) => {
                left.contains_ungrouped_reference() || right.contains_ungrouped_reference()
            }
            Self::Cast(inner, _)
            | Self::TemporalEpoch(inner, _)
            | Self::UtcDate(inner)
            | Self::InTimeZone(inner, _)
            | Self::TruncateTemporal(inner, _)
            | Self::Minute(inner)
            | Self::ContainsRegex { subject: inner, .. }
            | Self::InQuery(inner, _)
            | Self::TryParseJson(inner)
            | Self::Negate(inner)
            | Self::Not(inner)
            | Self::IsNull(inner)
            | Self::IsNotNull(inner) => inner.contains_ungrouped_reference(),
        }
    }
}

/// Calendar and clock buckets for temporal truncation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TemporalBucket {
    Day,
    Hour,
    QuarterHour,
}

#[derive(Clone, Copy)]
pub enum TimestampUnit {
    Milliseconds,
    Microseconds,
    Nanoseconds,
}

impl TimestampUnit {
    pub(crate) fn scale(self) -> u8 {
        match self {
            Self::Milliseconds => 3,
            Self::Microseconds => 6,
            Self::Nanoseconds => 9,
        }
    }
}

#[derive(Clone)]
pub(crate) enum Aggregate {
    CountAll,
    Count(Box<Expr>),
    Sum(Box<Expr>),
    SumOrZero(Box<Expr>),
}

#[derive(Clone, Copy)]
pub(crate) enum BinaryOp {
    Eq,
    Ne,
    EqMissing,
    NeMissing,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
}

impl BinaryOp {
    pub(crate) fn sql(self) -> &'static str {
        match self {
            Self::Eq => " = ",
            Self::Ne => " <> ",
            Self::EqMissing => " IS NOT DISTINCT FROM ",
            Self::NeMissing => " IS DISTINCT FROM ",
            Self::Lt => " < ",
            Self::Le => " <= ",
            Self::Gt => " > ",
            Self::Ge => " >= ",
            Self::And => " AND ",
            Self::Or => " OR ",
            Self::Add => " + ",
            Self::Subtract => " - ",
            Self::Multiply => " * ",
            Self::Divide => " / ",
            Self::Modulo => " % ",
        }
    }
}
