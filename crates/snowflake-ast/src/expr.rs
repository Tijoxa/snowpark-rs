use std::{ops::Not, sync::Arc};

use crate::{BindValue, plan::LogicalPlan};

/// A reference, literal or predicate in a DataFrame expression.
#[derive(Clone)]
#[must_use]
pub struct Column(pub(crate) Expr);

/// An output expression with an optional SQL alias.
#[derive(Clone)]
#[must_use]
pub struct Projection {
    pub(crate) expr: Expr,
    pub(crate) alias: Option<String>,
}

/// Reference a column by an unquoted or double-quoted Snowflake identifier.
pub fn col(name: impl Into<String>) -> Column {
    Column(Expr::Reference {
        name: name.into(),
        relation: None,
    })
}

/// Bind a string, integer or boolean as a SQL parameter.
pub fn lit(value: impl Into<BindValue>) -> Column {
    Column(Expr::Literal(value.into()))
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

impl Column {
    pub fn alias(self, name: impl Into<String>) -> Projection {
        Projection {
            expr: self.0,
            alias: Some(name.into()),
        }
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
        Self {
            expr: column.0,
            alias: None,
        }
    }
}

impl From<&str> for Projection {
    fn from(name: &str) -> Self {
        col(name).into()
    }
}

impl From<String> for Projection {
    fn from(name: String) -> Self {
        col(name).into()
    }
}

#[derive(Clone)]
pub(crate) enum Expr {
    Reference {
        name: String,
        relation: Option<Arc<LogicalPlan>>,
    },
    Aggregate(Aggregate),
    Literal(BindValue),
    Binary(Box<Self>, BinaryOp, Box<Self>),
    Not(Box<Self>),
    IsNull(Box<Self>),
    IsNotNull(Box<Self>),
}

#[derive(Clone)]
pub(crate) enum Aggregate {
    CountAll,
    Count(Box<Expr>),
    Sum(Box<Expr>),
}

#[derive(Clone, Copy)]
pub(crate) enum BinaryOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
}

impl BinaryOp {
    pub(crate) fn sql(self) -> &'static str {
        match self {
            Self::Eq => " = ",
            Self::Ne => " <> ",
            Self::Lt => " < ",
            Self::Le => " <= ",
            Self::Gt => " > ",
            Self::Ge => " >= ",
            Self::And => " AND ",
            Self::Or => " OR ",
        }
    }
}
