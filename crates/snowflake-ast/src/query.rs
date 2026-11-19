use std::fmt;

use crate::{DataType, Decimal};

/// A value bound to a Snowflake SQL placeholder.
#[derive(Clone, Debug, PartialEq)]
pub enum BindValue {
    Text(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Null(DataType),
    Decimal(Decimal),
}

impl From<Decimal> for BindValue {
    fn from(value: Decimal) -> Self {
        Self::Decimal(value)
    }
}

impl From<&str> for BindValue {
    fn from(value: &str) -> Self {
        Self::Text(value.to_owned())
    }
}

impl From<String> for BindValue {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<i64> for BindValue {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<i32> for BindValue {
    fn from(value: i32) -> Self {
        Self::Int(i64::from(value))
    }
}

impl From<bool> for BindValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

/// Snowflake SQL and its parameters in textual placeholder order.
#[derive(Clone, Default, PartialEq)]
pub struct CompiledQuery {
    pub sql: String,
    pub binds: Vec<BindValue>,
}

impl fmt::Debug for CompiledQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompiledQuery")
            .field("sql", &self.sql)
            .field("bind_count", &self.binds.len())
            .finish()
    }
}

impl CompiledQuery {
    pub(crate) fn append(&mut self, fragment: Self) {
        self.sql.push_str(&fragment.sql);
        self.binds.extend(fragment.binds);
    }
}
