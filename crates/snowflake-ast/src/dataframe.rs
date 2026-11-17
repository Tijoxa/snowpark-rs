use std::{fmt, sync::Arc};

use crate::{
    Column, CompileError, CompiledQuery, Projection, compile, ident::ObjectName, plan::LogicalPlan,
};

/// An immutable Snowflake query that can be compiled offline.
#[derive(Clone)]
#[must_use]
pub struct DataFrame {
    plan: Arc<LogicalPlan>,
}

impl DataFrame {
    /// Read a table by its name, `[schema, table]` or `[database, schema, table]`.
    pub fn table(parts: impl IntoIterator<Item = impl AsRef<str>>) -> Result<Self, CompileError> {
        Ok(Self {
            plan: Arc::new(LogicalPlan::Scan(ObjectName::parse(parts)?)),
        })
    }

    /// Read a table from a Snowflake object name such as `DB.SCHEMA."TABLE.WITH.DOTS"`.
    pub fn table_name(name: &str) -> Result<Self, CompileError> {
        let mut quoted = false;
        let parts = name.split(|character| {
            if character == '"' {
                // Each doubled quote toggles twice, leaving the quoted state unchanged.
                quoted = !quoted;
            }
            character == '.' && !quoted
        });
        Self::table(parts.map(str::trim))
    }

    /// Keep rows for which the predicate evaluates to true.
    pub fn filter(&self, predicate: Column) -> Self {
        Self {
            plan: Arc::new(LogicalPlan::Filter {
                input: Arc::clone(&self.plan),
                predicate: predicate.0,
            }),
        }
    }

    /// Select column names, expressions or aliased projections.
    pub fn select(&self, projections: impl IntoIterator<Item = impl Into<Projection>>) -> Self {
        Self {
            plan: Arc::new(LogicalPlan::Project {
                input: Arc::clone(&self.plan),
                projections: projections.into_iter().map(Into::into).collect(),
            }),
        }
    }

    /// Compile the plan to Snowflake SQL and ordered bind values.
    pub fn to_sql(&self) -> Result<CompiledQuery, CompileError> {
        compile::compile(&self.plan)
    }
}

impl fmt::Debug for DataFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataFrame")
            .field("query", &self.to_sql())
            .finish()
    }
}
