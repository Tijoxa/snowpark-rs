use std::fmt;

use crate::{Column, CompileError, CompiledQuery, Projection};

#[cfg(feature = "session")]
use crate::session::{self, ArrowBatches, ExecuteError, RecordBatch, Session};

/// An immutable Snowflake query that can be compiled offline.
#[derive(Clone)]
#[must_use]
pub struct DataFrame {
    plan: snowflake_ast::DataFrame,
}

impl DataFrame {
    /// Read a table by its name, `[schema, table]` or `[database, schema, table]`.
    pub fn table(parts: impl IntoIterator<Item = impl AsRef<str>>) -> Result<Self, CompileError> {
        Ok(Self {
            plan: snowflake_ast::DataFrame::table(parts)?,
        })
    }
    /// Read a table from a qualified Snowflake object name.
    pub fn table_name(name: &str) -> Result<Self, CompileError> {
        Ok(Self {
            plan: snowflake_ast::DataFrame::table_name(name)?,
        })
    }

    /// Keep rows for which the predicate evaluates to true.
    pub fn filter(&self, predicate: Column) -> Self {
        Self {
            plan: self.plan.filter(predicate),
        }
    }

    /// Select column names, expressions or aliased projections.
    pub fn select(&self, projections: impl IntoIterator<Item = impl Into<Projection>>) -> Self {
        Self {
            plan: self.plan.select(projections),
        }
    }

    /// Compile the plan to Snowflake SQL and ordered bind values.
    pub fn to_sql(&self) -> Result<CompiledQuery, CompileError> {
        self.plan.to_sql()
    }

    /// Execute the plan with an open session and materialize the Arrow result.
    #[cfg(feature = "session")]
    pub async fn to_arrow(&self, session: &Session) -> Result<RecordBatch, ExecuteError> {
        session::execute_arrow(session, self.to_sql()?).await
    }

    /// Execute the plan with an open session and read Arrow batches incrementally.
    #[cfg(feature = "session")]
    pub async fn to_arrow_batches(&self, session: &Session) -> Result<ArrowBatches, ExecuteError> {
        session::execute_arrow_batches(session, self.to_sql()?).await
    }
}

impl fmt::Debug for DataFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.plan.fmt(f)
    }
}
