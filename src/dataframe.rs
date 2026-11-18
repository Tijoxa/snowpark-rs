use std::fmt;

use crate::{Column, CompileError, CompiledQuery, Projection, SortKey};

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

    /// Reference a column exported by this frame in a join condition.
    pub fn col(&self, name: impl Into<String>) -> Column {
        self.plan.col(name)
    }

    /// Group rows by the supplied keys before aggregating.
    pub fn group_by(
        &self,
        keys: impl IntoIterator<Item = impl Into<Projection>>,
    ) -> GroupedDataFrame {
        GroupedDataFrame(self.plan.group_by(keys))
    }

    /// Aggregate all input rows as a single group.
    pub fn agg(&self, aggregates: impl IntoIterator<Item = impl Into<Projection>>) -> Self {
        Self {
            plan: self.plan.agg(aggregates),
        }
    }

    /// Order the result by the supplied keys.
    pub fn sort(&self, keys: impl IntoIterator<Item = impl Into<SortKey>>) -> Self {
        Self {
            plan: self.plan.sort(keys),
        }
    }

    /// Return at most `count` rows.
    pub fn limit(&self, count: u64) -> Self {
        Self {
            plan: self.plan.limit(count),
        }
    }

    /// Skip `offset` rows, then return at most `count` rows.
    pub fn limit_with_offset(&self, count: u64, offset: u64) -> Self {
        Self {
            plan: self.plan.limit_with_offset(count, offset),
        }
    }

    /// Inner join two frames with explicit, named projections.
    pub fn join(&self, right: &Self, condition: Column) -> Self {
        Self {
            plan: self.plan.join(&right.plan, condition),
        }
    }

    /// Inner join, using suffixes for overlapping output names when supplied.
    pub fn join_with_suffixes(
        &self,
        right: &Self,
        condition: Column,
        left_suffix: &str,
        right_suffix: &str,
    ) -> Self {
        Self {
            plan: self
                .plan
                .join_with_suffixes(&right.plan, condition, left_suffix, right_suffix),
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

/// Grouping keys for a subsequent aggregate over a DataFrame.
#[derive(Clone)]
#[must_use]
pub struct GroupedDataFrame(snowflake_ast::GroupedDataFrame);

impl GroupedDataFrame {
    pub fn agg(&self, aggregates: impl IntoIterator<Item = impl Into<Projection>>) -> DataFrame {
        DataFrame {
            plan: self.0.agg(aggregates),
        }
    }
}
