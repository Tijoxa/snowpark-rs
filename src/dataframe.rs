use std::fmt;

use crate::{Column, CompileError, CompiledQuery, Field, JoinType, Projection, SortKey};

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

    /// Read a table with its complete ordered schema.
    pub fn table_with_schema(
        parts: impl IntoIterator<Item = impl AsRef<str>>,
        schema: Vec<Field>,
    ) -> Result<Self, CompileError> {
        Ok(Self {
            plan: snowflake_ast::DataFrame::table_with_schema(parts, schema)?,
        })
    }

    /// Read a qualified table name with its complete ordered schema.
    pub fn table_name_with_schema(name: &str, schema: Vec<Field>) -> Result<Self, CompileError> {
        Ok(Self {
            plan: snowflake_ast::DataFrame::table_name_with_schema(name, schema)?,
        })
    }

    /// Return the field metadata available offline.
    pub fn schema(&self) -> Result<Option<Vec<Field>>, CompileError> {
        self.plan.schema()
    }

    /// Return the column identifiers available offline.
    pub fn columns(&self) -> Result<Option<Vec<String>>, CompileError> {
        self.plan.columns()
    }

    /// Add or replace a column, placing it last.
    pub fn with_column(&self, name: impl Into<String>, column: Column) -> Self {
        Self {
            plan: self.plan.with_column(name, column),
        }
    }

    /// Add or replace a column with control over the position of a replaced column.
    pub fn with_column_order(
        &self,
        name: impl Into<String>,
        column: Column,
        keep_column_order: bool,
    ) -> Self {
        Self {
            plan: self.plan.with_column_order(name, column, keep_column_order),
        }
    }

    /// Rename one column without changing its position.
    pub fn rename(&self, old: impl Into<Column>, new: impl Into<String>) -> Self {
        Self {
            plan: self.plan.rename(old, new),
        }
    }

    /// Rename columns simultaneously, preserving their order.
    pub fn rename_columns(
        &self,
        names: impl IntoIterator<Item = (impl Into<Column>, impl Into<String>)>,
    ) -> Self {
        Self {
            plan: self.plan.rename_columns(names),
        }
    }

    /// Remove duplicate rows.
    pub fn distinct(&self) -> Self {
        Self {
            plan: self.plan.distinct(),
        }
    }

    pub fn union(&self, other: &Self) -> Self {
        Self {
            plan: self.plan.union(&other.plan),
        }
    }

    pub fn union_all(&self, other: &Self) -> Self {
        Self {
            plan: self.plan.union_all(&other.plan),
        }
    }

    pub fn union_by_name(&self, other: &Self, allow_missing_columns: bool) -> Self {
        Self {
            plan: self.plan.union_by_name(&other.plan, allow_missing_columns),
        }
    }

    pub fn union_all_by_name(&self, other: &Self, allow_missing_columns: bool) -> Self {
        Self {
            plan: self
                .plan
                .union_all_by_name(&other.plan, allow_missing_columns),
        }
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

    /// Join two frames with an ON condition and the requested join type.
    pub fn join_as(&self, right: &Self, condition: Column, how: JoinType) -> Self {
        Self {
            plan: self.plan.join_as(&right.plan, condition, how),
        }
    }

    /// Join two frames with the requested join type and suffixes for overlapping names.
    pub fn join_as_with_suffixes(
        &self,
        right: &Self,
        condition: Column,
        how: JoinType,
        left_suffix: &str,
        right_suffix: &str,
    ) -> Self {
        Self {
            plan: self.plan.join_as_with_suffixes(
                &right.plan,
                condition,
                how,
                left_suffix,
                right_suffix,
            ),
        }
    }

    /// Compile the plan to Snowflake SQL and ordered bind values.
    pub fn to_sql(&self) -> Result<CompiledQuery, CompileError> {
        self.plan.to_sql()
    }

    /// Read Snowflake's result schema using a zero-row query.
    #[cfg(feature = "session")]
    pub async fn schema_with_session(
        &self,
        session: &Session,
    ) -> Result<arrow_schema::SchemaRef, ExecuteError> {
        Ok(self
            .limit(0)
            .to_arrow_batches(session)
            .await?
            .schema()
            .clone())
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
