use std::{fmt, sync::Arc};

use crate::{
    Column, CompileError, CompiledQuery, Projection, SortKey, compile, expr::Expr,
    ident::ObjectName, plan::LogicalPlan,
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

    /// Reference a column exported by this frame in a join condition.
    pub fn col(&self, name: impl Into<String>) -> Column {
        Column(Expr::Reference {
            name: name.into(),
            relation: Some(Arc::clone(&self.plan)),
        })
    }

    /// Group rows by the supplied keys before aggregating.
    pub fn group_by(
        &self,
        keys: impl IntoIterator<Item = impl Into<Projection>>,
    ) -> GroupedDataFrame {
        GroupedDataFrame {
            input: Arc::clone(&self.plan),
            keys: keys.into_iter().map(Into::into).collect(),
        }
    }

    /// Aggregate all input rows as a single group.
    pub fn agg(&self, aggregates: impl IntoIterator<Item = impl Into<Projection>>) -> Self {
        self.group_by(std::iter::empty::<Projection>())
            .agg(aggregates)
    }

    /// Order the result by the supplied keys.
    pub fn sort(&self, keys: impl IntoIterator<Item = impl Into<SortKey>>) -> Self {
        Self {
            plan: Arc::new(LogicalPlan::Sort {
                input: Arc::clone(&self.plan),
                keys: keys.into_iter().map(Into::into).collect(),
            }),
        }
    }

    /// Return at most `count` rows.
    pub fn limit(&self, count: u64) -> Self {
        self.limit_with_offset(count, 0)
    }

    /// Skip `offset` rows, then return at most `count` rows.
    pub fn limit_with_offset(&self, count: u64, offset: u64) -> Self {
        Self {
            plan: Arc::new(LogicalPlan::Limit {
                input: Arc::clone(&self.plan),
                count,
                offset,
            }),
        }
    }

    /// Inner join two frames with explicit, named projections.
    pub fn join(&self, right: &Self, condition: Column) -> Self {
        self.join_with_suffixes(right, condition, "", "")
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
            plan: Arc::new(LogicalPlan::Join {
                left: Arc::clone(&self.plan),
                right: Arc::clone(&right.plan),
                condition: condition.0,
                left_suffix: left_suffix.into(),
                right_suffix: right_suffix.into(),
            }),
        }
    }

    /// Compile the plan to Snowflake SQL and ordered bind values.
    pub fn to_sql(&self) -> Result<CompiledQuery, CompileError> {
        compile::compile(&self.plan)
    }
}

/// Grouping keys for a subsequent aggregate over a DataFrame.
#[derive(Clone)]
#[must_use]
pub struct GroupedDataFrame {
    input: Arc<LogicalPlan>,
    keys: Vec<Projection>,
}

impl GroupedDataFrame {
    pub fn agg(&self, aggregates: impl IntoIterator<Item = impl Into<Projection>>) -> DataFrame {
        DataFrame {
            plan: Arc::new(LogicalPlan::Aggregate {
                input: Arc::clone(&self.input),
                keys: self.keys.clone(),
                aggregates: aggregates.into_iter().map(Into::into).collect(),
            }),
        }
    }
}

impl fmt::Debug for DataFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataFrame")
            .field("query", &self.to_sql())
            .finish()
    }
}
