use std::{fmt, sync::Arc};

use crate::{
    Column, CompileError, CompiledQuery, Field, JoinType, Projection, SortKey, compile,
    expr::Expr,
    ident::ObjectName,
    plan::{LogicalPlan, UnionAlignment},
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
            plan: Arc::new(LogicalPlan::Scan {
                table: ObjectName::parse(parts)?,
                schema: None,
            }),
        })
    }

    /// Read a table from a Snowflake object name such as `DB.SCHEMA."TABLE.WITH.DOTS"`.
    pub fn table_name(name: &str) -> Result<Self, CompileError> {
        Self::table(table_parts(name))
    }

    /// Read a table whose complete ordered schema is known to the caller.
    pub fn table_with_schema(
        parts: impl IntoIterator<Item = impl AsRef<str>>,
        schema: Vec<Field>,
    ) -> Result<Self, CompileError> {
        if schema.is_empty() {
            return Err(CompileError::EmptySchema);
        }
        for (index, field) in schema.iter().enumerate() {
            if let Some(data_type) = field.data_type {
                data_type.validate()?;
            }
            if schema[..index].iter().any(|other| other.name == field.name) {
                return Err(CompileError::DuplicateOutput(field.name()));
            }
        }
        Ok(Self {
            plan: Arc::new(LogicalPlan::Scan {
                table: ObjectName::parse(parts)?,
                schema: Some(schema),
            }),
        })
    }

    /// Read a qualified table name with a supplied schema.
    pub fn table_name_with_schema(name: &str, schema: Vec<Field>) -> Result<Self, CompileError> {
        Self::table_with_schema(table_parts(name), schema)
    }

    /// Describe the outputs known offline. A field's type or nullability may be unknown.
    /// Returns `None` when the plan cannot determine all output names.
    pub fn schema(&self) -> Result<Option<Vec<Field>>, CompileError> {
        compile::schema(&self.plan)
    }

    /// Return the ordered Snowflake identifiers known offline.
    pub fn columns(&self) -> Result<Option<Vec<String>>, CompileError> {
        Ok(self
            .schema()?
            .map(|fields| fields.iter().map(Field::name).collect()))
    }

    /// Add a column or replace the existing column, placing it last.
    pub fn with_column(&self, name: impl Into<String>, column: Column) -> Self {
        self.with_column_order(name, column, false)
    }

    /// Add or replace a column, optionally retaining the position of an existing column.
    pub fn with_column_order(
        &self,
        name: impl Into<String>,
        column: Column,
        keep_column_order: bool,
    ) -> Self {
        Self {
            plan: Arc::new(LogicalPlan::WithColumn {
                input: Arc::clone(&self.plan),
                name: name.into(),
                expr: column.0,
                keep_column_order,
            }),
        }
    }

    /// Rename one column without changing its position.
    pub fn rename(&self, old: impl Into<Column>, new: impl Into<String>) -> Self {
        self.rename_columns([(old, new)])
    }

    /// Rename columns simultaneously, preserving their order.
    pub fn rename_columns(
        &self,
        names: impl IntoIterator<Item = (impl Into<Column>, impl Into<String>)>,
    ) -> Self {
        let names: Vec<_> = names
            .into_iter()
            .map(|(old, new)| (old.into().0, new.into()))
            .collect();
        if names.is_empty() {
            return self.clone();
        }
        Self {
            plan: Arc::new(LogicalPlan::Rename {
                input: Arc::clone(&self.plan),
                names,
            }),
        }
    }

    /// Remove duplicate rows.
    pub fn distinct(&self) -> Self {
        Self {
            plan: Arc::new(LogicalPlan::Distinct {
                input: Arc::clone(&self.plan),
            }),
        }
    }

    /// Combine inputs by position, removing duplicate rows.
    pub fn union(&self, other: &Self) -> Self {
        self.union_aligned(other, false, UnionAlignment::Position)
    }

    /// Combine inputs by position, retaining duplicate rows.
    pub fn union_all(&self, other: &Self) -> Self {
        self.union_aligned(other, true, UnionAlignment::Position)
    }

    /// Align columns by name and remove duplicate rows. Missing fields may be filled with NULL.
    pub fn union_by_name(&self, other: &Self, allow_missing_columns: bool) -> Self {
        self.union_aligned(
            other,
            false,
            UnionAlignment::Name {
                allow_missing_columns,
            },
        )
    }

    /// Align columns by name and retain duplicate rows. Missing fields may be filled with NULL.
    pub fn union_all_by_name(&self, other: &Self, allow_missing_columns: bool) -> Self {
        self.union_aligned(
            other,
            true,
            UnionAlignment::Name {
                allow_missing_columns,
            },
        )
    }

    fn union_aligned(&self, other: &Self, all: bool, alignment: UnionAlignment) -> Self {
        Self {
            plan: Arc::new(LogicalPlan::Union {
                left: Arc::clone(&self.plan),
                right: Arc::clone(&other.plan),
                all,
                alignment,
            }),
        }
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
        self.join_as_with_suffixes(right, condition, JoinType::Inner, left_suffix, right_suffix)
    }

    /// Join two frames with an ON condition and the requested join type.
    pub fn join_as(&self, right: &Self, condition: Column, how: JoinType) -> Self {
        self.join_as_with_suffixes(right, condition, how, "", "")
    }

    /// Join two frames with explicit suffixes for overlapping output names.
    pub fn join_as_with_suffixes(
        &self,
        right: &Self,
        condition: Column,
        how: JoinType,
        left_suffix: &str,
        right_suffix: &str,
    ) -> Self {
        Self {
            plan: Arc::new(LogicalPlan::Join {
                left: Arc::clone(&self.plan),
                right: Arc::clone(&right.plan),
                condition: condition.0,
                how,
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

fn table_parts(name: &str) -> impl Iterator<Item = &str> {
    let mut quoted = false;
    name.split(move |character| {
        if character == '"' {
            // Each doubled quote toggles twice, leaving the quoted state unchanged.
            quoted = !quoted;
        }
        character == '.' && !quoted
    })
    .map(str::trim)
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
