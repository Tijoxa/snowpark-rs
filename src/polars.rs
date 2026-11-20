//! Lazy translation of typed Polars expressions into Snowflake queries.

mod translate;

use std::collections::HashMap;

pub use polars_core::prelude::DataType;
pub use polars_plan::{
    dsl,
    prelude::{Expr, all, col, len, lit},
};
use snowflake_ast::DataFrame as AstFrame;
use translate::{Shape, Translator, projections};

use crate::{CompileError, DataFrame, Projection};

type Result<T> = std::result::Result<T, CompileError>;
type MembershipResolver<'a> = dyn Fn(&str) -> Result<Option<AstFrame>> + 'a;

#[derive(Default)]
pub(crate) struct Context<'a> {
    pub(crate) plugin_path: Option<&'a std::path::Path>,
    pub(crate) membership: Option<&'a MembershipResolver<'a>>,
}

#[derive(Clone, Copy)]
pub(crate) enum PredicateClause {
    Filter,
    Having,
    Qualify,
}

impl PredicateClause {
    pub(crate) fn operation(self) -> &'static str {
        match self {
            Self::Filter => "filter",
            Self::Having => "having",
            Self::Qualify => "qualify",
        }
    }
}

/// A Polars sort expression with explicit direction and NULL placement.
#[derive(Clone)]
pub struct SortKey {
    pub expr: Expr,
    pub descending: bool,
    pub nulls_last: bool,
}

/// Typed Polars grouping keys for a subsequent aggregate.
#[derive(Clone)]
#[must_use]
pub struct GroupedDataFrame {
    input: DataFrame,
    keys: Vec<Expr>,
}

impl DataFrame {
    /// Apply a Polars predicate when the Snowflake plan is first resolved.
    pub fn filter_polars(&self, predicate: Expr) -> Self {
        self.defer_polars(move |input| {
            filter(
                input,
                std::slice::from_ref(&predicate),
                &Context::default(),
                PredicateClause::Filter,
            )
        })
    }

    /// Apply a Polars predicate to the current aggregation.
    pub fn having_polars(&self, predicate: Expr) -> Self {
        self.defer_polars(move |input| {
            filter(
                input,
                std::slice::from_ref(&predicate),
                &Context::default(),
                PredicateClause::Having,
            )
        })
    }

    /// Apply a Polars predicate after computing windows.
    pub fn qualify_polars(&self, predicate: Expr) -> Self {
        self.defer_polars(move |input| {
            filter(
                input,
                std::slice::from_ref(&predicate),
                &Context::default(),
                PredicateClause::Qualify,
            )
        })
    }

    /// Select Polars expressions, expanding selectors against the input schema.
    pub fn select_polars(&self, exprs: impl IntoIterator<Item = Expr>) -> Self {
        let exprs: Vec<_> = exprs.into_iter().collect();
        self.defer_polars(move |input| select(input, &exprs, &Context::default()))
    }

    /// Add or replace columns, resolving every expression against the same input.
    pub fn with_columns_polars(&self, exprs: impl IntoIterator<Item = Expr>) -> Self {
        let exprs: Vec<_> = exprs.into_iter().collect();
        self.defer_polars(move |input| with_columns(input, &exprs, &Context::default()))
    }

    /// Group by Polars expressions.
    pub fn group_by_polars(&self, keys: impl IntoIterator<Item = Expr>) -> GroupedDataFrame {
        GroupedDataFrame {
            input: self.clone(),
            keys: keys.into_iter().collect(),
        }
    }

    /// Aggregate all input rows as a single group using Polars expressions.
    pub fn agg_polars(&self, exprs: impl IntoIterator<Item = Expr>) -> Self {
        self.group_by_polars([]).agg(exprs)
    }

    /// Order the result by Polars expressions.
    pub fn sort_polars(&self, keys: impl IntoIterator<Item = SortKey>) -> Self {
        let keys: Vec<_> = keys.into_iter().collect();
        self.defer_polars(move |input| sort(input, &keys, &Context::default()))
    }

    fn defer_polars(
        &self,
        lower: impl Fn(&AstFrame) -> Result<AstFrame> + Send + Sync + 'static,
    ) -> Self {
        Self {
            plan: self.plan.defer(lower),
        }
    }
}

impl GroupedDataFrame {
    /// Apply Polars reductions in each group.
    pub fn agg(&self, exprs: impl IntoIterator<Item = Expr>) -> DataFrame {
        let keys = self.keys.clone();
        let exprs: Vec<_> = exprs.into_iter().collect();
        self.input
            .defer_polars(move |input| aggregate(input, &keys, &exprs, &Context::default()))
    }
}

pub(crate) fn unsupported(
    operation: &'static str,
    node: impl Into<String>,
    reason: impl Into<String>,
) -> CompileError {
    CompileError::UnsupportedExpression {
        operation,
        node: node.into(),
        reason: reason.into(),
    }
}

pub(crate) fn select(input: &AstFrame, exprs: &[Expr], context: &Context<'_>) -> Result<AstFrame> {
    if matches!(
        exprs,
        [Expr::Selector(polars_plan::dsl::Selector::Wildcard)]
    ) {
        return Ok(input.clone());
    }
    let translator = Translator::new(input, "select", context)?;
    let values = translator.expressions(exprs)?;
    let output = projections(&translator, &values)?;
    if values.iter().any(|expr| expr.shape == Shape::Aggregate) {
        if values.iter().any(|expr| expr.shape == Shape::Row) {
            return Err(translator.error("Agg", translate::BROADCAST));
        }
        return Ok(input.agg(output));
    }
    if values.iter().all(|expr| expr.shape == Shape::Scalar) {
        return Ok(input.select_scalar(output));
    }
    Ok(input.select(output))
}

pub(crate) fn filter(
    input: &AstFrame,
    exprs: &[Expr],
    context: &Context<'_>,
    clause: PredicateClause,
) -> Result<AstFrame> {
    let mut translator = Translator::new(input, clause.operation(), context)?;
    let filter_rows = matches!(clause, PredicateClause::Filter);
    if !filter_rows {
        translator.set_clause_source(input.clause_input_schema()?);
    }
    let mut predicates = Vec::with_capacity(exprs.len());
    for expr in exprs {
        let [value] = translator
            .expressions(std::slice::from_ref(expr))?
            .try_into()
            .map_err(|_| {
                translator.error(
                    "Selector",
                    "a predicate must expand to exactly one expression",
                )
            })?;
        if (filter_rows && value.shape == Shape::Aggregate)
            || value.untyped_null
            || value
                .dtype
                .is_some_and(|dtype| dtype != crate::DataType::Boolean)
        {
            let reason = if filter_rows {
                "a row-wise Boolean expression is required"
            } else {
                "a Boolean expression is required"
            };
            return Err(translator.error("predicate", reason));
        }
        predicates.push(value.column);
    }
    let Some(predicate) = predicates.into_iter().reduce(crate::Column::and) else {
        return Ok(input.clone());
    };
    Ok(match clause {
        PredicateClause::Filter => input.filter(predicate),
        PredicateClause::Having => input.having(predicate),
        PredicateClause::Qualify => input.qualify(predicate),
    })
}

pub(crate) fn with_columns(
    input: &AstFrame,
    exprs: &[Expr],
    context: &Context<'_>,
) -> Result<AstFrame> {
    let translator = Translator::new(input, "with_columns", context)?;
    let values = translator.expressions(exprs)?;
    if values.is_empty() {
        return Ok(input.clone());
    }
    let schema = translator
        .schema
        .as_ref()
        .ok_or_else(|| translator.error("projection", translate::DECLARE_SCHEMA))?;
    if values.iter().any(|expr| expr.shape == Shape::Aggregate) {
        return Err(translator.error("Agg", translate::BROADCAST));
    }
    let output = projections(&translator, &values)?;
    let mut changes: HashMap<_, _> = values
        .iter()
        .map(|value| value.name.as_str())
        .zip(output)
        .collect();
    let mut output: Vec<Projection> = schema
        .iter()
        .map(|(name, _)| {
            changes
                .remove(name.as_str())
                .unwrap_or_else(|| crate::col(translate::identifier(name)).into())
        })
        .collect();
    output.extend(
        values
            .iter()
            .filter_map(|value| changes.remove(value.name.as_str())),
    );
    Ok(input.select(output))
}

pub(crate) fn aggregate(
    input: &AstFrame,
    keys: &[Expr],
    exprs: &[Expr],
    context: &Context<'_>,
) -> Result<AstFrame> {
    let grouping = Translator::new(input, "group_by", context)?;
    let mut by = grouping.expressions(keys)?;
    if (!keys.is_empty() && by.is_empty()) || by.iter().any(|expr| expr.shape != Shape::Row) {
        return Err(grouping.error("group key", "grouping requires row-wise expressions"));
    }
    let translator = grouping.for_operation("agg");
    let values = translator.expressions(exprs)?;
    if values.iter().any(|expr| expr.shape == Shape::Row) {
        return Err(translator.error(
            "Agg",
            "use scalar reductions such as sum(), count() or pl.len()",
        ));
    }
    let key_count = by.len();
    let has_values = !values.is_empty();
    let scalar = values.iter().all(|expr| expr.shape == Shape::Scalar);
    by.extend(values);
    let mut output = projections(&translator, &by)?;
    if !has_values {
        return Ok(input.select(output).distinct());
    }
    if key_count == 0 && scalar {
        return Ok(input.select_scalar(output));
    }
    let values = output.split_off(key_count);
    Ok(input.group_by(output).agg(values))
}

pub(crate) fn sort(input: &AstFrame, keys: &[SortKey], context: &Context<'_>) -> Result<AstFrame> {
    let translator = Translator::new(input, "sort", context)?;
    let mut output = Vec::new();
    for key in keys {
        for expr in translator.expressions(std::slice::from_ref(&key.expr))? {
            let column = match expr.shape {
                Shape::Aggregate => {
                    return Err(
                        translator.error("sort key", "reductions cannot be used as sort keys")
                    );
                }
                Shape::Scalar => continue,
                Shape::Row => expr.column,
            };
            output.push(match (key.descending, key.nulls_last) {
                (false, false) => column.asc_nulls_first(),
                (false, true) => column.asc_nulls_last(),
                (true, false) => column.desc_nulls_first(),
                (true, true) => column.desc_nulls_last(),
            });
        }
    }
    Ok(if output.is_empty() {
        input.clone()
    } else {
        input.sort(output)
    })
}

#[cfg(feature = "python")]
pub(crate) fn join_keys(input: &AstFrame, keys: &[Expr]) -> Result<Vec<String>> {
    let context = Context::default();
    let translator = Translator::new(input, "join", &context)?;
    translator
        .expressions(keys)?
        .into_iter()
        .map(|item| {
            if item.reference.as_deref() == Some(item.name.as_str()) {
                Ok(item.name)
            } else {
                Err(translator.error("join keys", "join keys must be direct column references"))
            }
        })
        .collect()
}
