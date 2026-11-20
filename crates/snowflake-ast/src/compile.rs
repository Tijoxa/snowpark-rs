use std::{cell::Cell, collections::HashSet, sync::Arc};

use crate::{
    BindValue, CompiledQuery, DataType, Field, JoinType, Projection, SortKey, col,
    expr::{Aggregate, BinaryOp, Expr},
    ident::Ident,
    plan::{LogicalPlan, PredicateClause, UnionAlignment},
};

/// An invalid name, expression or column reference in a DataFrame plan.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum CompileError {
    #[error("{operation}: {node}: {reason}")]
    UnsupportedExpression {
        operation: &'static str,
        node: String,
        reason: String,
    },
    #[error("invalid Snowflake identifier {0:?}")]
    InvalidIdentifier(String),
    #[error("table names must contain one to three separate identifier parts")]
    InvalidTableName,
    #[error("select requires at least one projection")]
    EmptyProjection,
    #[error("column {0:?} is not exported by the input projection")]
    UnknownColumn(String),
    #[error("column {0:?} is ambiguous in the input projection")]
    AmbiguousColumn(String),
    #[error("column {0:?} refers to a DataFrame outside this operation's input scope")]
    OutOfScopeColumn(String),
    #[error("agg requires at least one aggregate expression")]
    EmptyAggregate,
    #[error("agg expects COUNT or SUM expressions")]
    ExpectedAggregate,
    #[error("aggregate expressions must appear directly in agg")]
    UnexpectedAggregate,
    #[error(
        "having requires an aggregation in the current query block before qualify, distinct or limit"
    )]
    HavingWithoutAggregate,
    #[error("qualify requires a window expression in its predicate or in the current projection")]
    QualifyWithoutWindow,
    #[error("window expressions require a row projection or a qualify predicate")]
    UnexpectedWindow,
    #[error("over expects a COUNT, SUM or ROW_NUMBER with an explicit ordering")]
    InvalidWindow,
    #[error("tail requires an explicit sort with at least one key")]
    TailWithoutOrder,
    #[error("an intermediate sort must be followed by limit before further transformations")]
    IntermediateSort,
    #[error("both sides of a join require named outputs from a schema or an explicit projection")]
    UnnamedJoinInput,
    #[error("a join requires distinct inputs; select each side separately for a self-join")]
    IdenticalJoinInputs,
    #[error("the join produces duplicate column name {0}")]
    DuplicateJoinOutput(String),
    #[error("unsupported or invalid Snowflake data type {0:?}")]
    InvalidDataType(String),
    #[error(
        "expected a finite decimal with at most 38 digits of precision and 37 fractional digits"
    )]
    InvalidDecimal,
    #[error(
        "{0} requires known column names; declare the source schema or select named columns first"
    )]
    UnknownSchema(&'static str),
    #[error("a source schema requires at least one field")]
    EmptySchema,
    #[error("rename expects an input column reference")]
    ExpectedReference,
    #[error("duplicate output column {0}")]
    DuplicateOutput(String),
    #[error("union inputs have different column counts ({left} and {right})")]
    UnionArity { left: usize, right: usize },
    #[error("union by name inputs have different column names")]
    UnionNames,
    #[error("is_in requires a single-column query, got {0} columns")]
    MembershipArity(usize),
}

pub(crate) fn compile(plan: &LogicalPlan) -> Result<CompiledQuery, CompileError> {
    Ok(Compiler::default().block(plan)?.finish().0)
}

pub(crate) fn schema(plan: &LogicalPlan) -> Result<Option<Vec<Field>>, CompileError> {
    let (_, columns) = Compiler::default().block(plan)?.finish();
    Ok(columns.and_then(|columns| columns.into_iter().map(Output::into_field).collect()))
}

pub(crate) fn clause_input_schema(plan: &LogicalPlan) -> Result<Option<Vec<Field>>, CompileError> {
    let block = Compiler::default().block(plan)?;
    let columns = if block.distinct || block.limit.is_some() {
        block.finish().1
    } else {
        block.columns
    };
    Ok(columns.and_then(|columns| columns.into_iter().map(Output::into_field).collect()))
}

#[derive(Default)]
struct Compiler {
    next_alias: usize,
}

impl Compiler {
    fn source(&mut self, from: CompiledQuery, columns: Option<Columns>) -> Block {
        let alias = self.next_alias;
        self.next_alias += 1;
        Block {
            from,
            alias,
            columns,
            projection: None,
            distinct: false,
            predicate: None,
            having: None,
            qualify: None,
            grouped: false,
            group_keys: 0,
            order: None,
            limit: None,
        }
    }

    fn subquery(&mut self, (query, columns): (CompiledQuery, Option<Columns>)) -> Block {
        self.source(parenthesized(query), columns)
    }

    fn block(&mut self, plan: &LogicalPlan) -> Result<Block, CompileError> {
        match plan {
            LogicalPlan::Deferred { lower, resolved } => {
                self.block(resolved.get_or_init(lower).as_ref().map_err(Clone::clone)?)
            }
            LogicalPlan::Scan { table, schema } => Ok(self.source(
                CompiledQuery {
                    sql: table.to_string(),
                    binds: Vec::new(),
                },
                schema
                    .as_ref()
                    .map(|schema| schema.iter().map(Output::from).collect()),
            )),
            LogicalPlan::Filter {
                input,
                predicate,
                clause,
            } => self.filter(input, predicate, *clause),
            LogicalPlan::Project { input, projections } => {
                if projections.is_empty() {
                    return Err(CompileError::EmptyProjection);
                }
                let mut block = self.projection_input(input)?;
                block.project(input, projections, &[], true)?;
                Ok(block)
            }
            LogicalPlan::ScalarProject { input, projections } => {
                if projections.is_empty() {
                    return Err(CompileError::EmptyProjection);
                }
                self.block(input)?;
                let mut block = self.source(
                    CompiledQuery {
                        sql: "(SELECT 1)".into(),
                        binds: Vec::new(),
                    },
                    Some(Vec::new()),
                );
                block.project(input, projections, &[], false)?;
                Ok(block)
            }
            LogicalPlan::Aggregate {
                input,
                keys,
                aggregates,
            } => {
                if aggregates.is_empty() {
                    return Err(CompileError::EmptyAggregate);
                }
                let mut block = self.projection_input(input)?;
                block.project(input, keys, aggregates, false)?;
                block.grouped = true;
                block.group_keys = keys.len();
                Ok(block)
            }
            LogicalPlan::Sort { input, keys } => {
                let mut block = self.block(input)?;
                block.check_intermediate_order()?;
                if block.projection.is_some() || block.limit.is_some() {
                    block = self.subquery(block.finish());
                }
                block.set_order(input, keys, false)?;
                Ok(block)
            }
            LogicalPlan::Limit {
                input,
                count,
                offset,
            } => {
                let mut block = self.block(input)?;
                if block.limit.is_some() {
                    let order = block.order.clone();
                    block = self.subquery(block.finish());
                    if let Some(order) = order {
                        block.set_order(&order.input, &order.keys, false)?;
                    }
                }
                block.limit = Some((*count, *offset));
                Ok(block)
            }
            LogicalPlan::Tail { input, count } => self.tail(input, *count),
            LogicalPlan::Join {
                left,
                right,
                condition,
                how,
                left_suffix,
                right_suffix,
            } => self.join(left, right, condition, *how, [left_suffix, right_suffix]),
            LogicalPlan::WithColumn {
                input,
                name,
                expr,
                keep_column_order,
            } => self.with_column(input, name, expr, *keep_column_order),
            LogicalPlan::Rename { input, names } => self.rename(input, names),
            LogicalPlan::Distinct { input } => {
                let mut block = self.block(input)?;
                block.check_intermediate_order()?;
                if block.limit.is_some() {
                    block = self.subquery(block.finish());
                }
                block.distinct = true;
                Ok(block)
            }
            LogicalPlan::Union {
                left,
                right,
                all,
                alignment,
            } => self.union(left, right, *all, *alignment),
        }
    }

    fn filter(
        &mut self,
        input: &LogicalPlan,
        predicate: &Expr,
        clause: PredicateClause,
    ) -> Result<Block, CompileError> {
        let mut block = self.block(input)?;
        block.check_intermediate_order()?;
        match clause {
            PredicateClause::Where => {
                if block.projection.is_some()
                    || block.predicate.is_some()
                    || block.qualify.is_some()
                    || block.limit.is_some()
                    || block.distinct
                {
                    block = self.subquery(block.finish());
                }
                let mut sql = CompiledQuery::default();
                block.scope(input).emit_expr(predicate, &mut sql, false)?;
                block.predicate = Some(sql);
            }
            PredicateClause::Having => {
                if !block.grouped
                    || block.qualify.is_some()
                    || block.distinct
                    || block.limit.is_some()
                {
                    return Err(CompileError::HavingWithoutAggregate);
                }
                let sql = block.emit_clause(input, predicate, None)?;
                block.having = Some(and_predicates(block.having.take(), sql));
            }
            PredicateClause::Qualify => {
                if block.distinct || block.limit.is_some() {
                    block = self.subquery(block.finish());
                }
                let has_window = Cell::new(
                    block.qualify.is_some()
                        || block
                            .projection
                            .as_ref()
                            .is_some_and(|projection| projection.has_window),
                );
                let sql = block.emit_clause(input, predicate, Some(&has_window))?;
                if !has_window.get() {
                    return Err(CompileError::QualifyWithoutWindow);
                }
                block.qualify = Some(and_predicates(block.qualify.take(), sql));
            }
        }
        Ok(block)
    }

    fn tail(&mut self, input: &LogicalPlan, count: u64) -> Result<Block, CompileError> {
        let mut block = self.block(input)?;
        let order = block.order.clone().ok_or(CompileError::TailWithoutOrder)?;
        if order.keys.is_empty() {
            return Err(CompileError::TailWithoutOrder);
        }
        if block.limit.is_some() {
            block = self.subquery(block.finish());
        }
        block.set_order(&order.input, &order.keys, true)?;
        block.limit = Some((count, 0));

        let mut block = self.subquery(block.finish());
        block.set_order(&order.input, &order.keys, false)?;
        block.limit = Some((count, 0));
        Ok(block)
    }

    fn projection_input(&mut self, input: &LogicalPlan) -> Result<Block, CompileError> {
        let mut block = self.block(input)?;
        block.check_intermediate_order()?;
        if block.projection.is_some()
            || block.qualify.is_some()
            || block.limit.is_some()
            || block.distinct
        {
            block = self.subquery(block.finish());
        }
        Ok(block)
    }

    fn with_column(
        &mut self,
        input: &Arc<LogicalPlan>,
        name: &str,
        expr: &Expr,
        keep_column_order: bool,
    ) -> Result<Block, CompileError> {
        let mut block = self.projection_input(input)?;
        let name = Ident::parse(name)?;
        let names = output_names(block.relation(input).named_columns("with_column")?);
        let original = names.iter().position(|old_name| **old_name == name);
        let mut projections: Vec<Projection> = names
            .into_iter()
            .map(|name| col(name.to_string()).into())
            .collect();
        let column = Projection {
            expr: expr.clone(),
            alias: Some(name.to_string()),
        };
        match original {
            Some(index) if keep_column_order => projections[index] = column,
            Some(index) => {
                drop(projections.remove(index));
                projections.push(column);
            }
            None => projections.push(column),
        }
        block.project(input, &projections, &[], true)?;
        Ok(block)
    }

    fn rename(
        &mut self,
        input: &LogicalPlan,
        names: &[(Expr, String)],
    ) -> Result<Block, CompileError> {
        let mut block = self.projection_input(input)?;
        let scope = block.scope(input);
        let mut renames = Vec::with_capacity(names.len());
        for (expr, name) in names {
            let Expr::Reference {
                name: old,
                relation,
            } = expr
            else {
                return Err(CompileError::ExpectedReference);
            };
            let (_, old) = scope.reference(old, relation.as_deref())?;
            if renames.iter().any(|(source, _)| *source == old) {
                return Err(CompileError::AmbiguousColumn(old.to_string()));
            }
            renames.push((old, Ident::parse(name)?));
        }
        let mut columns = block.columns.take();
        if let Some(columns) = &mut columns {
            let mut outputs = HashSet::with_capacity(columns.len());
            for name in columns.iter_mut().filter_map(|output| output.name.as_mut()) {
                if let Some((_, renamed)) = renames.iter().find(|(old, _)| old == name) {
                    *name = renamed.clone();
                }
                if !outputs.insert(&*name) {
                    return Err(CompileError::DuplicateOutput(name.to_string()));
                }
            }
        }
        let renamed = renames
            .iter()
            .map(|(old, name)| format!("{old} AS {name}"))
            .collect::<Vec<_>>();
        block.projection = Some(Projected {
            sql: CompiledQuery {
                sql: format!("t{}.* RENAME ({})", block.alias, renamed.join(", ")),
                binds: Vec::new(),
            },
            columns,
            ..Projected::default()
        });
        Ok(block)
    }

    fn union(
        &mut self,
        left: &LogicalPlan,
        right: &LogicalPlan,
        all: bool,
        alignment: UnionAlignment,
    ) -> Result<Block, CompileError> {
        let left_block = self.block(left)?;
        left_block.check_intermediate_order()?;
        let right_block = self.block(right)?;
        right_block.check_intermediate_order()?;
        let (left_query, left_columns) = left_block.finish();
        let (right_query, right_columns) = right_block.finish();
        let columns = match alignment {
            UnionAlignment::Position => union_by_position(left_columns, right_columns)?,
            UnionAlignment::Name {
                allow_missing_columns,
            } => union_by_name(left_columns, right_columns, allow_missing_columns)?,
        };
        let mut query = parenthesized(left_query);
        query.sql.push_str("\nUNION");
        if all {
            query.sql.push_str(" ALL");
        }
        if matches!(alignment, UnionAlignment::Name { .. }) {
            query.sql.push_str(" BY NAME");
        }
        query.sql.push('\n');
        query.append(parenthesized(right_query));
        Ok(self.subquery((query, columns)))
    }

    fn join(
        &mut self,
        left: &LogicalPlan,
        right: &LogicalPlan,
        condition: &Expr,
        how: JoinType,
        [left_suffix, right_suffix]: [&str; 2],
    ) -> Result<Block, CompileError> {
        if std::ptr::eq(left, right) {
            return Err(CompileError::IdenticalJoinInputs);
        }
        let left_block = self.block(left)?;
        left_block.check_intermediate_order()?;
        let left_input = self.subquery(left_block.finish());
        let right_block = self.block(right)?;
        right_block.check_intermediate_order()?;
        let right_input = self.subquery(right_block.finish());
        let left_relation = left_input.relation(left);
        let right_relation = right_input.relation(right);
        let left_columns = left_relation.join_columns()?;
        let right_columns = right_relation.join_columns()?;
        let left_names = output_names(left_columns);
        let right_names = output_names(right_columns);
        let scope = Scope {
            left: left_relation,
            right: Some(right_relation),
            outputs: None,
            has_window: None,
        };
        let mut on = CompiledQuery::default();
        scope.emit_expr(condition, &mut on, false)?;

        let mut outputs = left_names
            .iter()
            .chain(&right_names)
            .filter(|name| !(left_names.contains(name) && right_names.contains(name)))
            .map(|name| (*name).clone())
            .collect::<Vec<_>>();
        let use_prefixes = left_suffix.is_empty() && right_suffix.is_empty();
        if !use_prefixes && (left_suffix.is_empty() || right_suffix.is_empty()) {
            outputs.extend(
                left_names
                    .iter()
                    .filter(|name| right_names.contains(name))
                    .map(|name| (*name).clone()),
            );
        }
        let mut columns = Vec::new();
        let mut query = CompiledQuery::default();
        query.sql.push_str("SELECT ");
        for (relation, fields, other, prefix, suffix, pads_nulls) in [
            (
                &left_input,
                left_columns,
                &right_names,
                "L",
                left_suffix,
                matches!(how, JoinType::Right | JoinType::Full),
            ),
            (
                &right_input,
                right_columns,
                &left_names,
                "R",
                right_suffix,
                matches!(how, JoinType::Left | JoinType::Full),
            ),
        ] {
            for field in fields {
                let name = field.name.as_ref().unwrap();
                if !columns.is_empty() {
                    query.sql.push_str(", ");
                }
                query.sql.push_str(&format!("t{}.{name}", relation.alias));
                let output = if other.contains(&name) && (use_prefixes || !suffix.is_empty()) {
                    let alias = join_alias(name, prefix, suffix, &outputs)?;
                    query.sql.push_str(&format!(" AS {alias}"));
                    outputs.push(alias.clone());
                    alias
                } else {
                    name.clone()
                };
                columns.push(Output {
                    name: Some(output),
                    nullable: if pads_nulls {
                        Some(true)
                    } else {
                        field.nullable
                    },
                    data_type: field.data_type,
                });
            }
        }
        query.sql.push_str("\nFROM ");
        query.append(left_input.from);
        query
            .sql
            .push_str(&format!(" AS t{}\n{} ", left_input.alias, how.sql()));
        query.append(right_input.from);
        query
            .sql
            .push_str(&format!(" AS t{} ON ", right_input.alias));
        query.append(on);
        Ok(self.subquery((query, Some(columns))))
    }
}

fn join_alias(
    name: &Ident,
    prefix: &str,
    suffix: &str,
    reserved: &[Ident],
) -> Result<Ident, CompileError> {
    if !suffix.is_empty() {
        let alias = name.with_suffix(suffix)?;
        if reserved.contains(&alias) {
            return Err(CompileError::DuplicateJoinOutput(alias.to_string()));
        }
        return Ok(alias);
    }
    let mut alias = name.with_prefix(&format!("{prefix}_"));
    let mut index = 2;
    while reserved.contains(&alias) {
        alias = name.with_prefix(&format!("{prefix}{index}_"));
        index += 1;
    }
    Ok(alias)
}

fn parenthesized(mut query: CompiledQuery) -> CompiledQuery {
    query.sql.insert(0, '(');
    query.sql.push(')');
    query
}

type Columns = Vec<Output>;

#[derive(Clone)]
struct Output {
    name: Option<Ident>,
    data_type: Option<DataType>,
    nullable: Option<bool>,
}

impl From<&Field> for Output {
    fn from(field: &Field) -> Self {
        Self {
            name: Some(field.name.clone()),
            data_type: field.data_type,
            nullable: field.nullable,
        }
    }
}

impl Output {
    fn into_field(self) -> Option<Field> {
        Some(Field {
            name: self.name?,
            data_type: self.data_type,
            nullable: self.nullable,
        })
    }

    fn union_with(mut self, other: &Self) -> Self {
        if self.data_type != other.data_type {
            self.data_type = None;
        }
        self.nullable = nullable_either(self.nullable, other.nullable);
        self
    }
}

fn nullable_either(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

fn output_names(columns: &[Output]) -> Vec<&Ident> {
    columns
        .iter()
        .filter_map(|field| field.name.as_ref())
        .collect()
}

fn and_predicates(previous: Option<CompiledQuery>, predicate: CompiledQuery) -> CompiledQuery {
    let Some(previous) = previous else {
        return predicate;
    };
    let mut sql = parenthesized(previous);
    sql.sql.push_str(" AND ");
    sql.append(parenthesized(predicate));
    sql
}

fn validate_names(columns: &[Output], unknown: CompileError) -> Result<(), CompileError> {
    let mut names = HashSet::with_capacity(columns.len());
    for output in columns {
        let Some(name) = &output.name else {
            return Err(unknown);
        };
        if !names.insert(name) {
            return Err(CompileError::AmbiguousColumn(name.to_string()));
        }
    }
    Ok(())
}

fn union_by_position(
    left: Option<Columns>,
    right: Option<Columns>,
) -> Result<Option<Columns>, CompileError> {
    let Some(left) = left else { return Ok(None) };
    let Some(right) = right else {
        return Ok(Some(
            left.into_iter()
                .map(|mut output| {
                    output.data_type = None;
                    if output.nullable != Some(true) {
                        output.nullable = None;
                    }
                    output
                })
                .collect(),
        ));
    };
    if left.len() != right.len() {
        return Err(CompileError::UnionArity {
            left: left.len(),
            right: right.len(),
        });
    }
    Ok(Some(
        left.into_iter()
            .zip(&right)
            .map(|(left, right)| left.union_with(right))
            .collect(),
    ))
}

fn union_by_name(
    left: Option<Columns>,
    right: Option<Columns>,
    allow_missing: bool,
) -> Result<Option<Columns>, CompileError> {
    let unknown = || CompileError::UnknownSchema("union by name");
    for columns in [left.as_deref(), right.as_deref()].into_iter().flatten() {
        validate_names(columns, unknown())?;
    }
    let (Some(left), Some(right)) = (left, right) else {
        return if allow_missing {
            Ok(None)
        } else {
            Err(unknown())
        };
    };
    let mut columns = Vec::with_capacity(left.len().max(right.len()));
    for mut output in left {
        if let Some(other) = right.iter().find(|other| other.name == output.name) {
            columns.push(output.union_with(other));
        } else if allow_missing {
            output.nullable = Some(true);
            columns.push(output);
        } else {
            return Err(CompileError::UnionNames);
        }
    }
    let left_len = columns.len();
    for mut output in right {
        if columns[..left_len]
            .iter()
            .all(|left| left.name != output.name)
        {
            if !allow_missing {
                return Err(CompileError::UnionNames);
            }
            output.nullable = Some(true);
            columns.push(output);
        }
    }
    Ok(Some(columns))
}

#[derive(Clone, Copy)]
struct Relation<'a> {
    plan: &'a LogicalPlan,
    alias: usize,
    columns: Option<&'a Columns>,
}

impl<'a> Relation<'a> {
    fn join_columns(&self) -> Result<&'a [Output], CompileError> {
        self.checked_columns(|| CompileError::UnnamedJoinInput)
    }

    fn named_columns(&self, operation: &'static str) -> Result<&'a [Output], CompileError> {
        self.checked_columns(|| CompileError::UnknownSchema(operation))
    }

    fn checked_columns(
        &self,
        unknown: impl Fn() -> CompileError,
    ) -> Result<&'a [Output], CompileError> {
        let columns = self.columns.ok_or_else(&unknown)?;
        validate_names(columns, unknown())?;
        Ok(columns)
    }
}

#[derive(Clone, Copy)]
struct Scope<'a> {
    left: Relation<'a>,
    right: Option<Relation<'a>>,
    outputs: Option<(&'a LogicalPlan, &'a Projected)>,
    has_window: Option<&'a Cell<bool>>,
}

impl Scope<'_> {
    fn reference(
        &self,
        name: &str,
        plan: Option<&LogicalPlan>,
    ) -> Result<(usize, Ident), CompileError> {
        let ident = Ident::parse(name)?;
        let mut in_scope = false;
        let mut alias = None;
        for relation in std::iter::once(&self.left).chain(self.right.as_ref()) {
            if plan.is_some_and(|plan| !std::ptr::eq(plan, relation.plan)) {
                continue;
            }
            in_scope = true;
            let matches = relation.columns.map_or(1, |columns| {
                columns
                    .iter()
                    .filter(|column| column.name.as_ref() == Some(&ident))
                    .count()
            });
            if matches > 1 || (matches == 1 && alias.is_some()) {
                return Err(CompileError::AmbiguousColumn(name.to_owned()));
            }
            if matches == 1 {
                alias = Some(relation.alias);
            }
        }
        if !in_scope {
            return Err(CompileError::OutOfScopeColumn(name.to_owned()));
        }
        alias
            .map(|alias| (alias, ident))
            .ok_or_else(|| CompileError::UnknownColumn(name.to_owned()))
    }

    fn emit_expr(
        &self,
        expr: &Expr,
        query: &mut CompiledQuery,
        nested: bool,
    ) -> Result<(), CompileError> {
        self.emit_expression(expr, query, nested, false)
    }

    fn emit_expression(
        &self,
        expr: &Expr,
        query: &mut CompiledQuery,
        nested: bool,
        allow_aggregate: bool,
    ) -> Result<(), CompileError> {
        let parens = nested
            && matches!(
                expr,
                Expr::Binary(..)
                    | Expr::InList { .. }
                    | Expr::InQuery(..)
                    | Expr::Negate(_)
                    | Expr::Not(_)
                    | Expr::IsNull(_)
                    | Expr::IsNotNull(_)
            );
        if parens {
            query.sql.push('(');
        }
        match expr {
            Expr::Reference { name, relation } => {
                self.emit_reference(name, relation.as_deref(), query)?;
            }
            Expr::Literal(value) => {
                match value {
                    BindValue::Null(data_type) => {
                        data_type.validate()?;
                        let input = match data_type {
                            DataType::Array | DataType::Object => "TO_VARIANT(?)",
                            _ => "?",
                        };
                        query.sql.push_str(&format!("CAST({input} AS {data_type})"));
                    }
                    BindValue::Decimal(value) => {
                        query
                            .sql
                            .push_str(&format!("CAST(? AS {})", value.data_type()));
                    }
                    _ => query.sql.push('?'),
                }
                query.binds.push(value.clone());
            }
            Expr::DateFromEpochDays(days) => {
                query.sql.push_str("DATEADD(day, ?, DATE '1970-01-01')");
                query.binds.push(BindValue::Int(*days));
            }
            Expr::TimestampFromEpoch(value, unit, with_timezone) => {
                let function = if *with_timezone {
                    "TO_TIMESTAMP_TZ"
                } else {
                    "TO_TIMESTAMP_NTZ"
                };
                query
                    .sql
                    .push_str(&format!("{function}(?, {})", unit.scale()));
                query.binds.push(BindValue::Int(*value));
            }
            Expr::TemporalEpoch(inner, data_type) => {
                self.emit_temporal_epoch(inner, *data_type, query, allow_aggregate)?;
            }
            Expr::UtcDate(inner) => {
                query.sql.push_str("CAST(CONVERT_TIMEZONE('UTC', ");
                self.emit_expression(inner, query, false, allow_aggregate)?;
                query.sql.push_str(") AS DATE)");
            }
            Expr::InList {
                expr,
                values,
                nulls_equal,
                has_null,
            } => {
                self.emit_in_list(
                    expr,
                    values,
                    *nulls_equal,
                    *has_null,
                    query,
                    allow_aggregate,
                )?;
            }
            Expr::InQuery(inner, plan) => {
                let (subquery, columns) = Compiler::default().block(plan)?.finish();
                if let Some(columns) = columns
                    && columns.len() != 1
                {
                    return Err(CompileError::MembershipArity(columns.len()));
                }
                self.emit_expression(inner, query, true, allow_aggregate)?;
                query.sql.push_str(" IN (SELECT membership_source.* FROM (");
                query.append(subquery);
                query.sql.push_str(") AS membership_source)");
            }
            Expr::Binary(left, op, right) => {
                self.emit_expression(left, query, true, allow_aggregate)?;
                query.sql.push_str(op.sql());
                self.emit_expression(right, query, true, allow_aggregate)?;
            }
            Expr::Cast(inner, data_type) => {
                data_type.validate()?;
                query.sql.push_str("CAST(");
                self.emit_expression(inner, query, false, allow_aggregate)?;
                query.sql.push_str(&format!(" AS {data_type})"));
            }
            Expr::Negate(inner) => {
                query.sql.push_str("-(");
                self.emit_expression(inner, query, false, allow_aggregate)?;
                query.sql.push(')');
            }
            Expr::Power(base, exponent) | Expr::Coalesce(base, exponent) => {
                query.sql.push_str(if matches!(expr, Expr::Power(_, _)) {
                    "POWER("
                } else {
                    "COALESCE("
                });
                self.emit_expression(base, query, false, allow_aggregate)?;
                query.sql.push_str(", ");
                self.emit_expression(exponent, query, false, allow_aggregate)?;
                query.sql.push(')');
            }
            Expr::TryParseJson(inner) => {
                query.sql.push_str("TRY_PARSE_JSON(");
                self.emit_expression(inner, query, false, allow_aggregate)?;
                query.sql.push(')');
            }
            Expr::Not(inner) => {
                query.sql.push_str("NOT (");
                self.emit_expression(inner, query, false, allow_aggregate)?;
                query.sql.push(')');
            }
            Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
                self.emit_expression(inner, query, true, allow_aggregate)?;
                query.sql.push_str(match expr {
                    Expr::IsNull(_) => " IS NULL",
                    _ => " IS NOT NULL",
                });
            }
            Expr::Aggregate(aggregate) if allow_aggregate => {
                self.emit_aggregate(aggregate, query)?
            }
            Expr::Aggregate(_) => return Err(CompileError::UnexpectedAggregate),
            Expr::Over {
                function,
                partition_by,
                order_by,
            } => self.emit_window(function, partition_by, order_by, query, allow_aggregate)?,
            Expr::RowNumber => return Err(CompileError::InvalidWindow),
        }
        if parens {
            query.sql.push(')');
        }
        Ok(())
    }

    fn emit_reference(
        &self,
        name: &str,
        relation: Option<&LogicalPlan>,
        query: &mut CompiledQuery,
    ) -> Result<(), CompileError> {
        let mut source = relation;
        if let Some((plan, projection)) = self.outputs
            && relation.is_none_or(|relation| std::ptr::eq(relation, plan))
        {
            let ident = Ident::parse(name)?;
            let columns = projection.columns.as_deref().unwrap_or_default();
            let matches = columns
                .iter()
                .filter(|column| column.name.as_ref() == Some(&ident))
                .count();
            if matches > 1 {
                return Err(CompileError::AmbiguousColumn(name.to_owned()));
            }
            if let Some(alias) = projection.aliases.iter().find(|alias| alias.name == ident) {
                let may_collide = self.left.columns.is_none_or(|columns| {
                    columns
                        .iter()
                        .any(|column| column.name.as_ref() == Some(&ident))
                });
                if may_collide && alias.group_key {
                    // A group key is constant within its group. ANY_VALUE also lets us
                    // reference keys with binds, which acquire new positions in this clause.
                    query.sql.push_str("ANY_VALUE(");
                    query.append(alias.expression.clone());
                    query.sql.push(')');
                } else if may_collide {
                    query.append(parenthesized(alias.expression.clone()));
                } else {
                    query.sql.push_str(&ident.to_string());
                }
                return Ok(());
            }
            if relation.is_some() {
                if matches == 0 && projection.columns.is_some() {
                    return Err(CompileError::UnknownColumn(name.to_owned()));
                }
                source = None;
            }
        }
        let (alias, ident) = self.reference(name, source)?;
        query.sql.push_str(&format!("t{alias}.{ident}"));
        Ok(())
    }

    fn emit_window(
        &self,
        function: &Expr,
        partition_by: &[Expr],
        order_by: &[SortKey],
        query: &mut CompiledQuery,
        allow_aggregate: bool,
    ) -> Result<(), CompileError> {
        let has_window = self.has_window.ok_or(CompileError::UnexpectedWindow)?;
        has_window.set(true);
        let scope = Self {
            has_window: None,
            outputs: None,
            ..*self
        };
        let sum_or_zero = matches!(function, Expr::Aggregate(Aggregate::SumOrZero(_)));
        match function {
            Expr::Aggregate(aggregate) => scope.emit_aggregate_call(aggregate, query)?,
            Expr::RowNumber if !order_by.is_empty() => query.sql.push_str("ROW_NUMBER()"),
            _ => return Err(CompileError::InvalidWindow),
        }
        query.sql.push_str(" OVER (");
        if !partition_by.is_empty() {
            query.sql.push_str("PARTITION BY ");
            for (index, expr) in partition_by.iter().enumerate() {
                if index > 0 {
                    query.sql.push_str(", ");
                }
                scope.emit_expression(expr, query, false, allow_aggregate)?;
            }
        }
        if !order_by.is_empty() {
            if !partition_by.is_empty() {
                query.sql.push(' ');
            }
            query.sql.push_str("ORDER BY ");
            scope.emit_sort_keys(order_by, query, false, allow_aggregate)?;
        }
        query.sql.push(')');
        if sum_or_zero {
            query.sql.push_str(", 0)");
        }
        Ok(())
    }

    fn emit_temporal_epoch(
        &self,
        expr: &Expr,
        data_type: Option<DataType>,
        query: &mut CompiledQuery,
        allow_aggregate: bool,
    ) -> Result<(), CompileError> {
        if let Some(data_type) = data_type {
            query.sql.push_str("DATE_PART(EPOCH_NANOSECOND, ");
            if data_type == DataType::Date {
                query.sql.push_str("CAST(");
            }
            self.emit_expression(expr, query, false, allow_aggregate)?;
            if data_type == DataType::Date {
                query.sql.push_str(" AS TIMESTAMP_NTZ)");
            }
            query.sql.push(')');
        } else {
            // AS_* keeps the timestamp's original kind when the scan schema is unknown.
            query.sql.push_str("COALESCE(");
            for function in ["AS_TIMESTAMP_NTZ", "AS_TIMESTAMP_LTZ", "AS_TIMESTAMP_TZ"] {
                query.sql.push_str(&format!(
                    "DATE_PART(EPOCH_NANOSECOND, {function}(TO_VARIANT("
                ));
                self.emit_expression(expr, query, false, allow_aggregate)?;
                query.sql.push_str("))), ");
            }
            query
                .sql
                .push_str("DATE_PART(EPOCH_NANOSECOND, CAST(AS_DATE(TO_VARIANT(");
            self.emit_expression(expr, query, false, allow_aggregate)?;
            query.sql.push_str(")) AS TIMESTAMP_NTZ)))");
        }
        Ok(())
    }

    fn emit_in_list(
        &self,
        expr: &Expr,
        values: &[Expr],
        nulls_equal: bool,
        has_null: bool,
        query: &mut CompiledQuery,
        allow_aggregate: bool,
    ) -> Result<(), CompileError> {
        if values.is_empty() {
            if nulls_equal && has_null {
                self.emit_expression(expr, query, true, allow_aggregate)?;
                query.sql.push_str(" IS NULL");
            } else {
                query.sql.push_str("CASE WHEN ");
                self.emit_expression(expr, query, true, allow_aggregate)?;
                query.sql.push_str(if nulls_equal {
                    " IS NULL THEN FALSE ELSE FALSE END"
                } else {
                    " IS NULL THEN NULL ELSE FALSE END"
                });
            }
            return Ok(());
        }
        if nulls_equal {
            query.sql.push_str("COALESCE(");
        }
        self.emit_expression(expr, query, true, allow_aggregate)?;
        query.sql.push_str(" IN (");
        for (index, value) in values.iter().enumerate() {
            if index > 0 {
                query.sql.push_str(", ");
            }
            self.emit_expression(value, query, false, allow_aggregate)?;
        }
        query.sql.push(')');
        if nulls_equal {
            query.sql.push_str(", ");
            if has_null {
                self.emit_expression(expr, query, true, allow_aggregate)?;
                query.sql.push_str(" IS NULL)");
            } else {
                query.sql.push_str("FALSE)");
            }
        }
        Ok(())
    }

    fn expression_type(
        &self,
        expr: &Expr,
    ) -> Result<(Option<DataType>, Option<bool>), CompileError> {
        Ok(match expr {
            Expr::Reference { name, relation } => {
                let (alias, ident) = self.reference(name, relation.as_deref())?;
                std::iter::once(&self.left)
                    .chain(self.right.as_ref())
                    .find(|relation| relation.alias == alias)
                    .and_then(|relation| relation.columns)
                    .and_then(|columns| {
                        columns
                            .iter()
                            .find(|output| output.name.as_ref() == Some(&ident))
                    })
                    .map_or((None, None), |output| (output.data_type, output.nullable))
            }
            Expr::Literal(value) => match value {
                BindValue::Null(data_type) => (Some(*data_type), Some(true)),
                BindValue::Decimal(value) => (Some(value.data_type()), Some(false)),
                BindValue::Bool(_) => (Some(DataType::Boolean), Some(false)),
                BindValue::Text(_) => (Some(DataType::Varchar), Some(false)),
                BindValue::Int(_) => (None, Some(false)),
                BindValue::Float(_) => (Some(DataType::Float), Some(false)),
            },
            Expr::Cast(inner, data_type) => (Some(*data_type), self.expression_type(inner)?.1),
            Expr::DateFromEpochDays(_) => (Some(DataType::Date), Some(false)),
            Expr::TimestampFromEpoch(_, _, with_timezone) => (
                Some(if *with_timezone {
                    DataType::TimestampTz
                } else {
                    DataType::TimestampNtz
                }),
                Some(false),
            ),
            Expr::TemporalEpoch(inner, _) => (
                Some(DataType::Number {
                    precision: 38,
                    scale: 0,
                }),
                self.expression_type(inner)?.1,
            ),
            Expr::UtcDate(inner) => (Some(DataType::Date), self.expression_type(inner)?.1),
            Expr::InList {
                expr, nulls_equal, ..
            } => (
                Some(DataType::Boolean),
                if *nulls_equal {
                    Some(false)
                } else {
                    self.expression_type(expr)?.1
                },
            ),
            Expr::InQuery(..) => (Some(DataType::Boolean), None),
            Expr::TryParseJson(_) => (Some(DataType::Variant), Some(true)),
            Expr::IsNull(_) | Expr::IsNotNull(_) => (Some(DataType::Boolean), Some(false)),
            Expr::Not(inner) => (Some(DataType::Boolean), self.expression_type(inner)?.1),
            Expr::Negate(inner) => (None, self.expression_type(inner)?.1),
            Expr::Binary(left, op, right) => {
                let data_type = match op {
                    BinaryOp::Eq
                    | BinaryOp::Ne
                    | BinaryOp::EqMissing
                    | BinaryOp::NeMissing
                    | BinaryOp::Lt
                    | BinaryOp::Le
                    | BinaryOp::Gt
                    | BinaryOp::Ge
                    | BinaryOp::And
                    | BinaryOp::Or => Some(DataType::Boolean),
                    _ => None,
                };
                (
                    data_type,
                    if matches!(op, BinaryOp::EqMissing | BinaryOp::NeMissing) {
                        Some(false)
                    } else {
                        nullable_either(
                            self.expression_type(left)?.1,
                            self.expression_type(right)?.1,
                        )
                    },
                )
            }
            Expr::Power(left, right) => (
                None,
                nullable_either(
                    self.expression_type(left)?.1,
                    self.expression_type(right)?.1,
                ),
            ),
            Expr::Coalesce(left, right) => {
                let (left_type, left_nullable) = self.expression_type(left)?;
                let (right_type, right_nullable) = self.expression_type(right)?;
                (
                    if left_type == right_type {
                        left_type
                    } else {
                        None
                    },
                    match (left_nullable, right_nullable) {
                        (Some(false), _) | (_, Some(false)) => Some(false),
                        (Some(true), Some(true)) => Some(true),
                        _ => None,
                    },
                )
            }
            Expr::Aggregate(Aggregate::CountAll | Aggregate::Count(_)) => (None, Some(false)),
            Expr::Aggregate(Aggregate::Sum(_)) => (None, Some(true)),
            Expr::Aggregate(Aggregate::SumOrZero(_)) | Expr::RowNumber => (None, Some(false)),
            Expr::Over { function, .. } => self.expression_type(function)?,
        })
    }

    fn project(
        &self,
        keys: &[Projection],
        aggregates: &[Projection],
        allow_window: bool,
    ) -> Result<Projected, CompileError> {
        let mut result = Projected::default();
        let has_window = Cell::new(false);
        let scope = Scope {
            has_window: allow_window.then_some(&has_window),
            ..*self
        };
        let mut columns = Vec::new();
        let projections = keys
            .iter()
            .map(|projection| (projection, false))
            .chain(aggregates.iter().map(|projection| (projection, true)));
        if aggregates
            .iter()
            .any(|projection| projection.expr.contains_ungrouped_reference())
            || (keys.is_empty()
                && !aggregates.is_empty()
                && !aggregates
                    .iter()
                    .any(|projection| projection.expr.contains_aggregate()))
        {
            return Err(CompileError::ExpectedAggregate);
        }
        for (index, (projection, aggregate)) in projections.enumerate() {
            if index > 0 {
                result.sql.sql.push_str(", ");
            }
            let mut expression = CompiledQuery::default();
            scope.emit_expression(&projection.expr, &mut expression, false, aggregate)?;
            let name = if let Some(alias) = &projection.alias {
                let alias = Ident::parse(alias)?;
                result.sql.append(expression.clone());
                result.sql.sql.push_str(&format!(" AS {alias}"));
                result.aliases.push(SelectAlias {
                    name: alias.clone(),
                    expression,
                    group_key: !aggregate && !aggregates.is_empty(),
                });
                Some(alias)
            } else {
                result.sql.append(expression);
                if let Expr::Reference { name, .. } = &projection.expr {
                    Some(Ident::parse(name)?)
                } else {
                    None
                }
            };
            let (data_type, nullable) = self.expression_type(&projection.expr)?;
            columns.push(Output {
                name,
                data_type,
                nullable,
            });
        }
        result.columns = Some(columns);
        result.has_window = has_window.get();
        Ok(result)
    }

    fn emit_aggregate(
        &self,
        aggregate: &Aggregate,
        query: &mut CompiledQuery,
    ) -> Result<(), CompileError> {
        self.emit_aggregate_call(aggregate, query)?;
        if matches!(aggregate, Aggregate::SumOrZero(_)) {
            query.sql.push_str(", 0)");
        }
        Ok(())
    }

    fn emit_aggregate_call(
        &self,
        aggregate: &Aggregate,
        query: &mut CompiledQuery,
    ) -> Result<(), CompileError> {
        let (function, arg) = match aggregate {
            Aggregate::CountAll => {
                query.sql.push_str("COUNT(*)");
                return Ok(());
            }
            Aggregate::Count(arg) => ("COUNT", arg),
            Aggregate::Sum(arg) => ("SUM", arg),
            Aggregate::SumOrZero(arg) => ("COALESCE(SUM", arg),
        };
        query.sql.push_str(function);
        query.sql.push('(');
        // Aggregate inputs refer to source rows even when a projection alias has the same name.
        Self {
            outputs: None,
            has_window: None,
            ..*self
        }
        .emit_expr(arg, query, false)?;
        query.sql.push(')');
        Ok(())
    }

    fn emit_sort_keys(
        &self,
        keys: &[SortKey],
        query: &mut CompiledQuery,
        reverse: bool,
        allow_aggregate: bool,
    ) -> Result<(), CompileError> {
        if keys.is_empty() {
            query.sql.push_str("ALL");
            return Ok(());
        }
        for (index, key) in keys.iter().enumerate() {
            if index > 0 {
                query.sql.push_str(", ");
            }
            self.emit_expression(&key.expr, query, false, allow_aggregate)?;
            query.sql.push_str(if key.ascending != reverse {
                " ASC"
            } else {
                " DESC"
            });
            query.sql.push_str(if key.nulls_first != reverse {
                " NULLS FIRST"
            } else {
                " NULLS LAST"
            });
        }
        Ok(())
    }
}

#[derive(Default)]
struct Projected {
    sql: CompiledQuery,
    columns: Option<Columns>,
    input: Option<Arc<LogicalPlan>>,
    aliases: Vec<SelectAlias>,
    has_window: bool,
}

struct SelectAlias {
    name: Ident,
    expression: CompiledQuery,
    group_key: bool,
}

#[derive(Clone)]
struct Order {
    input: Arc<LogicalPlan>,
    keys: Vec<SortKey>,
    sql: CompiledQuery,
}

struct Block {
    from: CompiledQuery,
    alias: usize,
    columns: Option<Columns>,
    projection: Option<Projected>,
    distinct: bool,
    predicate: Option<CompiledQuery>,
    having: Option<CompiledQuery>,
    qualify: Option<CompiledQuery>,
    grouped: bool,
    group_keys: usize,
    order: Option<Order>,
    limit: Option<(u64, u64)>,
}

impl Block {
    fn relation<'a>(&'a self, plan: &'a LogicalPlan) -> Relation<'a> {
        Relation {
            plan,
            alias: self.alias,
            columns: self.columns.as_ref(),
        }
    }

    fn scope<'a>(&'a self, plan: &'a LogicalPlan) -> Scope<'a> {
        Scope {
            left: self.relation(plan),
            right: None,
            outputs: None,
            has_window: None,
        }
    }

    fn project(
        &mut self,
        input: &Arc<LogicalPlan>,
        keys: &[Projection],
        aggregates: &[Projection],
        allow_window: bool,
    ) -> Result<(), CompileError> {
        let mut projected = self.scope(input).project(keys, aggregates, allow_window)?;
        projected.input = Some(Arc::clone(input));
        self.projection = Some(projected);
        Ok(())
    }

    fn emit_clause(
        &self,
        input: &LogicalPlan,
        predicate: &Expr,
        has_window: Option<&Cell<bool>>,
    ) -> Result<CompiledQuery, CompileError> {
        let projection = self.projection.as_ref();
        let mut scope = self.scope(projection.and_then(|p| p.input.as_deref()).unwrap_or(input));
        scope.outputs = projection.map(|projection| (input, projection));
        scope.has_window = has_window;
        let mut sql = CompiledQuery::default();
        scope.emit_expression(predicate, &mut sql, false, self.grouped)?;
        Ok(sql)
    }

    fn set_order(
        &mut self,
        input: &Arc<LogicalPlan>,
        keys: &[SortKey],
        reverse: bool,
    ) -> Result<(), CompileError> {
        let mut sql = CompiledQuery::default();
        self.scope(input)
            .emit_sort_keys(keys, &mut sql, reverse, false)?;
        self.order = Some(Order {
            input: Arc::clone(input),
            keys: keys.to_vec(),
            sql,
        });
        Ok(())
    }

    fn check_intermediate_order(&self) -> Result<(), CompileError> {
        if self.order.is_some() && self.limit.is_none() {
            return Err(CompileError::IntermediateSort);
        }
        Ok(())
    }

    fn finish(self) -> (CompiledQuery, Option<Columns>) {
        let mut query = CompiledQuery {
            sql: String::from("SELECT "),
            binds: Vec::new(),
        };
        if self.distinct {
            query.sql.push_str("DISTINCT ");
        }
        let columns = if let Some(projection) = self.projection {
            query.append(projection.sql);
            projection.columns
        } else {
            query.sql.push_str(&format!("t{}.*", self.alias));
            self.columns
        };
        // Append whole fragments in SQL order, even though the input was compiled first.
        query.sql.push_str("\nFROM ");
        query.append(self.from);
        query.sql.push_str(&format!(" AS t{}", self.alias));
        if let Some(predicate) = self.predicate {
            query.sql.push_str("\nWHERE ");
            query.append(predicate);
        }
        if self.group_keys > 0 {
            // Positions refer to the projected keys, including keys with aliases or binds.
            query.sql.push_str("\nGROUP BY 1");
            for position in 2..=self.group_keys {
                query.sql.push_str(&format!(", {position}"));
            }
        }
        if let Some(predicate) = self.having {
            query.sql.push_str("\nHAVING ");
            query.append(predicate);
        }
        if let Some(predicate) = self.qualify {
            query.sql.push_str("\nQUALIFY ");
            query.append(predicate);
        }
        if let Some(order) = self.order {
            query.sql.push_str("\nORDER BY ");
            query.append(order.sql);
        }
        if let Some((count, offset)) = self.limit {
            query.sql.push_str(&format!("\nLIMIT {count}"));
            if offset != 0 {
                query.sql.push_str(&format!(" OFFSET {offset}"));
            }
        }
        (query, columns)
    }
}
