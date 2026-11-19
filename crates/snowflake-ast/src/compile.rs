use std::collections::HashSet;

use crate::{
    BindValue, CompiledQuery, DataType, Field, JoinType, Projection, SortKey, col,
    expr::{Aggregate, BinaryOp, Expr},
    ident::Ident,
    plan::{LogicalPlan, UnionAlignment},
};

/// An invalid name, expression or column reference in a DataFrame plan.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CompileError {
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
}

pub(crate) fn compile(plan: &LogicalPlan) -> Result<CompiledQuery, CompileError> {
    Ok(Compiler::default().block(plan)?.finish().0)
}

pub(crate) fn schema(plan: &LogicalPlan) -> Result<Option<Vec<Field>>, CompileError> {
    let (_, columns) = Compiler::default().block(plan)?.finish();
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
            LogicalPlan::Scan { table, schema } => Ok(self.source(
                CompiledQuery {
                    sql: table.to_string(),
                    binds: Vec::new(),
                },
                schema
                    .as_ref()
                    .map(|schema| schema.iter().map(Output::from).collect()),
            )),
            LogicalPlan::Filter { input, predicate } => {
                let mut block = self.block(input)?;
                block.check_intermediate_order()?;
                if block.projection.is_some()
                    || block.predicate.is_some()
                    || block.limit.is_some()
                    || block.distinct
                {
                    block = self.subquery(block.finish());
                }
                let mut sql = CompiledQuery::default();
                block.scope(input).emit_expr(predicate, &mut sql, false)?;
                block.predicate = Some(sql);
                Ok(block)
            }
            LogicalPlan::Project { input, projections } => {
                if projections.is_empty() {
                    return Err(CompileError::EmptyProjection);
                }
                let mut block = self.projection_input(input)?;
                block.projection = Some(block.scope(input).project(projections, &[])?);
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
                block.projection = Some(block.scope(input).project(keys, aggregates)?);
                block.group_keys = keys.len();
                Ok(block)
            }
            LogicalPlan::Sort { input, keys } => {
                let mut block = self.block(input)?;
                block.check_intermediate_order()?;
                if block.projection.is_some() || block.limit.is_some() {
                    block = self.subquery(block.finish());
                }
                block.order = Some(block.scope(input).sort(keys)?);
                Ok(block)
            }
            LogicalPlan::Limit {
                input,
                count,
                offset,
            } => {
                let mut block = self.block(input)?;
                if block.limit.is_some() {
                    block = self.subquery(block.finish());
                }
                block.limit = Some((*count, *offset));
                Ok(block)
            }
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

    fn projection_input(&mut self, input: &LogicalPlan) -> Result<Block, CompileError> {
        let mut block = self.block(input)?;
        block.check_intermediate_order()?;
        if block.projection.is_some() || block.limit.is_some() || block.distinct {
            block = self.subquery(block.finish());
        }
        Ok(block)
    }

    fn with_column(
        &mut self,
        input: &LogicalPlan,
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
        block.projection = Some(block.scope(input).project(&projections, &[])?);
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

struct Scope<'a> {
    left: Relation<'a>,
    right: Option<Relation<'a>>,
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
        let parens = nested
            && matches!(
                expr,
                Expr::Binary(..)
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
                let (alias, ident) = self.reference(name, relation.as_deref())?;
                query.sql.push_str(&format!("t{alias}.{ident}"));
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
            Expr::Binary(left, op, right) => {
                self.emit_expr(left, query, true)?;
                query.sql.push_str(op.sql());
                self.emit_expr(right, query, true)?;
            }
            Expr::Cast(inner, data_type) => {
                data_type.validate()?;
                query.sql.push_str("CAST(");
                self.emit_expr(inner, query, false)?;
                query.sql.push_str(&format!(" AS {data_type})"));
            }
            Expr::Negate(inner) => {
                query.sql.push_str("-(");
                self.emit_expr(inner, query, false)?;
                query.sql.push(')');
            }
            Expr::Power(base, exponent) => {
                query.sql.push_str("POWER(");
                self.emit_expr(base, query, false)?;
                query.sql.push_str(", ");
                self.emit_expr(exponent, query, false)?;
                query.sql.push(')');
            }
            Expr::Not(inner) => {
                query.sql.push_str("NOT (");
                self.emit_expr(inner, query, false)?;
                query.sql.push(')');
            }
            Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
                self.emit_expr(inner, query, true)?;
                query.sql.push_str(match expr {
                    Expr::IsNull(_) => " IS NULL",
                    _ => " IS NOT NULL",
                });
            }
            Expr::Aggregate(_) => return Err(CompileError::UnexpectedAggregate),
        }
        if parens {
            query.sql.push(')');
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
            },
            Expr::Cast(inner, data_type) => (Some(*data_type), self.expression_type(inner)?.1),
            Expr::IsNull(_) | Expr::IsNotNull(_) => (Some(DataType::Boolean), Some(false)),
            Expr::Not(inner) => (Some(DataType::Boolean), self.expression_type(inner)?.1),
            Expr::Negate(inner) => (None, self.expression_type(inner)?.1),
            Expr::Binary(left, op, right) => {
                let data_type = match op {
                    BinaryOp::Eq
                    | BinaryOp::Ne
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
                    nullable_either(
                        self.expression_type(left)?.1,
                        self.expression_type(right)?.1,
                    ),
                )
            }
            Expr::Power(left, right) => (
                None,
                nullable_either(
                    self.expression_type(left)?.1,
                    self.expression_type(right)?.1,
                ),
            ),
            Expr::Aggregate(Aggregate::CountAll | Aggregate::Count(_)) => (None, Some(false)),
            Expr::Aggregate(Aggregate::Sum(_)) => (None, Some(true)),
        })
    }

    fn project(
        &self,
        keys: &[Projection],
        aggregates: &[Projection],
    ) -> Result<Projected, CompileError> {
        let mut result = Projected::default();
        let mut columns = Vec::new();
        let projections = keys
            .iter()
            .map(|projection| (projection, false))
            .chain(aggregates.iter().map(|projection| (projection, true)));
        for (index, (projection, aggregate)) in projections.enumerate() {
            if index > 0 {
                result.sql.sql.push_str(", ");
            }
            if aggregate {
                self.emit_aggregate(&projection.expr, &mut result.sql)?;
            } else {
                self.emit_expr(&projection.expr, &mut result.sql, false)?;
            }
            let name = if let Some(alias) = &projection.alias {
                let alias = Ident::parse(alias)?;
                result.sql.sql.push_str(&format!(" AS {alias}"));
                Some(alias)
            } else if let Expr::Reference { name, .. } = &projection.expr {
                Some(Ident::parse(name)?)
            } else {
                None
            };
            let (data_type, nullable) = self.expression_type(&projection.expr)?;
            columns.push(Output {
                name,
                data_type,
                nullable,
            });
        }
        result.columns = Some(columns);
        Ok(result)
    }

    fn emit_aggregate(&self, expr: &Expr, query: &mut CompiledQuery) -> Result<(), CompileError> {
        let (function, arg) = match expr {
            Expr::Aggregate(Aggregate::CountAll) => {
                query.sql.push_str("COUNT(*)");
                return Ok(());
            }
            Expr::Aggregate(Aggregate::Count(arg)) => ("COUNT", arg),
            Expr::Aggregate(Aggregate::Sum(arg)) => ("SUM", arg),
            _ => return Err(CompileError::ExpectedAggregate),
        };
        query.sql.push_str(function);
        query.sql.push('(');
        self.emit_expr(arg, query, false)?;
        query.sql.push(')');
        Ok(())
    }

    fn sort(&self, keys: &[SortKey]) -> Result<CompiledQuery, CompileError> {
        let mut query = CompiledQuery::default();
        if keys.is_empty() {
            query.sql.push_str("ALL");
            return Ok(query);
        }
        for (index, key) in keys.iter().enumerate() {
            if index > 0 {
                query.sql.push_str(", ");
            }
            self.emit_expr(&key.expr, &mut query, false)?;
            query
                .sql
                .push_str(if key.ascending { " ASC" } else { " DESC" });
            query.sql.push_str(if key.nulls_first {
                " NULLS FIRST"
            } else {
                " NULLS LAST"
            });
        }
        Ok(query)
    }
}

#[derive(Default)]
struct Projected {
    sql: CompiledQuery,
    columns: Option<Columns>,
}

struct Block {
    from: CompiledQuery,
    alias: usize,
    columns: Option<Columns>,
    projection: Option<Projected>,
    distinct: bool,
    predicate: Option<CompiledQuery>,
    group_keys: usize,
    order: Option<CompiledQuery>,
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
        }
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
        if let Some(order) = self.order {
            query.sql.push_str("\nORDER BY ");
            query.append(order);
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
