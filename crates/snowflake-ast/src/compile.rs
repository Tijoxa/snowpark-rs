use crate::{
    CompiledQuery, Projection, SortKey,
    expr::{Aggregate, Expr},
    ident::Ident,
    plan::LogicalPlan,
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
    #[error("both sides of an inner join require explicit projections with named outputs")]
    UnnamedJoinInput,
    #[error("an inner join requires distinct inputs; select each side separately for a self-join")]
    IdenticalJoinInputs,
    #[error("the join produces duplicate column name {0}")]
    DuplicateJoinOutput(String),
}

pub(crate) fn compile(plan: &LogicalPlan) -> Result<CompiledQuery, CompileError> {
    Ok(Compiler::default().block(plan)?.finish().0)
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
            LogicalPlan::Scan(table) => Ok(self.source(
                CompiledQuery {
                    sql: table.to_string(),
                    binds: Vec::new(),
                },
                None,
            )),
            LogicalPlan::Filter { input, predicate } => {
                let mut block = self.block(input)?;
                block.check_intermediate_order()?;
                if block.projection.is_some() || block.predicate.is_some() || block.limit.is_some()
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
                let mut block = self.block(input)?;
                block.check_intermediate_order()?;
                if block.projection.is_some() || block.limit.is_some() {
                    block = self.subquery(block.finish());
                }
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
                let mut block = self.block(input)?;
                block.check_intermediate_order()?;
                if block.projection.is_some() || block.limit.is_some() {
                    block = self.subquery(block.finish());
                }
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
                left_suffix,
                right_suffix,
            } => self.join(left, right, condition, left_suffix, right_suffix),
        }
    }

    fn join(
        &mut self,
        left: &LogicalPlan,
        right: &LogicalPlan,
        condition: &Expr,
        left_suffix: &str,
        right_suffix: &str,
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
        let left_names = left_relation.join_columns()?;
        let right_names = right_relation.join_columns()?;
        let scope = Scope {
            left: left_relation,
            right: Some(right_relation),
        };
        let mut on = CompiledQuery::default();
        scope.emit_expr(condition, &mut on, false)?;

        let mut outputs = left_names
            .iter()
            .chain(right_names)
            .filter(|name| !(left_names.contains(name) && right_names.contains(name)))
            .cloned()
            .collect::<Vec<_>>();
        let use_prefixes = left_suffix.is_empty() && right_suffix.is_empty();
        if !use_prefixes && (left_suffix.is_empty() || right_suffix.is_empty()) {
            outputs.extend(
                left_names
                    .iter()
                    .filter(|name| right_names.contains(name))
                    .cloned(),
            );
        }
        let mut columns = Columns::default();
        let mut query = CompiledQuery::default();
        query.sql.push_str("SELECT ");
        for (relation, names, other, prefix, suffix) in [
            (&left_input, left_names, right_names, "L", left_suffix),
            (&right_input, right_names, left_names, "R", right_suffix),
        ] {
            for name in names {
                if !columns.names.is_empty() {
                    query.sql.push_str(", ");
                }
                query.sql.push_str(&format!("t{}.{name}", relation.alias));
                let output = if other.contains(name) && (use_prefixes || !suffix.is_empty()) {
                    let alias = join_alias(name, prefix, suffix, &outputs)?;
                    query.sql.push_str(&format!(" AS {alias}"));
                    outputs.push(alias.clone());
                    alias
                } else {
                    name.clone()
                };
                columns.names.push(output);
            }
        }
        query.sql.push_str("\nFROM ");
        query.append(left_input.from);
        query
            .sql
            .push_str(&format!(" AS t{}\nINNER JOIN ", left_input.alias));
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

#[derive(Default)]
struct Columns {
    names: Vec<Ident>,
    has_unnamed: bool,
}

struct Relation<'a> {
    plan: &'a LogicalPlan,
    alias: usize,
    columns: Option<&'a Columns>,
}

impl<'a> Relation<'a> {
    fn join_columns(&self) -> Result<&'a [Ident], CompileError> {
        let columns = self
            .columns
            .filter(|columns| !columns.has_unnamed)
            .ok_or(CompileError::UnnamedJoinInput)?;
        for (index, name) in columns.names.iter().enumerate() {
            if columns.names[..index].contains(name) {
                return Err(CompileError::AmbiguousColumn(name.to_string()));
            }
        }
        Ok(&columns.names)
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
                    .names
                    .iter()
                    .filter(|column| **column == ident)
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
        let parens = nested && !matches!(expr, Expr::Reference { .. } | Expr::Literal(_));
        if parens {
            query.sql.push('(');
        }
        match expr {
            Expr::Reference { name, relation } => {
                let (alias, ident) = self.reference(name, relation.as_deref())?;
                query.sql.push_str(&format!("t{alias}.{ident}"));
            }
            Expr::Literal(value) => {
                query.sql.push('?');
                query.binds.push(value.clone());
            }
            Expr::Binary(left, op, right) => {
                self.emit_expr(left, query, true)?;
                query.sql.push_str(op.sql());
                self.emit_expr(right, query, true)?;
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

    fn project(
        &self,
        keys: &[Projection],
        aggregates: &[Projection],
    ) -> Result<Projected, CompileError> {
        let mut result = Projected::default();
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
            if let Some(alias) = &projection.alias {
                let alias = Ident::parse(alias)?;
                result.sql.sql.push_str(&format!(" AS {alias}"));
                result.columns.names.push(alias);
            } else if let Expr::Reference { name, .. } = &projection.expr {
                result.columns.names.push(Ident::parse(name)?);
            } else {
                result.columns.has_unnamed = true;
            }
        }
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
    columns: Columns,
}

struct Block {
    from: CompiledQuery,
    alias: usize,
    columns: Option<Columns>,
    projection: Option<Projected>,
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
        let columns = if let Some(projection) = self.projection {
            query.append(projection.sql);
            Some(projection.columns)
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
