use crate::{CompiledQuery, Projection, expr::Expr, ident::Ident, plan::LogicalPlan};

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
}

pub(crate) fn compile(plan: &LogicalPlan) -> Result<CompiledQuery, CompileError> {
    Ok(compile_block(plan)?.finish().0)
}

fn compile_block(plan: &LogicalPlan) -> Result<Block, CompileError> {
    match plan {
        LogicalPlan::Scan(table) => Ok(Block {
            from: CompiledQuery {
                sql: table.to_string(),
                binds: Vec::new(),
            },
            scope: Scope {
                alias: 0,
                names: None,
            },
            projection: None,
            predicate: None,
        }),
        LogicalPlan::Filter { input, predicate } => {
            let mut block = compile_block(input)?;
            if block.projection.is_some() || block.predicate.is_some() {
                block = block.into_subquery();
            }
            let mut sql = CompiledQuery::default();
            block.scope.emit_expr(predicate, &mut sql, false)?;
            block.predicate = Some(sql);
            Ok(block)
        }
        LogicalPlan::Project { input, projections } => {
            let mut block = compile_block(input)?;
            if block.projection.is_some() {
                block = block.into_subquery();
            }
            block.projection = Some(block.scope.project(projections)?);
            Ok(block)
        }
    }
}

struct Scope {
    alias: usize,
    // A scan has no local schema. A subquery of a projection exports only its output names.
    names: Option<Vec<Ident>>,
}

impl Scope {
    fn reference(&self, name: &str) -> Result<Ident, CompileError> {
        let ident = Ident::parse(name)?;
        if let Some(names) = &self.names {
            let mut matches = names.iter().filter(|candidate| **candidate == ident);
            if matches.next().is_none() {
                return Err(CompileError::UnknownColumn(name.to_owned()));
            }
            if matches.next().is_some() {
                return Err(CompileError::AmbiguousColumn(name.to_owned()));
            }
        }
        Ok(ident)
    }

    fn emit_expr(
        &self,
        expr: &Expr,
        query: &mut CompiledQuery,
        nested: bool,
    ) -> Result<(), CompileError> {
        let parens = nested && !matches!(expr, Expr::Reference(_) | Expr::Literal(_));
        if parens {
            query.sql.push('(');
        }
        match expr {
            Expr::Reference(name) => {
                let ident = self.reference(name)?;
                query.sql.push_str(&format!("t{}.{ident}", self.alias));
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
        }
        if parens {
            query.sql.push(')');
        }
        Ok(())
    }

    fn project(&self, projections: &[Projection]) -> Result<Projected, CompileError> {
        if projections.is_empty() {
            return Err(CompileError::EmptyProjection);
        }
        let mut sql = CompiledQuery::default();
        let mut names = Vec::new();
        for (index, projection) in projections.iter().enumerate() {
            if index > 0 {
                sql.sql.push_str(", ");
            }
            self.emit_expr(&projection.expr, &mut sql, false)?;
            if let Some(alias) = &projection.alias {
                let alias = Ident::parse(alias)?;
                sql.sql.push_str(&format!(" AS {alias}"));
                names.push(alias);
            } else if let Expr::Reference(name) = &projection.expr {
                names.push(Ident::parse(name)?);
            }
        }
        Ok(Projected { sql, names })
    }
}

struct Projected {
    sql: CompiledQuery,
    names: Vec<Ident>,
}

struct Block {
    from: CompiledQuery,
    scope: Scope,
    projection: Option<Projected>,
    predicate: Option<CompiledQuery>,
}

impl Block {
    fn into_subquery(self) -> Self {
        let alias = self.scope.alias + 1;
        let (mut from, names) = self.finish();
        from.sql.insert(0, '(');
        from.sql.push(')');
        Self {
            from,
            scope: Scope { alias, names },
            projection: None,
            predicate: None,
        }
    }

    fn finish(self) -> (CompiledQuery, Option<Vec<Ident>>) {
        let mut query = CompiledQuery {
            sql: String::from("SELECT "),
            binds: Vec::new(),
        };
        let names = if let Some(projection) = self.projection {
            query.append(projection.sql);
            Some(projection.names)
        } else {
            query.sql.push_str(&format!("t{}.*", self.scope.alias));
            self.scope.names
        };
        // Append whole fragments in SQL order, even though the input was compiled first.
        query.sql.push_str("\nFROM ");
        query.append(self.from);
        query.sql.push_str(&format!(" AS t{}", self.scope.alias));
        if let Some(predicate) = self.predicate {
            query.sql.push_str("\nWHERE ");
            query.append(predicate);
        }
        (query, names)
    }
}
