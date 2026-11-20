use std::sync::{Arc, OnceLock};

use crate::{CompileError, Field, Projection, SortKey, expr::Expr, ident::ObjectName};

type LowerPlan = dyn Fn() -> Result<Arc<LogicalPlan>, CompileError> + Send + Sync;

/// How a join handles rows that do not satisfy its condition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

impl JoinType {
    pub(crate) fn sql(self) -> &'static str {
        match self {
            Self::Inner => "INNER JOIN",
            Self::Left => "LEFT OUTER JOIN",
            Self::Right => "RIGHT OUTER JOIN",
            Self::Full => "FULL OUTER JOIN",
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum UnionAlignment {
    Position,
    Name { allow_missing_columns: bool },
}

#[derive(Clone, Copy)]
pub(crate) enum PredicateClause {
    Where,
    Having,
    Qualify,
}

pub(crate) enum LogicalPlan {
    Deferred {
        lower: Box<LowerPlan>,
        resolved: OnceLock<Result<Arc<Self>, CompileError>>,
    },
    Scan {
        table: ObjectName,
        schema: Option<Vec<Field>>,
    },
    Filter {
        input: Arc<Self>,
        predicate: Expr,
        clause: PredicateClause,
    },
    Project {
        input: Arc<Self>,
        projections: Vec<Projection>,
    },
    ScalarProject {
        input: Arc<Self>,
        projections: Vec<Projection>,
    },
    Aggregate {
        input: Arc<Self>,
        keys: Vec<Projection>,
        aggregates: Vec<Projection>,
    },
    Sort {
        input: Arc<Self>,
        keys: Vec<SortKey>,
    },
    Limit {
        input: Arc<Self>,
        count: u64,
        offset: u64,
    },
    Tail {
        input: Arc<Self>,
        count: u64,
    },
    Join {
        left: Arc<Self>,
        right: Arc<Self>,
        condition: Expr,
        how: JoinType,
        left_suffix: String,
        right_suffix: String,
    },
    WithColumn {
        input: Arc<Self>,
        name: String,
        expr: Expr,
        keep_column_order: bool,
    },
    Distinct {
        input: Arc<Self>,
    },
    Union {
        left: Arc<Self>,
        right: Arc<Self>,
        all: bool,
        alignment: UnionAlignment,
    },
}
