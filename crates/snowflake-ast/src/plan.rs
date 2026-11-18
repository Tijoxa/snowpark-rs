use std::sync::Arc;

use crate::{Projection, SortKey, expr::Expr, ident::ObjectName};

pub(crate) enum LogicalPlan {
    Scan(ObjectName),
    Filter {
        input: Arc<Self>,
        predicate: Expr,
    },
    Project {
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
    Join {
        left: Arc<Self>,
        right: Arc<Self>,
        condition: Expr,
        left_suffix: String,
        right_suffix: String,
    },
}
