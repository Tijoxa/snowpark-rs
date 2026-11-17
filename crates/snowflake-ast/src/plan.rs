use std::sync::Arc;

use crate::{Projection, expr::Expr, ident::ObjectName};

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
}
