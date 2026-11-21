use std::{
    cell::{Cell, RefCell},
    collections::BTreeSet,
};

use snowflake_ast::{Reduction, row_number};

use super::{
    AstFrame, Column, DataType, Expr, FunctionExpr, LiteralValue, Projection, Result, Shape,
    Translation, Translator, Value, col, identifier, integral, lit, numeric, unalias,
};

pub(super) struct GroupProjection {
    keys: Vec<Column>,
    source_names: BTreeSet<String>,
    next_alias: Cell<usize>,
    windows: RefCell<Vec<Projection>>,
    lengths: RefCell<Vec<(Expr, Column)>>,
}

impl GroupProjection {
    fn push(&self, column: Column) -> Column {
        loop {
            let index = self.next_alias.get();
            self.next_alias.set(index + 1);
            let name = format!("__SNOWPARK_POLARS_{index}");
            if !self.source_names.contains(&name) {
                let name = identifier(&name);
                self.windows.borrow_mut().push(column.alias(name.clone()));
                return col(name);
            }
        }
    }
}

pub(super) struct ReductionInput {
    value: Value,
    predicate: Option<Column>,
    reordered: bool,
}

impl ReductionInput {
    pub(super) fn mask(mut self) -> Value {
        if let Some(predicate) = self.predicate {
            self.value.column = predicate.case_when(self.value.column, Column::null());
        }
        self.value
    }

    fn restrict(&mut self, predicate: Column) {
        self.predicate = Some(match self.predicate.take() {
            Some(current) => current.and(predicate),
            None => predicate,
        });
    }
}

impl Translator<'_> {
    pub(in crate::polars) fn group_values(
        &self,
        input: &AstFrame,
        key_exprs: &[Expr],
        keys: &[Translation],
        exprs: &[Expr],
    ) -> Result<(AstFrame, Vec<Translation>)> {
        let exprs = self.expand_all(exprs)?;
        if !exprs
            .iter()
            .flat_map(|expr| expr.into_iter())
            .any(|node| matches!(node, Expr::Slice { .. }))
        {
            return Ok((input.clone(), self.translate_expanded(&exprs)?));
        }
        let group = GroupProjection {
            keys: keys.iter().map(|key| key.column.clone()).collect(),
            source_names: key_exprs
                .iter()
                .chain(&exprs)
                .flat_map(|expr| expr.into_iter())
                .filter_map(|expr| match expr {
                    Expr::Column(name) => Some(name.to_string()),
                    _ => None,
                })
                .collect(),
            next_alias: Cell::new(0),
            windows: RefCell::default(),
            lengths: RefCell::default(),
        };
        let translator = Translator {
            group: Some(&group),
            ..self.source_translator()
        };
        let values = translator.translate_expanded(&exprs)?;
        let windows = group.windows.into_inner();
        let source = if windows.is_empty() {
            input.clone()
        } else {
            input.select(
                group
                    .source_names
                    .into_iter()
                    .map(|name| col(identifier(&name)).into())
                    .chain(windows),
            )
        };
        Ok((source, values))
    }

    pub(super) fn in_partition(&self, column: Column) -> Column {
        match self.partition {
            Some(keys) => column.over(keys.iter().cloned()),
            None => column,
        }
    }

    pub(super) fn ternary(&self, predicate: &Expr, truthy: &Expr, falsy: &Expr) -> Result<Value> {
        let node = "Ternary";
        let predicate = self.visit(predicate, false)?;
        self.require_boolean(node, &predicate)?;
        let truthy = self.visit(truthy, false)?;
        let falsy = self.visit(falsy, false)?;
        let shape = self.combine_shape(node, truthy.shape, falsy.shape)?;
        let shape = self.combine_shape(node, shape, predicate.shape)?;
        let dtype = self.branch_type(node, &truthy, &falsy)?;
        let untyped_null = truthy.untyped_null && falsy.untyped_null;
        let column = predicate
            .column
            .case_when(case_value(truthy), case_value(falsy));
        let column = if dtype == Some(DataType::Float) {
            column.cast(DataType::Float)
        } else {
            column
        };
        let mut value = Value::new(column, shape, dtype);
        value.untyped_null = untyped_null;
        Ok(value)
    }

    pub(super) fn fill_null(&self, expr: &Expr, fallback: &Expr) -> Result<Value> {
        let node = "Function.FillNull";
        let inner = self.visit(expr, false)?;
        let fallback = self.visit(fallback, false)?;
        let shape = self.combine_shape(node, inner.shape, fallback.shape)?;
        let dtype = self.branch_type(node, &inner, &fallback)?;
        Ok(Value::new(
            case_value(inner).coalesce(case_value(fallback)),
            shape,
            dtype,
        ))
    }

    pub(super) fn min_horizontal(&self, exprs: &[Expr]) -> Result<Value> {
        let node = "Function.MinHorizontal";
        let values = exprs
            .iter()
            .map(|expr| self.visit(expr, false))
            .collect::<Result<Vec<_>>>()?;
        let mut shape = Shape::Scalar;
        let mut dtype = None;
        for value in &values {
            self.require_numeric(node, &[value])?;
            shape = self.combine_shape(node, shape, value.shape)?;
            dtype = numeric_supertype(dtype, value.dtype);
        }
        let column = Column::least_ignore_nulls(values.into_iter().map(case_value));
        Ok(Value::new(column, shape, dtype))
    }

    fn require_boolean(&self, node: &str, value: &Value) -> Result<()> {
        if value.dtype.is_some_and(|dtype| dtype != DataType::Boolean) {
            return Err(self.error(node, "a Boolean predicate is required"));
        }
        Ok(())
    }

    fn branch_type(&self, node: &str, left: &Value, right: &Value) -> Result<Option<DataType>> {
        match (left.dtype, right.dtype) {
            (left, right) if left == right => Ok(left),
            (None, right) if left.untyped_null => Ok(right),
            (left, None) if right.untyped_null => Ok(left),
            (None, _) | (_, None) => Ok(None),
            (left, right) if numeric(left) && numeric(right) => Ok(numeric_supertype(left, right)),
            _ => Err(self.error(node, "branches require compatible Snowflake types")),
        }
    }

    pub(super) fn reduce(&self, expr: &Expr, reduction: Reduction) -> Result<Value> {
        let node = match reduction {
            Reduction::Mean => "Agg.Mean",
            Reduction::Min => "Agg.Min",
            Reduction::Max => "Agg.Max",
            Reduction::All => "Function.Boolean.All",
            Reduction::Any => "Function.Boolean.Any",
        };
        let mut value = if self.clause_source.is_some() {
            self.source_translator().reduction_input(expr)?
        } else {
            self.reduction_input(expr)?
        }
        .mask();
        if value.shape == Shape::Aggregate {
            return Err(self.error(node, "nested aggregates are unsupported"));
        }
        let identity = match reduction {
            Reduction::All | Reduction::Any => {
                self.require_boolean(node, &value)?;
                value.dtype = Some(DataType::Boolean);
                Some(matches!(reduction, Reduction::All))
            }
            Reduction::Mean => {
                if value.dtype != Some(DataType::Boolean) {
                    self.require_numeric(node, &[&value])?;
                }
                if value.dtype == Some(DataType::Boolean) {
                    value.column = value.column.cast(super::number(1, 0));
                }
                value.column = value.column.cast(DataType::Float);
                value.dtype = Some(DataType::Float);
                None
            }
            Reduction::Min | Reduction::Max => None,
        };
        if value.shape == Shape::Row {
            value.column = self.in_partition(value.column.reduce(reduction));
            value.shape = Shape::Aggregate;
        }
        if let Some(identity) = identity {
            value.column = value.column.coalesce(lit(identity));
        }
        value.untyped_null = false;
        Ok(value)
    }

    pub(super) fn reduction_input(&self, expr: &Expr) -> Result<ReductionInput> {
        match unalias(expr) {
            Expr::Filter { input, by } => {
                let mut input = self.reduction_input(input)?;
                let by = self.visit(by, false)?;
                self.require_boolean("Filter", &by)?;
                if input.value.shape != Shape::Row || by.shape == Shape::Aggregate {
                    return Err(
                        self.error("Filter", "a series and a row-wise predicate are required")
                    );
                }
                if by.shape == Shape::Row && (input.reordered || input.predicate.is_some()) {
                    return Err(self.error(
                        "Filter",
                        "the predicate must align with the input series after selection or sorting",
                    ));
                }
                input.restrict(case_value(by));
                Ok(input)
            }
            Expr::Function {
                function: FunctionExpr::DropNulls,
                input,
            } if input.len() == 1 => {
                let mut input = self.reduction_input(&input[0])?;
                if input.value.shape != Shape::Row {
                    return Err(self.error("DropNulls", "a row-wise series is required"));
                }
                input.restrict(input.value.column.clone().is_not_null());
                Ok(input)
            }
            Expr::Sort { expr, options } if options.limit.is_none() => {
                let mut input = self.reduction_input(expr)?;
                input.reordered = true;
                Ok(input)
            }
            Expr::Slice {
                input,
                offset,
                length,
            } => self.ordered_slice(input, offset, length),
            _ => Ok(ReductionInput {
                value: self.visit(expr, false)?,
                predicate: None,
                reordered: false,
            }),
        }
    }

    fn ordered_slice(&self, expr: &Expr, offset: &Expr, length: &Expr) -> Result<ReductionInput> {
        let node = "Slice";
        let Some(group) = self.group else {
            return Err(self.error(
                node,
                "ordered selection requires a group aggregation or select",
            ));
        };
        let Expr::Sort { expr, options } = unalias(expr) else {
            return Err(self.error(
                node,
                "head and tail under a reduction require an explicit sort",
            ));
        };
        let tail = slice_end(offset, length).ok_or_else(|| {
            self.error(
                node,
                "only head and tail of an explicitly sorted series are supported",
            )
        })?;
        if options.limit.is_some()
            || expr
                .into_iter()
                .any(|node| matches!(node, Expr::Slice { .. } | Expr::Over { .. }))
        {
            return Err(self.error(
                node,
                "nested slices or windows in the sorted series are unsupported",
            ));
        }
        let mut input = self.reduction_input(expr)?;
        if input.value.shape != Shape::Row {
            return Err(self.error(
                node,
                "the sorted expression must produce a value for each candidate row",
            ));
        }
        let length = self.slice_length(group, length)?;

        // Put candidates first, while preserving NULL ordering inside the
        // candidate series. Reverse its sort to number a tail from the end.
        let mut order = Vec::with_capacity(2);
        if let Some(predicate) = &input.predicate {
            order.push(predicate.clone().desc_nulls_last());
        }
        let value = input.value.column.clone();
        order.push(
            match (options.descending ^ tail, options.nulls_last ^ tail) {
                (false, false) => value.asc_nulls_first(),
                (false, true) => value.asc_nulls_last(),
                (true, false) => value.desc_nulls_first(),
                (true, true) => value.desc_nulls_last(),
            },
        );
        let rank = group.push(row_number(group.keys.iter().cloned(), order));
        let within = rank.le(length.clone());
        input.restrict(if tail {
            within
        } else {
            within.or(length.is_null())
        });
        input.reordered = true;
        Ok(input)
    }

    pub(super) fn series_len(&self, expr: &Expr) -> Result<Value> {
        let input = if self.clause_source.is_some() {
            self.source_translator().reduction_input(expr)?
        } else {
            self.reduction_input(expr)?
        };
        if input.value.shape != Shape::Row {
            return Err(self.error("Agg.Count", "Expr.len() requires a row-wise series"));
        }
        let count = match input.predicate {
            Some(predicate) => super::count(predicate.case_when(lit(1), Column::null())),
            None => super::count_all(),
        };
        Ok(Value::new(
            self.in_partition(count),
            Shape::Aggregate,
            Some(super::ROW_COUNT),
        ))
    }

    fn slice_length(&self, group: &GroupProjection, expr: &Expr) -> Result<Column> {
        if let Some((_, column)) = group
            .lengths
            .borrow()
            .iter()
            .find(|(saved, _)| saved == expr)
        {
            return Ok(column.clone());
        }
        let translator = Translator {
            partition: Some(&group.keys),
            ..self.source_translator()
        };
        let value = translator.visit(expr, false)?;
        if value.shape == Shape::Row
            || value.dtype.is_some_and(|dtype| !integral(dtype))
            || integer_literal(expr).is_some_and(|length| length < 0)
        {
            return Err(self.error("Slice", "length must be one nonnegative integer per group"));
        }
        if value.shape == Shape::Scalar {
            return Ok(case_value(value));
        }
        let column = group.push(value.column);
        group
            .lengths
            .borrow_mut()
            .push((expr.clone(), column.clone()));
        Ok(column)
    }
}

fn slice_end(offset: &Expr, length: &Expr) -> Option<bool> {
    if let Some(offset) = integer_literal(offset) {
        if offset == 0 {
            return Some(false);
        }
        let n = offset.checked_neg()?;
        return (n > 0 && integer_literal(length) == Some(n)).then_some(true);
    }
    if let Expr::Function {
        function: FunctionExpr::Negate,
        input,
    } = unalias(offset)
        && let [negated] = input.as_slice()
    {
        // Python's Expr.tail wraps n in a cast before negating unsigned n.
        let negated = match unalias(negated) {
            Expr::Cast {
                expr,
                dtype: super::DataTypeExpr::Literal(super::PolarsType::Int64),
                options: super::CastOptions::Overflowing,
            } => expr.as_ref(),
            other => other,
        };
        return (negated == length).then_some(true);
    }
    None
}

fn integer_literal(expr: &Expr) -> Option<i64> {
    match unalias(expr) {
        Expr::Literal(literal @ (LiteralValue::Dyn(_) | LiteralValue::Scalar(_))) => {
            literal.extract_i64().ok()
        }
        _ => None,
    }
}

fn case_value(value: Value) -> Column {
    if value.untyped_null {
        Column::null()
    } else {
        value.column
    }
}

fn numeric_supertype(left: Option<DataType>, right: Option<DataType>) -> Option<DataType> {
    match (left, right) {
        (Some(DataType::Float), _) | (_, Some(DataType::Float)) => Some(DataType::Float),
        (
            Some(DataType::Number {
                precision: p1,
                scale: s1,
            }),
            Some(DataType::Number {
                precision: p2,
                scale: s2,
            }),
        ) => {
            let scale = s1.max(s2);
            Some(super::number(
                (p1 - s1).max(p2 - s2).saturating_add(scale).min(38),
                scale,
            ))
        }
        (left, right) => left.or(right),
    }
}
