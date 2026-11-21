use super::{Column, DataType, Expr, Result, Shape, Translator, Value, integral, lit, number};

impl Translator<'_> {
    pub(super) fn concat_strings(
        &self,
        args: &[Expr],
        separator: &str,
        ignore_nulls: bool,
    ) -> Result<Value> {
        let node = "Function.StringExpr.ConcatHorizontal";
        let mut shape = Shape::Scalar;
        let mut values = Vec::with_capacity(args.len());
        for arg in args {
            let value = self.visit(arg, false)?;
            if value
                .dtype
                .is_some_and(|dtype| dtype != DataType::Varchar && !integral(dtype))
            {
                return Err(self.error(
                    node,
                    "only strings and integers are supported; cast other types explicitly",
                ));
            }
            shape = self.combine_shape(node, shape, value.shape)?;
            values.push(value.column.cast(DataType::Varchar));
        }
        Ok(Value::new(
            Column::concat_text(lit(separator), values, ignore_nulls),
            shape,
            Some(DataType::Varchar),
        ))
    }

    pub(super) fn string_length(&self, expr: &Expr, bytes: bool) -> Result<Value> {
        let node = if bytes {
            "Function.StringExpr.LenBytes"
        } else {
            "Function.StringExpr.LenChars"
        };
        let value = self.text(node, expr)?;
        Ok(Value::new(
            value.column.text_length(bytes),
            value.shape,
            Some(number(10, 0)),
        ))
    }

    pub(super) fn string_boundary(
        &self,
        expr: &Expr,
        pattern: &Expr,
        suffix: bool,
    ) -> Result<Value> {
        let node = if suffix {
            "Function.StringExpr.EndsWith"
        } else {
            "Function.StringExpr.StartsWith"
        };
        let subject = self.text(node, expr)?;
        let pattern = self.text(node, pattern)?;
        let shape = self.combine_shape(node, subject.shape, pattern.shape)?;
        Ok(Value::new(
            subject.column.text_boundary(pattern.column, suffix),
            shape,
            Some(DataType::Boolean),
        ))
    }

    fn text(&self, node: &str, expr: &Expr) -> Result<Value> {
        let mut value = self.visit(expr, false)?;
        if value.dtype.is_some_and(|dtype| dtype != DataType::Varchar) {
            return Err(self.error(node, "expected string operands"));
        }
        value.column = value.column.cast(DataType::Varchar);
        Ok(value)
    }
}
