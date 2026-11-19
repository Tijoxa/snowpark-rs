use std::{cell::RefCell, path::Path};

// Enable the same expression variants as the installed Polars runtime.
use polars_python as _;
use pyo3::{
    IntoPyObjectExt, create_exception,
    exceptions::{PyRuntimeError, PyValueError},
    prelude::*,
    types::PyString,
};
use pyo3_polars::PyExpr;

use crate::{BindValue, Column, CompileError, DataFrame, Field, JoinType, polars};

create_exception!(
    snowpark_rs,
    UnsupportedPolarsExpression,
    PyRuntimeError,
    "An expression or combination has no supported Snowflake translation."
);

#[pyclass(name = "Field", module = "snowpark_rs", frozen, from_py_object)]
#[derive(Clone, PartialEq)]
struct PyField(Field);

#[pymethods]
impl PyField {
    #[new]
    #[pyo3(signature = (name, data_type=None, nullable=None))]
    fn new(name: &str, data_type: Option<&str>, nullable: Option<bool>) -> PyResult<Self> {
        let data_type = data_type
            .map(str::parse)
            .transpose()
            .map_err(compile_error)?;
        Ok(Self(
            Field::new(name, data_type, nullable).map_err(compile_error)?,
        ))
    }

    #[getter]
    fn name(&self) -> String {
        let name = self.0.name();
        match name
            .strip_prefix('"')
            .and_then(|name| name.strip_suffix('"'))
        {
            Some(quoted) => quoted.replace("\"\"", "\""),
            None => name,
        }
    }

    #[getter]
    fn data_type(&self) -> Option<String> {
        self.0.data_type.map(|data_type| data_type.to_string())
    }

    #[getter]
    fn nullable(&self) -> Option<bool> {
        self.0.nullable
    }
}

#[pyclass(name = "_Frame", frozen)]
struct PyDataFrame(DataFrame);

#[pymethods]
impl PyDataFrame {
    #[new]
    #[pyo3(signature = (name, schema=None))]
    fn new(name: &Bound<'_, PyAny>, schema: Option<Vec<PyField>>) -> PyResult<Self> {
        let schema = schema.map(|fields| fields.into_iter().map(|field| field.0).collect());
        let frame = if let Ok(name) = name.cast::<PyString>() {
            match schema {
                Some(schema) => DataFrame::table_name_with_schema(name.to_str()?, schema),
                None => DataFrame::table_name(name.to_str()?),
            }
        } else {
            let parts = name
                .try_iter()?
                .map(|part| part?.extract::<String>())
                .collect::<PyResult<Vec<_>>>()?;
            match schema {
                Some(schema) => DataFrame::table_with_schema(parts, schema),
                None => DataFrame::table(parts),
            }
        };
        Ok(Self(frame.map_err(compile_error)?))
    }

    #[getter]
    fn schema(&self) -> PyResult<Option<Vec<PyField>>> {
        Ok(self
            .0
            .schema()
            .map_err(compile_error)?
            .map(|fields| fields.into_iter().map(PyField).collect()))
    }

    fn polars_filter(
        &self,
        exprs: Vec<Bound<'_, PyAny>>,
        plugin_path: &str,
        membership: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let exprs = extract_exprs(exprs, "filter")?;
        let callback_error = RefCell::new(None);
        let lookup = |token: &str| {
            membership
                .call1((token,))
                .and_then(|frame| {
                    let frame = frame.extract::<Option<PyRef<'_, Self>>>()?;
                    Ok(frame.map(|frame| frame.0.plan.clone()))
                })
                .map_err(|error| {
                    *callback_error.borrow_mut() = Some(error);
                    polars::unsupported(
                        "filter",
                        "IsIn",
                        "the membership query could not be resolved",
                    )
                })
        };
        let context = polars::Context {
            plugin_path: Some(Path::new(plugin_path)),
            membership: Some(&lookup),
        };
        let result = polars::filter(&self.0.plan, &exprs, &context);
        if let Some(error) = callback_error.into_inner() {
            return Err(error);
        }
        Self::translated(result)
    }

    fn polars_select(&self, exprs: Vec<Bound<'_, PyAny>>, plugin_path: &str) -> PyResult<Self> {
        Self::translated(polars::select(
            &self.0.plan,
            &extract_exprs(exprs, "select")?,
            &plugin_context(plugin_path),
        ))
    }

    fn polars_with_columns(
        &self,
        exprs: Vec<Bound<'_, PyAny>>,
        plugin_path: &str,
    ) -> PyResult<Self> {
        Self::translated(polars::with_columns(
            &self.0.plan,
            &extract_exprs(exprs, "with_columns")?,
            &plugin_context(plugin_path),
        ))
    }

    fn polars_aggregate(
        &self,
        keys: Vec<Bound<'_, PyAny>>,
        exprs: Vec<Bound<'_, PyAny>>,
        plugin_path: &str,
    ) -> PyResult<Self> {
        Self::translated(polars::aggregate(
            &self.0.plan,
            &extract_exprs(keys, "group_by")?,
            &extract_exprs(exprs, "agg")?,
            &plugin_context(plugin_path),
        ))
    }

    fn polars_sort(
        &self,
        keys: Vec<(Bound<'_, PyAny>, bool, bool)>,
        plugin_path: &str,
    ) -> PyResult<Self> {
        let keys = keys
            .into_iter()
            .map(|(expr, descending, nulls_last)| {
                Ok(polars::SortKey {
                    expr: extract_expr(&expr, "sort")?,
                    descending,
                    nulls_last,
                })
            })
            .collect::<PyResult<Vec<_>>>()?;
        Self::translated(polars::sort(
            &self.0.plan,
            &keys,
            &plugin_context(plugin_path),
        ))
    }

    fn polars_join_keys(&self, keys: Vec<Bound<'_, PyAny>>) -> PyResult<Vec<String>> {
        polars::join_keys(&self.0.plan, &extract_exprs(keys, "join")?).map_err(compile_error)
    }

    fn select_join_output(&self, columns: Vec<(String, Option<String>, String)>) -> Self {
        Self(
            self.0
                .select(columns.into_iter().map(|(name, fallback, alias)| {
                    let column = crate::col(name);
                    match fallback {
                        Some(fallback) => column.coalesce(crate::col(fallback)).alias(alias),
                        None => column.alias(alias),
                    }
                })),
        )
    }

    fn limit(&self, n: u64, offset: u64) -> Self {
        Self(self.0.limit_with_offset(n, offset))
    }

    fn rename(&self, names: Vec<(String, String)>) -> Self {
        Self(self.0.rename_columns(names))
    }

    fn distinct(&self) -> Self {
        Self(self.0.distinct())
    }

    fn union(&self, other: &Self, all: bool, by_name: bool, allow_missing: bool) -> Self {
        Self(match (all, by_name) {
            (false, false) => self.0.union(&other.0),
            (true, false) => self.0.union_all(&other.0),
            (false, true) => self.0.union_by_name(&other.0, allow_missing),
            (true, true) => self.0.union_all_by_name(&other.0, allow_missing),
        })
    }

    fn join(
        &self,
        right: &Self,
        keys: Vec<(String, String)>,
        how: &str,
        nulls_equal: bool,
    ) -> PyResult<Self> {
        let how = match how {
            "inner" => JoinType::Inner,
            "left" => JoinType::Left,
            "right" => JoinType::Right,
            "full" => JoinType::Full,
            _ => {
                return Err(PyValueError::new_err(
                    "join type must be inner, left, right, or full",
                ));
            }
        };
        let predicate = keys
            .into_iter()
            .map(|(left, right_name)| {
                let left = self.0.col(left);
                let right = right.0.col(right_name);
                if nulls_equal {
                    left.eq_missing(right)
                } else {
                    left.eq(right)
                }
            })
            .reduce(Column::and)
            .ok_or_else(|| PyValueError::new_err("join requires at least one key"))?;
        Ok(Self(self.0.join_as(&right.0, predicate, how)))
    }

    fn to_sql(&self, py: Python<'_>) -> PyResult<(String, Vec<Py<PyAny>>)> {
        let query = self.0.to_sql().map_err(compile_error)?;
        let binds = query
            .binds
            .into_iter()
            .map(|value| match value {
                BindValue::Text(text) => text.into_py_any(py),
                BindValue::Int(int) => int.into_py_any(py),
                BindValue::Float(float) => float.into_py_any(py),
                BindValue::Bool(boolean) => boolean.into_py_any(py),
                BindValue::Null(_) => Ok(py.None()),
                BindValue::Decimal(value) => Ok(py
                    .import("decimal")?
                    .getattr("Decimal")?
                    .call1((value.to_string(),))?
                    .unbind()),
            })
            .collect::<PyResult<_>>()?;
        Ok((compact_sql(&query.sql), binds))
    }
}

fn compact_sql(sql: &str) -> String {
    let mut inside_identifier = false;
    sql.chars()
        .map(|character| match character {
            '"' => {
                inside_identifier = !inside_identifier;
                character
            }
            '\n' if !inside_identifier => ' ',
            _ => character,
        })
        .collect()
}

impl PyDataFrame {
    fn translated(result: Result<snowflake_ast::DataFrame, CompileError>) -> PyResult<Self> {
        Ok(Self(DataFrame {
            plan: result.map_err(compile_error)?,
        }))
    }
}

fn plugin_context(path: &str) -> polars::Context<'_> {
    polars::Context {
        plugin_path: Some(Path::new(path)),
        ..Default::default()
    }
}

fn extract_expr(expr: &Bound<'_, PyAny>, operation: &'static str) -> PyResult<polars::Expr> {
    expr.extract::<PyExpr>()
        .map(|expr| expr.0)
        .map_err(|error| {
            UnsupportedPolarsExpression::new_err(format!("{operation}: expression import: {error}"))
        })
}

fn extract_exprs(
    exprs: Vec<Bound<'_, PyAny>>,
    operation: &'static str,
) -> PyResult<Vec<polars::Expr>> {
    exprs
        .iter()
        .map(|expr| extract_expr(expr, operation))
        .collect()
}

fn compile_error(error: CompileError) -> PyErr {
    match error {
        CompileError::UnsupportedExpression { .. } => {
            UnsupportedPolarsExpression::new_err(error.to_string())
        }
        _ => PyValueError::new_err(error.to_string()),
    }
}

#[pymodule]
fn _core(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyField>()?;
    module.add_class::<PyDataFrame>()?;
    module.add(
        "UnsupportedPolarsExpression",
        module.py().get_type::<UnsupportedPolarsExpression>(),
    )?;
    Ok(())
}
