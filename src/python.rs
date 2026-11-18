use pyo3::{
    IntoPyObjectExt,
    exceptions::{PyTypeError, PyValueError},
    prelude::*,
    types::{PyBool, PyDict, PyInt, PyString, PyTuple},
};

use sql_dialect_fmt_formatter::{Dialect, FormatOptions, format};

use crate::{BindValue, Column, CompileError, DataFrame, Projection};

#[pyclass(name = "Column", module = "snowpark_rs", frozen)]
struct PyColumn(Column);

#[pymethods]
impl PyColumn {
    fn alias(&self, name: String) -> PyProjection {
        PyProjection(self.0.clone().alias(name))
    }

    fn eq(&self, other: &Self) -> Self {
        Self(self.0.clone().eq(other.0.clone()))
    }

    fn ne(&self, other: &Self) -> Self {
        Self(self.0.clone().ne(other.0.clone()))
    }

    fn lt(&self, other: &Self) -> Self {
        Self(self.0.clone().lt(other.0.clone()))
    }

    fn le(&self, other: &Self) -> Self {
        Self(self.0.clone().le(other.0.clone()))
    }

    fn gt(&self, other: &Self) -> Self {
        Self(self.0.clone().gt(other.0.clone()))
    }

    fn ge(&self, other: &Self) -> Self {
        Self(self.0.clone().ge(other.0.clone()))
    }

    fn __and__(&self, other: &Self) -> Self {
        Self(self.0.clone().and(other.0.clone()))
    }

    fn __or__(&self, other: &Self) -> Self {
        Self(self.0.clone().or(other.0.clone()))
    }

    fn __invert__(&self) -> Self {
        Self(!self.0.clone())
    }

    fn is_null(&self) -> Self {
        Self(self.0.clone().is_null())
    }

    fn is_not_null(&self) -> Self {
        Self(self.0.clone().is_not_null())
    }
}

#[pyclass(name = "Projection", module = "snowpark_rs", frozen)]
struct PyProjection(Projection);

#[derive(FromPyObject)]
enum Selection<'py> {
    Name(String),
    Column(PyRef<'py, PyColumn>),
    Projection(PyRef<'py, PyProjection>),
}

impl From<Selection<'_>> for Projection {
    fn from(selection: Selection<'_>) -> Self {
        match selection {
            Selection::Name(name) => name.into(),
            Selection::Column(column) => column.0.clone().into(),
            Selection::Projection(projection) => projection.0.clone(),
        }
    }
}

#[pyclass(name = "DataFrame", module = "snowpark_rs", frozen, subclass)]
struct PyDataFrame(DataFrame);

/// A named Snowflake table that can be used to build queries offline.
#[pyclass(name = "Table", module = "snowpark_rs", extends = PyDataFrame, frozen)]
struct PyTable;

#[pymethods]
impl PyTable {
    #[new]
    fn new(name: &Bound<'_, PyAny>) -> PyResult<(Self, PyDataFrame)> {
        let frame = if let Ok(name) = name.cast::<PyString>() {
            DataFrame::table_name(name.to_str()?)
        } else {
            let parts = name
                .try_iter()?
                .map(|part| part?.extract::<String>())
                .collect::<PyResult<Vec<_>>>()?;
            DataFrame::table(parts)
        }
        .map_err(compile_error)?;
        Ok((Self, PyDataFrame(frame)))
    }
}

#[pymethods]
impl PyDataFrame {
    fn filter(&self, predicate: &PyColumn) -> Self {
        Self(self.0.filter(predicate.0.clone()))
    }

    fn select(&self, projections: Vec<Selection<'_>>) -> Self {
        Self(self.0.select(projections))
    }

    /// Call a function with this DataFrame as its first argument and return its result.
    #[pyo3(signature = (function, *args, **kwargs))]
    fn pipe<'py>(
        slf: &Bound<'py, Self>,
        function: &Bound<'py, PyAny>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut positional = Vec::with_capacity(args.len() + 1);
        positional.push(slf.as_any().clone());
        positional.extend(args.iter());
        function.call(PyTuple::new(slf.py(), positional)?, kwargs)
    }

    /// Return parameterized Snowflake SQL and its bind values in placeholder order.
    #[pyo3(signature = (*, prettify = false))]
    fn to_sql(&self, py: Python<'_>, prettify: bool) -> PyResult<(String, Vec<Py<PyAny>>)> {
        let query = self.0.to_sql().map_err(compile_error)?;
        let sql = if prettify {
            let options = FormatOptions::default().with_dialect(Dialect::Snowflake);
            format(&query.sql, &options)
        } else {
            single_line_sql(query.sql)
        };
        let binds = query
            .binds
            .into_iter()
            .map(|value| match value {
                BindValue::Text(text) => text.into_py_any(py),
                BindValue::Int(int) => int.into_py_any(py),
                BindValue::Bool(boolean) => boolean.into_py_any(py),
            })
            .collect::<PyResult<_>>()?;
        Ok((sql, binds))
    }

    /// Execute with the supplied session and return a PyArrow table.
    fn to_arrow<'py>(&self, session: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let py = session.py();
        py.import("snowpark_rs._session")?
            .call_method1("to_arrow", (session, self.to_sql(py, false)?))
    }

    /// Execute with the supplied session and iterate over PyArrow tables.
    fn to_arrow_batches<'py>(&self, session: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let py = session.py();
        py.import("snowpark_rs._session")?
            .call_method1("to_arrow_batches", (session, self.to_sql(py, false)?))
    }

    /// Convert an Arrow result to Polars, optionally one batch at a time.
    #[pyo3(signature = (session, *, use_batches = false))]
    fn to_polars<'py>(
        slf: &Bound<'py, Self>,
        session: &Bound<'py, PyAny>,
        use_batches: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        slf.py()
            .import("snowpark_rs._polars")?
            .call_method1("to_polars", (slf, session, use_batches))
    }
}

fn single_line_sql(sql: String) -> String {
    if !sql.contains('\n') {
        return sql;
    }

    let mut compact = String::with_capacity(sql.len());
    // SQL whitespace can be collapsed while preserving it inside quoted identifiers.
    for token in sql_dialect_fmt_lexer::tokenize(&sql).tokens {
        if token.text.contains('\n') && token.text.trim().is_empty() {
            compact.push(' ');
        } else {
            compact.push_str(token.text);
        }
    }
    compact
}

fn compile_error(error: CompileError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

#[pyfunction]
fn col(name: String) -> PyColumn {
    PyColumn(crate::col(name))
}

#[pyfunction]
fn lit(value: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    // bool is a subclass of int in Python and has its own Snowflake bind type.
    let bind = if value.is_instance_of::<PyBool>() {
        BindValue::Bool(value.extract()?)
    } else if value.is_instance_of::<PyInt>() {
        BindValue::Int(value.extract()?)
    } else if value.is_instance_of::<PyString>() {
        BindValue::Text(value.extract()?)
    } else {
        return Err(PyTypeError::new_err("expected a str, int, or bool"));
    };
    Ok(PyColumn(crate::lit(bind)))
}

#[pymodule]
fn _core(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyColumn>()?;
    module.add_class::<PyProjection>()?;
    module.add_class::<PyDataFrame>()?;
    module.add_class::<PyTable>()?;
    module.add_function(wrap_pyfunction!(col, module)?)?;
    module.add_function(wrap_pyfunction!(lit, module)?)?;
    Ok(())
}
