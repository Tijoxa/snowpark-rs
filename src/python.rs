use pyo3::{
    IntoPyObjectExt,
    exceptions::{PyTypeError, PyValueError},
    prelude::*,
    types::{PyBool, PyDict, PyInt, PyString, PyTuple},
};

use crate::{BindValue, Column, CompileError, DataFrame, GroupedDataFrame, Projection, SortKey};

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

    fn asc(&self) -> PySortKey {
        PySortKey(self.0.clone().asc())
    }

    fn desc(&self) -> PySortKey {
        PySortKey(self.0.clone().desc())
    }

    fn asc_nulls_first(&self) -> PySortKey {
        PySortKey(self.0.clone().asc_nulls_first())
    }

    fn asc_nulls_last(&self) -> PySortKey {
        PySortKey(self.0.clone().asc_nulls_last())
    }

    fn desc_nulls_first(&self) -> PySortKey {
        PySortKey(self.0.clone().desc_nulls_first())
    }

    fn desc_nulls_last(&self) -> PySortKey {
        PySortKey(self.0.clone().desc_nulls_last())
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

#[derive(FromPyObject)]
enum ColumnInput<'py> {
    Name(String),
    Column(PyRef<'py, PyColumn>),
}

impl From<ColumnInput<'_>> for Column {
    fn from(input: ColumnInput<'_>) -> Self {
        match input {
            ColumnInput::Name(name) => name.into(),
            ColumnInput::Column(column) => column.0.clone(),
        }
    }
}

#[pyclass(name = "SortKey", module = "snowpark_rs", frozen)]
struct PySortKey(SortKey);

#[derive(FromPyObject)]
enum SortInput<'py> {
    Key(PyRef<'py, PySortKey>),
    Column(ColumnInput<'py>),
}

impl From<SortInput<'_>> for SortKey {
    fn from(input: SortInput<'_>) -> Self {
        match input {
            SortInput::Key(key) => key.0.clone(),
            SortInput::Column(column) => Column::from(column).into(),
        }
    }
}

#[derive(FromPyObject)]
enum Ascending {
    All(Direction),
    PerKey(Vec<Direction>),
}

#[derive(Clone, Copy, FromPyObject)]
enum Direction {
    Boolean(bool),
    Integer(i64),
}

impl Direction {
    fn ascending(self) -> PyResult<bool> {
        match self {
            Self::Boolean(value) => Ok(value),
            Self::Integer(0) => Ok(false),
            Self::Integer(1) => Ok(true),
            Self::Integer(_) => Err(PyValueError::new_err(
                "ascending values must be bool, 0, or 1",
            )),
        }
    }
}

fn unpack_args<'py>(args: &Bound<'py, PyTuple>) -> PyResult<Vec<Bound<'py, PyAny>>> {
    if args.len() == 1 {
        let first = args.get_item(0)?;
        if !first.is_instance_of::<PyString>() {
            match first.try_iter() {
                Ok(items) => return items.collect(),
                Err(error) if !error.is_instance_of::<PyTypeError>(args.py()) => return Err(error),
                Err(_) => {}
            }
        }
    }
    Ok(args.iter().collect())
}

#[derive(FromPyObject)]
enum AggregateInput<'py> {
    Column(PyRef<'py, PyColumn>),
    Projection(PyRef<'py, PyProjection>),
    Pair((ColumnInput<'py>, String)),
}

impl AggregateInput<'_> {
    fn into_projection(self) -> PyResult<Projection> {
        match self {
            Self::Column(column) => Ok(column.0.clone().into()),
            Self::Projection(projection) => Ok(projection.0.clone()),
            Self::Pair((column, function)) => aggregate_function(column, &function),
        }
    }
}

fn aggregate_function(column: ColumnInput<'_>, function: &str) -> PyResult<Projection> {
    if function.eq_ignore_ascii_case("count") {
        Ok(count(column).0.into())
    } else if function.eq_ignore_ascii_case("sum") {
        Ok(sum(column).0.into())
    } else {
        Err(PyValueError::new_err(
            "supported aggregates are COUNT and SUM",
        ))
    }
}

fn aggregate_args(args: &Bound<'_, PyTuple>) -> PyResult<Vec<Projection>> {
    if args.len() == 1 {
        let first = args.get_item(0)?;
        if let Ok(mapping) = first.cast::<PyDict>() {
            return mapping
                .iter()
                .map(|(name, function)| {
                    aggregate_function(name.extract()?, function.extract::<String>()?.as_str())
                })
                .collect();
        }
        // A single (column, function) tuple is one aggregate, not a list of arguments.
        if first.is_instance_of::<PyTuple>() {
            if let Ok(agg) = first.extract::<AggregateInput<'_>>() {
                return Ok(vec![agg.into_projection()?]);
            }
        }
    }
    unpack_args(args)?
        .iter()
        .map(|arg| arg.extract::<AggregateInput<'_>>()?.into_projection())
        .collect()
}

#[pyclass(name = "RelationalGroupedDataFrame", module = "snowpark_rs", frozen)]
struct PyGroupedDataFrame(GroupedDataFrame);

#[pymethods]
impl PyGroupedDataFrame {
    #[pyo3(signature = (*exprs))]
    fn agg(&self, exprs: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        Ok(PyDataFrame(self.0.agg(aggregate_args(exprs)?)))
    }
}

#[pyclass(name = "DataFrame", module = "snowpark_rs", frozen)]
struct PyDataFrame(DataFrame);

#[pymethods]
impl PyDataFrame {
    #[staticmethod]
    fn table(name: &Bound<'_, PyAny>) -> PyResult<Self> {
        let frame = if let Ok(name) = name.cast::<PyString>() {
            DataFrame::table_name(name.to_str()?)
        } else {
            let parts = name
                .try_iter()?
                .map(|part| part?.extract::<String>())
                .collect::<PyResult<Vec<_>>>()?;
            DataFrame::table(parts)
        };
        frame.map(Self).map_err(compile_error)
    }

    fn filter(&self, predicate: &PyColumn) -> Self {
        Self(self.0.filter(predicate.0.clone()))
    }

    fn select(&self, projections: Vec<Selection<'_>>) -> Self {
        Self(self.0.select(projections))
    }

    fn col(&self, col_name: String) -> PyColumn {
        PyColumn(self.0.col(col_name))
    }

    fn __getitem__(&self, name: String) -> PyColumn {
        self.col(name)
    }

    #[pyo3(signature = (*cols))]
    fn group_by(&self, cols: &Bound<'_, PyTuple>) -> PyResult<PyGroupedDataFrame> {
        let cols = unpack_args(cols)?
            .iter()
            .map(|col| col.extract::<Selection<'_>>().map(Projection::from))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyGroupedDataFrame(self.0.group_by(cols)))
    }

    #[pyo3(signature = (*exprs))]
    fn agg(&self, exprs: &Bound<'_, PyTuple>) -> PyResult<Self> {
        Ok(Self(self.0.agg(aggregate_args(exprs)?)))
    }

    #[pyo3(signature = (*cols, ascending=None))]
    fn sort(&self, cols: &Bound<'_, PyTuple>, ascending: Option<Ascending>) -> PyResult<Self> {
        let keys = unpack_args(cols)?
            .iter()
            .map(|col| col.extract::<SortInput<'_>>().map(SortKey::from))
            .collect::<PyResult<Vec<_>>>()?;
        let directions = match ascending {
            None => return Ok(Self(self.0.sort(keys))),
            Some(Ascending::All(direction)) => vec![direction; keys.len()],
            Some(Ascending::PerKey(directions)) if directions.len() == keys.len() => directions,
            Some(Ascending::PerKey(_)) => {
                return Err(PyValueError::new_err(
                    "ascending must contain one value per sort key",
                ));
            }
        };
        let keys = keys
            .into_iter()
            .zip(directions)
            .map(|(key, direction)| Ok(key.with_direction(direction.ascending()?)))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(Self(self.0.sort(keys)))
    }

    #[pyo3(signature = (n, offset=0))]
    fn limit(&self, n: u64, offset: u64) -> Self {
        Self(self.0.limit_with_offset(n, offset))
    }

    #[pyo3(signature = (right, on, how="inner", *, lsuffix="", rsuffix=""))]
    fn join(
        &self,
        right: &Self,
        on: &PyColumn,
        how: &str,
        lsuffix: &str,
        rsuffix: &str,
    ) -> PyResult<Self> {
        if !how.eq_ignore_ascii_case("inner") {
            return Err(PyValueError::new_err("only inner joins are supported"));
        }
        Ok(Self(self.0.join_with_suffixes(
            &right.0,
            on.0.clone(),
            lsuffix,
            rsuffix,
        )))
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

    fn to_sql(&self, py: Python<'_>) -> PyResult<(String, Vec<Py<PyAny>>)> {
        let query = self.0.to_sql().map_err(compile_error)?;
        let binds = query
            .binds
            .into_iter()
            .map(|value| match value {
                BindValue::Text(text) => text.into_py_any(py),
                BindValue::Int(int) => int.into_py_any(py),
                BindValue::Bool(boolean) => boolean.into_py_any(py),
            })
            .collect::<PyResult<_>>()?;
        Ok((query.sql, binds))
    }

    /// Execute with the supplied session and return a PyArrow table.
    fn to_arrow<'py>(&self, session: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let py = session.py();
        py.import("snowpark_rs._session")?
            .call_method1("to_arrow", (session, self.to_sql(py)?))
    }

    /// Execute with the supplied session and iterate over PyArrow tables.
    fn to_arrow_batches<'py>(&self, session: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let py = session.py();
        py.import("snowpark_rs._session")?
            .call_method1("to_arrow_batches", (session, self.to_sql(py)?))
    }
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

#[pyfunction]
fn count(e: ColumnInput<'_>) -> PyColumn {
    PyColumn(crate::count(e))
}

#[pyfunction]
fn sum(e: ColumnInput<'_>) -> PyColumn {
    PyColumn(crate::sum(e))
}

#[pymodule]
fn _core(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyColumn>()?;
    module.add_class::<PyProjection>()?;
    module.add_class::<PyDataFrame>()?;
    module.add_class::<PyGroupedDataFrame>()?;
    module.add_class::<PySortKey>()?;
    module.add_function(wrap_pyfunction!(col, module)?)?;
    module.add_function(wrap_pyfunction!(lit, module)?)?;
    module.add_function(wrap_pyfunction!(count, module)?)?;
    module.add_function(wrap_pyfunction!(sum, module)?)?;
    Ok(())
}
