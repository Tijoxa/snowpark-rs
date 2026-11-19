use pyo3::{
    IntoPyObjectExt,
    exceptions::{PyTypeError, PyValueError},
    prelude::*,
    types::{PyBool, PyDict, PyInt, PyString, PyTuple},
};

use crate::{
    BindValue, Column, CompileError, DataFrame, DataType, Decimal, Field, GroupedDataFrame,
    JoinType, Projection, SortKey,
};

#[pyclass(name = "Column", module = "snowpark_rs", frozen)]
struct PyColumn(Column);

#[pymethods]
impl PyColumn {
    fn alias(&self, name: String) -> PyProjection {
        PyProjection(self.0.clone().alias(name))
    }

    fn eq(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self(self.0.clone().eq(literal_column(other)?)))
    }

    fn ne(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self(self.0.clone().ne(literal_column(other)?)))
    }

    fn lt(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self(self.0.clone().lt(literal_column(other)?)))
    }

    fn le(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self(self.0.clone().le(literal_column(other)?)))
    }

    fn gt(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self(self.0.clone().gt(literal_column(other)?)))
    }

    fn ge(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self(self.0.clone().ge(literal_column(other)?)))
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.eq(other)
    }

    fn __ne__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.ne(other)
    }

    fn __lt__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.lt(other)
    }

    fn __le__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.le(other)
    }

    fn __gt__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.gt(other)
    }

    fn __ge__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.ge(other)
    }

    fn __bool__(&self) -> PyResult<bool> {
        Err(PyTypeError::new_err(
            "Cannot convert a Column to bool; use &, | and ~ for filter expressions",
        ))
    }

    fn __hash__(slf: PyRef<'_, Self>) -> isize {
        // Keep identity hashing so columns can be used as rename mapping keys.
        slf.as_ptr() as isize
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

    fn cast(&self, to: &str) -> PyResult<Self> {
        Ok(Self(
            self.0.clone().cast(to.parse().map_err(compile_error)?),
        ))
    }

    fn __neg__(&self) -> Self {
        Self(-self.0.clone())
    }

    fn __add__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.arithmetic(other, false, |left, right| left + right)
    }

    fn __radd__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.arithmetic(other, true, |left, right| left + right)
    }

    fn __sub__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.arithmetic(other, false, |left, right| left - right)
    }

    fn __rsub__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.arithmetic(other, true, |left, right| left - right)
    }

    fn __mul__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.arithmetic(other, false, |left, right| left * right)
    }

    fn __rmul__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.arithmetic(other, true, |left, right| left * right)
    }

    fn __truediv__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.arithmetic(other, false, |left, right| left / right)
    }

    fn __rtruediv__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.arithmetic(other, true, |left, right| left / right)
    }

    fn __mod__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.arithmetic(other, false, |left, right| left % right)
    }

    fn __rmod__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        self.arithmetic(other, true, |left, right| left % right)
    }

    fn __pow__(
        &self,
        other: &Bound<'_, PyAny>,
        modulo: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        Self::check_modulo(modulo)?;
        self.arithmetic(other, false, Column::pow)
    }

    fn __rpow__(
        &self,
        other: &Bound<'_, PyAny>,
        modulo: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        Self::check_modulo(modulo)?;
        self.arithmetic(other, true, Column::pow)
    }
}

impl PyColumn {
    fn arithmetic(
        &self,
        other: &Bound<'_, PyAny>,
        reverse: bool,
        operation: impl FnOnce(Column, Column) -> Column,
    ) -> PyResult<Self> {
        let this = self.0.clone();
        let other = literal_column(other)?;
        Ok(Self(if reverse {
            operation(other, this)
        } else {
            operation(this, other)
        }))
    }

    fn check_modulo(modulo: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
        if modulo.is_some_and(|value| !value.is_none()) {
            return Err(PyTypeError::new_err(
                "Column exponentiation does not take a modulus",
            ));
        }
        Ok(())
    }
}

#[pyclass(name = "Field", module = "snowpark_rs", frozen, from_py_object, eq)]
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
        self.0.name()
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
        if first.is_instance_of::<PyTuple>()
            && let Ok(agg) = first.extract::<AggregateInput<'_>>()
        {
            return Ok(vec![agg.into_projection()?]);
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

#[pyclass(name = "DataFrame", module = "snowpark_rs", frozen, subclass)]
struct PyDataFrame(DataFrame);

/// A named Snowflake table that can be used to build queries offline.
#[pyclass(name = "Table", module = "snowpark_rs", extends = PyDataFrame, frozen)]
struct PyTable;

#[pymethods]
impl PyTable {
    #[new]
    #[pyo3(signature = (name, *, schema=None))]
    fn new(name: &Bound<'_, PyAny>, schema: Option<Vec<PyField>>) -> PyResult<(Self, PyDataFrame)> {
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
        }
        .map_err(compile_error)?;
        Ok((Self, PyDataFrame(frame)))
    }
}

#[pymethods]
impl PyDataFrame {
    #[getter]
    fn schema(&self) -> PyResult<Option<Vec<PyField>>> {
        Ok(self
            .0
            .schema()
            .map_err(compile_error)?
            .map(|fields| fields.into_iter().map(PyField).collect()))
    }

    #[getter]
    fn columns(&self) -> PyResult<Option<Vec<String>>> {
        self.0.columns().map_err(compile_error)
    }

    #[pyo3(signature = (col_name, col, *, keep_column_order=false))]
    fn with_column(&self, col_name: String, col: &PyColumn, keep_column_order: bool) -> Self {
        Self(
            self.0
                .with_column_order(col_name, col.0.clone(), keep_column_order),
        )
    }

    #[pyo3(signature = (col_or_mapper, new_column=None))]
    fn rename(
        &self,
        col_or_mapper: &Bound<'_, PyAny>,
        new_column: Option<String>,
    ) -> PyResult<Self> {
        if let Ok(mapping) = col_or_mapper.cast::<PyDict>() {
            if new_column.is_some() {
                return Err(PyTypeError::new_err(
                    "new_column cannot be supplied with a mapping",
                ));
            }
            let names = mapping
                .iter()
                .map(|(old, new)| {
                    Ok((
                        Column::from(old.extract::<ColumnInput<'_>>()?),
                        new.extract::<String>()?,
                    ))
                })
                .collect::<PyResult<Vec<_>>>()?;
            return Ok(Self(self.0.rename_columns(names)));
        }
        let old = col_or_mapper.extract::<ColumnInput<'_>>()?;
        let new = new_column.ok_or_else(|| {
            PyTypeError::new_err("rename requires new_column for a single column")
        })?;
        Ok(Self(self.0.rename(Column::from(old), new)))
    }

    fn distinct(&self) -> Self {
        Self(self.0.distinct())
    }

    fn union(&self, other: &Self) -> Self {
        Self(self.0.union(&other.0))
    }

    fn union_all(&self, other: &Self) -> Self {
        Self(self.0.union_all(&other.0))
    }

    #[pyo3(signature = (other, allow_missing_columns=false))]
    fn union_by_name(&self, other: &Self, allow_missing_columns: bool) -> Self {
        Self(self.0.union_by_name(&other.0, allow_missing_columns))
    }

    #[pyo3(signature = (other, allow_missing_columns=false))]
    fn union_all_by_name(&self, other: &Self, allow_missing_columns: bool) -> Self {
        Self(self.0.union_all_by_name(&other.0, allow_missing_columns))
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
        let how = match how.to_ascii_lowercase().as_str() {
            "inner" => JoinType::Inner,
            "left" | "leftouter" | "left_outer" | "left outer" => JoinType::Left,
            "right" | "rightouter" | "right_outer" | "right outer" => JoinType::Right,
            "full" | "outer" | "fullouter" | "full_outer" | "full outer" => JoinType::Full,
            _ => {
                return Err(PyValueError::new_err(
                    "join type must be inner, left, right, or full",
                ));
            }
        };
        Ok(Self(self.0.join_as_with_suffixes(
            &right.0,
            on.0.clone(),
            how,
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

    /// Return parameterized Snowflake SQL and its bind values in placeholder order.
    fn to_sql(&self, py: Python<'_>) -> PyResult<(String, Vec<Py<PyAny>>)> {
        let query = self.0.to_sql().map_err(compile_error)?;
        let binds = query
            .binds
            .into_iter()
            .map(|value| match value {
                BindValue::Text(text) => text.into_py_any(py),
                BindValue::Int(int) => int.into_py_any(py),
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

    /// Read the result schema from Snowflake with a zero-row query.
    fn schema_with_session<'py>(&self, session: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        Self(self.0.limit(0)).to_arrow(session)?.getattr("schema")
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

fn compact_sql(sql: &str) -> String {
    let mut inside_identifier = false;
    sql.chars()
        .map(|character| match character {
            // Escaped double quotes toggle the state twice.
            '"' => {
                inside_identifier = !inside_identifier;
                character
            }
            '\n' if !inside_identifier => ' ',
            _ => character,
        })
        .collect()
}

fn compile_error(error: CompileError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

#[pyfunction]
fn col(name: String) -> PyColumn {
    PyColumn(crate::col(name))
}

#[pyfunction]
#[pyo3(signature = (value, datatype=None))]
fn lit(value: &Bound<'_, PyAny>, datatype: Option<&str>) -> PyResult<PyColumn> {
    let data_type = datatype
        .map(str::parse::<DataType>)
        .transpose()
        .map_err(compile_error)?;
    let column = match data_type {
        Some(data_type) if value.is_none() => crate::lit_null(data_type),
        Some(data_type) => literal_column(value)?.cast(data_type),
        None => literal_column(value)?,
    };
    Ok(PyColumn(column))
}

#[pyfunction]
fn lit_null(datatype: &str) -> PyResult<PyColumn> {
    Ok(PyColumn(crate::lit_null(
        datatype.parse().map_err(compile_error)?,
    )))
}

fn literal_column(value: &Bound<'_, PyAny>) -> PyResult<Column> {
    // bool is a subclass of int in Python and has its own Snowflake bind type.
    let bind = if value.is_instance_of::<PyBool>() {
        BindValue::Bool(value.extract()?)
    } else if value.is_instance_of::<PyInt>() {
        BindValue::Int(value.extract()?)
    } else if value.is_instance_of::<PyString>() {
        BindValue::Text(value.extract()?)
    } else if value.is_none() {
        BindValue::Null(DataType::Varchar)
    } else if let Ok(column) = value.extract::<PyRef<'_, PyColumn>>() {
        return Ok(column.0.clone());
    } else if value.is_instance(&value.py().import("decimal")?.getattr("Decimal")?)? {
        BindValue::Decimal(
            value
                .str()?
                .to_str()?
                .parse::<Decimal>()
                .map_err(compile_error)?,
        )
    } else {
        return Err(PyTypeError::new_err(
            "expected a Column, str, int, bool, Decimal, or None",
        ));
    };
    Ok(crate::lit(bind))
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
    module.add_class::<PyTable>()?;
    module.add_class::<PyGroupedDataFrame>()?;
    module.add_class::<PySortKey>()?;
    module.add_class::<PyField>()?;
    module.add_function(wrap_pyfunction!(col, module)?)?;
    module.add_function(wrap_pyfunction!(lit, module)?)?;
    module.add_function(wrap_pyfunction!(lit_null, module)?)?;
    module.add_function(wrap_pyfunction!(count, module)?)?;
    module.add_function(wrap_pyfunction!(sum, module)?)?;
    Ok(())
}
