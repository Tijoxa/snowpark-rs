use std::sync::{LazyLock, Mutex};

use arrow_ipc::writer::StreamWriter;
use futures_util::StreamExt;
use pyo3::{
    exceptions::{PyRuntimeError, PyTypeError},
    prelude::*,
    types::{PyBool, PyBytes, PyInt, PyString},
};
use snowpark_rs::{
    BindValue, CompiledQuery, DataType, Decimal, Session,
    session::{ArrowBatches, RecordBatch, connect_profile, execute_arrow, execute_arrow_batches},
};
use tokio::runtime::Runtime;

static RUNTIME: LazyLock<std::io::Result<Runtime>> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
});

fn runtime() -> PyResult<&'static Runtime> {
    RUNTIME.as_ref().map_err(runtime_error)
}

#[pyclass(name = "Connection", frozen)]
struct PyConnection(Session);

#[pymethods]
impl PyConnection {
    #[new]
    #[pyo3(signature = (connection_name=None))]
    fn new(py: Python<'_>, connection_name: Option<String>) -> PyResult<Self> {
        let conn = py.detach(|| {
            runtime()?
                .block_on(connect_profile(connection_name.as_deref()))
                .map_err(runtime_error)
        })?;
        Ok(Self(conn))
    }

    fn to_arrow<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        binds: Vec<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let query = compiled_query(sql, binds)?;
        let bytes = py.detach(|| {
            let batch = runtime()?
                .block_on(execute_arrow(&self.0, query))
                .map_err(runtime_error)?;
            encode(&batch)
        })?;
        Ok(PyBytes::new(py, &bytes))
    }

    fn to_arrow_batches(
        &self,
        py: Python<'_>,
        sql: String,
        binds: Vec<Bound<'_, PyAny>>,
    ) -> PyResult<PyBatches> {
        let query = compiled_query(sql, binds)?;
        let batches = py.detach(|| {
            runtime()?
                .block_on(execute_arrow_batches(&self.0, query))
                .map_err(runtime_error)
        })?;
        Ok(PyBatches(Mutex::new(batches)))
    }
}

#[pyclass(frozen)]
struct PyBatches(Mutex<ArrowBatches>);

#[pymethods]
impl PyBatches {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let bytes = py.detach(|| {
            let mut batches = self.0.lock().map_err(runtime_error)?;
            let batch = runtime()?
                .block_on(batches.next())
                .transpose()
                .map_err(runtime_error)?;
            batch.as_ref().map(encode).transpose()
        })?;
        Ok(bytes.map(|bytes| PyBytes::new(py, &bytes)))
    }
}

fn compiled_query(sql: String, binds: Vec<Bound<'_, PyAny>>) -> PyResult<CompiledQuery> {
    let binds = binds
        .into_iter()
        .map(|value| {
            if value.is_instance_of::<PyBool>() {
                value.extract().map(BindValue::Bool)
            } else if value.is_instance_of::<PyInt>() {
                value.extract().map(BindValue::Int)
            } else if value.is_instance_of::<PyString>() {
                value.extract().map(BindValue::Text)
            } else if value.is_none() {
                // The target type is carried by the CAST in the compiled SQL.
                Ok(BindValue::Null(DataType::Varchar))
            } else if value.is_instance(&value.py().import("decimal")?.getattr("Decimal")?)? {
                value
                    .str()?
                    .to_str()?
                    .parse::<Decimal>()
                    .map(BindValue::Decimal)
                    .map_err(|error| PyTypeError::new_err(error.to_string()))
            } else {
                Err(PyTypeError::new_err(
                    "expected a str, int, bool, Decimal, or None bind value",
                ))
            }
        })
        .collect::<PyResult<Vec<_>>>()?;
    Ok(CompiledQuery { sql, binds })
}

fn encode(batch: &RecordBatch) -> PyResult<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, &batch.schema()).map_err(runtime_error)?;
    writer.write(batch).map_err(runtime_error)?;
    writer.finish().map_err(runtime_error)?;
    Ok(bytes)
}

fn runtime_error(error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

#[pymodule]
fn _snowpark_rs_session(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyConnection>()?;
    Ok(())
}
