mod db;

use db::async_duckdb::{execute_async, query_async};
use duckdb::Connection;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::sync::Arc;
use tokio::sync::Mutex;

#[pyclass]
struct PyAsyncDuckDB {
    pub connect: Arc<Mutex<Connection>>,
}

#[pymethods]
impl PyAsyncDuckDB {
    #[new]
    fn new() -> PyResult<Self> {
        let connect =
            Connection::open(":memory:").map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyAsyncDuckDB {
            connect: Arc::new(Mutex::new(connect)),
        })
    }

    fn query<'py>(&self, py: Python<'py>, sql: String) -> PyResult<&'py PyAny> {
        let conn = Arc::clone(&self.connect);

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let results = query_async(conn, &sql)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            Ok(serde_json::to_string(&results).unwrap())
        })
    }

    fn execute<'py>(&self, py: Python<'py>, sql: String) -> PyResult<&'py PyAny> {
        let conn = Arc::clone(&self.connect);

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let results = execute_async(conn, &sql)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            Ok(results)
        })
    }
}

#[pymodule]
fn duckdb_async(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyAsyncDuckDB>()?;
    Ok(())
}
