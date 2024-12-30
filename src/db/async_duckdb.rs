use duckdb::Result as DuckDBResult;
use duckdb::{params, Connection};
use serde_json::{Map, Number, Value};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task;

pub async fn query_async(
    connect: Arc<Mutex<Connection>>,
    sql: &str,
) -> DuckDBResult<Vec<Map<String, Value>>> {
    let conn = connect.lock().await.try_clone()?;
    let sql = sql.to_string();

    task::spawn_blocking(move || {
        // execute query
        let mut stmt = conn.prepare(sql.as_ref())?;
        let mut rows = stmt.query(params![])?;

        // parse result to json
        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            // get column names
            let column_names = row.as_ref().column_names();

            // parse record
            let mut record = Map::new();
            for (i, col_name) in column_names.iter().enumerate() {
                // Cast output values to json Value
                // https://docs.rs/serde_json/1.0.134/serde_json/value/enum.Value.html
                let value: Value = if let Ok(val) = row.get::<usize, f64>(i) {
                    if val.fract() == 0.0 {
                        Number::from(val as i64).into()
                    } else {
                        Number::from_f64(val).map_or(Value::Null, Value::Number)
                    }
                } else if let Ok(val) = row.get::<usize, bool>(i) {
                    Value::Bool(val)
                } else if let Ok(val) = row.get::<usize, String>(i) {
                    Value::String(val)
                } else {
                    Value::Null
                };

                record.insert(col_name.to_string(), value);
            }

            // append record to results
            results.push(record);
        }
        Ok(results)
    })
    .await
    .unwrap()
}

pub async fn execute_async(connect: Arc<Mutex<Connection>>, sql: &str) -> DuckDBResult<usize> {
    let conn = connect.lock().await.try_clone()?;
    let sql = sql.to_string();

    task::spawn_blocking(move || {
        // execute query
        let output = conn.execute(&sql, [])?;
        Ok(output)
    })
    .await
    .unwrap()
}
