import asyncio

from duckdb_async import PyAsyncDuckDB


async def main():
    connection = PyAsyncDuckDB()

    # Create table
    n_records = await connection.execute("""
        CREATE TABLE test_table (
            id INTEGER,
            name TEXT,
            age INTEGER,
            created_at TIMESTAMP
        );
    """)
    print(f"Execute Create table: {n_records}.")
    # データ挿入
    n_records = await connection.execute("""
        INSERT INTO test_table (id, name, age, created_at) VALUES
        (1, 'Alice', 30, '2024-01-01 10:00:00'),
        (2, 'Bob', 25, '2024-01-02 11:30:00'),
        (3, 'Charlie', 35, '2024-01-03 12:45:00');
    """)
    print(f"Insert query: {n_records}.")

    # Select data
    results = await connection.query("SELECT * FROM test_table;")
    print(results)


# 明示的にイベントループを開始
asyncio.run(main())
