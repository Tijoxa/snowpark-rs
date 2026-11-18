# snowpark-rs

`snowpark-rs` is a Rust library and a Python package built with Maturin. The
`snowflake-ast` crate owns the query plans and SQL compiler.
The optional `session` feature uses `snowflake-connector-rs` to execute them.

## Python

Install the project and its Snowpark Python development dependency with Python
3.13 or 3.14 and Rust 1.94 or newer.

```sh
uv sync --dev
```

```python
from snowpark_rs import Table
from snowpark_rs.functions import col, lit

frame = Table("PUBLIC.PUBLIC.EXAMPLE")
query = frame.filter(col("category").eq(lit("A"))).select(["id", "name"])
sql, binds = query.to_sql()
pretty_sql, binds = query.to_sql(prettify=True)
```

The Python API compiles queries offline and returns single-line SQL with ordered
bind values. Use `prettify=True` for indented SQL formatted in the Snowflake
dialect. `frame.pipe(function, *args, **kwargs)` passes the frame as the
function's first argument and returns its result. Run the tests with
`uv run pytest`.

### Execute with a Snowflake session

Enable the optional backend in this checkout with `uv sync --dev --extra session`.
The equivalent extra for a packaged installation is `snowpark-rs[session]`.
Run the Arrow integration test locally with `uv run --extra session pytest`.

```python
from snowpark_rs import Session

session = Session.builder.config("connection_name", "myconnection").create()
table = query.to_arrow(session)
for batch in query.to_arrow_batches(session):
    print(batch.num_rows)
```

The builder reads the named profile in `connections.toml`. It supports password,
programmatic access token, OAuth token and PKCS#8 key-pair profiles. Each
Arrow call starts a separate query with the supplied session.
`to_arrow()` returns a `pyarrow.Table`, while `to_arrow_batches()` reads
partitions incrementally as tables. Exact `NUMBER` values use Arrow decimal
columns. `TIMESTAMP_TZ` values carry a UTC timestamp and the original offset
in seconds. Semi-structured values are represented as JSON text.

### Convert results to Polars

Install `snowpark-rs[session,polars]` to convert Arrow results to Polars. In this
checkout, run `uv sync --dev --extra session --extra polars`.

```python
data = query.to_polars(session)
for batch in query.to_polars(session, use_batches=True):
    print(batch.height)
```

## Rust tests

```sh
cargo test --locked --no-default-features
cargo test --locked -p snowpark-rs --features session
```

The default workspace members run SQL compilation tests in `snowflake-ast`
and `snowpark-rs`. Run the live integration tests with a configured Snowflake
profile and the `PUBLIC.PUBLIC.EXAMPLE` fixture.

```sh
cargo test --locked -p snowpark-rs --features session --test snowflake_smoke -- --ignored
```
