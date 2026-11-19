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
from snowpark_rs import Table, functions as F

frame = Table("PUBLIC.PUBLIC.EXAMPLE")
sql, binds = frame.filter(F.col("NAME") == "Alice").to_sql()
assert (sql, binds) == frame.filter(F.col("NAME").eq("Alice")).to_sql()
```

The Python API compiles queries offline and returns single-line SQL with ordered
bind values. In this example, the SQL contains `WHERE t0."NAME" = ?` and
`binds` is `["Alice"]`. `frame.pipe(function, *args, **kwargs)` passes the frame
as the function's first argument and returns its result. Run the tests with
`uv run pytest`.

### Group, sort, and join

```python
from snowpark_rs import Table
from snowpark_rs.functions import col, count, sum

source = Table("PUBLIC.PUBLIC.EXAMPLE")
totals = (
    source.group_by("category")
    .agg(count("*").alias("rows"), sum("amount").alias("total"))
    .sort(col("total").desc_nulls_last())
    .limit(10)
)

left = source.select(["id", "name"])
right = source.select(["id", "amount"])
joined = left.join(right, left["id"].eq(right["id"]), how="left", rsuffix="_right")
```

Use `agg(...)` directly on a DataFrame for a global aggregate. A sort can end a
query or precede a limit. Inner, left, right, and full joins take inputs with named
outputs. By default, overlapping names receive deterministic `L_` and `R_`
prefixes. Pass `lsuffix` or `rsuffix` to choose the output names.

### Schemas, expressions, and set operations

```python
from decimal import Decimal

from snowpark_rs import Field, Table
from snowpark_rs.functions import lit, lit_null

items = Table("ITEMS", schema=[Field("id", "int"), Field("amount", "number(10,2)")])
current = (
    items.with_column("total", (items["amount"] * Decimal("1.20")).cast("number(12,2)"))
    .rename("id", "item_id")
    .distinct()
)
archived = Table("ARCHIVED_ITEMS").select(["item_id", lit_null("number(12,2)").alias("total")])
combined = current.select(["item_id", "total"]).union_all_by_name(archived)
print(combined.columns)
```

`schema` describes the fields known to the local plan and `columns` gives their
Snowflake identifiers in order. They return `None` when the output names cannot
be determined offline. Supply a complete ordered schema at the table or select
named columns first when using `with_column` or strict name-based unions.
`schema_with_session(session)` returns the Arrow schema from a zero-row Snowflake
query, including types and nullability as reported by Snowflake.

`lit(Decimal(...))` keeps fixed-point precision, and `lit_null("number(12,2)")`
or `lit(None, datatype="number(12,2)")` creates a typed SQL null. Python
`to_sql()` exposes those bind values as `Decimal` and `None`.
`union` and `union_all` match columns by position. The `_by_name` variants
match names and accept `allow_missing_columns=True`.

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
