# snowpark-rs

`snowpark-rs` is a Rust library and a Python package built with Maturin. The
`snowflake-ast` crate owns the query plans and SQL compiler.
The optional `session` feature uses `snowflake-connector-rs` to execute them.

## Python

Install with Python 3.13 or 3.14 and Rust 1.95 or newer.

```sh
uv sync --dev
```

```python
import polars as pl
import snowpark_rs as pls

query = (
    pls.Table("PUBLIC.PUBLIC.SALES")
    .filter(pl.col("AMOUNT") > 100, REGION="EU")
    .select("REGION", DOUBLE_AMOUNT=pl.col("AMOUNT") * 2)
)
sql, binds = query.to_sql()
assert binds == [2, 100, "EU"]
```

`Table` builds a lazy Snowflake query. `select`, `filter`, `with_columns`,
`group_by().agg()` and `sort` take Polars expressions. Pass a column name as
a string or use `pl.lit("value")` for a string value in a projection. Both
`to_sql()` and the execution methods translate the expressions before
sending any SQL. An unsupported expression raises
`snowpark_rs.UnsupportedPolarsExpression` with the operation and reason.

### Group, sort, and join

```python
source = pls.Table("PUBLIC.PUBLIC.SALES")
totals = (
    source.group_by("CATEGORY")
    .agg(rows=pl.len(), total=pl.col("AMOUNT").sum())
    .sort("total", descending=True, nulls_last=True)
    .limit(10)
)

left = source.select("ID", "NAME")
right = source.select("ID", "AMOUNT")
joined = left.join(right, on="ID", how="left")
```

The compiler supports column references, scalar literals, aliases, comparisons,
null tests, Boolean logic, numeric arithmetic, a subset of strict casts,
`sum()`, `count()` and `pl.len()`. `sum()` returns zero for an empty or
all-null input. Polars-style null placement is explicit in `ORDER BY`.
True division requires a nonzero literal divisor. Windows, list reductions,
and broadcasting aggregates over source rows produce an error.
A sort can end a query or precede a limit.

### Dates, datetimes, and membership

```python
from datetime import date

source = pls.Table("PUBLIC.PUBLIC.EXAMPLE")
query = source.filter(pl.col("CREATED_AT") >= pl.date(2024, 1, 1))
same = source.filter(pl.col("CREATED_AT") >= date(2024, 1, 1))

ids = source.filter(pl.col("CATEGORY") == "A").select("ID")
matching = source.filter(pl.col("ID").is_in(ids))
matching_imploded = source.filter(pl.col("ID").is_in(ids.implode()))
```

The temporal comparisons also accept `datetime.datetime`. Date-to-datetime
comparisons use midnight, in UTC for timestamps with a time zone. Declare
the source schema to check the compatibility of comparisons between columns
offline. Constant `pl.date` and `pl.datetime` constructors are supported.

`is_in` accepts a literal list, an imploded Polars Series, or a Snowflake query
with exactly one column. Snowflake query operands are supported in positive
filters, including conjunctions. A multi-column query raises an error during
compilation when its schema is known, or during Snowflake execution otherwise.

### Schemas, selectors, and replacements

```python
items = pls.Table("ITEMS", schema=[
    pls.Field("ID", "number(10,0)"),
    pls.Field("AMOUNT", "number(12,2)"),
])
current = (
    items.with_columns(total=pl.col("AMOUNT") * 2)
    .rename({"ID": "item_id"})
    .unique()
)
print(current.columns)
```

Column names and aliases use exact case. For a table created with unquoted
Snowflake identifiers, use names such as `pl.col("AMOUNT")`. If an input
schema is declared, use SQL identifiers for the `Field` constructors
(`Field('"mixedCase"')` for a quoted name). `schema` and `columns` describe
the outputs known to the local compiler. Supply a complete schema or select
named columns first for `with_columns`, name-based selectors that require the
schema, and joins. Dtype selectors also need the corresponding field types.

`union` and `union_all` match columns by position. The `_by_name` variants
match names and accept `allow_missing_columns=True`. `pipe` forwards the
query to a Python callable.

### Snowflake expressions

```python
payloads = pls.Table("PAYLOADS")
query = payloads.select(
    parsed=pls.col("PAYLOAD").snowflake.try_parse_json(),
    invalid=pls.try_parse_json("PAYLOAD").is_null(),
)
```

Both forms produce a genuine `pl.Expr`. The native plugin reports a Polars
`ComputeError` if evaluated locally. A Snowflake query compiles it to
`TRY_PARSE_JSON`, which returns a `VARIANT` or SQL `NULL` for invalid JSON.
The package pins Polars 2.1.0 and uses that release's 32-bit runtime.
`pyo3-polars` imports expressions at compilation time for the shared Rust
translator. The expression format and plugin ABI are tied to that release.

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
columns. `TIMESTAMP_TZ` is exported as Arrow `timestamp[ns, tz=UTC]` and
Polars `datetime[ns, UTC]`. `TIMESTAMP_LTZ` is also exported in UTC.
`TIMESTAMP_NTZ` remains timezone-naive. The Arrow units of `LTZ` and `NTZ`
follow their Snowflake precision, with nanoseconds at the default precision.
Semi-structured values are represented as JSON text.

### Convert results to Polars

The `session` extra enables `to_polars` through Arrow. The result uses
Snowflake's Arrow types, including decimal columns for Snowflake `NUMBER`
and JSON text for `VARIANT`.

```python
data = query.to_polars(session)
for batch in query.to_polars(session, use_batches=True):
    print(batch.height)
```

## Polars expressions in Rust

Enable the optional Cargo feature `polars` and import the expression
constructors from `snowpark_rs::polars`. The query retains the typed expressions
until SQL compilation, schema resolution, or execution.

```rust
use snowpark_rs::{DataFrame, polars::{col, lit}};

let query = DataFrame::table_name("DB.PUBLIC.SALES")?
    .filter_polars(col("AMOUNT").gt(lit(100)))
    .select_polars([(col("AMOUNT") * lit(2)).alias("double")]);
let compiled = query.to_sql()?;
```

`with_columns_polars`, `group_by_polars().agg()`, `agg_polars` and
`sort_polars` also accept typed expressions. Polars column names preserve case.

## Rust tests

```sh
cargo test --locked --no-default-features
cargo test --locked -p snowpark-rs --features polars
cargo test --locked -p snowpark-rs --features session
```

The default workspace members run SQL compilation tests in `snowflake-ast`
and `snowpark-rs`. Run the live integration tests with a configured Snowflake
profile and the `PUBLIC.PUBLIC.EXAMPLE` fixture.

```sh
cargo test --locked -p snowpark-rs --features session --test snowflake_smoke -- --ignored
```

The Python parity test uses a Snowflake test profile named by
`SNOWPARK_RS_TEST_CONNECTION`. It reads `PUBLIC.PUBLIC.EXAMPLE` by default.
Set `SNOWPARK_RS_TEST_TABLE` to use another fixture with `CATEGORY` and
`AMOUNT` columns.

```sh
uv run --locked --extra session pytest tests/test_polars_snowflake.py
```
