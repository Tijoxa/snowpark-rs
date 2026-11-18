import pytest

from snowpark_rs import DataFrame
from snowpark_rs.functions import col, count, lit, sum


@pytest.mark.parametrize(
    ("name", "parts"),
    [
        ("example", ["example"]),
        ("schema.example", ["schema", "example"]),
        ('DB.SCHEMA."quoted.""table"', ["DB", "SCHEMA", '"quoted.""table"']),
    ],
)
def test_compiles_qualified_strings_and_iterables_identically(name, parts):
    assert DataFrame.table(name=name).to_sql() == DataFrame.table(iter(parts)).to_sql()


def test_compiles_mixed_projections_and_preserves_bind_types():
    frame = DataFrame.table(("public", "public", "example"))
    query = frame.filter(col("id").eq(lit(1))).select(
        ["id", col("name"), lit(True).alias("flag"), lit("A").alias("category")]
    )

    sql, binds = query.to_sql()

    assert sql == (
        'SELECT t0."ID", t0."NAME", ? AS "FLAG", ? AS "CATEGORY"\n'
        'FROM "PUBLIC"."PUBLIC"."EXAMPLE" AS t0\n'
        'WHERE t0."ID" = ?'
    )
    assert binds == [True, "A", 1]
    assert [type(value) for value in binds] == [bool, str, int]


def test_maps_compile_and_conversion_errors():
    with pytest.raises(ValueError):
        DataFrame.table(["public.example"])
    with pytest.raises(TypeError):
        lit(1.5)  # ty: ignore[invalid-argument-type] -- exercise the runtime check
    with pytest.raises(OverflowError):
        lit(2**63)


def test_pipe_forwards_arguments_and_can_chain_transformations():
    frame = DataFrame.table(["example"])

    def filter_above(data: DataFrame, name: str, *, threshold: int) -> DataFrame:
        return data.filter(col(name).gt(lit(threshold)))

    expected = filter_above(frame, "amount", threshold=10).select(["id"]).to_sql()
    result = (
        frame.pipe(filter_above, "amount", threshold=10)
        .select(["id"])
        .pipe(DataFrame.to_sql)
    )
    assert result == expected


def test_pipe_passes_the_same_frame_and_returns_the_callback_result():
    frame = DataFrame.table(["example"])
    marker = object()

    def inspect(data: DataFrame) -> object:
        assert data is frame
        return marker

    assert frame.pipe(function=inspect) is marker


def test_pipe_propagates_callback_exceptions():
    frame = DataFrame.table(["example"])
    error = ValueError("callback failed")

    def fail(_: DataFrame) -> None:
        raise error

    with pytest.raises(ValueError) as raised:
        frame.pipe(fail)

    assert raised.value is error


def test_relational_aggregation_through_python():
    frame = DataFrame.table("example").filter(col("id").gt(lit(5)))

    sql, binds = (
        frame.group_by(iter(["category"]))
        .agg([count("*").alias("total_rows"), count("amount").alias("present"), sum("amount").alias("total")])
        .sort("category")
        .limit(n=2)
        .to_sql()
    )

    assert 'COUNT(*) AS "TOTAL_ROWS", COUNT(t0."AMOUNT") AS "PRESENT"' in sql
    assert 'SUM(t0."AMOUNT") AS "TOTAL"' in sql
    assert sql.endswith('GROUP BY 1) AS t1\nORDER BY t1."CATEGORY" ASC NULLS LAST\nLIMIT 2')
    assert binds == [5]
    assert frame.agg(("amount", "sum")).to_sql() == frame.group_by().agg({"amount": "SUM"}).to_sql()


def test_relational_sort_directions_and_validation():
    frame = DataFrame.table("example")

    sql, _ = frame.sort("amount", "id", ascending=[0, True]).limit(3, offset=1).to_sql()
    assert sql.endswith('ORDER BY t0."AMOUNT" DESC NULLS FIRST, t0."ID" ASC NULLS LAST\nLIMIT 3 OFFSET 1')
    assert frame.sort(col("amount").asc_nulls_first()).to_sql()[0].endswith(
        't0."AMOUNT" ASC NULLS FIRST'
    )
    assert frame.sort().to_sql()[0].endswith("ORDER BY ALL")
    with pytest.raises(ValueError, match="one value per sort key"):
        frame.sort("id", "amount", ascending=[False])
    with pytest.raises(ValueError, match="intermediate sort"):
        frame.sort("id").filter(col("id").is_not_null()).to_sql()


def test_relational_join_uses_exported_columns_and_ordered_binds():
    frame = DataFrame.table("example")
    left = frame.filter(col("id").gt(lit(1))).select(["id"])
    right = frame.filter(col("id").lt(lit(7))).select(["id"])
    predicate = left["id"].eq(right.col("id")) & right["id"].ne(lit(3))

    sql, binds = left.join(right, on=predicate, rsuffix="_right").select(
        [lit(True).alias("flag"), "id", "id_right"]
    ).to_sql()

    assert 'ON (t1."ID" = t3."ID") AND (t3."ID" <> ?)' in sql
    assert sql.startswith('SELECT ? AS "FLAG", t4."ID", t4."ID_RIGHT"')
    assert binds == [True, 1, 7, 3]
    with pytest.raises(ValueError, match="ambiguous"):
        left.join(right, col("id").eq(right["id"])).to_sql()
