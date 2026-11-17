import pytest

from snowpark_rs import DataFrame
from snowpark_rs.functions import col, lit


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
