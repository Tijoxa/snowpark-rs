import pytest

from snowpark_rs import DataFrame, Table
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
    table = Table(name=name)
    assert isinstance(table, DataFrame)
    assert table.to_sql() == Table(iter(parts)).to_sql()


def test_compiles_mixed_projections_and_preserves_bind_types():
    frame = Table(("public", "public", "example"))
    query = frame.filter(col("id").eq(lit(1))).select(
        ["id", col("name"), lit(True).alias("flag"), lit("A").alias("category")]
    )

    sql, binds = query.to_sql()

    assert sql == (
        'SELECT t0."ID", t0."NAME", ? AS "FLAG", ? AS "CATEGORY" '
        'FROM "PUBLIC"."PUBLIC"."EXAMPLE" AS t0 '
        'WHERE t0."ID" = ?'
    )
    assert binds == [True, "A", 1]
    assert [type(value) for value in binds] == [bool, str, int]


def test_prettify_preserves_nested_binds_and_quoted_identifiers():
    frame = (
        Table('db."weird.schema"."table?"')
        .filter(col('"id?"').eq(lit("A?")))
        .select([lit(21).alias('"v?""alue"')])
        .filter(col('"v?""alue"').gt(lit(5)))
    )

    compact, binds = frame.to_sql()
    pretty, pretty_binds = frame.to_sql(prettify=True)

    assert frame.to_sql(prettify=False) == (compact, binds)
    assert "\n" not in compact
    assert "\n" in pretty
    assert any(line.startswith("  ") for line in pretty.splitlines())
    assert binds == pretty_binds == [21, "A?", 5]
    assert 'AS "v?""alue"' in pretty and '"id?" = ?' in pretty
    assert '"DB"."weird.schema"."table?"' in pretty
    assert 't1."v?""alue" > ?' in pretty
    with pytest.raises(TypeError):
        frame.to_sql(True)  # ty: ignore[too-many-positional-arguments]


def test_one_line_sql_keeps_newlines_inside_quoted_identifiers():
    table = Table(['"table\nname"']).select(['"field\nname"'])

    compact, binds = table.to_sql()

    assert compact == 'SELECT t0."field\nname" FROM "table\nname" AS t0'
    assert binds == []


def test_maps_compile_and_conversion_errors():
    with pytest.raises(ValueError):
        Table(["public.example"])
    with pytest.raises(TypeError):
        lit(1.5)  # ty: ignore[invalid-argument-type] -- exercise the runtime check
    with pytest.raises(OverflowError):
        lit(2**63)


def test_pipe_forwards_arguments_and_can_chain_transformations():
    frame = Table(["example"])

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
    frame = Table(["example"])
    marker = object()

    def inspect(data: DataFrame) -> object:
        assert data is frame
        return marker

    assert frame.pipe(function=inspect) is marker


def test_pipe_propagates_callback_exceptions():
    frame = Table(["example"])
    error = ValueError("callback failed")

    def fail(_: DataFrame) -> None:
        raise error

    with pytest.raises(ValueError) as raised:
        frame.pipe(fail)

    assert raised.value is error
