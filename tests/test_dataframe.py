from decimal import Decimal

import pytest

from snowpark_rs import DataFrame, Field, Table
from snowpark_rs.functions import col, count, lit, lit_null, sum


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


def test_filter_accepts_a_string_literal_in_both_equality_forms():
    frame = Table("PUBLIC.PUBLIC.EXAMPLE")
    name = col("NAME")
    expected = ('SELECT t0.* FROM "PUBLIC"."PUBLIC"."EXAMPLE" AS t0 WHERE t0."NAME" = ?', ["Alice"])

    assert frame.filter(name == "Alice").to_sql() == expected
    assert frame.filter(name.eq("Alice")).to_sql() == expected


def test_to_sql_preserves_nested_binds_and_quoted_identifiers():
    frame = (
        Table('db."weird.schema"."table?"')
        .filter(col('"id?"').eq(lit("A?")))
        .select([lit(21).alias('"v?""alue"')])
        .filter(col('"v?""alue"').gt(lit(5)))
    )

    sql, binds = frame.to_sql()

    assert "\n" not in sql
    assert binds == [21, "A?", 5]
    assert 'AS "v?""alue"' in sql
    assert '"id?" = ?' in sql
    assert '"DB"."weird.schema"."table?"' in sql
    assert 't1."v?""alue" > ?' in sql


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


def test_relational_aggregation_through_python():
    frame = Table("example").filter(col("id").gt(lit(5)))

    query = (
        frame.group_by(iter(["category"]))
        .agg([count("*").alias("total_rows"), count("amount").alias("present"), sum("amount").alias("total")])
        .sort("category")
        .limit(n=2)
    )
    sql, binds = query.to_sql()

    assert 'COUNT(*) AS "TOTAL_ROWS", COUNT(t0."AMOUNT") AS "PRESENT"' in sql
    assert 'SUM(t0."AMOUNT") AS "TOTAL"' in sql
    assert sql.endswith('GROUP BY 1) AS t1 ORDER BY t1."CATEGORY" ASC NULLS LAST LIMIT 2')
    assert binds == [5]
    assert frame.agg(("amount", "sum")).to_sql() == frame.group_by().agg({"amount": "SUM"}).to_sql()


def test_relational_sort_directions_and_validation():
    frame = Table("example")

    sql, _ = frame.sort("amount", "id", ascending=[0, True]).limit(3, offset=1).to_sql()
    assert sql.endswith('ORDER BY t0."AMOUNT" DESC NULLS FIRST, t0."ID" ASC NULLS LAST LIMIT 3 OFFSET 1')
    assert frame.sort(col("amount").asc_nulls_first()).to_sql()[0].endswith(
        't0."AMOUNT" ASC NULLS FIRST'
    )
    assert frame.sort().to_sql()[0].endswith("ORDER BY ALL")
    with pytest.raises(ValueError, match="one value per sort key"):
        frame.sort("id", "amount", ascending=[False])


def test_relational_join_uses_exported_columns_and_ordered_binds():
    frame = Table("example")
    left = frame.filter(col("id").gt(lit(1))).select(["id"])
    right = frame.filter(col("id").lt(lit(7))).select(["id"])
    predicate = left["id"].eq(right.col("id")) & right["id"].ne(lit(3))

    sql, binds = left.join(right, on=predicate, rsuffix="_right").select(
        [lit(True).alias("flag"), "id", "id_right"]
    ).to_sql()

    assert 'ON (t1."ID" = t3."ID") AND (t3."ID" <> ?)' in sql
    assert sql.startswith('SELECT ? AS "FLAG", t4."ID", t4."ID_RIGHT"')
    assert binds == [True, 1, 7, 3]


def test_decimal_cast_null_and_reflected_arithmetic():
    frame = Table("example")
    amount = Decimal("1.2300")
    expression = (amount + frame["amount"] * 2 - 3 / frame["amount"]) % 4
    query = frame.select([
        expression.cast(" DECIMAL(12,4) ").alias("total"),
        lit(None, datatype="BOOLEAN").alias("missing"),
        lit_null("DATE").alias("day"),
        (2 ** -frame["amount"]).alias("power"),
    ])

    sql, binds = query.to_sql()
    assert sql.startswith(
        'SELECT CAST(((CAST(? AS NUMBER(5,4)) + (t0."AMOUNT" * ?)) - (? / t0."AMOUNT"))'
        ' % ? AS NUMBER(12,4)) AS "TOTAL"'
    )
    assert 'CAST(? AS BOOLEAN) AS "MISSING", CAST(? AS DATE) AS "DAY"' in sql
    assert 'POWER(?, -(t0."AMOUNT"))' in sql
    assert binds == [amount, 2, 3, 4, None, None, 2]
    assert type(binds[0]) is Decimal
    assert binds[0].as_tuple() == amount.as_tuple()
    assert query.schema is not None
    assert [(field.data_type, field.nullable) for field in query.schema][:3] == [
        ("NUMBER(12,4)", None), ("BOOLEAN", True), ("DATE", True)
    ]
    with pytest.raises(ValueError, match="finite decimal"):
        lit(Decimal("Infinity"))
    with pytest.raises(ValueError, match="data type"):
        col("id").cast("number(3,4)")


def test_with_column_rename_and_distinct_use_local_output_names():
    source = Table("example", schema=[Field("id"), Field('"Tenant.ID"', "varchar", False)])
    assert source.columns == ["ID", '"Tenant.ID"']
    added = source.with_column("n", source["id"] + 1)
    assert added.columns == ["ID", '"Tenant.ID"', "N"]
    replaced = source.with_column("id", lit_null("integer"))
    assert replaced.columns == ['"Tenant.ID"', "ID"]
    assert source.with_column("id", lit(1), keep_column_order=True).columns == source.columns
    renamed = replaced.rename({replaced["id"]: '"Id"'}).distinct()
    assert renamed.columns == ['"Tenant.ID"', '"Id"']
    assert renamed.to_sql()[0].startswith("SELECT DISTINCT")
    assert renamed.to_sql()[1] == [None]
    with pytest.raises(ValueError, match="duplicate"):
        source.rename("id", '"Tenant.ID"').to_sql()

    bare = Table("example")
    assert bare.schema is None
    assert 'RENAME ("ID" AS "KEY")' in bare.rename("id", "key").to_sql()[0]
    with pytest.raises(ValueError, match="known column names"):
        bare.with_column("id", lit(1)).to_sql()


def test_outer_join_names_map_to_sql():
    left = Table("left_table").select(["id"])
    right = Table("right_table").select(["id"])
    on = left["id"].eq(right["id"])
    for how, keyword in [
        ("left", "LEFT"), ("rightouter", "RIGHT"), ("full_outer", "FULL")
    ]:
        sql, _ = left.join(right, on=on, how=how).to_sql()
        assert f" {keyword} OUTER JOIN " in sql


def test_python_exposes_all_union_variants():
    left = Table("left_table").select(["id"])
    right = Table("right_table").select(["id"])
    for operation, keyword in [
        (left.union, "UNION"),
        (left.union_all, "UNION ALL"),
        (left.union_by_name, "UNION BY NAME"),
        (left.union_all_by_name, "UNION ALL BY NAME"),
    ]:
        statement, _ = operation(other=right).to_sql()
        assert f") {keyword} (SELECT" in statement
    missing = left.union_by_name(right.rename("id", "other"), allow_missing_columns=True)
    assert missing.columns == ["ID", "OTHER"]
