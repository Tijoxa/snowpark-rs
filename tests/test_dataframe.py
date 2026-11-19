from decimal import Decimal

import pytest

import polars as pl
import snowpark_rs as pls
from snowpark_rs import Field, Table, UnsupportedPolarsExpression


@pytest.mark.parametrize(
    ("name", "parts"),
    [
        ("SALES", ["SALES"]),
        ("PUBLIC.SALES", ["PUBLIC", "SALES"]),
        ("DB.PUBLIC.SALES", ["DB", "PUBLIC", "SALES"]),
        ('DB.PUBLIC."Table.With.Dots"', ["DB", "PUBLIC", '"Table.With.Dots"']),
    ],
)
def test_table_name_forms_compile_equivalently(name, parts):
    assert Table(name).to_sql() == Table(iter(parts)).to_sql()


def test_positional_named_and_iterable_expressions_compile_with_ordered_binds():
    frame = Table("DB.PUBLIC.SALES").filter(
        pl.col("AMOUNT") > 100,
        pl.col("ACTIVE").eq_missing(True),
        REGION="O'Neil",
    )
    query = frame.select(
        "REGION",
        (pl.col("AMOUNT") * 2).alias("Double Amount"),
        exact=pl.lit(Decimal("1.20")),
    )
    sql, binds = query.to_sql()
    assert query.columns == ["REGION", "Double Amount", "exact"]
    assert sql == (
        'SELECT t0."REGION", t0."AMOUNT" * ? AS "Double Amount", CAST(? AS NUMBER(3,2)) AS "exact" '
        'FROM "DB"."PUBLIC"."SALES" AS t0 WHERE ((t0."AMOUNT" > ?) AND '
        '(t0."ACTIVE" IS NOT DISTINCT FROM ?)) AND (t0."REGION" = ?)'
    )
    assert binds == [2, Decimal("1.20"), 100, True, "O'Neil"]
    assert type(binds[1]) is Decimal
    assert frame.select(["REGION"]).to_sql() == frame.select("REGION").to_sql()


def test_nested_projections_resolve_exact_aliases_and_keep_sql_binds_in_order():
    source = Table("EXAMPLE").filter(pl.col("LIMIT") > 14)
    base = source.select("LIMIT", (pl.col("LIMIT") + 5).alias('A"name\nB'))
    query = base.filter(pl.col('A"name\nB') < 9).select(pl.col('A"name\nB') * 2)
    sql, binds = query.to_sql()
    assert 'AS "A""name\nB"' in sql
    assert binds == [2, 5, 14, 9]
    with pytest.raises(UnsupportedPolarsExpression, match="filter: Column.*absent"):
        base.filter(pl.col('A"NAME\nB') > 1).to_sql()


def test_typed_expression_bridge_matches_rust_without_json(monkeypatch):
    def reject_json(*_args, **_kwargs):
        raise AssertionError("the typed bridge must not serialize expressions as JSON")

    monkeypatch.setattr(type(pl.col("AMOUNT").meta), "serialize", reject_json)
    sales = Table("SALES")
    query = (
        sales.filter(pl.col("AMOUNT") > 100)
        .select((pl.col("AMOUNT") * 2).alias("double"))
        .sort("double", descending=True, nulls_last=True)
        .limit(5)
    )
    assert query.columns == ["double"]
    assert query.to_sql() == (
        (
            'SELECT t1.* FROM (SELECT t0."AMOUNT" * ? AS "double" FROM "SALES" AS t0 '
            'WHERE t0."AMOUNT" > ?) AS t1 ORDER BY t1."double" DESC NULLS LAST LIMIT 5'
        ),
        [2, 100],
    )
    invalid = sales.select((pl.col("AMOUNT") // 2) + 1).limit(5)
    with pytest.raises(UnsupportedPolarsExpression) as error:
        _ = invalid.schema
    assert (
        str(error.value)
        == "select: BinaryExpr.FloorDivide: no translation to Snowflake SQL"
    )


def test_scalar_selection_has_one_row_and_typed_binds_even_after_empty_input():
    query = (
        Table("EXAMPLE")
        .limit(0)
        .select(
            amount=pl.lit(Decimal("2.05")),
            flag=pl.lit(None, pl.Boolean),
            rate=pl.lit(2.5),
        )
    )
    sql, binds = query.to_sql()
    assert sql == (
        'SELECT CAST(? AS NUMBER(3,2)) AS "amount", CAST(? AS BOOLEAN) AS "flag", ? AS "rate" '
        "FROM (SELECT 1) AS t1"
    )
    assert binds == [Decimal("2.05"), None, 2.5]
    schema = query.schema
    assert schema is not None
    assert [field.data_type for field in schema] == [
        "NUMBER(3,2)",
        "BOOLEAN",
        "FLOAT",
    ]


def test_with_columns_updates_all_expressions_against_the_same_input():
    frame = Table("T", schema=[Field("X", "int"), Field("Y", "int")])
    query = frame.with_columns(X=pl.col("Y") + 2, Y=pl.col("X") - 1, flag=pl.lit(True))
    sql, binds = query.to_sql()
    assert query.columns == ["X", "Y", "flag"]
    assert (
        sql == 'SELECT t0."Y" + ? AS "X", t0."X" - ? AS "Y", ? AS "flag" FROM "T" AS t0'
    )
    assert binds == [2, 1, True]
    assert query.rename({"X": "Y", "Y": "X"}).unique().columns == ["Y", "X", "flag"]


def test_selectors_expand_with_the_schema_in_column_order():
    source = Table(
        "T",
        schema=[Field("X", "number(7,2)"), Field("Y", "varchar"), Field("Z", "float")],
    )
    query = source.select(pl.selectors.numeric() + 2, pl.selectors.string())
    assert query.columns == ["X", "Z", "Y"]
    assert query.to_sql() == (
        'SELECT t0."X" + ? AS "X", t0."Z" + ? AS "Z", t0."Y" FROM "T" AS t0',
        [2, 2],
    )
    assert Table("T").select(pl.all()).to_sql() == Table("T").to_sql()
    assert Table("T").select(pl.col("X", "Y")).columns == ["X", "Y"]
    unknown_schema = Table("T").select(pl.selectors.numeric())
    with pytest.raises(UnsupportedPolarsExpression, match="select: Selector.*schema"):
        unknown_schema.to_sql()
    sorted_sql, _ = source.sort(pl.selectors.numeric(), descending=[True]).to_sql()
    assert sorted_sql.endswith('t0."X" DESC NULLS FIRST, t0."Z" DESC NULLS FIRST')
    with pytest.raises(
        UnsupportedPolarsExpression, match="filter: Selector.*exactly one"
    ):
        source.filter(pl.selectors.numeric() > 1).to_sql()


def test_grouped_and_global_reductions_preserve_scalar_composition_and_sum_empty_behavior():
    frame = Table("T").filter(pl.col("X") > 1)
    query = frame.group_by(category="K").agg(
        total=pl.col("X").sum() + 2, rows=pl.len(), count=pl.col("X").count()
    )
    sql, binds = query.to_sql()
    assert sql == (
        'SELECT t0."K" AS "category", COALESCE(SUM(t0."X"), 0) + ? AS "total", COUNT(*) AS "rows", '
        'COUNT(t0."X") AS "count" FROM "T" AS t0 WHERE t0."X" > ? GROUP BY 1'
    )
    assert binds == [2, 1]
    assert query.columns == ["category", "total", "rows", "count"]
    assert Table("T").select(pl.col("X").sum()).to_sql() == (
        'SELECT COALESCE(SUM(t0."X"), 0) AS "X" FROM "T" AS t0',
        [],
    )
    assert frame.group_by("K").agg(flag=pl.lit(True)).to_sql() == (
        'SELECT t0."K", ? AS "flag" FROM "T" AS t0 WHERE t0."X" > ? GROUP BY 1',
        [True, 1],
    )
    scalar_sql, scalar_binds = frame.select(total=pl.lit(5).sum()).to_sql()
    assert scalar_sql.startswith(
        'SELECT COALESCE(?, CAST(? AS NUMBER(38,0))) AS "total"'
    )
    assert scalar_sql.endswith("FROM (SELECT 1) AS t1")
    assert scalar_binds == [5, 0]


def test_sort_cast_and_arithmetic_emit_explicit_null_order_and_types():
    frame = Table("T", schema=[Field("X", "int"), Field("Y", "int")])
    query = (
        frame.select(
            pl.col("X").cast(pl.Int64), Y=pl.col("Y") / 2, remainder=pl.col("X") % 3
        )
        .sort(["X", "Y"], descending=[True, False], nulls_last=[False, True])
        .limit(3, offset=2)
    )
    sql, binds = query.to_sql()
    assert 'CAST(t0."X" AS NUMBER(19,0))' in sql
    assert 'CAST(t0."Y" AS FLOAT) / ?' in sql
    assert '((t0."X" % ?) + ?) % ?' in sql
    assert sql.endswith(
        'ORDER BY t1."X" DESC NULLS FIRST, t1."Y" ASC NULLS LAST LIMIT 3 OFFSET 2'
    )
    assert binds == [2, 3, 3, 3]


@pytest.mark.parametrize(
    ("expression", "diagnostic"),
    [
        (pl.col("X").rolling_sum(2) + 1, "Function.RollingExpr"),
        (pl.col("X") // 2, "BinaryExpr.FloorDivide"),
        (pl.col("X") / pl.col("Y"), "nonzero numeric literal divisor"),
        (pl.col("X").sum() + pl.col("X"), "broadcasting"),
        (pl.col("X").cast(pl.Int64, strict=False), "only strict casts"),
        (pl.lit(float("nan")), "representable as a Snowflake bind"),
    ],
)
def test_unsupported_expressions_fail_when_compiled(expression, diagnostic):
    frame = Table("T").select(expression)
    with pytest.raises(UnsupportedPolarsExpression, match=rf"^select: .*{diagnostic}"):
        frame.to_sql()


def test_snowflake_json_plugin_composes_and_errors_in_local_polars():
    assert pls.col is pl.col
    frame = Table("T", schema=[Field("PAYLOAD", "varchar")])
    namespace = pls.col("PAYLOAD").snowflake.try_parse_json()
    shortcut = pls.try_parse_json("PAYLOAD")
    assert isinstance(namespace, pl.Expr)
    query = frame.select(parsed=namespace, invalid=shortcut.is_null())
    assert query.to_sql() == (
        (
            'SELECT TRY_PARSE_JSON(t0."PAYLOAD") AS "parsed", '
            'TRY_PARSE_JSON(t0."PAYLOAD") IS NULL AS "invalid" FROM "T" AS t0'
        ),
        [],
    )
    assert frame.select(parsed=pls.try_parse_json(pl.lit('{"x":1}'))).to_sql()[1] == [
        '{"x":1}'
    ]
    schema = query.schema
    assert schema is not None
    assert schema[0].data_type == "VARIANT"
    assert schema[1].nullable is False
    with pytest.raises(
        pl.exceptions.ComputeError, match=r"requires a snowpark_rs\.Table"
    ):
        pl.DataFrame({"PAYLOAD": ['{"x":1}']}).select(namespace)


def test_compiler_rejects_plugins_from_other_libraries(tmp_path):
    from polars.plugins import register_plugin_function

    other = tmp_path / "other.dll"
    other.touch()
    expr = register_plugin_function(
        plugin_path=other,
        function_name="try_parse_json",
        args="PAYLOAD",
        use_abs_path=True,
    )
    with pytest.raises(UnsupportedPolarsExpression, match="Function.FfiPlugin"):
        Table("T").select(expr).to_sql()


def test_join_uses_polars_key_and_suffix_conventions_and_preserves_bind_order():
    source = Table("T")
    left = source.filter(pl.col("ID") > 0).select("ID", pl.lit("L").alias("name"))
    right = source.filter(pl.col("ID") < 10).select("ID", pl.lit("R").alias("name"))
    join = left.join(right, on="ID", how="left", nulls_equal=True)
    assert join.columns == ["ID", "name", "name_right"]
    sql, binds = join.to_sql()
    assert "LEFT OUTER JOIN" in sql
    assert "IS NOT DISTINCT FROM" in sql
    assert binds == ["L", 0, "R", 10]
    assert left.join(right, on="ID", how="full").columns == [
        "ID",
        "name",
        "ID_right",
        "name_right",
    ]


def test_union_uses_migrated_branches_and_validates_names():
    left = Table("T1").select("X", label=pl.lit("left"))
    right = Table("T2").select("X", category=pl.lit("right"))
    sql, binds = left.union_all_by_name(right, allow_missing_columns=True).to_sql()
    assert "UNION ALL" in sql
    assert binds == ["left", "right"]
    assert left.union(right).columns == ["X", "label"]
    with pytest.raises(ValueError, match="different column names"):
        left.union_by_name(right).to_sql()


def test_pipe_forwards_arguments_and_callback_errors():
    source = Table("T")
    query = source.pipe(
        lambda frame, column, *, n: frame.select(column).limit(n), "X", n=3
    )
    assert query.columns == ["X"]
    assert query.to_sql()[0].endswith("LIMIT 3")
    assert source.pipe(lambda frame: frame) is source

    def broken(_frame):
        raise ValueError("failed")

    with pytest.raises(ValueError, match="failed"):
        source.pipe(broken)
