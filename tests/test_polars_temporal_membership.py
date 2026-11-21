from datetime import UTC, date, datetime, timedelta

import pytest

import polars as pl
from snowpark_rs import Field, Table, UnsupportedPolarsExpression

CUTOFF = date(2024, 1, 1)


@pytest.mark.parametrize(
    ("every", "bucket_sql"),
    [
        ("1d", 'DATE_TRUNC(DAY, t0."TS")'),
        ("1h", 'DATE_TRUNC(HOUR, t0."TS")'),
        ("15m", "TIME_SLICE(t0.\"TS\", 15, 'MINUTE')"),
        (timedelta(minutes=15), "TIME_SLICE(t0.\"TS\", 15, 'MINUTE')"),
    ],
)
def test_truncate_is_a_typed_group_key(every, bucket_sql):
    table = Table("T", schema=[Field("TS", "TIMESTAMP_NTZ", nullable=True)])
    query = table.group_by(pl.col("TS").dt.truncate(every).alias("BUCKET")).agg(
        pl.len()
    )
    sql, binds = query.to_sql()
    assert sql.startswith(f'SELECT {bucket_sql} AS "BUCKET", COUNT(*)')
    assert sql.endswith("GROUP BY 1")
    assert binds == []
    assert query.schema is not None
    field = query.schema[0]
    assert (field.name, field.data_type, field.nullable) == (
        "BUCKET",
        "TIMESTAMP_NTZ",
        True,
    )


def test_subday_truncate_compiles_without_a_declared_schema():
    bucket = pl.col("TS").dt.truncate("15m").alias("BUCKET")
    sql, binds = Table("T").group_by(bucket).agg(pl.len()).to_sql()
    assert 'DATE_PART(EPOCH_NANOSECOND, local_time)' in sql
    assert 'AS_TIMESTAMP_LTZ(TO_VARIANT(original))' in sql
    assert sql.endswith("GROUP BY 1")
    assert binds == []


def test_temporal_components_and_date_truncation_keep_their_types():
    table = Table("T", schema=[Field("TS", "TIMESTAMP_LTZ"), Field("D", "DATE")])
    query = table.select(
        day=pl.col("TS").dt.date(),
        minute=pl.col("TS").dt.minute(),
        bucket=pl.col("D").dt.truncate("1h"),
    )
    sql, binds = query.to_sql()
    assert 'CAST(t0."TS" AS DATE) AS "day"' in sql
    assert 'CAST(DATE_PART(MINUTE, t0."TS") AS NUMBER(2, 0)) AS "minute"' in sql
    assert 't0."D" AS "bucket"' in sql
    assert binds == []
    assert query.schema is not None
    assert [field.data_type for field in query.schema] == [
        "DATE",
        "NUMBER(2,0)",
        "DATE",
    ]
    with pytest.raises(UnsupportedPolarsExpression, match="a time or timestamp"):
        table.select(pl.col("D").dt.minute()).to_sql()


@pytest.mark.parametrize("every", ["-1w", "0w", "1w2d", "3invalid", "123456789123456789123ns"])
def test_truncate_rejects_invalid_constant_periods(every):
    query = Table("T").group_by(pl.col("TS").dt.truncate(every)).agg(pl.len())
    with pytest.raises(
        UnsupportedPolarsExpression, match=r"group_by: .*Truncate: "
    ):
        query.to_sql()


def test_truncate_combines_duration_shape_type_and_bind_order():
    source = Table("T", schema=[Field("D", "DATE", nullable=False), Field("N", "NUMBER")])
    every = pl.when(pl.col("N") > 5).then(pl.lit("2w")).otherwise(pl.lit(None, pl.String))
    query = source.group_by(pl.col("D").dt.truncate(every).alias("BUCKET")).agg(pl.len())
    sql, binds = query.to_sql()
    assert 'CASE WHEN t0."N" > ? THEN ? ELSE CAST(? AS VARCHAR) END' in sql
    assert binds == [5, "2w", None]
    assert query.schema is not None
    assert query.schema[0].data_type == "DATE"
    assert query.schema[0].nullable is True

    with pytest.raises(UnsupportedPolarsExpression, match="duration expression must produce strings"):
        source.select(pl.col("D").dt.truncate(pl.col("N"))).to_sql()


def test_polars_and_python_dates_compile_to_the_same_filter_without_a_schema():
    source = Table("PUBLIC.PUBLIC.EXAMPLE")
    column = pl.col("CREATED_AT")
    sql, binds = source.filter(column >= CUTOFF).to_sql()

    assert source.filter(column >= pl.date(2024, 1, 1)).to_sql() == (sql, binds)
    assert binds == [19723]
    assert "AS_TIMESTAMP_TZ(TO_VARIANT(" in sql
    assert "AS_TIMESTAMP_LTZ(TO_VARIANT(" in sql
    assert "AS_DATE(TO_VARIANT(" in sql
    assert (
        " >= DATE_PART(EPOCH_NANOSECOND, CAST(DATEADD(day, ?, DATE '1970-01-01')" in sql
    )


@pytest.mark.parametrize(
    ("dtype", "zone", "timestamp_sql"),
    [
        ("TIMESTAMP_NTZ", None, "TO_TIMESTAMP_NTZ"),
        ("TIMESTAMP_TZ", UTC, "TO_TIMESTAMP_TZ"),
    ],
)
def test_crossed_date_and_datetime_comparisons_use_the_same_midnight(
    dtype, zone, timestamp_sql
):
    table = Table("T", schema=[Field("TS", dtype), Field("D", "DATE")])
    instant = datetime(2024, 1, 1, 12, 30, 0, 123456, tzinfo=zone)
    sql, binds = table.select(
        pl.col("TS").eq_missing(CUTOFF).alias("same"),
        (CUTOFF < pl.col("TS")).alias("later"),
        (pl.col("D") <= instant).alias("early"),
        (pl.col("D") != pl.col("TS")).alias("different"),
    ).to_sql()

    assert 'DATE_PART(EPOCH_NANOSECOND, t0."TS") IS NOT DISTINCT FROM' in sql
    assert " > DATE_PART(EPOCH_NANOSECOND" in sql
    assert (
        'CAST(t0."D" AS TIMESTAMP_NTZ)) <= '
        f"DATE_PART(EPOCH_NANOSECOND, {timestamp_sql}(?, 6))" in sql
    )
    assert (
        'CAST(t0."D" AS TIMESTAMP_NTZ)) <> DATE_PART(EPOCH_NANOSECOND, t0."TS")' in sql
    )
    assert binds == [19723, 19723, 1704112200123456]


def test_datetime_selectors_use_the_arrow_default_units_and_time_zone():
    table = Table(
        "T",
        schema=[
            Field("CREATED_AT_TZ", "TIMESTAMP_TZ"),
            Field("CREATED_AT_LTZ", "TIMESTAMP_LTZ"),
            Field("CREATED_AT_NTZ", "TIMESTAMP_NTZ"),
            Field("CREATED_AT", "DATE"),
        ],
    )
    assert table.select(pl.selectors.datetime("ns", "UTC")).columns == [
        "CREATED_AT_TZ",
        "CREATED_AT_LTZ",
    ]
    assert table.select(pl.selectors.datetime("ns", None)).columns == ["CREATED_AT_NTZ"]
    with pytest.raises(
        UnsupportedPolarsExpression, match="matching datetime time zones"
    ):
        table.filter(pl.col("CREATED_AT_TZ") == datetime(2024, 1, 1)).to_sql()


def test_is_in_accepts_imploded_series_and_preserves_polars_null_semantics():
    values = pl.Series([1, 3, None]).implode()
    expr = pl.col("A").is_in(values)
    local = pl.LazyFrame({"A": [1, 2, 3, None]})
    assert local.filter(expr).collect()["A"].to_list() == [1, 3]
    source = Table("T")
    sql, binds = source.filter(expr).to_sql()
    assert 'WHERE t0."A" IN (' in sql
    assert binds == [1, 3]

    sql, binds = source.select(
        found=pl.col("A").is_in([1, None], nulls_equal=True)
    ).to_sql()
    assert 'COALESCE(t0."A" IN (' in sql
    assert 't0."A" IS NULL)' in sql
    assert binds == [1]

    sql, binds = source.select(found=pl.col("A").is_in([])).to_sql()
    assert 'CASE WHEN t0."A" IS NULL THEN NULL ELSE FALSE END' in sql
    assert not binds


def test_is_in_uses_a_single_column_subquery_for_both_table_forms():
    def rhs():
        return Table("IDS").filter(pl.col("ID") > 3).select("ID")

    source = Table("ITEMS").filter(pl.col("A") < 20)
    imploded = source.filter(pl.col("A").is_in(rhs().implode()) & (pl.col("A") > 5))
    direct = source.filter(pl.col("A").is_in(rhs()) & (pl.col("A") > 5))
    sql, binds = direct.to_sql()

    assert imploded.to_sql() == (sql, binds)
    assert 't1."A" IN (SELECT membership_source.* FROM (' in sql
    assert 'SELECT t0."ID" FROM "IDS" AS t0 WHERE t0."ID" > ?' in sql
    assert binds == [20, 3, 5]


def test_is_in_rejects_multiple_known_columns_and_keeps_unknown_sources_deferred():
    source = Table("ITEMS")
    invalid = source.filter(
        pl.col("A").is_in(Table("IDS").select("ID", "NAME").implode())
    )
    with pytest.raises(UnsupportedPolarsExpression, match="single-column query, got 2"):
        invalid.to_sql()

    bare = source.filter(pl.col("A").is_in(Table("IDS")))  # ty: ignore[invalid-argument-type]
    assert (
        'IN (SELECT membership_source.* FROM (SELECT t0.* FROM "IDS"'
        in bare.to_sql()[0]
    )


def test_table_is_in_reports_contexts_snowflake_cannot_compile():
    condition = pl.col("A").is_in(Table("IDS").select("ID").implode())
    with pytest.raises(UnsupportedPolarsExpression, match="positive filter"):
        Table("ITEMS").filter(condition | (pl.col("A") == 2)).to_sql()
