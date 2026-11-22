"""Read-only parity test with a configured Snowflake test profile and fixture."""

import os
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from polars.testing import assert_frame_equal

import polars as pl
import snowpark_rs as pls
from snowpark_rs._session import to_arrow


@pytest.fixture
def snowflake_session():
    profile = os.environ.get("SNOWPARK_RS_TEST_CONNECTION", "")
    if not profile:
        pytest.skip("set SNOWPARK_RS_TEST_CONNECTION to a Snowflake test profile")
    return pls.Session.builder.config("connection_name", profile).create()


def complex_expr(nb_hours: int, period: int | pl.Expr | None = None) -> pl.Expr:
    if period is None:
        period_expr = pl.when((pl.col("TS").dt.minute() != 0).any()).then(15).otherwise(60)
    elif isinstance(period, int):
        if period <= 0:
            raise ValueError("Period must be positive")
        period_expr = pl.lit(period)
    else:
        period_expr = period

    n_elem = 60 // period_expr
    n_elem = pl.when(n_elem.is_in((1, 4))).then(n_elem).otherwise(None)
    k = (nb_hours * n_elem).floor().cast(pl.Int64)

    sym = pl.col("VALUE").drop_nulls().sort()
    up = pl.col("VALUE").filter(pl.col("DIRECTION") == "UP").drop_nulls().sort()
    down = pl.col("VALUE").filter(pl.col("DIRECTION") == "DOWN").drop_nulls().sort()

    sym_spread = (
        pl.when((k > 0) & (sym.count() >= k))
        .then(sym.tail(k).mean() - sym.head(k).mean())
        .otherwise(None)
    )
    k_asym = pl.min_horizontal(k.fill_null(0), up.count(), down.count())
    asym_spread = (
        pl.when(k_asym > 0)
        .then(up.tail(k_asym).mean() - down.head(k_asym).mean())
        .otherwise(None)
    )
    return (
        pl.when(pl.col("DIRECTION").eq("SYMMETRIC").all())
        .then(sym_spread)
        .otherwise(asym_spread)
        .alias("VALUE")
    )


def _spread_keys():
    return pl.col("TS").dt.truncate("1d").alias("DAY"), "MARKET", "ZONE"


def _group_period():
    return pl.when(pl.col("TS").dt.minute().max() > 0).then(15).otherwise(60)


@pytest.mark.parametrize("period", [None, 60, _group_period()], ids=["inferred", "fixed", "group"])
def test_complex_expr_compiles_for_grouped_and_global_queries(period):
    source = pls.Table("DB_CH_FC.FC.PRICE")
    expr = complex_expr(4, period)
    grouped = source.group_by(*_spread_keys()).agg(expr)
    global_ = source.select(expr)

    for query in (grouped, global_):
        sql, binds = query.to_sql()
        assert sql.count("ROW_NUMBER() OVER") == 4
        assert sql.count("AVG(") == 4
        assert "BOOLAND_AGG(" in sql
        assert "LEAST_IGNORE_NULLS(" in sql
        assert sql.count("?") == len(binds)
        schema = query.schema
        assert schema is not None
        assert schema[-1].data_type == "FLOAT"
    assert "PARTITION BY " in grouped.to_sql()[0]
    assert "GROUP BY 1, 2, 3" in grouped.to_sql()[0]
    assert "PARTITION BY " not in global_.to_sql()[0]


def _price_fixture():
    zone = ZoneInfo("Europe/Paris")
    spring = datetime(2025, 3, 30, tzinfo=zone).astimezone(UTC)
    autumn = datetime(2025, 10, 26, tzinfo=zone).astimezone(UTC)
    rows: list[tuple[datetime | None, str, str | None, float | None, str | None]] = [
        (spring + timedelta(hours=i), "DA", "FR", float(i + 1), "SYMMETRIC")
        for i in range(23)
    ]
    rows += [
        (autumn + timedelta(minutes=15 * i), "DA", "FR", float(i + 1), "SYMMETRIC")
        for i in range(100)
    ]
    rows += [
        (spring + timedelta(hours=i), "SHORT", "FR", value, direction)
        for i, (value, direction) in enumerate(
            [(None, "SYMMETRIC"), (1.0, None), (2.0, "SYMMETRIC"), (3.0, "SYMMETRIC")]
        )
    ]
    rows += [(None, "NULL_TS", None, float(i), "SYMMETRIC") for i in range(1, 7)]
    rows += [(spring, "NULL_DIR", "FR", float(i), None) for i in range(1, 7)]
    rows += [
        (spring + timedelta(hours=i), market, "FR", value, direction)
        for market, prices in (
            (
                "ASYM",
                [
                    (10.0, "UP"),
                    (14.0, "UP"),
                    (18.0, "UP"),
                    (2.0, "DOWN"),
                    (6.0, "DOWN"),
                    (None, "DOWN"),
                    (500.0, None),
                ],
            ),
            ("NO_DOWN", [(1.0, "UP"), (None, "DOWN")]),
        )
        for i, (value, direction) in enumerate(prices)
    ]
    local = pl.DataFrame(rows, schema=["TS", "MARKET", "ZONE", "VALUE", "DIRECTION"], orient="row")
    local = local.with_columns(pl.col("TS").dt.convert_time_zone(zone.key))
    values = ", ".join(
        str((int(ts.timestamp()) if ts is not None else None, market, zone, value, direction))
        .replace("None", "NULL")
        for ts, market, zone, value, direction in rows
    )
    fixture = (
        'WITH "SNOWPARK_RS_PRICE_FIXTURE" AS ('
        'SELECT TO_TIMESTAMP_LTZ(COLUMN1, 0)::TIMESTAMP_LTZ(9) AS TS, '
        'COLUMN2::VARCHAR AS MARKET, COLUMN4::FLOAT AS VALUE, '
        "'EUR/MWh'::VARCHAR AS UNIT, COLUMN3::VARCHAR AS ZONE, "
        "'Y2025S1'::VARCHAR AS FORECAST_PERIOD, 'Central'::VARCHAR AS SCENARIO, "
        "'test'::VARCHAR AS ID_MODEL, 0::NUMBER(38,0) AS VERSION, "
        "'test'::VARCHAR AS ID_INFERENCE_DATASET, COLUMN5::VARCHAR AS DIRECTION, "
        "NULL::TIMESTAMP_LTZ(9) AS UPLOAD_TIMESTAMP, 'flex_00'::VARCHAR AS ID_FLEX "
        f"FROM VALUES {values})\n"
    )
    return local, fixture


def test_complex_expr_matches_polars_on_snowflake(snowflake_session):
    to_arrow(snowflake_session, ("ALTER SESSION SET TIMEZONE = 'Europe/Paris'", []))
    local, fixture = _price_fixture()
    source = pls.Table("SNOWPARK_RS_PRICE_FIXTURE")
    keys = _spread_keys()

    def compare(query, expected):
        sql, binds = query.to_sql()
        actual = pl.from_arrow(to_arrow(snowflake_session, (fixture + sql, binds)))
        assert isinstance(actual, pl.DataFrame)
        assert actual.schema["VALUE"] == pl.Float64
        if "DAY" in expected.columns:
            expected = expected.with_columns(
                pl.col("DAY").dt.convert_time_zone("UTC").dt.cast_time_unit("ns"),
            )
        assert_frame_equal(actual, expected, check_row_order=False)

    for expr in (
        complex_expr(4),
        complex_expr(4, 60),
        complex_expr(4, 45),
        complex_expr(4, _group_period()),
        complex_expr(0),
    ):
        compare(source.group_by(*keys).agg(expr), local.group_by(*keys).agg(expr))
    compare(source.select(complex_expr(4)), local.select(complex_expr(4)))
    compare(source.filter(pl.lit(False)).select(complex_expr(4)), local.clear().select(complex_expr(4)))

    # The guarded Snowflake expression yields NULL for the unsupported period.
    # Polars raises when a NULL length is also used as the offset of tail().
    compare(
        source.select(complex_expr(4, 20)),
        pl.DataFrame({"VALUE": [None]}, schema={"VALUE": pl.Float64}),
    )


def test_integer_floor_division_matches_polars_on_snowflake(snowflake_session):
    rows = [(1, 2), (-1, 2), (1_999_999, 2_000_000), (1, -2), (1, 0)]
    local = pl.DataFrame(rows, schema=["X", "Y"], orient="row").with_row_index("ID")
    values = ", ".join(f"({i}, {x}, {y})" for i, (x, y) in enumerate(rows))
    fixture = f'WITH "NUMBERS" AS (SELECT COLUMN1 AS ID, COLUMN2 AS X, COLUMN3 AS Y FROM VALUES {values})\n'
    expr = (pl.col("X") // pl.col("Y")).alias("QUOTIENT")
    sql, binds = pls.Table("NUMBERS").select("ID", expr).sort("ID").to_sql()
    actual = pl.from_arrow(to_arrow(snowflake_session, (fixture + sql, binds)))
    assert isinstance(actual, pl.DataFrame)
    assert actual["QUOTIENT"].to_list() == local.select(expr)["QUOTIENT"].to_list()


def test_truncated_group_keys_match_polars_across_dst(snowflake_session):
    zone = "Europe/Paris"
    to_arrow(snowflake_session, ("ALTER SESSION SET TIMEZONE = 'Europe/Paris'", []))
    instants = [
        datetime(2025, 3, 29, 22, 59, tzinfo=UTC),
        datetime(2025, 3, 29, 23, 7, tzinfo=UTC),
        datetime(2025, 3, 30, 1, 12, tzinfo=UTC),
        datetime(2025, 10, 26, 0, 37, 56, 123456, tzinfo=UTC),
        datetime(2025, 10, 26, 1, 37, 56, 123456, tzinfo=UTC),
        None,
    ]
    epochs = [
        int(instant.timestamp()) * 1_000_000 + instant.microsecond
        if instant is not None
        else "NULL"
        for instant in instants
    ]
    values = ", ".join(f"({index}, {micros})" for index, micros in enumerate(epochs))
    fixture = (
        'WITH "EVENTS" AS (SELECT COLUMN1 AS ID, TO_TIMESTAMP_LTZ(COLUMN2, 6)::TIMESTAMP_LTZ(9) AS TS '
        f"FROM VALUES {values})\n"
    )
    source = pls.Table("EVENTS")
    local = pl.DataFrame({"ID": range(len(instants)), "TS": instants}).with_columns(
        pl.col("TS").dt.convert_time_zone(zone),
    )

    for every in ("1d", "1h", "15m", "2h", "2d", "2w", "3mo"):
        key = pl.col("TS").dt.truncate(every).alias("BUCKET")
        query = source.group_by(key).agg(pl.len()).sort("BUCKET", nulls_last=True)
        sql, binds = query.to_sql()
        actual = pl.from_arrow(to_arrow(snowflake_session, (fixture + sql, binds)))
        expected = (
            local.group_by(key)
            .agg(pl.len())
            .sort("BUCKET", nulls_last=True)
            .with_columns(pl.col("BUCKET").dt.convert_time_zone("UTC"))
        )
        assert isinstance(actual, pl.DataFrame)
        assert actual.columns == ["BUCKET", "len"]
        assert actual.schema["BUCKET"] == pl.Datetime("ns", "UTC")
        assert actual.rows() == expected.rows()

    components = {"DAY": pl.col("TS").dt.date(), "MINUTE": pl.col("TS").dt.minute()}
    sql, binds = source.select("ID", **components).sort("ID").to_sql()
    actual = pl.from_arrow(to_arrow(snowflake_session, (fixture + sql, binds)))
    assert isinstance(actual, pl.DataFrame)
    assert actual.drop("ID").rows() == local.select(**components).rows()

    # At this instant New York has already switched offsets, but its midnight has not.
    instant = datetime(2025, 11, 2, 6, 37, tzinfo=UTC).astimezone(
        ZoneInfo("America/New_York"),
    )
    bucket = pl.lit(instant).dt.truncate("1d")
    sql, binds = source.select(BUCKET=bucket).to_sql()
    actual = pl.from_arrow(to_arrow(snowflake_session, (fixture + sql, binds)))
    expected = local.select(BUCKET=bucket).with_columns(
        pl.col("BUCKET").dt.convert_time_zone("UTC"),
    )
    assert isinstance(actual, pl.DataFrame)
    assert actual.rows() == expected.rows()


def test_truncate_duration_families_and_dynamic_values_match_polars(snowflake_session):
    instants = [
        datetime(1969, 12, 28, 13, 58, 23, 234567),
        datetime(2024, 2, 29, 10, 33, 26, 123456),
        None,
    ]
    periods = [
        "1ns", "31ns", "23us", "137ms", "17s", "11m", "75m", "2h30m",
        "3d", "1w", "2w", "5mo", "2q", "2y", "+1y2mo",
    ]
    rows = [(i, ts, every) for i, (ts, every) in enumerate((ts, every) for ts in instants for every in [*periods, None])]
    rows.append((len(rows), None, "invalid"))
    local = pl.DataFrame(rows, schema=["ID", "TS", "PERIOD"], orient="row").with_columns(
        pl.col("TS").dt.cast_time_unit("ns"),
        pl.col("TS").dt.date().alias("D"),
    )
    values = ", ".join(
        f"({i}, {ts.isoformat()!r}, {every!r})" if ts is not None else f"({i}, NULL, {every!r})"
        for i, ts, every in rows
    ).replace("None", "NULL")
    fixture = (
        'WITH "EVENTS" AS (SELECT COLUMN1 AS ID, COLUMN2::TIMESTAMP_NTZ(9) AS TS, '
        f'COLUMN2::DATE AS D, COLUMN3::VARCHAR AS PERIOD FROM VALUES {values})\n'
    )
    source = pls.Table("EVENTS", schema=[
        pls.Field("ID", "NUMBER"), pls.Field("TS", "TIMESTAMP_NTZ"),
        pls.Field("D", "DATE"), pls.Field("PERIOD", "VARCHAR"),
    ])
    expressions = [pl.col("TS").dt.truncate(every).alias(f"T_{i}") for i, every in enumerate(periods)]
    expressions += [
        pl.col("TS").dt.truncate("1d5h").alias("MIXED"),
        pl.col("TS").dt.truncate(pl.col("PERIOD")).alias("DYNAMIC"),
        pl.col("D").dt.truncate(pl.col("PERIOD")).alias("DYNAMIC_DATE"),
        pl.lit(datetime(2024, 2, 29, 10, 33, 26, 123456)).dt.truncate("2001ns").alias("US"),
    ]
    sql, binds = source.select("ID", *expressions).sort("ID").to_sql()
    actual = pl.from_arrow(to_arrow(snowflake_session, (fixture + sql, binds)))
    expected = local.select("ID", *expressions).with_columns(pl.col("US").dt.cast_time_unit("ns"))
    assert isinstance(actual, pl.DataFrame)
    assert_frame_equal(actual.drop("ID"), expected.drop("ID"))

    mixed = pl.col("TS").dt.truncate(pl.concat_str(pl.lit("1d"), pl.lit("5h")))
    sql, binds = pls.Table("EVENTS").select("ID", mixed).sort("ID").to_sql()
    actual = pl.from_arrow(to_arrow(snowflake_session, (fixture + sql, binds)))
    assert isinstance(actual, pl.DataFrame)
    assert_frame_equal(actual.select("TS"), local.select(mixed))


def test_composed_reductions_preserve_candidate_sets_and_nulls(snowflake_session):
    rows = [
        (None, None, True), (None, 5, True), (None, 5, True), (None, 90, False),
        ("A", 10, True), ("A", 4, False), ("A", None, True), ("B", 200, False),
    ]
    local = pl.DataFrame(rows, schema=["G", "X", "KEEP"], orient="row")
    values = ", ".join(str(row) for row in rows).replace("None", "NULL")
    fixture = (
        'WITH "NUMBERS" AS (SELECT COLUMN1::VARCHAR AS G, COLUMN2 AS X, '
        f'COLUMN3 AS KEEP FROM VALUES {values})\n'
    )
    source = pls.Table("NUMBERS")
    selected = pl.col("X").filter(pl.col("KEEP"))
    candidates = selected.drop_nulls().sort()
    exprs = [
        selected.len().alias("WITH_NULLS"),
        candidates.count().alias("NON_NULLS"),
        candidates.head(2).mean().alias("FIRST"),
        pl.when(candidates.count() > 0)
        .then(candidates.tail(candidates.count()).mean())
        .otherwise(pl.col("X").filter(pl.col("KEEP").eq(False)).mean()).alias("LAST"),
    ]
    for query, expected in (
        (source.select(*exprs), local.select(*exprs)),
        (source.group_by("G").agg(*exprs), local.group_by("G").agg(*exprs)),
    ):
        sql, binds = query.to_sql()
        actual = pl.from_arrow(to_arrow(snowflake_session, (fixture + sql, binds)))
        assert isinstance(actual, pl.DataFrame)
        actual = actual.with_columns(pl.col("WITH_NULLS", "NON_NULLS").cast(pl.UInt32))
        assert_frame_equal(actual, expected, check_row_order=False)


def test_string_operations_match_polars_with_collation_and_nulls(snowflake_session):
    fixture = """WITH "STRINGS" AS (
        SELECT COLUMN1 AS ID, COLLATE(COLUMN2, 'en-ci-ai') AS TEXT, COLUMN3 AS PAT
        FROM VALUES (0, 'fooBIKEbar', 'BIKE'), (1, 'éclair', 'e'),
            (2, 'eclair', 'é'), (3, '[rail]_100%', '[rail]_'),
            (4, '', ''), (5, NULL, 'x'), (6, 'train.', NULL), (7, 'Train', 'train'),
            (8, NULL, NULL)
    )
    """
    source = pls.Table("STRINGS")
    local = pl.DataFrame(
        {
            "ID": range(9),
            "TEXT": [
                "fooBIKEbar",
                "éclair",
                "eclair",
                "[rail]_100%",
                "",
                None,
                "train.",
                "Train",
                None,
            ],
            "PAT": ["BIKE", "e", "é", "[rail]_", "", "x", None, "train", None],
        }
    )
    text = pl.col("TEXT")
    predicates = {
        "LITERAL": text.str.contains(pl.col("PAT"), literal=True),
        "REGEX": text.str.contains("(?i)bike|train"),
        "ANCHORED": text.str.contains(r"^é|\.$"),
        "EMPTY": text.str.contains(""),
        "EMPTY_SUBJECT": text.str.contains("^$"),
        "INVALID": text.str.contains("[", strict=False),
        "PREFIX": text.str.starts_with(pl.col("PAT")),
        "SUFFIX": text.str.ends_with(pl.col("PAT")),
        "CHARS": text.str.len_chars(),
        "BYTES": text.str.len_bytes(),
        "CONCAT": pl.concat_str(text, pl.col("PAT"), separator="-"),
        "CONCAT_SKIP": pl.concat_str(text, pl.col("PAT"), separator="-", ignore_nulls=True),
    }
    sql, binds = source.select("ID", **predicates).sort("ID").to_sql()
    actual = pl.from_arrow(to_arrow(snowflake_session, (fixture + sql, binds)))
    assert isinstance(actual, pl.DataFrame)
    actual = actual.with_columns(pl.col("CHARS", "BYTES").cast(pl.UInt32))
    assert actual.drop("ID").equals(local.select(**predicates))

    sql, binds = (
        source.filter(predicates["REGEX"]).select("ID", "TEXT").sort("ID").to_sql()
    )
    actual = pl.from_arrow(to_arrow(snowflake_session, (fixture + sql, binds)))
    assert isinstance(actual, pl.DataFrame)
    assert actual.drop("ID").equals(local.filter(predicates["REGEX"]).select("TEXT"))


def test_snowflake_conversions_and_json_keep_distinct_nulls(snowflake_session):
    fixture = """WITH "DOCUMENTS" AS (
        SELECT COLUMN1 AS ID, COLUMN2::VARCHAR AS TXT
        FROM VALUES (0, '{"x":null}'), (1, '{}'), (2, NULL), (3, '42'), (4, 'bad')
    )
    """
    txt = pls.col("TXT").snowflake
    source = pls.Table("DOCUMENTS").select("ID", "TXT", DOC=txt.try_parse_json())
    source = source.with_columns(VALUE=pls.col("DOC").snowflake.get_path("x"))
    value = pls.col("VALUE").snowflake
    result = source.select(
        "ID",
        TYPE=value.typeof(),
        JSON_NULL=value.is_null_value(),
        SQL_NULL=pl.col("VALUE").is_null(),
        NUMBER=txt.try_to_number(),
        SIZE=pls.col("DOC").snowflake.array_construct(pl.col("VALUE")).is_not_null(),
        OBJECT=pls.col("TXT").snowflake.object_construct_keep_null(pl.col("VALUE")).is_not_null(),
    ).sort("ID")
    sql, binds = result.to_sql()
    actual = to_arrow(snowflake_session, (fixture + sql, binds)).to_pydict()
    assert actual["TYPE"] == ["NULL_VALUE", None, None, None, None]
    assert actual["JSON_NULL"] == [True, None, None, None, None]
    assert actual["SQL_NULL"] == [False, True, True, True, True]
    assert actual["NUMBER"] == [None, None, None, 42, None]
    assert actual["SIZE"] == actual["OBJECT"] == [True] * 5


def test_polars_translation_on_snowflake(snowflake_session):
    session = snowflake_session
    fixture = os.environ.get("SNOWPARK_RS_TEST_TABLE", "PUBLIC.PUBLIC.EXAMPLE")
    source = pls.Table(fixture).select("CATEGORY", "AMOUNT")
    local = source.to_polars(session)
    assert not local.is_empty()

    def aggregate(frame):
        return (
            frame.filter(pl.col("CATEGORY").is_not_null())
            .group_by("CATEGORY")
            .agg(
                total=pl.col("AMOUNT").sum(),
                count=pl.col("AMOUNT").count(),
                rows=pl.len(),
            )
            .sort("CATEGORY")
        )

    actual = aggregate(source).to_polars(session)
    expected = aggregate(local)
    assert actual.columns == expected.columns
    assert actual.schema["total"].is_decimal()
    assert expected.schema["total"].is_decimal()
    assert actual.to_dicts() == expected.to_dicts()

    all_null = source.with_columns(AMOUNT=pl.lit(None, pl.Decimal(12, 2))).select(
        total=pl.col("AMOUNT").sum(),
        count=pl.col("AMOUNT").count(),
    )
    assert all_null.to_polars(session).row(0) == (0, 0)

    json = source.select(
        valid=pls.try_parse_json(pl.lit('{"a":1}')).is_not_null(),
        invalid=pls.try_parse_json(pl.lit("invalid")).is_null(),
    )
    assert json.to_polars(session).row(0) == (True, True)


def test_temporal_filters_and_table_membership_on_example(snowflake_session):
    import pyarrow as pa

    session = snowflake_session
    fixture = os.environ.get("SNOWPARK_RS_TEST_TABLE", "PUBLIC.PUBLIC.EXAMPLE")
    source = pls.Table(fixture)
    timestamps = ("CREATED_AT", "CREATED_AT_TZ", "CREATED_AT_LTZ", "CREATED_AT_NTZ")
    local = source.select("ID", *timestamps).sort("ID").to_polars(session)
    assert not local.is_empty()
    assert local.schema["CREATED_AT_TZ"] == pl.Datetime("ns", "UTC")
    assert local.schema["CREATED_AT_LTZ"] == pl.Datetime("ns", "UTC")
    assert local.schema["CREATED_AT_NTZ"] == pl.Datetime("ns")
    tz_schema = source.select("CREATED_AT_TZ").schema_with_session(session)
    assert tz_schema.field("CREATED_AT_TZ").type == pa.timestamp("ns", tz="UTC")

    for name in timestamps:
        expected = local.filter(pl.col(name) >= date(2024, 1, 1))
        actual = (
            source.filter(pl.col(name) >= pl.date(2024, 1, 1))
            .select("ID", *timestamps)
            .sort("ID")
        )
        assert actual.to_polars(session).equals(expected)

    instant = datetime(2024, 1, 1, 0, 0, 0, 123456, tzinfo=UTC)
    condition = pl.col("CREATED_AT_TZ") <= instant
    actual = source.filter(condition).select("ID", *timestamps).sort("ID")
    assert actual.to_polars(session).equals(local.filter(condition))

    first_id = local["ID"].drop_nulls()[0]
    ids = source.filter(pl.col("ID") == first_id).select("ID")
    matching = (
        source.filter(pl.col("ID").is_in(ids.implode())).select("ID").to_polars(session)
    )
    assert (
        matching["ID"].to_list()
        == local.filter(pl.col("ID") == first_id)["ID"].to_list()
    )
    multiple_columns = source.filter(pl.col("ID").is_in(source.implode()))
    with pytest.raises(
        RuntimeError, match=r"Snowflake query submission failed.*001044"
    ):
        multiple_columns.to_polars(session)
