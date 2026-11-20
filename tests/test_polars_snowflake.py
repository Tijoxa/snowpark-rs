"""Read-only parity test with a configured Snowflake test profile and fixture."""

import os
from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

import pytest

import polars as pl
import snowpark_rs as pls
from snowpark_rs._session import to_arrow


@pytest.fixture
def snowflake_session():
    profile = os.environ.get("SNOWPARK_RS_TEST_CONNECTION", "")
    if not profile:
        pytest.skip("set SNOWPARK_RS_TEST_CONNECTION to a Snowflake test profile")
    return pls.Session.builder.config("connection_name", profile).create()


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

    for every in ("1d", "1h", "15m"):
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


def test_contains_matches_polars_with_collation_and_nulls(snowflake_session):
    fixture = """WITH "STRINGS" AS (
        SELECT COLUMN1 AS ID, COLLATE(COLUMN2, 'en-ci-ai') AS TEXT, COLUMN3 AS PAT
        FROM VALUES (0, 'fooBIKEbar', 'BIKE'), (1, 'éclair', 'e'),
            (2, 'eclair', 'é'), (3, '[rail]_100%', '[rail]_'),
            (4, '', ''), (5, NULL, 'x'), (6, 'train.', NULL), (7, 'Train', 'train')
    )
    """
    source = pls.Table("STRINGS")
    local = pl.DataFrame(
        {
            "ID": range(8),
            "TEXT": [
                "fooBIKEbar",
                "éclair",
                "eclair",
                "[rail]_100%",
                "",
                None,
                "train.",
                "Train",
            ],
            "PAT": ["BIKE", "e", "é", "[rail]_", "", "x", None, "train"],
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
    }
    sql, binds = source.select("ID", **predicates).sort("ID").to_sql()
    actual = pl.from_arrow(to_arrow(snowflake_session, (fixture + sql, binds)))
    assert isinstance(actual, pl.DataFrame)
    assert actual.drop("ID").equals(local.select(**predicates))

    sql, binds = (
        source.filter(predicates["REGEX"]).select("ID", "TEXT").sort("ID").to_sql()
    )
    actual = pl.from_arrow(to_arrow(snowflake_session, (fixture + sql, binds)))
    assert isinstance(actual, pl.DataFrame)
    assert actual.drop("ID").equals(local.filter(predicates["REGEX"]).select("TEXT"))


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
