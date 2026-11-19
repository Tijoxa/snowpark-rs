"""Read-only parity test with a configured Snowflake test profile and fixture."""

import os
from datetime import UTC, date, datetime

import pytest

import polars as pl
import snowpark_rs as pls


@pytest.fixture
def snowflake_session():
    profile = os.environ.get("SNOWPARK_RS_TEST_CONNECTION", "")
    if not profile:
        pytest.skip("set SNOWPARK_RS_TEST_CONNECTION to a Snowflake test profile")
    return pls.Session.builder.config("connection_name", profile).create()


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
