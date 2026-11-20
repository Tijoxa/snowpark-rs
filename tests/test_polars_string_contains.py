import pytest

import polars as pl
from snowpark_rs import Field, Table, UnsupportedPolarsExpression


def test_literal_contains_uses_utf8_collation_and_accepts_a_pattern_column():
    table = Table(
        "T",
        schema=[
            Field("TEXT", "VARCHAR", nullable=False),
            Field("PAT", "VARCHAR", nullable=True),
        ],
    )
    query = table.filter(pl.col("TEXT").str.contains("[a]_%", literal=True)).select(
        MATCH=pl.col("TEXT").str.contains(pl.col("PAT"), literal=True),
    )
    sql, binds = query.to_sql()
    assert (
        "POSITION(COLLATE(t0.\"PAT\", 'utf8'), COLLATE(t0.\"TEXT\", 'utf8')) > 0" in sql
    )
    assert "WHERE POSITION(COLLATE(?, 'utf8'), COLLATE(t0.\"TEXT\", 'utf8')) > 0" in sql
    assert binds == ["[a]_%"]
    assert query.schema is not None
    assert [
        (field.name, field.data_type, field.nullable) for field in query.schema
    ] == [
        ("MATCH", "BOOLEAN", True),
    ]


def test_regex_contains_is_unanchored_and_binds_patterns_in_sql_order():
    table = Table("T").filter(pl.col("TEXT").str.contains(r"^A|\.$"))
    sql, binds = table.select(
        matched=pl.col("TEXT").str.contains("(?i)bike|train"),
        empty_subject=pl.col("TEXT").str.contains("^$"),
    ).to_sql()
    assert sql.count("REGEXP_INSTR(") == 3
    assert ", 1, 1, 0, 'i') > 0 AS \"matched\"" in sql
    assert 'OR OCTET_LENGTH(t0."TEXT") = 0 AS "empty_subject"' in sql
    assert binds == ["bike|train", "^$", r"^A|\.$"]


def test_regex_strict_controls_invalid_patterns_without_changing_row_shape():
    table = Table("T")
    with pytest.raises(UnsupportedPolarsExpression, match="invalid regex pattern"):
        table.select(pl.col("TEXT").str.contains("[")).to_sql()

    sql, binds = table.select(pl.col("TEXT").str.contains("[", strict=False)).to_sql()
    assert " ".join(sql.split()) == 'SELECT CAST(? AS BOOLEAN) AS "TEXT" FROM "T" AS t0'
    assert binds == [None]


@pytest.mark.parametrize(
    ("pattern", "error"),
    [(pl.col("PAT"), "must be constant"), (r"\p{Greek}", "compatible subset")],
)
def test_regex_contains_reports_patterns_outside_the_supported_subset(pattern, error):
    query = Table("T").filter(pl.col("TEXT").str.contains(pattern, strict=False))
    with pytest.raises(UnsupportedPolarsExpression, match=error):
        query.to_sql()
