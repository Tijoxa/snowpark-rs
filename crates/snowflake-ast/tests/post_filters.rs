use snowflake_ast::{CompileError, DataFrame, Field, col, count_all, lit, row_number, sum};

#[test]
fn having_uses_group_keys_and_aggregate_aliases_in_the_aggregate_block() -> Result<(), CompileError>
{
    let source = DataFrame::table_with_schema(
        ["T"],
        ["K", "X"]
            .into_iter()
            .map(|name| Field::new(name, None, None).unwrap())
            .collect(),
    )?
    .filter(col("X").gt(lit(0)));
    let groups = source
        .group_by([col("K").eq(lit("A")).alias(r#""is A""#)])
        .agg([sum(source.col("X")).alias(r#""total amount""#)]);
    let query = groups
        .having(groups.col(r#""is A""#))
        .having(
            col(r#""total amount""#)
                .gt(lit(10))
                .and(count_all().gt(lit(2))),
        )
        .to_sql()?;
    assert_eq!(
        query.sql,
        "SELECT t0.\"K\" = ? AS \"is A\", SUM(t0.\"X\") AS \"total amount\"\n\
         FROM \"T\" AS t0\nWHERE t0.\"X\" > ?\nGROUP BY 1\n\
         HAVING (\"is A\") AND ((\"total amount\" > ?) AND (COUNT(*) > ?))"
    );
    assert_eq!(query.binds, ["A".into(), 0.into(), 10.into(), 2.into()]);
    Ok(())
}

#[test]
fn qualify_uses_a_window_alias_before_distinct_sort_and_limit() -> Result<(), CompileError> {
    let source = DataFrame::table_with_schema(
        ["T"],
        ["K", "X", "R"]
            .into_iter()
            .map(|name| Field::new(name, None, None).unwrap())
            .collect(),
    )?;
    let ranked = source.select([
        col("K").into(),
        (row_number([source.col("K")], [source.col("X").desc()]) + lit(2)).alias("R"),
    ]);
    let result = ranked
        .qualify(ranked.col("R").le(lit(4)))
        .distinct()
        .sort(["K"])
        .head(3);
    let query = result.to_sql()?;
    assert_eq!(
        query.sql,
        "SELECT t1.*\nFROM (SELECT DISTINCT t0.\"K\", ROW_NUMBER() OVER (PARTITION BY t0.\"K\" ORDER BY t0.\"X\" DESC NULLS FIRST) + ? AS \"R\"\n\
         FROM \"T\" AS t0\nQUALIFY (ROW_NUMBER() OVER (PARTITION BY t0.\"K\" ORDER BY t0.\"X\" DESC NULLS FIRST) + ?) <= ?) AS t1\n\
         ORDER BY t1.\"K\" ASC NULLS LAST\nLIMIT 3"
    );
    assert_eq!(query.binds, [2.into(), 2.into(), 4.into()]);
    assert_eq!(result.columns()?.unwrap(), ["K", "R"]);
    Ok(())
}

#[test]
fn qualify_after_limit_or_distinct_filters_the_new_row_set() -> Result<(), CompileError> {
    let source = DataFrame::table_name("T")?;
    let ordered = row_number([], [col("X").asc()]);
    let query = source
        .distinct()
        .head(5)
        .qualify(ordered.clone().eq(lit(1)))
        .filter(col("X").gt(lit(2)))
        .to_sql()?;
    assert_eq!(
        query.sql,
        "SELECT t2.*\nFROM (SELECT t1.*\nFROM (SELECT DISTINCT t0.*\nFROM \"T\" AS t0\nLIMIT 5) AS t1\n\
         QUALIFY ROW_NUMBER() OVER (ORDER BY t1.\"X\" ASC NULLS LAST) = ?) AS t2\nWHERE t2.\"X\" > ?"
    );
    assert_eq!(query.binds, [1.into(), 2.into()]);
    assert_eq!(
        source.having(col("X")).to_sql(),
        Err(CompileError::HavingWithoutAggregate)
    );
    assert_eq!(
        source.qualify(col("X")).to_sql(),
        Err(CompileError::QualifyWithoutWindow)
    );
    assert_eq!(
        source.filter(ordered.eq(lit(1))).to_sql(),
        Err(CompileError::UnexpectedWindow)
    );
    Ok(())
}

#[test]
fn tail_reverses_all_sort_keys_and_restores_order_after_a_limit() -> Result<(), CompileError> {
    let source = DataFrame::table_name("T")?;
    let ordered = source.sort([
        (source.col("X") + lit(1)).asc_nulls_first(),
        col("K").desc_nulls_last(),
    ]);
    let query = ordered.limit_with_offset(6, 2).tail(3).head(1).to_sql()?;
    assert!(
        query
            .sql
            .contains("t0.\"X\" + ? ASC NULLS FIRST, t0.\"K\" DESC NULLS LAST\nLIMIT 6 OFFSET 2")
    );
    assert!(
        query
            .sql
            .contains("t1.\"X\" + ? DESC NULLS LAST, t1.\"K\" ASC NULLS FIRST\nLIMIT 3")
    );
    assert!(
        query
            .sql
            .ends_with("ORDER BY t3.\"X\" + ? ASC NULLS FIRST, t3.\"K\" DESC NULLS LAST\nLIMIT 1")
    );
    assert_eq!(query.binds, [1.into(), 1.into(), 1.into(), 1.into()]);
    assert_eq!(source.tail(2).to_sql(), Err(CompileError::TailWithoutOrder));
    assert_eq!(source.tail(0).to_sql()?, source.limit(0).to_sql()?);
    Ok(())
}
