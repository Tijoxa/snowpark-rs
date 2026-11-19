use snowflake_ast::{
    BindValue, CompileError, DataFrame, Projection, col, count, count_all, lit, sum,
};

#[test]
fn groups_rows_and_exposes_only_keys_and_aggregate_aliases() -> Result<(), CompileError> {
    let frame = DataFrame::table_name("example")?.filter(col("id").gt(lit(5)));
    let grouped = frame.group_by(["category"]).agg([
        count_all().alias("rows"),
        count("amount").alias("present"),
        sum("amount").alias("total"),
    ]);
    let query = grouped.to_sql()?;
    assert_eq!(
        query.sql,
        "SELECT t0.\"CATEGORY\", COUNT(*) AS \"ROWS\", COUNT(t0.\"AMOUNT\") AS \"PRESENT\", SUM(t0.\"AMOUNT\") AS \"TOTAL\"\n\
         FROM \"EXAMPLE\" AS t0\nWHERE t0.\"ID\" > ?\nGROUP BY 1"
    );
    assert_eq!(query.binds, [BindValue::Int(5)]);
    let projected = grouped
        .filter(col("rows").gt(lit(1)))
        .select(["category", "total"])
        .to_sql()?;
    assert!(
        projected
            .sql
            .starts_with("SELECT t1.\"CATEGORY\", t1.\"TOTAL\"\nFROM (")
    );
    assert!(
        projected
            .sql
            .ends_with("GROUP BY 1) AS t1\nWHERE t1.\"ROWS\" > ?")
    );
    assert_eq!(projected.binds, [BindValue::Int(5), BindValue::Int(1)]);
    assert_eq!(
        grouped.select(["amount"]).to_sql(),
        Err(CompileError::UnknownColumn("amount".into()))
    );
    Ok(())
}

#[test]
fn compiles_global_and_computed_key_aggregations() -> Result<(), CompileError> {
    let frame = DataFrame::table_name("example")?.filter(col("id").gt(lit(0)));
    let global = frame.agg([count("*"), sum("amount")]).to_sql()?;
    assert_eq!(
        global.sql,
        "SELECT COUNT(*), SUM(t0.\"AMOUNT\")\nFROM \"EXAMPLE\" AS t0\nWHERE t0.\"ID\" > ?"
    );
    let grouped = frame
        .group_by([col("category").eq(lit("FR")).alias("is_fr")])
        .agg([count(lit(9)).alias("count")])
        .to_sql()?;
    assert!(grouped.sql.ends_with("\nGROUP BY 1"));
    assert_eq!(grouped.binds, ["FR".into(), 9.into(), 0.into()]);
    assert_eq!(
        frame.agg(Vec::<Projection>::new()).to_sql(),
        Err(CompileError::EmptyAggregate)
    );
    assert_eq!(
        frame.agg(["amount"]).to_sql(),
        Err(CompileError::ExpectedAggregate)
    );
    assert_eq!(
        frame.select([sum("amount")]).to_sql(),
        Err(CompileError::UnexpectedAggregate)
    );
    Ok(())
}

#[test]
fn preserves_order_clause_and_limit_boundaries() -> Result<(), CompileError> {
    let frame = DataFrame::table_name("example")?;
    let sorted = frame.sort([
        col("category").desc_nulls_last(),
        col("id").asc_nulls_first(),
    ]);
    let limited = sorted.limit_with_offset(0, 2);
    let query = limited.filter(col("id").gt(lit(8))).to_sql()?;
    assert_eq!(
        query.sql,
        "SELECT t1.*\nFROM (SELECT t0.*\nFROM \"EXAMPLE\" AS t0\n\
         ORDER BY t0.\"CATEGORY\" DESC NULLS LAST, t0.\"ID\" ASC NULLS FIRST\n\
         LIMIT 0 OFFSET 2) AS t1\nWHERE t1.\"ID\" > ?"
    );
    assert_eq!(query.binds, [BindValue::Int(8)]);
    let query = frame
        .select([col("id").alias("key")])
        .sort(["key"])
        .limit(100)
        .to_sql()?;
    assert!(
        query
            .sql
            .ends_with("ORDER BY t1.\"KEY\" ASC NULLS LAST\nLIMIT 100")
    );
    assert_eq!(
        sorted.select(["id"]).to_sql(),
        Err(CompileError::IntermediateSort)
    );
    assert_eq!(
        sorted.filter(col("id").is_not_null()).to_sql(),
        Err(CompileError::IntermediateSort)
    );
    Ok(())
}

#[test]
fn joins_projected_branches_with_unique_aliases_and_textual_bind_order() -> Result<(), CompileError>
{
    let frame = DataFrame::table_name("example")?;
    let left = frame
        .filter(col("category").eq(lit("left filter")))
        .select([
            col("id").into(),
            lit("left projection").alias("value"),
            col("name").alias("l_id"),
        ]);
    let right = frame.filter(col("id").gt(lit(5))).select(["id", "amount"]);
    let condition = left
        .col("id")
        .eq(right.col("id"))
        .and(right.col("id").lt(lit(9)));
    let joined = left.join(&right, condition.clone());
    let query = joined
        .select([
            lit(true).alias("flag"),
            col("l2_id").into(),
            col("r_id").into(),
            col("l_id").into(),
        ])
        .to_sql()?;
    assert_eq!(
        query.sql,
        "SELECT ? AS \"FLAG\", t4.\"L2_ID\", t4.\"R_ID\", t4.\"L_ID\"\n\
         FROM (SELECT t1.\"ID\" AS \"L2_ID\", t1.\"VALUE\", t1.\"L_ID\", t3.\"ID\" AS \"R_ID\", t3.\"AMOUNT\"\n\
         FROM (SELECT t0.\"ID\", ? AS \"VALUE\", t0.\"NAME\" AS \"L_ID\"\n\
         FROM \"EXAMPLE\" AS t0\nWHERE t0.\"CATEGORY\" = ?) AS t1\n\
         INNER JOIN (SELECT t2.\"ID\", t2.\"AMOUNT\"\n\
         FROM \"EXAMPLE\" AS t2\nWHERE t2.\"ID\" > ?) AS t3 ON (t1.\"ID\" = t3.\"ID\") AND (t3.\"ID\" < ?)) AS t4"
    );
    assert_eq!(
        query.binds,
        [
            true.into(),
            "left projection".into(),
            "left filter".into(),
            5.into(),
            9.into()
        ]
    );
    let suffixed = left
        .join_with_suffixes(&right, condition.clone(), "", "_right")
        .select(["id", "id_right"])
        .to_sql()?;
    assert!(
        suffixed
            .sql
            .starts_with("SELECT t4.\"ID\", t4.\"ID_RIGHT\"")
    );
    assert_eq!(
        left.join_with_suffixes(&right, condition, "_same", "_same")
            .to_sql(),
        Err(CompileError::DuplicateJoinOutput("\"ID_SAME\"".into()))
    );
    Ok(())
}

#[test]
fn rejects_ambiguous_missing_and_out_of_scope_join_columns() -> Result<(), CompileError> {
    let frame = DataFrame::table_name("example")?;
    let left = frame.select(["id"]);
    let right = frame.select(["id"]);
    assert_eq!(
        left.join(&right, col("id").eq(right.col("id"))).to_sql(),
        Err(CompileError::AmbiguousColumn("id".into()))
    );
    assert_eq!(
        left.join(&right, frame.col("id").eq(right.col("id")))
            .to_sql(),
        Err(CompileError::OutOfScopeColumn("id".into()))
    );
    assert_eq!(
        left.join(&right, left.col("hidden").eq(right.col("id")))
            .to_sql(),
        Err(CompileError::UnknownColumn("hidden".into()))
    );
    assert_eq!(
        left.join(&left, left.col("id").eq(left.col("id"))).to_sql(),
        Err(CompileError::IdenticalJoinInputs)
    );
    assert_eq!(
        left.join(&frame, left.col("id").eq(frame.col("id")))
            .to_sql(),
        Err(CompileError::UnnamedJoinInput)
    );
    let unnamed = frame.select([col("id"), lit(3)]);
    assert_eq!(
        left.join(&unnamed, left.col("id").eq(unnamed.col("id")))
            .to_sql(),
        Err(CompileError::UnnamedJoinInput)
    );
    assert_eq!(
        left.join(&right, left.col("id").eq(right.col("id")))
            .select([left.col("id")])
            .to_sql(),
        Err(CompileError::OutOfScopeColumn("id".into()))
    );
    Ok(())
}
