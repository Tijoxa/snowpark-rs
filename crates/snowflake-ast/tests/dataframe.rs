use snowflake_ast::{BindValue, CompileError, DataFrame, Projection, col, lit};

#[test]
fn compiles_filter_and_projection_without_a_session() -> Result<(), CompileError> {
    let query = DataFrame::table(["public", "public", "example"])?
        .filter(col("category").eq(lit("A")))
        .select(["id", "name"])
        .to_sql()?;
    assert_eq!(
        query.sql,
        "SELECT t0.\"ID\", t0.\"NAME\"\n\
         FROM \"PUBLIC\".\"PUBLIC\".\"EXAMPLE\" AS t0\n\
         WHERE t0.\"CATEGORY\" = ?"
    );
    assert_eq!(query.binds, [BindValue::Text("A".into())]);
    Ok(())
}

#[test]
fn preserves_quoted_identifiers_and_predicate_grouping() -> Result<(), CompileError> {
    let text = "O'Neil ? \" \\";
    let query = DataFrame::table(["_schema$1", r#""Table.With.Dot""#])?
        .filter(
            (!col(r#""Mixed""Case""#).is_null())
                .and(col("name").eq(lit(text)).or(col("id").le(lit(-5)))),
        )
        .select([col(r#""Mixed""Case""#).alias(r#""Label.with.dot""#)])
        .to_sql()?;
    assert_eq!(
        query.sql,
        "SELECT t0.\"Mixed\"\"Case\" AS \"Label.with.dot\"\n\
         FROM \"_SCHEMA$1\".\"Table.With.Dot\" AS t0\n\
         WHERE (NOT (t0.\"Mixed\"\"Case\" IS NULL)) AND ((t0.\"NAME\" = ?) OR (t0.\"ID\" <= ?))"
    );
    assert_eq!(
        query.binds,
        [BindValue::Text(text.into()), BindValue::Int(-5)]
    );
    assert!(!format!("{query:?}").contains("O'Neil"));
    Ok(())
}

#[test]
fn preserves_scope_and_bind_order_across_subqueries() -> Result<(), CompileError> {
    let query = DataFrame::table(["example"])?
        .filter(col("amount").gt(lit(10)))
        .select([lit("inside").alias("label")])
        .filter(col("label").ne(lit("outside")))
        .select([lit(true).alias("flag"), col("label").into()])
        .to_sql()?;
    assert_eq!(
        query.sql,
        "SELECT ? AS \"FLAG\", t1.\"LABEL\"\n\
         FROM (SELECT ? AS \"LABEL\"\n\
         FROM \"EXAMPLE\" AS t0\n\
         WHERE t0.\"AMOUNT\" > ?) AS t1\n\
         WHERE t1.\"LABEL\" <> ?"
    );
    assert_eq!(
        query.binds,
        [
            BindValue::Bool(true),
            BindValue::Text("inside".into()),
            BindValue::Int(10),
            BindValue::Text("outside".into()),
        ]
    );
    Ok(())
}

#[test]
fn derives_independent_branches_from_the_same_frame() -> Result<(), CompileError> {
    let parent = DataFrame::table(["example"])?.filter(col("name").is_not_null());
    let left = parent.select(["id"]).to_sql()?;
    let right = parent
        .filter(col("id").ge(lit(i64::MAX)))
        .select(["name"])
        .to_sql()?;
    assert_eq!(
        parent.to_sql()?.sql,
        "SELECT t0.*\nFROM \"EXAMPLE\" AS t0\nWHERE t0.\"NAME\" IS NOT NULL"
    );
    assert_eq!(
        left.sql,
        "SELECT t0.\"ID\"\nFROM \"EXAMPLE\" AS t0\nWHERE t0.\"NAME\" IS NOT NULL"
    );
    assert!(left.binds.is_empty());
    assert_eq!(
        right.sql,
        "SELECT t1.\"NAME\"\n\
         FROM (SELECT t0.*\n\
         FROM \"EXAMPLE\" AS t0\n\
         WHERE t0.\"NAME\" IS NOT NULL) AS t1\n\
         WHERE t1.\"ID\" >= ?"
    );
    assert_eq!(right.binds, [BindValue::Int(i64::MAX)]);
    Ok(())
}

#[test]
fn rejects_invalid_identifiers_and_projection_references() -> Result<(), CompileError> {
    for name in [
        "public.example",
        "",
        "1table",
        "table; SELECT 1",
        r#""bad"quote""#,
    ] {
        assert_eq!(
            DataFrame::table([name]).unwrap_err(),
            CompileError::InvalidIdentifier(name.into())
        );
    }
    assert_eq!(
        DataFrame::table(Vec::<&str>::new()).unwrap_err(),
        CompileError::InvalidTableName
    );
    assert_eq!(
        DataFrame::table(["a", "b", "c", "d"]).unwrap_err(),
        CompileError::InvalidTableName
    );

    let frame = DataFrame::table(["example"])?;
    assert_eq!(
        frame.select(Vec::<Projection>::new()).to_sql().unwrap_err(),
        CompileError::EmptyProjection
    );
    assert_eq!(
        frame
            .select(["name"])
            .filter(col("id").lt(lit(3)))
            .to_sql()
            .unwrap_err(),
        CompileError::UnknownColumn("id".into())
    );
    assert_eq!(
        frame
            .select(["id", r#""ID""#])
            .select(["id"])
            .to_sql()
            .unwrap_err(),
        CompileError::AmbiguousColumn("id".into())
    );
    assert_eq!(
        frame
            .select([col("id").alias("bad.alias")])
            .to_sql()
            .unwrap_err(),
        CompileError::InvalidIdentifier("bad.alias".into())
    );
    Ok(())
}
