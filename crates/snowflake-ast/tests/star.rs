use snowflake_ast::{BindValue, CompileError, DataFrame, DataType, Field, col, lit, star};

#[test]
fn star_modifiers_preserve_column_order_types_and_bind_order() -> Result<(), CompileError> {
    let input = DataFrame::table_with_schema(
        ["example"],
        vec![
            Field::new(
                "id",
                Some(DataType::Number {
                    precision: 10,
                    scale: 0,
                }),
                Some(false),
            )?,
            Field::new("category", Some(DataType::Varchar), Some(true))?,
            Field::new(r#""data?""#, Some(DataType::Varchar), Some(true))?,
        ],
    )?
    .filter(col("id").eq(lit(1)))
    .limit(5);
    let selected = input.select([
        star()
            .rename(input.col(r#""data?""#), r#""new""label""#)
            .replace(r#""data?""#, lit(true))
            .exclude(["category"])
            .into(),
        lit("extra").alias("tag"),
    ]);
    let query = selected.to_sql()?;
    assert_eq!(
        query.sql,
        concat!(
            "SELECT t1.* EXCLUDE (\"CATEGORY\") REPLACE (? AS \"data?\") ",
            "RENAME (\"data?\" AS \"new\"\"label\"), ? AS \"TAG\"\n",
            "FROM (SELECT t0.*\nFROM \"EXAMPLE\" AS t0\nWHERE t0.\"ID\" = ?\nLIMIT 5) AS t1"
        )
    );
    assert_eq!(
        query.binds,
        [
            BindValue::Bool(true),
            BindValue::Text("extra".into()),
            BindValue::Int(1)
        ]
    );
    let fields = selected.select(["*"]).schema()?.unwrap();
    assert_eq!(
        fields,
        [
            Field::new(
                "id",
                Some(DataType::Number {
                    precision: 10,
                    scale: 0
                }),
                Some(false)
            )?,
            Field::new(r#""new""label""#, Some(DataType::Boolean), Some(false))?,
            Field::new("tag", Some(DataType::Varchar), Some(false))?,
        ]
    );
    Ok(())
}

#[test]
fn ilike_resolves_quoted_names_and_preserves_unknown_schemas() -> Result<(), CompileError> {
    let input = DataFrame::table(["example"])?.select(["id", r#""Refé'?\A""#]);
    let selected = input.select([star().ilike("ref_'?%").rename(r#""Refé'?\A""#, "label")]);
    let query = selected.to_sql()?;
    assert!(
        query
            .sql
            .starts_with(r#"SELECT t1.* ILIKE 'ref_''?%' RENAME ("Refé'?\A" AS "LABEL")"#)
    );
    assert_eq!(selected.columns()?.unwrap(), ["LABEL"]);

    let escaped = input.select([star().ilike(r"ref_'?\\A")]);
    assert!(escaped.columns()?.is_none());
    assert!(
        escaped
            .to_sql()?
            .sql
            .starts_with(r"SELECT t1.* ILIKE 'ref_''?\\\\A'")
    );

    let unknown =
        DataFrame::table(["example"])?.select([star().ilike("%id%").replace("id", lit(42))]);
    assert!(unknown.columns()?.is_none());
    let query = unknown.select(["id"]).to_sql()?;
    assert!(
        query
            .sql
            .contains(r#"t0.* ILIKE '%id%' REPLACE (? AS "ID")"#)
    );
    assert_eq!(query.binds, [BindValue::Int(42)]);
    Ok(())
}

#[test]
fn star_errors_follow_the_resolved_input_columns() -> Result<(), CompileError> {
    let source = DataFrame::table(["example"])?;
    let input = source.select(["id", "name"]);
    let cases = [
        (
            star().ilike("%").exclude(["id"]),
            CompileError::IncompatibleStarFilters,
        ),
        (
            star().exclude(["id", "name"]),
            CompileError::EmptyProjection,
        ),
        (
            star().exclude(["id"]).replace("id", lit(1)),
            CompileError::UnknownColumn("id".into()),
        ),
        (
            star().ilike("n%").rename("id", "new_id"),
            CompileError::UnknownColumn("id".into()),
        ),
        (
            star().replace("id", lit(1)).replace("ID", lit(2)),
            CompileError::AmbiguousColumn(r#""ID""#.into()),
        ),
        (
            star().rename("id", "name"),
            CompileError::DuplicateOutput(r#""NAME""#.into()),
        ),
        (
            star().rename(source.col("id"), "new_id"),
            CompileError::OutOfScopeColumn("id".into()),
        ),
    ];
    for (selection, expected) in cases {
        assert_eq!(input.select([selection]).to_sql(), Err(expected));
    }
    let mixed = input.select([
        star().exclude(["id", "name"]).into(),
        lit(true).alias("extra"),
    ]);
    assert_eq!(mixed.columns()?.unwrap(), ["EXTRA"]);

    let duplicate = source.select(["id", "id"]).select(["*"]);
    assert_eq!(
        duplicate.select(["id"]).to_sql(),
        Err(CompileError::AmbiguousColumn("id".into()))
    );
    assert_eq!(
        input.agg([star()]).to_sql(),
        Err(CompileError::UnexpectedStar)
    );
    Ok(())
}
