use snowpark_rs::{CompileError, DataFrame};

#[test]
fn compiles_qualified_table_name_without_a_session() -> Result<(), CompileError> {
    let table = "DB.SCHEMA.\"a.\"\"b\"";
    let expected = DataFrame::table(["DB", "SCHEMA", "\"a.\"\"b\""])?;
    assert_eq!(DataFrame::table_name(table)?.to_sql()?, expected.to_sql()?);
    assert!(DataFrame::table_name("DB..TABLE").is_err());
    assert!(DataFrame::table_name("DB.\"unterminated.name").is_err());
    Ok(())
}

#[cfg(feature = "polars")]
#[test]
fn compiles_typed_polars_expressions_with_bind_order() -> Result<(), CompileError> {
    use snowpark_rs::{
        BindValue,
        polars::{SortKey, col, lit},
    };

    let query = DataFrame::table_name("SALES")?
        .filter_polars(col("AMOUNT").gt(lit(100)))
        .select_polars([(col("AMOUNT") * lit(2)).alias("double")])
        .sort_polars([SortKey {
            expr: col("double"),
            descending: true,
            nulls_last: true,
        }])
        .limit(5);
    assert_eq!(query.columns()?.unwrap(), [r#""double""#],);
    let query = query.to_sql()?;
    assert_eq!(
        query.sql.split_whitespace().collect::<Vec<_>>().join(" "),
        r#"SELECT t1.* FROM (SELECT t0."AMOUNT" * ? AS "double" FROM "SALES" AS t0 WHERE t0."AMOUNT" > ?) AS t1 ORDER BY t1."double" DESC NULLS LAST LIMIT 5"#,
    );
    assert_eq!(query.binds, [BindValue::Int(2), BindValue::Int(100)]);
    Ok(())
}

#[cfg(feature = "polars")]
#[test]
fn defers_polars_errors_through_native_plan_nodes() -> Result<(), CompileError> {
    use snowpark_rs::polars::{col, lit};

    let source = DataFrame::table_name("SALES")?;
    let invalid = source
        .select_polars([col("AMOUNT").floor_div(lit(2)) + lit(1)])
        .limit(5);
    let expected = CompileError::UnsupportedExpression {
        operation: "select",
        node: "BinaryExpr.FloorDivide".into(),
        reason: "no translation to Snowflake SQL".into(),
    };
    assert_eq!(invalid.schema().unwrap_err(), expected);
    assert_eq!(invalid.to_sql().unwrap_err(), expected);
    assert!(source.select_polars([col("AMOUNT")]).to_sql().is_ok());
    Ok(())
}
