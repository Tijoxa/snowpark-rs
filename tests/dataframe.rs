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
