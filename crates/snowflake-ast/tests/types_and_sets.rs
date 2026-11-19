use snowflake_ast::{
    BindValue, CompileError, DataFrame, DataType, Decimal, Field, JoinType, col, lit, lit_null,
};

#[test]
fn retains_typed_binds_and_parentheses() -> Result<(), CompileError> {
    let price: Decimal = "123456789012345678901234567890.1200".parse()?;
    let adjustment: Decimal = "-12.30e-1".parse()?;
    let number: DataType = "decimal(38, 4)".parse()?;
    let query = DataFrame::table_name("sales")?
        .filter(col("status").eq(lit("closed")))
        .select([
            ((lit(price) + lit(adjustment)) * -col("quantity"))
                .cast(number)
                .alias("total"),
            lit_null(number).alias("missing"),
        ])
        .to_sql()?;

    assert_eq!(
        query.sql,
        concat!(
            "SELECT CAST((CAST(? AS NUMBER(34,4)) + CAST(? AS NUMBER(4,3))) * (-(t0.\"QUANTITY\")) AS NUMBER(38,4)) AS \"TOTAL\", ",
            "CAST(? AS NUMBER(38,4)) AS \"MISSING\"\nFROM \"SALES\" AS t0\nWHERE t0.\"STATUS\" = ?"
        ),
    );
    assert_eq!(
        query.binds,
        [
            price.into(),
            adjustment.into(),
            BindValue::Null(number),
            "closed".into()
        ]
    );
    assert_eq!(adjustment.to_string(), "-1.230");
    assert_eq!(
        "1e+37".parse::<Decimal>()?.to_string(),
        format!("1{}", "0".repeat(37))
    );
    assert_eq!("-0.01".parse::<Decimal>()?.to_string(), "-0.01");
    for input in ["NaN", "Inf", "1e38", "1e-38", "1.0.1"] {
        assert_eq!(input.parse::<Decimal>(), Err(CompileError::InvalidDecimal));
    }
    assert!("NUMBER(38,38)".parse::<DataType>().is_err());
    assert!("INT); --".parse::<DataType>().is_err());
    Ok(())
}

#[test]
fn reports_schema_through_replacement_rename_and_distinct() -> Result<(), CompileError> {
    let source = DataFrame::table_name("sales")?;
    assert_eq!(source.schema()?, None);
    let source = DataFrame::table_name_with_schema(
        "sales",
        vec![
            Field::new("id", None, Some(false))?,
            Field::new("\"Prix.€\"", None, None)?,
        ],
    )?;
    let number = "number(10,2)".parse()?;
    let replaced = source.with_column("id", lit_null(number));
    assert_eq!(replaced.columns()?.unwrap(), ["\"Prix.€\"", "ID"]);
    let preserved = source.with_column_order("id", lit_null(number), true);
    assert_eq!(preserved.columns()?, source.columns()?);

    let renamed = replaced.rename(replaced.col("id"), "\"réf\"").distinct();
    assert_eq!(
        renamed.schema()?.unwrap(),
        [
            Field::new("\"Prix.€\"", None, None)?,
            Field::new("\"réf\"", Some(number), Some(true))?,
        ],
    );
    let query = renamed.select(["\"réf\""]).to_sql()?;
    assert!(query.sql.contains("FROM (SELECT DISTINCT "));
    assert_eq!(query.binds, [BindValue::Null(number)]);
    assert_eq!(source.columns()?.unwrap(), ["ID", "\"Prix.€\""]);
    Ok(())
}

#[test]
fn distinct_keeps_limits_and_later_projections_in_separate_blocks() -> Result<(), CompileError> {
    let source = DataFrame::table_name("sales")?;
    let distinct = source.limit(2).distinct();
    assert_eq!(
        distinct.to_sql()?.sql,
        "SELECT DISTINCT t1.*\nFROM (SELECT t0.*\nFROM \"SALES\" AS t0\nLIMIT 2) AS t1",
    );
    assert!(
        distinct
            .select(["id"])
            .to_sql()?
            .sql
            .starts_with("SELECT t2.\"ID\"\nFROM (SELECT DISTINCT")
    );
    Ok(())
}

#[test]
fn outer_joins_mark_only_the_padded_sides_nullable() -> Result<(), CompileError> {
    let key = Field::new("id", Some("int".parse()?), Some(false))?;
    let left = DataFrame::table_name_with_schema("left_table", vec![key.clone()])?;
    let right = DataFrame::table_name_with_schema("right_table", vec![key])?;
    for (how, clause, nullable) in [
        (JoinType::Left, "LEFT OUTER JOIN", [false, true]),
        (JoinType::Right, "RIGHT OUTER JOIN", [true, false]),
        (JoinType::Full, "FULL OUTER JOIN", [true, true]),
    ] {
        let frame = left.join_as(&right, left.col("id").eq(right.col("id")), how);
        assert!(frame.to_sql()?.sql.contains(clause));
        assert_eq!(frame.columns()?.unwrap(), ["L_ID", "R_ID"]);
        assert_eq!(
            frame
                .schema()?
                .unwrap()
                .iter()
                .map(|field| field.nullable)
                .collect::<Vec<_>>(),
            nullable.map(Some),
        );
    }
    Ok(())
}

#[test]
fn unions_preserve_output_order_scope_and_bind_order() -> Result<(), CompileError> {
    let left =
        DataFrame::table_name("left_table")?.select([lit(1).alias("a"), lit(true).alias("b")]);
    let right =
        DataFrame::table_name("right_table")?.select([lit(false).alias("b"), lit(2).alias("c")]);
    let union = left.union_all_by_name(&right, true);
    assert_eq!(union.columns()?.unwrap(), ["A", "B", "C"]);
    assert_eq!(
        union
            .schema()?
            .unwrap()
            .iter()
            .map(|field| field.nullable)
            .collect::<Vec<_>>(),
        [Some(true), Some(false), Some(true)],
    );
    let query = union
        .filter(col("a").is_not_null())
        .select([lit("before").alias("prefix"), col("b").into()])
        .to_sql()?;
    assert!(query.sql.contains(")\nUNION ALL BY NAME\n(SELECT"));
    assert_eq!(
        query.binds,
        [
            "before".into(),
            1.into(),
            true.into(),
            false.into(),
            2.into()
        ]
    );
    assert_eq!(
        left.union_by_name(&right, false).to_sql(),
        Err(CompileError::UnionNames)
    );
    assert_eq!(left.union(&right).columns()?.unwrap(), ["A", "B"]);
    assert_eq!(
        left.union_all(&right.select(["c"])).to_sql(),
        Err(CompileError::UnionArity { left: 2, right: 1 })
    );
    Ok(())
}
