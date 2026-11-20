use std::{error::Error, time::Duration};

use arrow_array::{BooleanArray, Decimal128Array, StringArray};
use futures_util::TryStreamExt;
use snowpark_rs::{
    DataFrame, Decimal, JoinType, Projection, Session, col, count, count_all, lit, lit_null,
    row_number, session::connect_profile, star, sum,
};

#[test]
#[ignore = "requires a named Snowflake profile and a live connection"]
fn snowflake_connection() -> Result<(), Box<dyn Error>> {
    with_session(async |session| {
        let rows: Vec<(i64,)> = session
            .query_as::<(i64,), _>("SELECT 1")
            .await?
            .collect()
            .await?;
        assert_eq!(rows, [(1,)]);
        Ok(())
    })
}

#[test]
#[ignore = "requires a named Snowflake profile with permission to create a temporary table in PUBLIC.PUBLIC"]
fn snowflake_dataframe_uses_the_explicit_session() -> Result<(), Box<dyn Error>> {
    with_session(async |first| {
        let second = connect_profile(None).await?;
        let frame = DataFrame::table_name("SNOWPARK_RS_SESSION_FIXTURE")?.select(["id"]);

        for (session, id) in [(&first, 1), (&second, 2)] {
            session.query("USE SCHEMA PUBLIC.PUBLIC").await?;
            session
                .query(format!(
                    "CREATE TEMPORARY TABLE SNOWPARK_RS_SESSION_FIXTURE AS SELECT {id} AS ID"
                ))
                .await?;
        }

        for (session, id) in [(&first, 1), (&second, 2)] {
            let rows = frame.to_arrow(session).await?;
            assert_eq!(decimals(&rows, 0).iter().collect::<Vec<_>>(), [Some(id)]);
        }
        Ok(())
    })
}

#[test]
#[ignore = "requires a named Snowflake profile and the PUBLIC.PUBLIC.EXAMPLE fixture"]
fn snowflake_selects_star_with_modifiers() -> Result<(), Box<dyn Error>> {
    with_session(async |session| {
        let input = DataFrame::table_name("PUBLIC.PUBLIC.EXAMPLE")?
            .filter(col("id").eq(lit(1)))
            .select([
                col("id").alias(r#""Refé'?\A""#),
                col("name").into(),
                col("category").into(),
            ]);
        let replaced = input.select([star()
            .exclude(["category"])
            .replace("name", lit("replacement"))]);
        let projected = replaced.select([
            star()
                .ilike(r"ref_'?\\A")
                .replace(r#""Refé'?\A""#, lit(42))
                .rename(r#""Refé'?\A""#, r#""result id""#)
                .into(),
            Projection::from("name"),
        ]);

        let rows = projected.to_arrow(&session).await?;
        assert_eq!(rows.schema().field(0).name(), "result id");
        assert_eq!(decimals(&rows, 0).iter().collect::<Vec<_>>(), [Some(42)]);
        let names = rows
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.iter().collect::<Vec<_>>(), [Some("replacement")]);

        let empty = projected.limit(0).to_arrow(&session).await?;
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(empty.schema(), rows.schema());
        Ok(())
    })
}

#[test]
#[ignore = "requires a named Snowflake profile and the PUBLIC.PUBLIC.EXAMPLE fixture"]
fn snowflake_dataframe() -> Result<(), Box<dyn Error>> {
    with_session(async |session| {
        let example = DataFrame::table_name("PUBLIC.PUBLIC.EXAMPLE")?;
        let filtered = example
            .filter(col("category").eq(lit("A")))
            .select(["id", "name"]);
        let result = filtered.to_arrow(&session).await?;
        let schema = result.schema();
        let names: Vec<_> = schema
            .fields()
            .iter()
            .map(|column| column.name().as_str())
            .collect();
        assert_eq!(names, ["ID", "NAME"]);

        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        let names = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut rows: Vec<_> = ids.iter().zip(names.iter()).collect();
        rows.sort_unstable();
        assert_eq!(rows, [(Some(1), Some("Alice")), (Some(3), Some("Charlie"))]);

        let batches: Vec<_> = filtered
            .to_arrow_batches(&session)
            .await?
            .try_collect()
            .await?;
        let batches = arrow_select::concat::concat_batches(&result.schema(), &batches)?;
        assert_eq!(batches.num_rows(), result.num_rows());
        assert_eq!(batches.schema(), result.schema());

        let empty = example
            .filter(col("name").eq(lit("O'Neil")))
            .select(["id", "name"])
            .to_arrow(&session)
            .await?;
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(empty.schema(), result.schema());

        let nested = example
            .filter(col("id").eq(lit(1)))
            .select([lit("inside").alias("label")])
            .filter(col("label").eq(lit("inside")))
            .select([lit(true).alias("flag")])
            .to_arrow(&session)
            .await?;
        let flags = nested
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(flags.iter().collect::<Vec<_>>(), [Some(true)]);
        Ok(())
    })
}

#[test]
#[ignore = "requires a named Snowflake profile with permission to create a temporary table in PUBLIC.PUBLIC"]
fn snowflake_identifier_resolution_and_quoted_question_marks() -> Result<(), Box<dyn Error>> {
    with_session(async |session| {
        session
            .query("ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE")
            .await?;
        session.query("USE SCHEMA PUBLIC.PUBLIC").await?;
        let ascii = "A".repeat(255);
        let unicode = "界".repeat(255);
        let table = r#""Snowpark.Régression?""#;
        session.query(format!(
            "CREATE TEMPORARY TABLE {table} (ID NUMBER(10,0), \"id\" NUMBER(10,0), \"Été?\" VARCHAR, {ascii} NUMBER(10,0), \"{unicode}\" NUMBER(10,0))"
        )).await?;
        session
            .query(format!("INSERT INTO {table} VALUES (1, 2, 'été?', 3, 4)"))
            .await?;

        for name in [
            table.to_owned(),
            format!("PUBLIC.{table}"),
            format!("PUBLIC.PUBLIC.{table}"),
        ] {
            let frame = DataFrame::table_name(&name)?;
            let result = frame
                .filter(col(r#""Été?""#).eq(lit("été?")))
                .select([
                    col("id").into(),
                    col(r#""id""#).into(),
                    col(&ascii).into(),
                    col(format!("\"{unicode}\"")).into(),
                    lit("bind?").alias(r#""alias?""#),
                ])
                .to_arrow(&session)
                .await?;
            assert_eq!(result.num_rows(), 1);
            for (index, expected) in [1, 2, 3, 4].into_iter().enumerate() {
                assert_eq!(
                    decimals(&result, index).iter().collect::<Vec<_>>(),
                    [Some(expected)]
                );
            }
            assert_eq!(result.schema().field(0).name(), "ID");
            assert_eq!(result.schema().field(1).name(), "id");
            assert_eq!(result.schema().field(2).name(), &ascii);
            assert_eq!(result.schema().field(3).name(), &unicode);
            assert_eq!(result.schema().field(4).name(), "alias?");
            assert_eq!(
                result
                    .column(4)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                "bind?"
            );
        }
        Ok(())
    })
}

#[test]
#[ignore = "requires a named Snowflake profile with permission to create a temporary table in PUBLIC.PUBLIC"]
fn snowflake_three_valued_logic_on_nullable_cells() -> Result<(), Box<dyn Error>> {
    with_session(async |session| {
        session.query(
            "CREATE TEMPORARY TABLE PUBLIC.PUBLIC.SNOWPARK_RS_BOOLEAN_FIXTURE (ID NUMBER(10,0), L BOOLEAN, R BOOLEAN)"
        ).await?;
        session
            .query(
                "INSERT INTO PUBLIC.PUBLIC.SNOWPARK_RS_BOOLEAN_FIXTURE VALUES \
            (1, TRUE, TRUE), (2, TRUE, FALSE), (3, TRUE, NULL), \
            (4, FALSE, TRUE), (5, FALSE, FALSE), (6, FALSE, NULL), \
            (7, NULL, TRUE), (8, NULL, FALSE), (9, NULL, NULL)",
            )
            .await?;
        let frame = DataFrame::table_name("PUBLIC.PUBLIC.SNOWPARK_RS_BOOLEAN_FIXTURE")?;
        let l = col("l");
        let r = col("r");
        let result = frame
            .select([
                col("id").into(),
                l.clone().and(r.clone()).alias("both"),
                l.clone().or(r.clone()).alias("either"),
                (!l.clone()).alias("negated"),
                l.clone().eq(r.clone()).alias("equal"),
                l.clone().is_null().alias("missing"),
                l.clone().is_not_null().alias("present"),
                l.clone().eq_missing(r.clone()).alias("equal_missing"),
            ])
            .sort(["id"])
            .to_arrow(&session)
            .await?;
        assert_eq!(
            decimals(&result, 0).values().as_ref(),
            [1, 2, 3, 4, 5, 6, 7, 8, 9]
        );
        let t = Some(true);
        let f = Some(false);
        let u = None;
        for (column, expected) in [
            [t, f, u, f, f, f, u, f, u],
            [t, t, t, t, f, u, t, u, u],
            [f, f, f, t, t, t, u, u, u],
            [t, f, u, f, t, u, u, u, u],
            [f, f, f, f, f, f, t, t, t],
            [t, t, t, t, t, t, f, f, f],
            [t, f, f, f, t, f, f, f, t],
        ]
        .into_iter()
        .enumerate()
        {
            let values = result
                .column(column + 1)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            assert_eq!(
                values.iter().collect::<Vec<_>>(),
                expected,
                "{}",
                result.schema().field(column + 1).name()
            );
        }
        for (predicate, expected) in [
            (l.clone().and(r.clone()), vec![1]),
            (l.clone().or(r.clone()), vec![1, 2, 3, 4, 7]),
            (!l.clone(), vec![4, 5, 6]),
            (l.clone().eq(r.clone()), vec![1, 5]),
            (l.is_null(), vec![7, 8, 9]),
        ] {
            let filtered = frame
                .filter(predicate)
                .select(["id"])
                .sort(["id"])
                .to_arrow(&session)
                .await?;
            assert_eq!(decimals(&filtered, 0).values().as_ref(), expected);
        }
        Ok(())
    })
}

#[test]
#[ignore = "requires a named Snowflake profile and the PUBLIC.PUBLIC.EXAMPLE fixture"]
fn snowflake_filters_groups_and_windows_before_selecting_the_tail() -> Result<(), Box<dyn Error>> {
    with_session(async |session| {
        let source = DataFrame::table_name("PUBLIC.PUBLIC.EXAMPLE")?;
        let groups = source
            .group_by(["category"])
            .agg([sum("amount").alias("total")])
            .having(col("total").gt(lit(20)));
        let rank = row_number([], [col("total").desc()]);
        let result = groups
            .select([col("category").into(), rank.alias("position")])
            .qualify(col("position").le(lit(2)))
            .distinct()
            .sort(["position"])
            .head(10)
            .tail(2)
            .to_arrow(&session)
            .await?;
        let categories = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            categories.iter().collect::<Vec<_>>(),
            [Some("B"), Some("A")]
        );
        assert_eq!(decimals(&result, 1).values().as_ref(), &[1, 2]);

        let smallest = source
            .select(["category", "amount"])
            .group_by([col("category").eq(lit("C")).alias("category")])
            .agg([count_all().alias("rows")])
            .having(col("category"))
            .to_arrow(&session)
            .await?;
        assert_eq!(smallest.num_rows(), 1);
        Ok(())
    })
}

#[test]
#[ignore = "requires a named Snowflake profile and the PUBLIC.PUBLIC.EXAMPLE fixture"]
fn snowflake_grouped_totals() -> Result<(), Box<dyn Error>> {
    with_session(async |session| {
        let result = DataFrame::table_name("PUBLIC.PUBLIC.EXAMPLE")?
            .group_by(["category"])
            .agg([sum("amount").alias("total")])
            .sort(["category"])
            .to_arrow(&session)
            .await?;
        let categories = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let totals = decimals(&result, 1);
        assert_eq!(totals.scale(), 2);
        assert_eq!(
            categories.iter().zip(totals.iter()).collect::<Vec<_>>(),
            [
                (Some("A"), Some(36050)),
                (Some("B"), Some(40019)),
                (Some("C"), Some(1575))
            ]
        );
        Ok(())
    })
}

#[test]
#[ignore = "requires a named Snowflake profile with permission to create a temporary table"]
fn snowflake_relational_nulls_limits_and_join_cardinality() -> Result<(), Box<dyn Error>> {
    with_session(async |session| {
        session
            .query(
                "CREATE TEMPORARY TABLE PUBLIC.PUBLIC.SNOWPARK_RS_RELATIONAL_FIXTURE \
             (ROW_ID NUMBER(10,0), JOIN_KEY NUMBER(10,0), AMOUNT NUMBER(10,2))",
            )
            .await?;
        session
            .query(
                "INSERT INTO PUBLIC.PUBLIC.SNOWPARK_RS_RELATIONAL_FIXTURE VALUES \
             (1, 1, 12.50), (2, 1, NULL), (3, 2, 7.25), (4, NULL, NULL)",
            )
            .await?;
        let frame = DataFrame::table_name("PUBLIC.PUBLIC.SNOWPARK_RS_RELATIONAL_FIXTURE")?;
        let aggregates = [
            count_all().alias("total_rows"),
            count("amount").alias("present"),
            sum("amount").alias("total"),
        ];
        let all = frame.agg(aggregates.clone()).to_arrow(&session).await?;
        assert_eq!(decimals(&all, 0).iter().collect::<Vec<_>>(), [Some(4)]);
        assert_eq!(decimals(&all, 1).iter().collect::<Vec<_>>(), [Some(2)]);
        assert_eq!(decimals(&all, 2).iter().collect::<Vec<_>>(), [Some(1975)]);
        for (input, count) in [
            (frame.limit(0), 0),
            (frame.filter(col("amount").is_null()), 2),
        ] {
            let result = input.agg(aggregates.clone()).to_arrow(&session).await?;
            assert_eq!(
                decimals(&result, 0).iter().collect::<Vec<_>>(),
                [Some(count)]
            );
            assert_eq!(decimals(&result, 1).iter().collect::<Vec<_>>(), [Some(0)]);
            assert_eq!(decimals(&result, 2).iter().collect::<Vec<_>>(), [None]);
        }

        let nulls_first = frame
            .sort([col("join_key").asc_nulls_first(), col("row_id").desc()])
            .limit(10)
            .to_arrow(&session)
            .await?;
        assert_eq!(decimals(&nulls_first, 0).values().as_ref(), [4, 2, 1, 3]);
        let limited_then_filtered = frame
            .sort([col("join_key").asc_nulls_last(), col("row_id").desc()])
            .limit(2)
            .filter(col("row_id").gt(lit(2)))
            .to_arrow(&session)
            .await?;
        assert_eq!(limited_then_filtered.num_rows(), 0);

        let left = frame.select(["row_id", "join_key"]);
        let right = frame.select(["row_id", "join_key"]);
        let joined = left
            .join(&right, left.col("join_key").eq(right.col("join_key")))
            .select(["l_row_id", "r_row_id"])
            .sort(["l_row_id", "r_row_id"])
            .to_arrow(&session)
            .await?;
        assert_eq!(
            decimals(&joined, 0)
                .iter()
                .zip(decimals(&joined, 1).iter())
                .collect::<Vec<_>>(),
            [
                (Some(1), Some(1)),
                (Some(1), Some(2)),
                (Some(2), Some(1)),
                (Some(2), Some(2)),
                (Some(3), Some(3))
            ]
        );

        let outer = left
            .join_as(
                &right,
                left.col("join_key").eq(right.col("join_key")),
                JoinType::Full,
            )
            .select(["l_row_id", "r_row_id"])
            .to_arrow(&session)
            .await?;
        let rows: Vec<_> = decimals(&outer, 0)
            .iter()
            .zip(decimals(&outer, 1).iter())
            .collect();
        assert_eq!(rows.len(), 7);
        assert!(rows.contains(&(Some(4), None)));
        assert!(rows.contains(&(None, Some(4))));
        Ok(())
    })
}

#[test]
#[ignore = "requires a named Snowflake profile and the PUBLIC.PUBLIC.EXAMPLE fixture"]
fn snowflake_typed_binds_and_name_based_union() -> Result<(), Box<dyn Error>> {
    with_session(async |session| {
        let source = DataFrame::table_name("PUBLIC.PUBLIC.EXAMPLE")?;
        let number = "number(38,4)".parse()?;
        let price: Decimal = "1234567890123456789012345678901234.5678".parse()?;
        let coefficient = 12345678901234567890123456789012345678_i128;
        let first = source
            .filter(col("id").eq(lit(1)))
            .select(["id"])
            .with_column("price", lit(price))
            .with_column("missing", lit_null(number));
        let second = source.filter(col("id").eq(lit(2))).select(["id"]);
        let combined = first.union_all_by_name(&second, true);
        assert_eq!(
            combined.schema_with_session(&session).await?.fields().len(),
            3
        );
        assert_eq!(
            combined
                .union_all(&first)
                .to_arrow(&session)
                .await?
                .num_rows(),
            3
        );
        let rows = combined
            .union_all(&first)
            .distinct()
            .sort(["id"])
            .to_arrow(&session)
            .await?;
        assert_eq!(rows.num_rows(), 2);
        let prices = decimals(&rows, 1);
        assert_eq!(prices.scale(), 4);
        assert_eq!(prices.iter().collect::<Vec<_>>(), [Some(coefficient), None]);
        assert_eq!(decimals(&rows, 2).iter().collect::<Vec<_>>(), [None, None]);
        Ok(())
    })
}

fn decimals(batch: &arrow_array::RecordBatch, column: usize) -> &Decimal128Array {
    batch.column(column).as_any().downcast_ref().unwrap()
}

fn with_session(
    test: impl AsyncFnOnce(Session) -> Result<(), Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(30), async {
            test(connect_profile(None).await?).await
        })
        .await?
    })
}
