use std::{error::Error, time::Duration};

use arrow_array::{BooleanArray, Decimal128Array, StringArray};
use futures_util::TryStreamExt;
use snowpark_rs::{
    DataFrame, Decimal, JoinType, Session, col, count, count_all, lit, lit_null,
    session::connect_profile, sum,
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
