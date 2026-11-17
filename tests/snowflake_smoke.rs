use std::{error::Error, time::Duration};

use arrow_array::{BooleanArray, Decimal128Array, StringArray};
use futures_util::TryStreamExt;
use snowpark_rs::{DataFrame, Session, col, lit, session::connect_profile};

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
