mod arrow;
mod profile;

use std::{
    pin::Pin,
    task::{Context, Poll},
};

pub use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};
use futures_util::{Stream, TryStreamExt};
pub use profile::{SessionError, connect_profile};
pub use snowflake_connector_rs::Session;
use snowflake_connector_rs::{Error, Statement};

use crate::{BindValue, CompileError, CompiledQuery};

/// The stage at which an Arrow action failed.
#[derive(Debug, thiserror::Error)]
pub enum ExecuteError {
    #[error(transparent)]
    Compile(#[from] CompileError),
    #[error("Snowflake query submission failed ({:?}, code {:?}, query ID {:?})", .0.kind(), .0.snowflake_code(), .0.query_id())]
    Submit(#[source] Error),
    #[error("Snowflake result read failed ({:?}, code {:?}, query ID {query_id})", source.kind(), source.snowflake_code())]
    Read {
        query_id: String,
        #[source]
        source: Error,
    },
    #[error("Arrow conversion failed (query ID {query_id}): {source}")]
    Arrow {
        query_id: String,
        #[source]
        source: ArrowError,
    },
}

/// Ordered Arrow record batches backed by the Snowflake result cursor.
#[must_use]
pub struct ArrowBatches {
    schema: SchemaRef,
    query_id: String,
    stream: Pin<Box<dyn Stream<Item = Result<RecordBatch, ExecuteError>> + Send>>,
}

impl ArrowBatches {
    /// The result schema, including for queries that return zero rows.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// The Snowflake identifier for this query.
    pub fn query_id(&self) -> &str {
        &self.query_id
    }
}

impl Stream for ArrowBatches {
    type Item = Result<RecordBatch, ExecuteError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }
}

/// Execute an already-compiled query and return batches as they are read.
pub async fn execute_arrow_batches(
    session: &Session,
    query: CompiledQuery,
) -> Result<ArrowBatches, ExecuteError> {
    let mut statement = Statement::new_positional(query.sql);
    for value in query.binds {
        statement = match value {
            BindValue::Text(value) => statement.bind(value),
            BindValue::Int(value) => statement.bind(value),
            BindValue::Bool(value) => statement.bind(value),
            // The compiler casts these binds to the target Snowflake type.
            BindValue::Decimal(value) => statement.bind(value.to_string()),
            BindValue::Null(_) => statement.bind(Option::<String>::None),
        };
    }
    let mut cursor = session
        .query(statement)
        .await
        .map_err(ExecuteError::Submit)?;
    let query_id = cursor.query_id().to_owned();
    let schema = arrow::schema(cursor.schema()).map_err(|source| ExecuteError::Arrow {
        query_id: query_id.clone(),
        source,
    })?;

    let stream_schema = schema.clone();
    let stream_query_id = query_id.clone();
    let stream = async_stream::try_stream! {
        while let Some(table) = cursor.next_table().await.map_err(|source| ExecuteError::Read {
            query_id: stream_query_id.clone(),
            source,
        })? {
            if !table.is_empty() {
                yield arrow::batch(&table, stream_schema.clone())?;
            }
        }
    };
    Ok(ArrowBatches {
        schema,
        query_id,
        stream: Box::pin(stream),
    })
}

/// Execute an already-compiled query and materialize its Arrow result.
pub async fn execute_arrow(
    session: &Session,
    query: CompiledQuery,
) -> Result<RecordBatch, ExecuteError> {
    let batches = execute_arrow_batches(session, query).await?;
    let schema = batches.schema.clone();
    let query_id = batches.query_id.clone();
    let batches: Vec<_> = batches.try_collect().await?;
    arrow_select::concat::concat_batches(&schema, &batches)
        .map_err(|source| ExecuteError::Arrow { query_id, source })
}
