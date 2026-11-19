//! Build immutable DataFrame queries for Snowflake.
//!
//! ```
//! use snowpark_rs::{DataFrame, col, lit};
//!
//! let frame = DataFrame::table(["public", "public", "example"])?
//!     .filter(col("category").eq(lit("A")))
//!     .select(["id", "name"]);
//! let query = frame.to_sql()?;
//! assert_eq!(query.binds.len(), 1);
//! # Ok::<(), snowpark_rs::CompileError>(())
//! ```
//!
//! Enable the `session` feature to execute a frame and return Arrow results.

mod dataframe;
#[cfg(feature = "python")]
mod python;
#[cfg(feature = "session")]
pub mod session;

pub use dataframe::{DataFrame, GroupedDataFrame};
#[cfg(feature = "session")]
pub use session::Session;
pub use snowflake_ast::{
    BindValue, Column, CompileError, CompiledQuery, DataType, Decimal, Field, JoinType, Projection,
    SortKey, col, count, count_all, lit, lit_null, sum,
};

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}
