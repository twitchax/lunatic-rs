//! Sqlite database client.
//!
//! # Example
//!
//! ```no_run
//! use lunatic::sqlite::{SqliteClient, Query};
//!
//! # fn main() -> Result<(), lunatic::sqlite::SqliteError> {
//! let client = SqliteClient::connect("app.db")?;
//!
//! // Query users
//! let mut rows = client.query("select * from users");
//! for row in rows {
//!     // ...
//! }
//! # Ok(())
//! # }
//! ```

mod client;
mod error;
mod query;
mod value;

pub use client::*;
pub use error::*;
pub use query::*;
pub use value::*;
