//! Snowflake destination implementation.
//!
//! Provides an append-only destination for replicating Postgres data to Snowflake.
//! Events are written as a log table with CDC operation types and sequence numbers.

mod client;
mod encoding;

pub use client::SnowflakeClient;
