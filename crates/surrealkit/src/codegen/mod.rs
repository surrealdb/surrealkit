//! Per-query TypeScript from the `queries` section of schema document v2.
//!
//! [`document`] reads the document the engine's `types_document` produces, [`v2`] converts its
//! kinds to v2 [`Kind`](crate::typegen::document::Kind)s, and [`queries`] builds the section's
//! entries, which hold each query's typed result in v2's vocabulary. [`typescript`] renders those entries
//! as a module of types for the surrealdb JS SDK: a registry keyed by exact query text, and a
//! typed client over it. [`query_files`] reads named queries from `.surql` files, and [`section`]
//! collects them and the queries host code sends ([`crate::host_queries`]) and assembles the
//! section.
//! [`engine`] is the seam to the engine's query typing, and [`watch_cache`] keeps its answers
//! between the runs of `compile --watch`. Table types are not rendered here: the built-in typegen
//! output owns them.

pub mod document;
pub mod engine;
mod kind;
pub mod queries;
pub mod query_files;
pub mod section;
pub mod typescript;
pub mod v2;
pub mod watch_cache;
