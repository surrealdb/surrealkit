//! Per-query TypeScript from the `queries` section of schema document v2.
//!
//! [`document`] reads the document the engine's `types_document` produces, [`v2`] converts its
//! kinds to v2 [`Kind`](crate::typegen::document::Kind)s, and [`queries`] is the entry model
//! that holds each query's typed result in v2's vocabulary. [`typescript`] renders those entries
//! as a module of types for the surrealdb JS SDK: a registry keyed by exact query text, and a
//! typed client over it. [`extract`] finds the queries host code sends. Table types are not
//! rendered here: the built-in typegen output owns them.

pub mod document;
#[cfg(feature = "host-queries")]
pub mod extract;
mod kind;
pub mod queries;
pub mod typescript;
pub mod v2;
