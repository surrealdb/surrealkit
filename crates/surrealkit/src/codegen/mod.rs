//! `surrealkit generate`: TypeScript types from the engine's `TypesDocument`.
//!
//! [`document`] reads the document the engine's `types_document` produces,
//! [`typescript`] renders it as a module of types for the surrealdb JS SDK, and
//! [`extract`] finds the queries host code sends, so each one gets a typed
//! result in the rendered registry.

pub mod document;
#[cfg(feature = "host-queries")]
pub mod extract;
mod kind;
pub mod typescript;
pub mod v2;
