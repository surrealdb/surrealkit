//! The engine seam: asking a database to type queries.

use anyhow::Result;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;

use super::document::TypesDocument;

/// Asks `db` to type `inputs` against its schema without executing them, and returns the
/// engine's [`TypesDocument`]: one entry per input, each named as its input is, in input order.
/// An input is `(name, text)`; an inline query has no name.
///
/// Returns `None` when the engine cannot type queries (SurrealDB before 4.1). The `surrealdb`
/// SDK SurrealKit is built with has no `types` method, so it always returns `None`.
pub async fn types_document(
	_db: &Surreal<Any>,
	_inputs: &[(Option<String>, String)],
) -> Result<Option<TypesDocument>> {
	Ok(None)
}
