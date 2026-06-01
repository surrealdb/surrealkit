//! Output emitters for a [`SchemaTypes`] document.
//!
//! JSON is the only emitter today. A future TypeScript emitter would slot in
//! here as `to_typescript(&SchemaTypes) -> String`, consuming the same
//! structured document without re-introspecting the database.

use anyhow::Result;

use super::types::SchemaTypes;

pub fn to_json(doc: &SchemaTypes, pretty: bool) -> Result<String> {
	Ok(if pretty {
		serde_json::to_string_pretty(doc)?
	} else {
		serde_json::to_string(doc)?
	})
}
