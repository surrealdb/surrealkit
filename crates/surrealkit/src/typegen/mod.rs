//! `surrealkit typegen` — introspect a live database and emit a structured
//! schema document.
//!
//! JSON is the initial output format. The data model in [`types`] is designed
//! so additional emitters (e.g. TypeScript) can be added in [`emit`] without
//! re-introspecting the database.

mod emit;
mod introspect;
mod signature;
mod type_parser;
mod types;

use std::path::PathBuf;

use anyhow::Result;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
pub use types::{
	FieldDef, FieldType, FnArg, FunctionDef, NamedDef, ObjectField, ParamDef, PrimitiveType,
	SchemaTypes, TableDef,
};

/// Options for the `typegen` command.
#[derive(Debug, Clone, Default)]
pub struct TypegenOpts {
	/// Explicit output path. Overrides the default `{folder}/types/schema.json`.
	pub out: Option<PathBuf>,
	/// Write to stdout instead of a file.
	pub stdout: bool,
	/// Pretty-print the JSON.
	pub pretty: bool,
}

/// Introspect the database into a [`SchemaTypes`] document and stamp the
/// generation time. Performs no filesystem IO — used by emitters and tests.
pub async fn generate(db: &Surreal<Any>) -> Result<SchemaTypes> {
	let mut doc = introspect::introspect(db).await?;
	doc.generated_at = OffsetDateTime::now_utc().format(&Rfc3339)?;
	Ok(doc)
}

/// Run the `typegen` command: introspect the database, render JSON, and either
/// print it or write it to a file.
pub async fn run_typegen(
	db: &Surreal<Any>,
	folder: &str,
	namespace: &str,
	database: &str,
	opts: TypegenOpts,
) -> Result<()> {
	let mut doc = generate(db).await?;
	if !namespace.is_empty() {
		doc.namespace = Some(namespace.to_string());
	}
	if !database.is_empty() {
		doc.database = Some(database.to_string());
	}

	let json = emit::to_json(&doc, opts.pretty)?;

	if opts.stdout {
		println!("{json}");
		return Ok(());
	}

	let path = opts.out.unwrap_or_else(|| crate::constants::typegen_output_path(folder));
	if let Some(parent) = path.parent() {
		std::fs::create_dir_all(parent)?;
	}
	std::fs::write(&path, format!("{json}\n"))?;
	eprintln!("typegen: wrote {}", path.display());
	Ok(())
}
