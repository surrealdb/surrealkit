//! `surrealkit typegen` — introspect a live database and emit a structured
//! schema document.
//!
//! JSON is the initial output format. The data model ([`SchemaTypes`] and
//! friends) is designed so additional emitters can be added without
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
	/// When set, also emit TypeScript types: to this file when it ends in `.ts`
	/// (or `.mts`/`.cts`), otherwise to `index.ts` in this directory. Configured
	/// via `[typegen] typescript` and `filename` in `surrealkit.toml`.
	pub ts_out: Option<PathBuf>,
	/// Optional formatter command run on the generated TypeScript file after writing.
	/// Configured via `[typegen] format` in `surrealkit.toml`.
	pub ts_format: Option<String>,
}

/// Render a [`SchemaTypes`] document as TypeScript. Re-exported so callers
/// (e.g. the sync/watch loop) can emit types without re-introspecting.
pub fn render_typescript(doc: &SchemaTypes) -> Result<String> {
	emit::to_typescript(doc)
}

/// The file TypeScript types go to for `out`: `out` itself when it names a
/// `.ts`, `.mts` or `.cts` file, otherwise `index.ts` inside the directory `out`.
pub fn typescript_file(out: &std::path::Path) -> PathBuf {
	if crate::variables::is_typescript_file(out) {
		out.to_path_buf()
	} else {
		out.join(crate::variables::DEFAULT_TYPESCRIPT_FILE)
	}
}

/// Write the TypeScript types for `doc` to [`typescript_file`]`(out)`, creating
/// its directory if needed. Returns the path written.
pub fn write_typescript(doc: &SchemaTypes, out: &std::path::Path) -> Result<PathBuf> {
	let ts = emit::to_typescript(doc)?;
	let path = typescript_file(out);
	if let Some(dir) = path.parent().filter(|dir| !dir.as_os_str().is_empty()) {
		std::fs::create_dir_all(dir)?;
	}
	std::fs::write(&path, ts)?;
	Ok(path)
}

/// Write the TypeScript types and, when `format` is set, run that formatter
/// command on the written file so the output matches the project's house style
/// (Biome / ESLint / Prettier). Returns the path written.
pub fn write_typescript_formatted(
	doc: &SchemaTypes,
	out: &std::path::Path,
	format: Option<&str>,
) -> Result<PathBuf> {
	let path = write_typescript(doc, out)?;
	if let Some(cmd) = format {
		format_file(cmd, &path);
	}
	Ok(path)
}

/// Run a user-configured formatter on `path`. The command is split on
/// whitespace (first token is the program, the rest are arguments) and the
/// file path is appended as the final argument — e.g. `biome check --write`
/// becomes `biome check --write <path>`. The command inherits the current
/// working directory so the formatter discovers the project's own config.
///
/// Failures (missing binary, non-zero exit) are non-fatal: they are reported as
/// warnings so a missing formatter never breaks `typegen` or `sync --watch`.
pub fn format_file(command: &str, path: &std::path::Path) {
	let mut parts = command.split_whitespace();
	let Some(program) = parts.next() else {
		return; // empty / whitespace-only command: nothing to run
	};
	let args: Vec<&str> = parts.collect();
	match std::process::Command::new(program).args(&args).arg(path).status() {
		Ok(status) if status.success() => {
			log::info!("typegen: formatted {} with `{command}`", path.display());
		}
		Ok(status) => {
			log::warn!("typegen: formatter `{command}` exited with {status}");
		}
		Err(err) => {
			log::error!("typegen: failed to run formatter `{command}`: {err}");
		}
	}
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
#[doc(hidden)]
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
		log::info!("{json}");
		return Ok(());
	}

	let path = opts.out.unwrap_or_else(|| crate::constants::typegen_output_path(folder));
	if let Some(parent) = path.parent() {
		std::fs::create_dir_all(parent)?;
	}
	std::fs::write(&path, format!("{json}\n"))?;
	log::info!("typegen: wrote {}", path.display());

	if let Some(ts_out) = &opts.ts_out {
		let ts_path = write_typescript_formatted(&doc, ts_out, opts.ts_format.as_deref())?;
		log::info!("typegen: wrote {}", ts_path.display());
	}
	Ok(())
}
