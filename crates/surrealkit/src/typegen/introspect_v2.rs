//! Reading a database into schema document v2.
//!
//! [`capture`] runs the `INFO` statements; [`assemble`] is pure and turns what was captured
//! into a document, so the corpus snapshots under `tests/fixtures/typegen_corpus/introspection`
//! go through exactly the code a live database does.
//!
//! A supported engine minor is read through its `INFO … STRUCTURE` adapter, with plain `INFO`
//! attached as `define` evidence. Any other minor is read from plain `INFO` by the fallback
//! reader, and the document carries the error `structure-unsupported-engine`, so a strict
//! consumer refuses it while a lenient one still gets the best available description.

use anyhow::{Context, Result};
use serde_json::Value;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb_types::SurrealValue;

use super::build_v2::build;
use super::diagnostics::{Diagnostic, Severity, codes};
use super::document::{
	Capabilities, Engine, EngineMode, Generator, Provenance, SchemaDocument, Source,
};
use super::structure::{Adapter, StructureInput};
use super::{info_v2, structure};

/// The `provenance.adapter` of a document read by the plain-`INFO` fallback.
pub const INFO_ADAPTER: &str = "info";

/// Everything one read of a database returned.
#[derive(Debug, Clone)]
pub struct Snapshot {
	/// `MAJOR.MINOR.PATCH` of the engine.
	pub version: String,
	/// The engine's full version string.
	pub build: Option<String>,
	pub db_info: Value,
	/// `INFO FOR DB STRUCTURE`; absent when the engine minor has no adapter.
	pub db_structure: Option<Value>,
	pub tables_info: Vec<(String, Value)>,
	pub tables_structure: Vec<(String, Value)>,
}

pub(crate) async fn query_json(db: &Surreal<Any>, sql: &str) -> Result<Value> {
	let mut response = db.query(sql).await?.check()?;
	let raw: surrealdb_types::Value = response.take(0)?;
	Value::from_value(raw).context("converting the INFO result to JSON")
}

fn quoted(name: &str) -> String {
	format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
}

/// Read the connected database's schema.
pub async fn capture(db: &Surreal<Any>) -> Result<Snapshot> {
	let semver = db.version().await.context("reading the engine version")?;
	let version = format!("{}.{}.{}", semver.major, semver.minor, semver.patch);
	let build = Some(format!("surrealdb-{semver}"));
	let structured = Adapter::for_engine(&version).is_some();

	let db_info = query_json(db, "INFO FOR DB;").await.context("INFO FOR DB")?;
	let db_structure = if structured {
		Some(query_json(db, "INFO FOR DB STRUCTURE;").await.context("INFO FOR DB STRUCTURE")?)
	} else {
		None
	};
	let names: Vec<String> = match db_info.get("tables") {
		Some(Value::Object(map)) => map.keys().cloned().collect(),
		_ => Vec::new(),
	};
	let mut tables_info = Vec::new();
	let mut tables_structure = Vec::new();
	for name in names {
		let info = query_json(db, &format!("INFO FOR TABLE {};", quoted(&name)))
			.await
			.with_context(|| format!("INFO FOR TABLE {name}"))?;
		tables_info.push((name.clone(), info));
		if structured {
			let info = query_json(db, &format!("INFO FOR TABLE {} STRUCTURE;", quoted(&name)))
				.await
				.with_context(|| format!("INFO FOR TABLE {name} STRUCTURE"))?;
			tables_structure.push((name, info));
		}
	}
	Ok(Snapshot {
		version,
		build,
		db_info,
		db_structure,
		tables_info,
		tables_structure,
	})
}

/// The namespace and database the session uses.
pub async fn session(db: &Surreal<Any>) -> Result<(String, String)> {
	let value = query_json(db, "RETURN [session::ns(), session::db()];").await?;
	Ok((
		value.get(0).and_then(Value::as_str).unwrap_or_default().to_string(),
		value.get(1).and_then(Value::as_str).unwrap_or_default().to_string(),
	))
}

/// The SurrealKit build, for `provenance.generator`.
pub fn generator() -> Generator {
	let commit = option_env!("SURREALKIT_COMMIT")
		.filter(|c| {
			(7..=40).contains(&c.len())
				&& c.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
		})
		.map(str::to_string);
	Generator {
		name: "surrealkit".to_string(),
		version: env!("CARGO_PKG_VERSION").to_string(),
		commit,
	}
}

/// How the document's source is described, and the compile engine's capabilities.
pub struct Origin {
	pub source: Source,
	pub mode: EngineMode,
	pub capabilities: Option<Capabilities>,
	/// Diagnostics found before the database was read (e.g. duplicate definitions).
	pub diagnostics: Vec<Diagnostic>,
}

/// Turn a [`Snapshot`] into a document. Pure.
pub fn assemble(snapshot: &Snapshot, origin: Origin) -> Result<SchemaDocument> {
	let adapter = Adapter::for_engine(&snapshot.version);
	let (mut raw, adapter_name) = match (adapter, &snapshot.db_structure) {
		(Some(adapter), Some(db)) => {
			let input = StructureInput {
				db,
				tables: &snapshot.tables_structure,
				db_info: Some(&snapshot.db_info),
				tables_info: &snapshot.tables_info,
			};
			(structure::read(adapter, &input), adapter.name().to_string())
		}
		_ => {
			let mut raw = info_v2::read(&snapshot.db_info, &snapshot.tables_info);
			raw.diagnostics.push(Diagnostic::new(
				Severity::Error,
				codes::STRUCTURE_UNSUPPORTED_ENGINE,
				snapshot.build.clone().unwrap_or_else(|| snapshot.version.clone()),
				format!(
					"SurrealDB {} has no INFO STRUCTURE adapter (supported: 3.2.x, 3.3.x); the \
					 document was read from plain INFO",
					snapshot.version
				),
			));
			(raw, INFO_ADAPTER.to_string())
		}
	};
	raw.diagnostics.extend(origin.diagnostics);
	let provenance = Provenance {
		generator: generator(),
		engine: Engine {
			version: snapshot.version.clone(),
			build: snapshot.build.clone(),
			mode: origin.mode,
		},
		adapter: adapter_name,
		source: origin.source,
		capabilities: origin.capabilities,
	};
	build(raw, provenance)
}

/// Read the connected database (a server, unless `mode` says otherwise) into a document.
pub async fn introspect(db: &Surreal<Any>, mode: EngineMode) -> Result<SchemaDocument> {
	let snapshot = capture(db).await?;
	let (namespace, database) = session(db).await.unwrap_or_default();
	assemble(
		&snapshot,
		Origin {
			source: Source::Introspect {
				namespace,
				database,
			},
			mode,
			capabilities: None,
			diagnostics: Vec::new(),
		},
	)
}

/// Read the same snapshot with the plain-`INFO` reader regardless of the engine minor.
#[cfg(test)]
pub(super) fn assemble_from_info(snapshot: &Snapshot) -> Result<SchemaDocument> {
	let unstructured = Snapshot {
		db_structure: None,
		tables_structure: Vec::new(),
		version: "0.0.0".to_string(),
		..snapshot.clone()
	};
	let mut doc = assemble(
		&unstructured,
		Origin {
			source: Source::Introspect {
				namespace: String::new(),
				database: String::new(),
			},
			mode: EngineMode::Remote,
			capabilities: None,
			diagnostics: Vec::new(),
		},
	)?;
	doc.provenance.engine.version.clone_from(&snapshot.version);
	Ok(doc)
}
