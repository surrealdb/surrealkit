//! `surrealkit compile`: schema document v2 from `.surql` files, with no database.
//!
//! The selected schema modules are applied, in dependency order, to a throwaway embedded
//! in-memory engine (`mem://`, the `kv-mem` engine SurrealKit already ships), and that engine
//! is read back through the same introspection `surrealkit typegen` uses. The result
//! describes what the engine stored, after its own normalisation, and equals what
//! introspecting a database synced from the same files would give **on the same engine
//! minor**.
//!
//! The embedded engine is the `surrealdb` version SurrealKit is built with (3.3), whatever
//! server a project deploys to. A store pinned to another minor (3.2) gets a document shaped
//! by 3.3: 3.3's normalisation and 3.3-only properties, and 3.2-only spellings (such as
//! `DEFINE MODULE … AS f"…"`) are refused. `CompileOpts::expected_engine` (`[typegen] engine`)
//! names the minor a project deploys to; when it differs from the embedded one, the document
//! carries the error `compile-engine-mismatch`.
//!
//! Hermetic by construction:
//!
//! - no connection settings are read: neither `SURREALDB_*` variables, `.env`, nor targets;
//!   template variables come from `surrealkit.toml` and `--var` only, never from
//!   `SURREALKIT_VAR_*` in the environment;
//! - the engine runs with restricted capabilities: no network targets (so `http::*` cannot
//!   reach anything), no scripting, only the experimental features asked for;
//! - nothing outlives the call: the engine is dropped with its data.
//!
//! Statements are applied the way `sync` deploys them ([`prepare_schema_sql`]): every `DEFINE`
//! except a sequence as `OVERWRITE`. A file `sync` would refuse (a non-`DEFINE` statement, a
//! `REMOVE`, `DEFINE NAMESPACE`) is refused here too. When the selected modules define one
//! entity more than once, the document carries the warning `definition-duplicate`, because
//! raw SurrealQL and SurrealKit would read such a module differently.
//!
//! A `DEFINE PARAM` stores its `VALUE` evaluated once, when it is defined. A value that is
//! different every time (`time::now()`, `rand::uuid()`) makes the document different on every
//! run, so compile defines each param a second time and reports one whose value changed with
//! the error `param-value-nondeterministic`.

use std::collections::BTreeMap;

use anyhow::{Context, Result, anyhow, bail};
use sha2::{Digest, Sha256};
use surrealdb::Surreal;
use surrealdb::engine::any::{Any, connect};
use surrealdb::opt::Config;
use surrealdb::opt::capabilities::{Capabilities, ExperimentalFeature};

use crate::schema_state::{SchemaFile, build_catalog_snapshot, prepare_schema_sql};
use crate::typegen::document::{
	Capabilities as DocCapabilities, EngineMode, SchemaDocument, Source, SourceFile,
};
use crate::typegen::introspect_v2::{Origin, assemble, capture};
use crate::typegen::{Diagnostic, SchemaTypes, Severity, codes};
use crate::variables::TemplateVars;

/// One schema module to compile: its name and its files in apply order.
#[derive(Debug, Clone)]
pub struct CompileModule {
	pub name: String,
	/// Files as `sync` collects them: `path` is the tracking key (relative to the project
	/// folder), `sql` the file's text.
	pub files: Vec<SchemaFile>,
}

/// Options for [`compile`].
#[derive(Debug, Clone, Default)]
pub struct CompileOpts {
	/// Experimental capabilities the engine allows: `files`, `surrealism`, `gql`.
	pub experimental: Vec<String>,
	/// Template variables substituted into each file before it is applied, as in `sync`.
	pub vars: TemplateVars,
	/// The engine minor (`"3.2"`) the project deploys to, `[typegen] engine`. When it differs
	/// from the embedded engine's minor the document carries the error
	/// `compile-engine-mismatch`, because it then describes the embedded engine's behaviour.
	pub expected_engine: Option<String>,
}

/// What one compile produced.
#[derive(Debug, Clone)]
pub struct Compiled {
	/// Schema document v2.
	pub document: SchemaDocument,
	/// The v1 document of the same engine, which the TypeScript interface emitter reads.
	pub v1: SchemaTypes,
	/// The v1 document's own diagnostics: what the TypeScript interfaces cannot represent.
	/// `run_compile` reports them when TypeScript is configured.
	pub v1_diagnostics: Vec<Diagnostic>,
}

/// The experimental features named in `names`, sorted and deduplicated.
fn experimental_features(names: &[String]) -> Result<(Vec<ExperimentalFeature>, Vec<String>)> {
	let mut canonical: Vec<String> = names.iter().map(|n| n.trim().to_ascii_lowercase()).collect();
	canonical.sort();
	canonical.dedup();
	let features = canonical
		.iter()
		.map(|name| match name.as_str() {
			"files" => Ok(ExperimentalFeature::Files),
			"surrealism" => Ok(ExperimentalFeature::Surrealism),
			"gql" => Ok(ExperimentalFeature::Gql),
			other => Err(anyhow!(
				"unknown experimental capability {other:?}; use files, surrealism or gql"
			)),
		})
		.collect::<Result<Vec<_>>>()?;
	Ok((features, canonical))
}

/// The capabilities of the compile engine: the defaults (every non-scripting function), no
/// network target, no scripting, and only the experimental features given.
fn capabilities(features: &[ExperimentalFeature]) -> Result<Capabilities> {
	Capabilities::new()
		.with_scripting(false)
		.with_no_net_targets_allowed()
		.with_all_net_targets_denied()
		.with_experimental_features_allowed(features)
		.with_function_denied("http")
		.map_err(|err| anyhow!("restricting the compile engine: {err}"))
}

/// The content hash `provenance.source.files` records: SHA-256 of the file after removing a
/// UTF-8 byte-order mark and turning CRLF into LF, so checkouts on every platform agree.
pub fn normalized_sha256(sql: &str) -> String {
	let text = sql.strip_prefix('\u{feff}').unwrap_or(sql).replace("\r\n", "\n");
	hex::encode(Sha256::digest(text.as_bytes()))
}

/// Open the hermetic engine.
async fn engine(features: &[ExperimentalFeature]) -> Result<Surreal<Any>> {
	let config = Config::new().capabilities(capabilities(features)?);
	let db = connect(("mem://", config)).await.map_err(|err| {
		anyhow!(
			"surrealkit compile needs the embedded in-memory engine (the `kv-mem` feature, on by \
			 default): {err}"
		)
	})?;
	db.use_ns("compile").use_db("compile").await.context("selecting the compile database")?;
	Ok(db)
}

/// A `definition-duplicate` warning for every entity the files define more than once.
fn duplicates(files: &[&SchemaFile]) -> Result<Vec<Diagnostic>> {
	let mut seen: BTreeMap<(String, Option<String>, String), Vec<String>> = BTreeMap::new();
	for file in files {
		let catalog = build_catalog_snapshot(std::slice::from_ref(*file), false)?;
		for entity in catalog.entities {
			seen.entry((
				entity.kind.as_str().to_string(),
				entity.scope.clone(),
				entity.name.clone(),
			))
			.or_default()
			.push(file.path.clone());
		}
	}
	Ok(seen
		.into_iter()
		.filter(|(_, paths)| paths.len() > 1)
		.map(|((kind, scope, name), paths)| {
			let what = match &scope {
				Some(scope) => format!("{kind} {name} ON {scope}"),
				None => format!("{kind} {name}"),
			};
			let mut d = Diagnostic::new(
				Severity::Warning,
				codes::DEFINITION_DUPLICATE,
				what,
				format!(
					"defined {} times ({}); SurrealKit applies every DEFINE but a sequence as \
					 OVERWRITE, so the last one wins, which raw SurrealQL would not do for IF NOT \
					 EXISTS",
					paths.len(),
					paths.join(", ")
				),
			);
			if kind == "field" {
				if let Some(table) = &scope {
					d = d.on_table(table).on_field(crate::typegen::field_path_of(&name));
				}
			} else if kind == "table" {
				d = d.on_table(&name);
			}
			d
		})
		.collect())
}

/// The `DEFINE PARAM` statements of prepared SQL, in order.
fn param_statements(prepared: &str) -> Result<Vec<String>> {
	let statements = crate::surql_scan::scan(prepared).map_err(|err| anyhow!("{err}"))?;
	Ok(statements
		.iter()
		.filter(|stmt| {
			crate::schema_state::define_head(stmt, prepared).is_some_and(|head| {
				stmt.tok_text(prepared, head.kind_index)
					.is_some_and(|kind| kind.eq_ignore_ascii_case("PARAM"))
			})
		})
		.map(|stmt| stmt.text(prepared).to_string())
		.collect())
}

/// Define every param in `statements` again and report each one whose stored value changed:
/// its `VALUE` is not a fixed value, so the document would differ on every run.
async fn nondeterministic_params(
	db: &Surreal<Any>,
	statements: &[String],
	db_info: &serde_json::Value,
) -> Result<Vec<Diagnostic>> {
	if statements.is_empty() {
		return Ok(Vec::new());
	}
	for statement in statements {
		db.query(statement.as_str())
			.await
			.and_then(|response| response.check())
			.map_err(|err| anyhow!("{err}"))
			.with_context(|| format!("defining a param again to check its value: {statement}"))?;
	}
	let again = crate::typegen::introspect_v2::query_json(db, "INFO FOR DB;")
		.await
		.context("INFO FOR DB")?;
	let params = |info: &serde_json::Value| -> BTreeMap<String, String> {
		info.get("params")
			.and_then(serde_json::Value::as_object)
			.map(|map| {
				map.iter()
					.map(|(name, define)| (name.clone(), define.as_str().unwrap_or("").to_string()))
					.collect()
			})
			.unwrap_or_default()
	};
	let (first, second) = (params(db_info), params(&again));
	Ok(first
		.into_iter()
		.filter(|(name, define)| second.get(name) != Some(define))
		.map(|(name, define)| {
			Diagnostic::new(
				Severity::Error,
				codes::PARAM_VALUE_NONDETERMINISTIC,
				define,
				format!(
					"${name} stores a different value each time it is defined (its VALUE calls \
					 something like time::now() or rand::*), so the compiled document changes on \
					 every run and a --check never passes; give it a literal VALUE"
				),
			)
		})
		.collect())
}

/// A warning `definition-unsupported` for every access and user the files defined on the
/// namespace or the root: the document describes one database, so they are not part of it,
/// and dropping them without a word would hide part of the schema.
async fn outside_the_database(db: &Surreal<Any>) -> Result<Vec<Diagnostic>> {
	let mut out = Vec::new();
	for (level, sql) in [("NAMESPACE", "INFO FOR NS;"), ("ROOT", "INFO FOR ROOT;")] {
		let info = crate::typegen::introspect_v2::query_json(db, sql)
			.await
			.with_context(|| format!("reading the compiled schema: {sql}"))?;
		for (category, what) in [("accesses", "ACCESS"), ("users", "USER")] {
			let names = info.get(category).and_then(serde_json::Value::as_object);
			for name in names.into_iter().flat_map(|map| map.keys()) {
				out.push(Diagnostic::new(
					Severity::Warning,
					codes::DEFINITION_UNSUPPORTED,
					format!("DEFINE {what} {name} ON {level}"),
					format!(
						"defined on the {}, outside the database schema document v2 describes; \
						 it is not part of the document",
						level.to_ascii_lowercase()
					),
				));
			}
		}
	}
	Ok(out)
}

/// The error `compile-engine-mismatch` when the project deploys to an engine minor other than
/// the embedded one (`version`, such as `3.3.0`).
fn engine_mismatch(expected: Option<&str>, version: &str) -> Result<Option<Diagnostic>> {
	let Some(expected) = expected.map(str::trim) else {
		return Ok(None);
	};
	let minor = |v: &str| -> Option<String> {
		let mut parts = v.split('.');
		let major =
			parts.next().filter(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))?;
		let minor =
			parts.next().filter(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))?;
		Some(format!("{major}.{minor}"))
	};
	let wanted = minor(expected).filter(|_| expected.split('.').count() == 2).ok_or_else(|| {
		anyhow!("[typegen] engine = {expected:?} is not an engine minor such as \"3.3\"")
	})?;
	let embedded = minor(version).ok_or_else(|| anyhow!("unexpected engine version {version}"))?;
	if wanted == embedded {
		return Ok(None);
	}
	Ok(Some(Diagnostic::new(
		Severity::Error,
		codes::COMPILE_ENGINE_MISMATCH,
		expected,
		format!(
			"the project deploys to SurrealDB {wanted} ([typegen] engine), but compile runs the \
			 embedded SurrealDB {version}; the document describes {embedded} behaviour \
			 (normalisation, properties, accepted spellings). Introspect a {wanted} database with \
			 `surrealkit typegen` instead, or acknowledge this code with a reason"
		),
	)))
}

/// Compile `modules` (already in dependency order) into schema document v2.
pub async fn compile(modules: &[CompileModule], opts: &CompileOpts) -> Result<Compiled> {
	let (features, experimental) = experimental_features(&opts.experimental)?;
	let db = engine(&features).await?;

	let files: Vec<&SchemaFile> = modules.iter().flat_map(|m| m.files.iter()).collect();
	let mut diagnostics = duplicates(&files)?;
	let mut params = Vec::new();
	for module in modules {
		for file in &module.files {
			let sql = opts
				.vars
				.apply(&file.sql)
				.with_context(|| format!("applying template variables in {}", file.path))?;
			let prepared = prepare_schema_sql(&sql)
				.with_context(|| format!("preparing {} for compile", file.path))?;
			for warning in &prepared.warnings {
				log::warn!("{}: {warning}", file.path);
			}
			params.extend(param_statements(&prepared.sql)?);
			db.query(prepared.sql.as_str())
				.await
				.and_then(|response| response.check())
				.map_err(|err| anyhow!("{err}"))
				.with_context(|| {
					format!("compiling schema module {:?}: applying {}", module.name, file.path)
				})?;
		}
	}

	let snapshot = capture(&db).await.context("reading the compiled schema")?;
	let (v1, v1_diagnostics) = crate::typegen::generate_with_diagnostics(&db)
		.await
		.context("reading the compiled schema")?;
	diagnostics.extend(engine_mismatch(opts.expected_engine.as_deref(), &snapshot.version)?);
	diagnostics.extend(outside_the_database(&db).await?);
	// Last: it defines the params again, changing what the engine holds.
	diagnostics.extend(nondeterministic_params(&db, &params, &snapshot.db_info).await?);

	let mut module_names: Vec<String> = modules.iter().map(|m| m.name.clone()).collect();
	module_names.sort();
	module_names.dedup();
	let mut source_files: Vec<SourceFile> = files
		.iter()
		.map(|f| SourceFile {
			path: f.path.replace('\\', "/"),
			sha256: normalized_sha256(&f.sql),
		})
		.collect();
	source_files.sort();
	let document = assemble(
		&snapshot,
		Origin {
			source: Source::Compile {
				modules: module_names,
				files: source_files,
			},
			mode: EngineMode::Embedded,
			capabilities: Some(DocCapabilities {
				experimental,
				network: false,
			}),
			diagnostics,
		},
	)?;
	Ok(Compiled {
		document,
		v1,
		v1_diagnostics,
	})
}

/// The `compile` command: collect `modules` (in dependency order) from `folder`, compile
/// them, and write or check the outputs `outputs` configures — schema document v2 as the
/// JSON, and the TypeScript interfaces when configured. `outputs.strict` fails on any error
/// diagnostic before anything is written.
#[doc(hidden)]
pub async fn run_compile(
	project: &crate::project::ProjectConfig,
	folder: &str,
	modules: &[crate::module::Module],
	opts: &CompileOpts,
	outputs: &crate::typegen::TypegenOpts,
) -> Result<Compiled> {
	let collected = modules
		.iter()
		.map(|module| collect_module(project, folder, module))
		.collect::<Result<Vec<_>>>()?;
	let compiled = compile(&collected, opts).await?;
	crate::typegen::report_diagnostics(
		&compiled.document.diagnostics,
		outputs.strict,
		&outputs.acknowledge,
	)?;
	// The TypeScript interfaces come from v1: what it cannot represent there fails a strict
	// compile too, rather than becoming `unknown` behind a clean document.
	if outputs.ts_out.is_some() {
		crate::typegen::report_diagnostics(
			&compiled.v1_diagnostics,
			outputs.strict,
			&outputs.acknowledge,
		)?;
	}
	crate::typegen::produce(&compiled.v1, Some(&compiled.document), folder, outputs)?;
	Ok(compiled)
}

/// Collect the schema files of `module` in `folder` the way `sync` does.
pub fn collect_module(
	project: &crate::project::ProjectConfig,
	folder: &str,
	module: &crate::module::Module,
) -> Result<CompileModule> {
	let layout = project.layout_for(folder, module);
	let dir = layout.schema_dir();
	let files = crate::schema_state::collect_schema_files_at(layout.folder(), &dir)?;
	if files.is_empty() {
		bail!(
			"refusing to compile: schema module {:?} has no .surql files in {}; check --folder \
			 and the module selection",
			module.name(),
			dir.display()
		);
	}
	Ok(CompileModule {
		name: module.name().to_string(),
		files,
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn the_source_hash_ignores_bom_and_line_endings() {
		let lf = normalized_sha256("DEFINE TABLE t;\nDEFINE TABLE u;\n");
		assert_eq!(normalized_sha256("\u{feff}DEFINE TABLE t;\r\nDEFINE TABLE u;\r\n"), lf);
		assert_ne!(normalized_sha256("DEFINE TABLE t;"), lf);
	}

	#[test]
	fn experimental_names_are_checked_and_sorted() {
		let (_, names) =
			experimental_features(&["surrealism".into(), "Files".into(), "files".into()]).unwrap();
		assert_eq!(names, vec!["files", "surrealism"]);
		assert!(experimental_features(&["nope".into()]).is_err());
	}
}

/// Compile against the golden corpus: the hermetic engine must describe the corpus exactly as
/// a live engine does (the contract's section 1).
#[cfg(all(test, feature = "kv-mem"))]
mod corpus {
	use std::path::Path;

	use super::*;
	use crate::typegen::corpus_tests::{Module, corpus_dir, corpus_modules};
	use crate::typegen::corpus_v2_tests::{
		apply_corpus, corpus_acknowledged, fixture_document, remote_has_experimental,
	};

	fn file(path: &str) -> SchemaFile {
		let sql = std::fs::read_to_string(corpus_dir().join(path)).expect("corpus module");
		SchemaFile {
			path: path.to_string(),
			hash: crate::core::sha256_hex(sql.as_bytes()),
			sql,
		}
	}

	/// The corpus modules for engine minor `minor` (with or without the experimental ones),
	/// as one compile module per corpus file, in corpus order.
	fn modules_for(minor: &str, experimental: bool) -> Vec<Module> {
		corpus_modules()
			.into_iter()
			.filter(|m| m.engines.as_ref().is_none_or(|e| e.iter().any(|v| v == minor)))
			.filter(|m| experimental || !m.experimental)
			.collect()
	}

	/// The same modules in the spelling the embedded 3.3 engine accepts: a version-specific
	/// file (`31_module.v3.2.surql`) is replaced by its 3.3 twin (`31_module.v3.3.surql`).
	fn in_33_spelling(modules: &[Module]) -> Vec<CompileModule> {
		modules
			.iter()
			.map(|m| {
				let path = match m.path.split_once(".v3.") {
					Some((stem, _)) if !m.path.ends_with(".v3.3.surql") => {
						format!("{stem}.v3.3.surql")
					}
					_ => m.path.clone(),
				};
				assert!(Path::new(&corpus_dir().join(&path)).is_file(), "{path}");
				CompileModule {
					name: path.clone(),
					files: vec![file(&path)],
				}
			})
			.collect()
	}

	fn experimental() -> CompileOpts {
		CompileOpts {
			experimental: vec!["files".into(), "surrealism".into()],
			..CompileOpts::default()
		}
	}

	#[tokio::test]
	async fn the_corpus_compiles_to_the_3_3_0_snapshot_document() {
		let compiled = compile(&in_33_spelling(&modules_for("3.3", true)), &experimental())
			.await
			.expect("compile the corpus");
		let doc = compiled.document;
		let snapshot = fixture_document("3.3.0");
		assert_eq!(doc.semantic_digest, snapshot.semantic_digest);
		// `define` evidence is identical too: both read plain INFO from a 3.3.0 engine.
		assert_eq!(doc.tables, snapshot.tables);
		assert_eq!(doc.provenance.adapter, "structure-3.3");
		assert_eq!(doc.provenance.engine.mode, EngineMode::Embedded);
		let caps = doc.provenance.capabilities.as_ref().expect("capabilities");
		assert_eq!(caps.experimental, vec!["files", "surrealism"]);
		assert!(!caps.network);
		let Source::Compile {
			modules,
			files,
		} = &doc.provenance.source
		else {
			panic!("a compile source: {:?}", doc.provenance.source);
		};
		assert_eq!(modules.len(), files.len());
		let mut sorted = files.clone();
		sorted.sort();
		assert_eq!(&sorted, files);
		assert_eq!(
			files.iter().find(|f| f.path == "schema/01_tables.surql").map(|f| f.sha256.as_str()),
			Some("703fb8d51e56246b2da86b71259e8fd44177eda0512055c366533bc1da2a8c5d"),
			"the hash the contract example records"
		);
		// No error but the corpus acknowledges (its bare `kinds.k_object`); the deliberate
		// double definition of person.flagged is diagnosed.
		let acknowledged = corpus_acknowledged();
		assert!(
			doc.diagnostics.iter().all(|d| !d.is_error() || acknowledged.contains(&d.code)),
			"{:?}",
			doc.diagnostics
		);
		let duplicate: Vec<_> =
			doc.diagnostics.iter().filter(|d| d.code == codes::DEFINITION_DUPLICATE).collect();
		assert_eq!(duplicate.len(), 1, "{duplicate:?}");
		assert_eq!(duplicate[0].table.as_deref(), Some("person"));
		assert_eq!(duplicate[0].field.as_deref(), Some(&["flagged".to_string()][..]));
		// The v1 document of the same engine, for the TypeScript emitter.
		assert!(compiled.v1.tables.iter().any(|t| t.name == "person"));
	}

	/// Compare `doc`, rendered exactly as `surrealkit compile` writes it, with the pinned file
	/// `tests/fixtures/typegen_corpus/compiled/<name>`. `SURREALKIT_BLESS=1` rewrites the pin.
	///
	/// The pins are real compile output for generators to test against (the Effect emitter in
	/// `packages/effect-surrealkit` reads them), so a change in what compile produces shows up
	/// in review as a diff of the documents generators see.
	///
	/// The comparison ignores the SurrealKit and engine versions in `provenance` (see
	/// [`without_versions`]), so a release or a `surrealdb` bump does not need a re-bless; the
	/// pinned files keep the versions they were blessed with.
	fn assert_pinned(name: &str, doc: &SchemaDocument) {
		let path = corpus_dir().join("compiled").join(name);
		let mut text = crate::typegen::v2_json(doc, true).expect("render");
		text.push('\n');
		if std::env::var_os("SURREALKIT_BLESS").is_some() {
			std::fs::create_dir_all(path.parent().expect("parent")).expect("compiled dir");
			std::fs::write(&path, &text).expect("bless");
			return;
		}
		let pinned = std::fs::read_to_string(&path).unwrap_or_else(|err| {
			panic!("{}: {err}; SURREALKIT_BLESS=1 writes it", path.display())
		});
		assert!(
			without_versions(&pinned) == without_versions(&text),
			"{} differs from what compile produces; review the change and rerun with \
			 SURREALKIT_BLESS=1 to update it",
			path.display()
		);
	}

	/// `text` (a rendered document) with `provenance.generator.version`,
	/// `provenance.engine.version` and `provenance.engine.build` replaced by `*`.
	fn without_versions(text: &str) -> serde_json::Value {
		let mut value: serde_json::Value = serde_json::from_str(text).expect("a JSON document");
		let provenance = value.get_mut("provenance").expect("provenance");
		provenance["generator"]["version"] = "*".into();
		provenance["engine"]["version"] = "*".into();
		if provenance["engine"].get("build").is_some() {
			provenance["engine"]["build"] = "*".into();
		}
		value
	}

	#[test]
	fn pins_ignore_the_generator_and_engine_versions() {
		let a = r#"{"version":2,"provenance":{"generator":{"name":"surrealkit","version":"1.0.0"},
			"engine":{"version":"3.3.0","build":"surrealdb-3.3.0","mode":"embedded"}},"tables":[]}"#;
		let b = a.replace("1.0.0", "1.0.1").replace("3.3.0", "3.3.1");
		assert_eq!(without_versions(a), without_versions(&b));
		let c = a.replace("\"tables\":[]", "\"tables\":[1]");
		assert_ne!(without_versions(a), without_versions(&c));
	}

	#[tokio::test]
	async fn the_compiled_corpus_is_pinned() {
		let compiled = compile(&in_33_spelling(&modules_for("3.3", true)), &experimental())
			.await
			.expect("compile the corpus");
		assert_pinned("corpus.v2.json", &compiled.document);
	}

	/// The tables of the field-presence evidence (`semantics/field_presence.surql`): only its
	/// `DEFINE` statements, which is what a project with that schema would hold.
	#[tokio::test]
	async fn the_compiled_field_presence_tables_are_pinned() {
		let evidence = std::fs::read_to_string(corpus_dir().join("semantics/field_presence.surql"))
			.expect("field presence evidence");
		let sql: String = evidence
			.lines()
			.filter(|line| line.starts_with("DEFINE "))
			.map(|line| format!("{line}\n"))
			.collect();
		let module = CompileModule {
			name: "field_presence".into(),
			files: vec![SchemaFile {
				path: "semantics/field_presence.defines.surql".into(),
				hash: crate::core::sha256_hex(sql.as_bytes()),
				sql,
			}],
		};
		let compiled =
			compile(&[module], &CompileOpts::default()).await.expect("compile the evidence tables");
		let names: Vec<&str> = compiled.document.tables.iter().map(|t| t.name.as_str()).collect();
		assert_eq!(names, ["e", "ip", "m", "oa", "p", "up"]);
		assert_pinned("field-presence.v2.json", &compiled.document);
	}

	/// The contract's section 1: compile describes what `sync` deploys. The portable corpus
	/// (`schema/`), synced into a fresh database the way `surrealkit sync` does it — setup
	/// first, so SurrealKit's metadata tables exist there too — and the same files compiled
	/// have the same semantic content.
	#[tokio::test]
	async fn a_compile_equals_a_synced_database() {
		let tmp = tempfile::TempDir::new().expect("tempdir");
		let folder = tmp.path().join("database");
		std::fs::create_dir_all(folder.join("schema")).expect("schema dir");
		let mut files = Vec::new();
		for module in modules_for("3.3", false).iter().filter(|m| m.path.starts_with("schema/")) {
			let mut copied = file(&module.path);
			std::fs::write(folder.join(&copied.path), &copied.sql).expect("copy");
			copied.hash = crate::core::sha256_hex(copied.sql.as_bytes());
			files.push(copied);
		}
		let folder = folder.to_string_lossy().to_string();
		let db = crate::test_db::fresh("compile_vs_sync").await;
		crate::setup::run_setup(&db, &folder).await.expect("setup");
		crate::sync::run_sync(
			&db,
			crate::sync::SyncOpts {
				watch: false,
				debounce_ms: 250,
				dry_run: false,
				fail_fast: true,
				prune: false,
				allow_shared_prune: false,
				allow_empty_prune: false,
				allow_all_statements: false,
				vars: TemplateVars::default(),
				folder: folder.clone(),
				module: crate::module::Module::default_module(),
				typegen_ts_out: None,
				typegen_ts_format: None,
				typegen_json_out: None,
				typegen_strict: false,
				typegen_omit_generated_at: false,
				typegen_schema_version: crate::typegen::SchemaVersion::V1,
				typegen_emitters: Vec::new(),
				typegen_acknowledge: Default::default(),
			},
		)
		.await
		.expect("sync the portable corpus");
		let synced = crate::typegen::introspect_v2::introspect(&db, EngineMode::Remote)
			.await
			.expect("introspect the synced database");
		assert!(
			synced.tables.iter().all(|t| !t.name.starts_with("__")),
			"SurrealKit's metadata tables are not schema: {:?}",
			synced.tables.iter().map(|t| &t.name).collect::<Vec<_>>()
		);
		let compiled = compile(
			&[CompileModule {
				name: "schema".into(),
				files,
			}],
			&CompileOpts::default(),
		)
		.await
		.expect("compile the portable corpus")
		.document;
		let semantic = |d: &SchemaDocument| {
			serde_json::to_string_pretty(&crate::typegen::canonical::projection(
				&serde_json::to_value(d).expect("json"),
			))
			.expect("text")
		};
		assert_eq!(semantic(&compiled), semantic(&synced));
		assert_eq!(compiled.semantic_digest, synced.semantic_digest);
	}

	#[tokio::test]
	async fn compile_is_deterministic() {
		let modules = in_33_spelling(&modules_for("3.3", true));
		let a = compile(&modules, &experimental()).await.unwrap().document;
		let b = compile(&modules, &experimental()).await.unwrap().document;
		assert_eq!(
			serde_json::to_string_pretty(&a).unwrap(),
			serde_json::to_string_pretty(&b).unwrap()
		);
	}

	#[tokio::test]
	async fn the_compile_engine_has_no_network() {
		for sql in [
			"DEFINE PARAM $page VALUE http::get('http://127.0.0.1:9/');",
			"DEFINE PARAM $page VALUE http::post('http://127.0.0.1:9/', {});",
		] {
			let module = CompileModule {
				name: "net".into(),
				files: vec![SchemaFile {
					path: "schema/net.surql".into(),
					sql: sql.into(),
					hash: String::new(),
				}],
			};
			let err =
				format!("{:#}", compile(&[module], &CompileOpts::default()).await.unwrap_err());
			assert!(err.contains("not allowed"), "{sql}: {err}");
			assert!(!err.to_lowercase().contains("connect"), "it must not even try: {err}");
		}
	}

	fn one_file(sql: &str) -> Vec<CompileModule> {
		vec![CompileModule {
			name: "one".into(),
			files: vec![SchemaFile {
				path: "schema/one.surql".into(),
				sql: sql.into(),
				hash: String::new(),
			}],
		}]
	}

	#[tokio::test]
	async fn accesses_and_users_outside_the_database_are_warnings() {
		let compiled = compile(
			&one_file(
				"DEFINE ACCESS ns_acc ON NAMESPACE TYPE JWT ALGORITHM HS256 KEY \
				 'abcdefghijklmnopqrstuvwxyz012345';\n\
				 DEFINE USER ns_user ON NAMESPACE PASSWORD 'pw' ROLES VIEWER;\n\
				 DEFINE USER root_user ON ROOT PASSWORD 'pw' ROLES VIEWER;\n\
				 DEFINE USER db_user ON DATABASE PASSWORD 'pw' ROLES EDITOR;\n",
			),
			&CompileOpts::default(),
		)
		.await
		.expect("compile");
		let doc = compiled.document;
		let mut sources: Vec<&str> = doc
			.diagnostics
			.iter()
			.filter(|d| d.code == codes::DEFINITION_UNSUPPORTED)
			.map(|d| {
				assert_eq!(d.severity, Severity::Warning);
				d.source.as_str()
			})
			.collect();
		sources.sort_unstable();
		assert_eq!(
			sources,
			[
				"DEFINE ACCESS ns_acc ON NAMESPACE",
				"DEFINE USER ns_user ON NAMESPACE",
				"DEFINE USER root_user ON ROOT",
			]
		);
		assert!(!doc.has_errors(), "{:?}", doc.diagnostics);
		assert_eq!(doc.users.iter().map(|u| u.name.as_str()).collect::<Vec<_>>(), ["db_user"]);
	}

	/// The TypeScript interfaces come from v1; its diagnostics travel with the compile so that
	/// `run_compile` can report them.
	#[tokio::test]
	async fn the_v1_diagnostics_are_kept_for_the_typescript() {
		let compiled = compile(
			&one_file("DEFINE TABLE t SCHEMAFULL;\nDEFINE FIELD loose ON t;\n"),
			&CompileOpts::default(),
		)
		.await
		.expect("compile");
		assert!(
			compiled
				.v1_diagnostics
				.iter()
				.any(|d| d.code == codes::FIELD_KIND_UNDECLARED && d.table.as_deref() == Some("t")),
			"{:?}",
			compiled.v1_diagnostics
		);
	}

	#[tokio::test]
	async fn a_param_with_a_changing_value_is_an_error() {
		let doc = compile(
			&one_file(
				"DEFINE PARAM $now VALUE time::now();\nDEFINE PARAM $id VALUE rand::uuid();\n\
				 DEFINE PARAM $fixed VALUE 'x';\nDEFINE PARAM $sum VALUE math::max([1, 2]);\n",
			),
			&CompileOpts::default(),
		)
		.await
		.expect("compile")
		.document;
		let flagged: Vec<&str> = doc
			.diagnostics
			.iter()
			.filter(|d| d.code == codes::PARAM_VALUE_NONDETERMINISTIC)
			.map(|d| {
				assert_eq!(d.severity, Severity::Error);
				d.source.split_whitespace().nth(2).unwrap_or_default()
			})
			.collect();
		assert_eq!(flagged, ["$id", "$now"], "{:?}", doc.diagnostics);
		// The document still describes the first definition.
		assert_eq!(doc.params.len(), 4);
	}

	#[tokio::test]
	async fn a_different_expected_engine_minor_is_an_error() {
		let compile_with = |expected: Option<&str>| {
			let opts = CompileOpts {
				expected_engine: expected.map(str::to_string),
				..CompileOpts::default()
			};
			async move { compile(&one_file("DEFINE TABLE t;"), &opts).await }
		};
		let mismatch = |doc: &SchemaDocument| {
			doc.diagnostics.iter().filter(|d| d.code == codes::COMPILE_ENGINE_MISMATCH).count()
		};
		let embedded = compile_with(None).await.expect("compile").document;
		assert_eq!(mismatch(&embedded), 0);
		let version = embedded.provenance.engine.version.clone();
		let minor = version.rsplit_once('.').map(|(m, _)| m.to_string()).expect("x.y.z");
		assert_eq!(mismatch(&compile_with(Some(&minor)).await.expect("same").document), 0);
		let other = compile_with(Some("3.2")).await.expect("compile").document;
		if minor != "3.2" {
			assert_eq!(mismatch(&other), 1, "{:?}", other.diagnostics);
			assert!(other.has_errors());
		}
		for bad in ["3", "3.x", "3.3.0", "latest"] {
			let err = format!("{:#}", compile_with(Some(bad)).await.unwrap_err());
			assert!(err.contains("not an engine minor"), "{bad}: {err}");
		}
	}

	#[tokio::test]
	async fn files_sync_would_refuse_are_refused() {
		for (sql, expect) in
			[("CREATE person SET name = 'x';", "non-DEFINE"), ("REMOVE TABLE person;", "REMOVE")]
		{
			let module = CompileModule {
				name: "bad".into(),
				files: vec![SchemaFile {
					path: "schema/bad.surql".into(),
					sql: sql.into(),
					hash: String::new(),
				}],
			};
			let err =
				format!("{:#}", compile(&[module], &CompileOpts::default()).await.unwrap_err());
			assert!(err.contains(expect), "{sql}: {err}");
		}
	}

	#[tokio::test]
	async fn gated_definitions_need_their_experimental_capability() {
		let module = CompileModule {
			name: "files".into(),
			files: vec![file("schema-gated/20_experimental_files.surql")],
		};
		let err = format!(
			"{:#}",
			compile(std::slice::from_ref(&module), &CompileOpts::default()).await.unwrap_err()
		);
		assert!(err.contains("schema-gated/20_experimental_files.surql"), "{err}");
		let opts = CompileOpts {
			experimental: vec!["files".into()],
			..CompileOpts::default()
		};
		compile(&[module], &opts).await.expect("with the files capability");
	}

	/// Access clauses the document does not carry (AUDIENCE, CONTEXT) compile
	/// with warnings, never errors, so a strict compile succeeds; on a 3.3 server the compiled
	/// and live documents agree.
	#[tokio::test]
	async fn access_clauses_the_document_does_not_carry_are_warnings() {
		let module = CompileModule {
			name: "access".into(),
			files: vec![file("semantics/access.v3.3.surql")],
		};
		let document = compile(std::slice::from_ref(&module), &CompileOpts::default())
			.await
			.expect("compile")
			.document;
		assert!(
			document.diagnostics.iter().all(|d| d.severity != Severity::Error),
			"{:?}",
			document.diagnostics
		);
		let unsupported = document
			.diagnostics
			.iter()
			.filter(|d| d.code == codes::DEFINITION_UNSUPPORTED && d.source.starts_with("access "))
			.count();
		assert_eq!(unsupported, 4, "{:?}", document.diagnostics);
		if !crate::test_db::is_remote() {
			return;
		}
		let db = crate::test_db::fresh("compile_access_vs_live").await;
		let version = db.version().await.expect("engine version");
		if (version.major, version.minor) != (3, 3) {
			return;
		}
		db.query(module.files[0].sql.as_str()).await.and_then(|r| r.check()).expect("apply");
		let live = crate::typegen::introspect_v2::introspect(&db, EngineMode::Remote)
			.await
			.expect("introspect");
		assert_eq!(document.semantic_digest, live.semantic_digest);
	}

	/// With a server at `SURREALKIT_TEST_URL`, the corpus applied to it and the same corpus
	/// compiled hermetically have the same semantic content. On a 3.2 server the compile
	/// applies the 3.3 spelling of the version-specific module (3.3 refuses the 3.2 one; see
	/// the module docs), and the 3.3-only relations are not part of the 3.2 corpus.
	#[tokio::test]
	async fn a_hermetic_compile_equals_live_introspection() {
		if !crate::test_db::is_remote() {
			return;
		}
		let experimental_server = remote_has_experimental();
		let db = crate::test_db::fresh("compile_vs_live").await;
		let exact = apply_corpus(&db, experimental_server).await;
		let minor = exact.rsplit_once('.').map_or(exact.as_str(), |(m, _)| m).to_string();
		let live = crate::typegen::introspect_v2::introspect(&db, EngineMode::Remote)
			.await
			.expect("introspect");
		let opts = if experimental_server {
			experimental()
		} else {
			CompileOpts::default()
		};
		let compiled = compile(&in_33_spelling(&modules_for(&minor, experimental_server)), &opts)
			.await
			.expect("compile")
			.document;
		let semantic = |d: &SchemaDocument| {
			serde_json::to_string_pretty(&crate::typegen::canonical::projection(
				&serde_json::to_value(d).unwrap(),
			))
			.unwrap()
		};
		assert_eq!(semantic(&compiled), semantic(&live), "compile vs live {exact}");
		assert_eq!(compiled.semantic_digest, live.semantic_digest);
	}
}
