//! Drift: how a target database differs from the schema files.
//!
//! `surrealkit check --target <name>` compiles the files with the embedded engine, reads the
//! target through the same introspection `typegen` uses, and compares the two schema documents.
//! The comparison is [`diff`], a pure function over two [`SchemaDocument`]s; [`findings`] turns
//! its result into the report's [`Finding`]s, with the locations, the managed or unmanaged
//! verdict on what only the target has, and the notes.
//!
//! What is compared is the tables and their fields, from the normalised facts the v2 document
//! carries. Indexes, events, functions, params, accesses and the other collections are not
//! compared: a difference there is not reported.
//!
//! Nothing here writes. [`compare`] reads the target with `INFO` statements and `SELECT`s of
//! SurrealKit's own catalog tables, and never runs the setup DDL.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde_json::{Map, Value};
use surrealdb::Surreal;
use surrealdb::engine::any::Any;

use super::TargetFindings;
use super::config::AnalyzeConfig;
use super::discover::{Source, Sources};
use super::finding::{
	Finding, K9101, K9102, K9103, K9104, K9105, K9106, K9107, Note, NoteKind, Severity,
};
use super::policy::Level;
use crate::compile::{CompileModule, CompileOpts, compile};
use crate::core::sha256_hex;
use crate::module::Module;
use crate::project::{ProjectConfig, Target};
use crate::schema_state::{CatalogEntity, EntityKind, SchemaFile, defined_entities};
use crate::typegen::canonical::semantic_digest;
use crate::typegen::document::{EngineMode, Field, SchemaDocument, Table};
use crate::typegen::field_path_of;
use crate::typegen::introspect_v2::introspect;
use crate::variables::TemplateVars;

/// The longest rendering of one side of a difference, in characters.
const SHOWN: usize = 160;

/// What kind of drift one [`Drift`] is. The code follows from it, but for an extra, which is
/// `K9105` or `K9106` by whether SurrealKit manages it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriftKind {
	/// A table the files define, missing on the target.
	TableMissing,
	/// A table both define, defined differently.
	TableDiffers,
	/// A field the files define, missing on the target.
	FieldMissing,
	/// A field both define, defined differently.
	FieldDiffers,
	/// A table only the target has.
	TableExtra,
	/// A field only the target has, on a table both define.
	FieldExtra,
}

/// One property that differs, each side rendered for a person.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Difference {
	/// The document's property: `kind`, `optional`, `default`, `schemafull`, and so on.
	pub facet: String,
	/// What the files say; `none` when the property is absent.
	pub files: String,
	/// What the target has.
	pub target: String,
}

/// One difference between the files and the target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Drift {
	pub kind: DriftKind,
	pub table: String,
	/// The field's path, for a field drift.
	pub field: Option<Vec<String>>,
	/// The properties that differ, for a `Differs` kind.
	pub differences: Vec<Difference>,
}

impl Drift {
	fn table(kind: DriftKind, table: &str, differences: Vec<Difference>) -> Self {
		Self {
			kind,
			table: table.to_string(),
			field: None,
			differences,
		}
	}

	fn field(kind: DriftKind, table: &str, path: &[String], differences: Vec<Difference>) -> Self {
		Self {
			kind,
			table: table.to_string(),
			field: Some(path.to_vec()),
			differences,
		}
	}
}

/// A field path as it is written: `phones[*].number`.
pub fn field_name(path: &[String]) -> String {
	let mut out = String::new();
	for segment in path {
		if segment == "*" {
			out.push_str("[*]");
			continue;
		}
		if !out.is_empty() {
			out.push('.');
		}
		out.push_str(segment);
	}
	out
}

/// The properties of a table that are compared: all but its name, its collections and the
/// `INFO` evidence.
fn table_facets(table: &Table) -> Map<String, Value> {
	let mut facets = match serde_json::to_value(table) {
		Ok(Value::Object(map)) => map,
		_ => Map::new(),
	};
	for key in ["name", "fields", "indexes", "events", "define"] {
		facets.remove(key);
	}
	facets
}

/// The properties of a field that are compared: all but its path, the `INFO` evidence and the
/// properties derived from an expression, which the digest leaves out too.
fn field_facets(field: &Field) -> Map<String, Value> {
	let mut facets = match serde_json::to_value(field) {
		Ok(Value::Object(map)) => map,
		_ => Map::new(),
	};
	for key in ["path", "define"] {
		facets.remove(key);
	}
	if let Some(Value::Object(value)) = facets.get_mut("value") {
		value.remove("readsInput");
	}
	if let Some(Value::Object(assert)) = facets.get_mut("assert") {
		assert.remove("recognized");
	}
	facets
}

/// One side of a difference: text as it is, an absent property as `none`, anything else as
/// compact JSON.
fn show(value: Option<&Value>) -> String {
	let text = match value {
		None | Some(Value::Null) => "none".to_string(),
		Some(Value::String(text)) => text.clone(),
		Some(other) => other.to_string(),
	};
	if text.chars().count() > SHOWN {
		let cut: String = text.chars().take(SHOWN).collect();
		format!("{cut}…")
	} else {
		text
	}
}

/// The properties on which `files` and `target` differ, in property order.
fn differences(files: &Map<String, Value>, target: &Map<String, Value>) -> Vec<Difference> {
	let keys: BTreeSet<&String> = files.keys().chain(target.keys()).collect();
	keys.into_iter()
		.filter(|key| files.get(*key) != target.get(*key))
		.map(|key| Difference {
			facet: key.clone(),
			files: show(files.get(key)),
			target: show(target.get(key)),
		})
		.collect()
}

/// How `target` differs from `files`: the tables by name and the fields by path, comparing the
/// normalised facts the v2 document carries.
///
/// Provenance, `define` evidence and diagnostics are not compared, and neither are the
/// collections other than tables and their fields. A table that is missing on the target is one
/// drift, not one per field. The drifts are in table order, and within a table the missing and
/// differing ones come before the extra fields.
pub fn diff(files: &SchemaDocument, target: &SchemaDocument) -> Vec<Drift> {
	let theirs: BTreeMap<&str, &Table> =
		target.tables.iter().map(|table| (table.name.as_str(), table)).collect();
	let ours: BTreeMap<&str, &Table> =
		files.tables.iter().map(|table| (table.name.as_str(), table)).collect();
	let mut drifts = Vec::new();
	for (name, table) in &ours {
		let Some(other) = theirs.get(name) else {
			drifts.push(Drift::table(DriftKind::TableMissing, name, Vec::new()));
			continue;
		};
		let differing = differences(&table_facets(table), &table_facets(other));
		if !differing.is_empty() {
			drifts.push(Drift::table(DriftKind::TableDiffers, name, differing));
		}
		diff_fields(name, &table.fields, &other.fields, &mut drifts);
	}
	for name in theirs.keys().filter(|name| !ours.contains_key(*name)) {
		drifts.push(Drift::table(DriftKind::TableExtra, name, Vec::new()));
	}
	drifts
}

fn diff_fields(table: &str, files: &[Field], target: &[Field], drifts: &mut Vec<Drift>) {
	let theirs: BTreeMap<&[String], &Field> =
		target.iter().map(|field| (field.path.as_slice(), field)).collect();
	let ours: BTreeMap<&[String], &Field> =
		files.iter().map(|field| (field.path.as_slice(), field)).collect();
	for (path, field) in &ours {
		let Some(other) = theirs.get(path) else {
			drifts.push(Drift::field(DriftKind::FieldMissing, table, path, Vec::new()));
			continue;
		};
		let differing = differences(&field_facets(field), &field_facets(other));
		if !differing.is_empty() {
			drifts.push(Drift::field(DriftKind::FieldDiffers, table, path, differing));
		}
	}
	for path in theirs.keys().filter(|path| !ours.contains_key(*path)) {
		drifts.push(Drift::field(DriftKind::FieldExtra, table, path, Vec::new()));
	}
}

/// A name as the engine stores it, without the quoting a file may have written it with.
fn plain(name: &str) -> String {
	field_path_of(name).join(".")
}

/// The tables and fields `[analyze] drift_ignore` leaves unreported.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DriftIgnore {
	tables: BTreeSet<String>,
	fields: BTreeSet<(String, Vec<String>)>,
}

impl DriftIgnore {
	/// Parse `config`'s `drift_ignore`. An entry is a table, or a field path `table.field` (a
	/// name with a `.` is a field path); an empty entry and an empty segment are configuration
	/// mistakes.
	pub fn from_config(config: &AnalyzeConfig) -> Result<Self> {
		let mut ignore = Self::default();
		for written in &config.drift_ignore {
			let entry = written.trim();
			if entry.is_empty() {
				bail!("[analyze] drift_ignore entry {written:?} names nothing");
			}
			let Some((table, field)) = entry.split_once('.') else {
				ignore.tables.insert(plain(entry));
				continue;
			};
			if table.is_empty() || field.is_empty() || field.split('.').any(str::is_empty) {
				bail!(
					"[analyze] drift_ignore entry {written:?} is not `table` or `table.field`: an \
					 entry with a `.` is a field path, and neither side of it may be empty"
				);
			}
			ignore.fields.insert((plain(table), field_path_of(field)));
		}
		Ok(ignore)
	}

	/// Whether the table, or its field `field`, is ignored. A table entry covers its fields.
	fn covers(&self, table: &str, field: Option<&[String]>) -> bool {
		self.tables.contains(table)
			|| field.is_some_and(|path| self.fields.contains(&(table.to_string(), path.to_vec())))
	}
}

/// What SurrealKit has recorded as managing on a target: the tables and fields of the
/// catalogs, by `(table, field)`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Managed {
	selected: BTreeSet<(String, Option<Vec<String>>)>,
	other: BTreeSet<(String, Option<Vec<String>>)>,
}

/// Who a target-only table or field belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Owner {
	/// A module of this run manages it: it was removed from the files and not yet synced.
	Selected,
	/// A module this run did not select manages it: it is not drift of this run.
	Other,
	/// Nothing recorded it.
	Nobody,
}

impl Managed {
	/// Record `entity`, a catalog row of a module that is in this run (`selected`) or not.
	pub fn record(&mut self, selected: bool, entity: &CatalogEntity) {
		let key = match (&entity.kind, &entity.scope) {
			(EntityKind::Table, _) => (plain(&entity.name), None),
			(EntityKind::Field, Some(table)) => (plain(table), Some(field_path_of(&entity.name))),
			_ => return,
		};
		if selected {
			self.selected.insert(key);
		} else {
			self.other.insert(key);
		}
	}

	fn owner(&self, table: &str, field: Option<&[String]>) -> Owner {
		let key = (table.to_string(), field.map(<[String]>::to_vec));
		if self.selected.contains(&key) {
			Owner::Selected
		} else if self.other.contains(&key) {
			Owner::Other
		} else {
			Owner::Nobody
		}
	}
}

/// A place in a file: where a drift finding points.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Location {
	pub path: PathBuf,
	pub span: Range<usize>,
	/// The statement's index among the file's statements; `None` for a line of a config file.
	pub statement: Option<usize>,
}

/// Where the files define each table and field: the `DEFINE` statement of the schema file.
#[derive(Debug, Clone, Default)]
pub struct Locator {
	tables: BTreeMap<String, Location>,
	/// The first statement that names the table, for one the engine created implicitly.
	mentioned: BTreeMap<String, Location>,
	fields: BTreeMap<(String, Vec<String>), Location>,
	first: Option<Location>,
}

impl Locator {
	/// The definitions in `files`, read the way the catalog reads them. When an entity is
	/// defined more than once, the last definition is the one that stands.
	pub fn new(files: &[&Source], vars: &TemplateVars) -> Self {
		let mut locator = Self::default();
		for source in files {
			for defined in defined_entities(&source.text, vars) {
				let at = Location {
					path: source.path.clone(),
					span: defined.span.clone(),
					statement: Some(defined.statement),
				};
				locator.first.get_or_insert_with(|| at.clone());
				match (&defined.key.kind, &defined.key.scope) {
					(EntityKind::Table, _) => {
						locator.tables.insert(plain(&defined.key.name), at);
					}
					(EntityKind::Field, Some(table)) => {
						locator.mentioned.entry(plain(table)).or_insert_with(|| at.clone());
						locator.fields.insert((plain(table), field_path_of(&defined.key.name)), at);
					}
					(_, Some(table)) => {
						locator.mentioned.entry(plain(table)).or_insert(at);
					}
					_ => {}
				}
			}
		}
		locator
	}

	fn table(&self, name: &str) -> Option<&Location> {
		self.tables.get(name).or_else(|| self.mentioned.get(name)).or(self.first.as_ref())
	}

	fn field(&self, table: &str, path: &[String]) -> Option<&Location> {
		self.fields.get(&(table.to_string(), path.to_vec())).or_else(|| self.table(table))
	}
}

/// What to say about one target, besides the drifts themselves.
pub struct TargetFacts<'a> {
	pub name: &'a str,
	/// The `[target.<name>]` header line of `surrealkit.toml`, where a finding about the target
	/// as a whole points.
	pub header: &'a Location,
	pub locator: &'a Locator,
	pub managed: &'a Managed,
	pub ignore: &'a DriftIgnore,
	/// The id of a rollout that was started and not completed on the target.
	pub rollout: Option<&'a str>,
	/// The files' engine minor and the target's, when they differ.
	pub minors: Option<(&'a str, &'a str)>,
}

fn located(
	code: super::finding::Code,
	severity: Severity,
	level: Level,
	message: String,
	at: &Location,
) -> Finding {
	let finding = Finding::new(code, severity, level, message, at.path.clone(), at.span.clone());
	match at.statement {
		Some(statement) => finding.in_statement(statement),
		None => finding,
	}
}

fn help(message: &str) -> Note {
	Note {
		kind: NoteKind::Help,
		message: message.to_string(),
		span: None,
		replacement: None,
	}
}

/// The findings of `drifts` on one target, and `K9107` when its engine minor differs.
///
/// A missing or differing table or field points at its `DEFINE` statement in the schema files.
/// What only the target has is `K9105` when SurrealKit recorded it as managed, `K9106` when it
/// did not, and nothing when `drift_ignore` names it or a module outside this run manages it;
/// those and `K9107` point at the target's header in `surrealkit.toml`. Every message names the
/// target, and every finding of a target with a rollout in progress says so.
pub fn findings(drifts: &[Drift], facts: &TargetFacts<'_>) -> Vec<Finding> {
	let target = facts.name;
	let mut out = Vec::new();
	if let Some((files, theirs)) = facts.minors {
		out.push(
			located(
				K9107,
				Severity::Hint,
				Level::Warn,
				format!(
					"target `{target}` runs SurrealDB {theirs}, and SurrealKit embeds {files}: only \
					 normalised facts were compared"
				),
				facts.header,
			)
			.with_note(help(
				"compare against a target on the embedded minor for an exact comparison",
			)),
		);
	}
	for drift in drifts {
		let table = &drift.table;
		let field = drift.field.as_deref();
		let name = match field {
			Some(path) => format!("field `{table}.{}`", field_name(path)),
			None => format!("table `{table}`"),
		};
		let finding = match drift.kind {
			DriftKind::TableMissing | DriftKind::FieldMissing => {
				let (code, at) = match field {
					Some(path) => (K9103, facts.locator.field(table, path)),
					None => (K9101, facts.locator.table(table)),
				};
				located(
					code,
					Severity::Error,
					Level::Deny,
					format!("{name} is not defined on target `{target}`"),
					at.unwrap_or(facts.header),
				)
			}
			DriftKind::TableDiffers | DriftKind::FieldDiffers => {
				let (code, at) = match field {
					Some(path) => (K9104, facts.locator.field(table, path)),
					None => (K9102, facts.locator.table(table)),
				};
				let facets: Vec<&str> =
					drift.differences.iter().map(|d| d.facet.as_str()).collect();
				let mut finding = located(
					code,
					Severity::Error,
					Level::Deny,
					format!("{name} differs on target `{target}`: {}", facets.join(", ")),
					at.unwrap_or(facts.header),
				);
				for difference in &drift.differences {
					finding = finding
						.with_note(Note::context(format!(
							"the files say {}: {}",
							difference.facet, difference.files
						)))
						.with_note(Note::context(format!(
							"target `{target}` has {}: {}",
							difference.facet, difference.target
						)));
				}
				finding
			}
			DriftKind::TableExtra | DriftKind::FieldExtra => {
				if facts.ignore.covers(table, field) {
					continue;
				}
				match facts.managed.owner(table, field) {
					Owner::Other => continue,
					Owner::Selected => located(
						K9105,
						Severity::Warning,
						Level::Warn,
						format!(
							"{name} is on target `{target}` and managed by SurrealKit, but it is \
							 no longer in the files"
						),
						facts.header,
					)
					.with_note(help(
						"`surrealkit sync` removes it, or define it again in the schema files",
					)),
					Owner::Nobody => located(
						K9106,
						Severity::Warning,
						Level::Warn,
						format!(
							"{name} is on target `{target}` and was never managed by SurrealKit"
						),
						facts.header,
					)
					.with_note(help(
						"if it is intentionally unmanaged, list it under `[analyze] drift_ignore`",
					)),
				}
			}
		};
		out.push(finding);
	}
	if let Some(id) = facts.rollout {
		for finding in &mut out {
			finding.notes.push(Note::context(format!(
				"rollout {id} is in progress on {target}; the target is expected to differ until \
				 it completes"
			)));
		}
	}
	out
}

/// The tables and fields SurrealKit's metadata owns, which no drift pass compares: whatever
/// `setup.surql` defines. (A schema document has no `__` table, the built-in metadata schema's
/// tables, whatever its source.)
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Excluded {
	tables: BTreeSet<String>,
	fields: BTreeSet<(String, Vec<String>)>,
}

impl Excluded {
	/// What the SurrealQL in `texts` defines. Each is a setup file's text, applied as written.
	pub fn from_setup(texts: &[&str]) -> Self {
		let mut excluded = Self::default();
		let no_vars = TemplateVars::default();
		for text in texts {
			for defined in defined_entities(text, &no_vars) {
				match (&defined.key.kind, &defined.key.scope) {
					(EntityKind::Table, _) => {
						excluded.tables.insert(plain(&defined.key.name));
					}
					(EntityKind::Field, Some(table)) => {
						excluded.fields.insert((plain(table), field_path_of(&defined.key.name)));
					}
					_ => {}
				}
			}
		}
		excluded
	}

	/// `doc` without them, and its `semantic_digest` recomputed over what is left.
	pub fn strip(&self, doc: &mut SchemaDocument) -> Result<()> {
		doc.tables.retain(|table| !self.tables.contains(&table.name));
		for table in &mut doc.tables {
			let name = table.name.clone();
			table.fields.retain(|field| !self.fields.contains(&(name.clone(), field.path.clone())));
		}
		let json = serde_json::to_value(&*doc).context("serialising the schema document")?;
		doc.semantic_digest = semantic_digest(&json)?;
		Ok(())
	}
}

/// What a drift pass needs besides the target.
pub struct Env<'a> {
	pub project: &'a ProjectConfig,
	/// The project root, which relative paths are under.
	pub root: &'a Path,
	/// The database folder, which holds `setup.surql`.
	pub folder: &'a str,
	pub vars: &'a TemplateVars,
	pub experimental: &'a [String],
	pub engine: Option<&'a str>,
	pub ignore: &'a DriftIgnore,
}

impl Env<'_> {
	fn setup_file(&self) -> PathBuf {
		let folder = Path::new(self.folder);
		let folder = if folder.is_absolute() {
			folder.to_path_buf()
		} else {
			self.root.join(folder)
		};
		crate::constants::setup_surql_path(&folder.to_string_lossy())
	}

	/// What the project's `setup.surql` defines. The document builder already leaves out every
	/// `__` table, which is where the built-in metadata schema lives.
	fn excluded(&self) -> Excluded {
		let file = std::fs::read_to_string(self.setup_file()).ok();
		Excluded::from_setup(&file.iter().map(String::as_str).collect::<Vec<_>>())
	}
}

/// The files, compiled for the modules a target accepts.
pub struct FilesSide {
	/// Schema document v2 of the files, without SurrealKit's metadata tables.
	pub document: SchemaDocument,
	pub locator: Locator,
	/// The modules compiled.
	pub modules: BTreeSet<String>,
}

/// Compile the schema files of `sources` that belong to `modules`, with the engine and the
/// preparation `compile` uses.
pub async fn files_side(
	sources: &Sources,
	modules: &BTreeSet<String>,
	env: &Env<'_>,
) -> Result<FilesSide> {
	let mut grouped: Vec<CompileModule> = Vec::new();
	let mut kept: Vec<&Source> = Vec::new();
	for source in &sources.schema {
		let Some(module) = source.module.as_deref().filter(|name| modules.contains(*name)) else {
			continue;
		};
		kept.push(source);
		let relative = source.path.strip_prefix(env.root).unwrap_or(&source.path);
		let file = SchemaFile {
			path: relative.to_string_lossy().replace('\\', "/"),
			sql: source.text.clone(),
			hash: sha256_hex(source.text.as_bytes()),
		};
		match grouped.last_mut().filter(|group| group.name == module) {
			Some(group) => group.files.push(file),
			None => grouped.push(CompileModule {
				name: module.to_string(),
				files: vec![file],
			}),
		}
	}
	let opts = CompileOpts {
		experimental: env.experimental.to_vec(),
		vars: env.vars.clone(),
		expected_engine: env.engine.map(str::to_string),
		// Drift compares schemas; no query is typed.
		queries: None,
	};
	let mut document =
		compile(&grouped, &opts).await.context("compiling the schema files")?.document;
	env.excluded().strip(&mut document)?;
	Ok(FilesSide {
		document,
		locator: Locator::new(&kept, env.vars),
		modules: modules.clone(),
	})
}

/// `MAJOR.MINOR` of an engine version.
fn minor_of(version: &str) -> &str {
	match version.match_indices('.').nth(1) {
		Some((at, _)) => &version[..at],
		None => version,
	}
}

/// What one comparison found.
#[derive(Debug, Clone)]
pub struct Compared {
	pub findings: Vec<Finding>,
}

/// Compare the files with the database `db`, which is target `name`.
///
/// Read-only: the target is introspected and its SurrealKit catalog and rollout records are
/// read, and that is all. When both documents have the same digest and the engine minors match,
/// there is no drift and no more than that is read.
pub async fn compare(
	db: &Surreal<Any>,
	name: &str,
	header: &Location,
	files: &FilesSide,
	env: &Env<'_>,
) -> Result<Compared> {
	let mut target = introspect(db, EngineMode::Remote)
		.await
		.with_context(|| format!("reading the schema of target `{name}`"))?;
	env.excluded().strip(&mut target)?;

	let files_version = files.document.provenance.engine.version.as_str();
	let target_version = target.provenance.engine.version.as_str();
	let same_minor = minor_of(files_version) == minor_of(target_version);
	if same_minor && files.document.semantic_digest == target.semantic_digest {
		return Ok(Compared {
			findings: Vec::new(),
		});
	}

	let drifts = diff(&files.document, &target);
	let (has_catalog, has_rollouts) = metadata_tables(db).await?;
	let mut managed = Managed::default();
	if has_catalog && drifts.iter().any(is_extra) {
		let mut every: BTreeSet<String> = env.project.schema.keys().cloned().collect();
		if every.is_empty() {
			every.insert(Module::DEFAULT_NAME.to_string());
		}
		every.extend(files.modules.iter().cloned());
		for module in &every {
			let selected = files.modules.contains(module);
			let module = Module::new(module.clone())?;
			let entities = crate::rollout::load_managed_entities(db, &module, None)
				.await
				.with_context(|| format!("reading the SurrealKit catalog of target `{name}`"))?;
			for record in &entities {
				managed.record(selected, &record.entity);
			}
		}
	}
	let rollout = if has_rollouts && !drifts.is_empty() {
		crate::rollout::load_active_rollout_id(db)
			.await
			.with_context(|| format!("reading the rollouts of target `{name}`"))?
	} else {
		None
	};
	let minors = (!same_minor).then_some((files_version, target_version));
	Ok(Compared {
		findings: findings(
			&drifts,
			&TargetFacts {
				name,
				header,
				locator: &files.locator,
				managed: &managed,
				ignore: env.ignore,
				rollout: rollout.as_deref(),
				minors,
			},
		),
	})
}

/// Whether the target has SurrealKit's `__entity` and `__rollout` tables. A schema document
/// leaves out every `__` table, so this reads `INFO FOR DB`.
async fn metadata_tables(db: &Surreal<Any>) -> Result<(bool, bool)> {
	let info = crate::typegen::introspect_v2::query_json(db, "INFO FOR DB;")
		.await
		.context("INFO FOR DB")?;
	let has = |table: &str| info.get("tables").and_then(|tables| tables.get(table)).is_some();
	Ok((has("__entity"), has("__rollout")))
}

fn is_extra(drift: &Drift) -> bool {
	matches!(drift.kind, DriftKind::TableExtra | DriftKind::FieldExtra)
}

/// The project's `surrealkit.toml`, where a finding about a target as a whole points.
#[derive(Debug, Clone)]
pub struct ConfigFile {
	/// Absolute.
	pub path: PathBuf,
	pub text: String,
}

/// Compare the files in `sources` with each of `targets`, which are connected to one by one.
///
/// A target is compared with the schema files of the modules it accepts (`Target::allows`). A
/// target that accepts none of them is an error, and so is one that cannot be reached or read:
/// a connection failure is not a finding. When the files do not compile the comparison is
/// skipped with a warning, since the check already reports what the engine rejected.
///
/// Targets are compared in the order given, each once.
pub async fn check_targets(
	sources: &Sources,
	targets: &[Target],
	config: &ConfigFile,
	env: &Env<'_>,
) -> Result<Vec<TargetFindings>> {
	let mut compiled: BTreeMap<BTreeSet<String>, Option<FilesSide>> = BTreeMap::new();
	let mut out: Vec<TargetFindings> = Vec::new();
	for target in targets {
		let name = target.name();
		if out.iter().any(|done| done.target == name) {
			continue;
		}
		let modules: BTreeSet<String> = sources
			.schema
			.iter()
			.filter_map(|source| source.module.as_deref())
			.filter(|module| target.allows(module))
			.map(str::to_string)
			.collect();
		if modules.is_empty() {
			bail!(
				"target `{name}` accepts none of the selected schema modules, so there is nothing \
				 to compare; pick a target whose `schemas` list includes them"
			);
		}
		if !compiled.contains_key(&modules) {
			let side = match files_side(sources, &modules, env).await {
				Ok(side) => Some(side),
				Err(err) => {
					log::warn!("target `{name}` was not compared: {err:#}");
					None
				}
			};
			compiled.insert(modules.clone(), side);
		}
		let Some(Some(files)) = compiled.get(&modules) else {
			continue;
		};
		let db = crate::config::connect(target.cfg())
			.await
			.with_context(|| format!("connecting to target `{name}`"))?;
		let header = Location {
			path: config.path.clone(),
			span: target_header(&config.text, name),
			statement: None,
		};
		let found = compare(&db, name, &header, files, env).await?;
		out.push(TargetFindings {
			target: name.to_string(),
			findings: found.findings,
		});
	}
	Ok(out)
}

/// Where `[target.<name>]` is in `text`, a `surrealkit.toml`: the header's line, without its
/// line break. A target written another way (an inline table) is the first line.
pub fn target_header(text: &str, name: &str) -> Range<usize> {
	let mut start = 0;
	for line in text.split_inclusive('\n') {
		let body = line.trim_end_matches(['\n', '\r']);
		let trimmed = body.trim();
		let at = start + (body.len() - body.trim_start().len());
		start += line.len();
		let Some(inner) = trimmed.strip_prefix('[').and_then(|rest| rest.split_once(']')) else {
			continue;
		};
		let Some(key) = inner.0.trim().strip_prefix("target") else {
			continue;
		};
		let key = key.trim_start().strip_prefix('.').map(str::trim);
		let unquoted = key.map(|key| key.trim_matches(['"', '\'']));
		if unquoted == Some(name) {
			return at..at + trimmed.trim_end().len();
		}
	}
	0..text.lines().next().map_or(0, str::len)
}

#[cfg(test)]
mod tests;
