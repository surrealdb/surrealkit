//! Properties of `rollout plan`, `up --complete` and `rollback`, over generated
//! pairs of schema versions, on the embedded engine (`mem://`).
//!
//! Each case is a bounded pair of schemas: up to three tables, each with up to
//! four fields over three kinds and up to two indexes, and the additions,
//! removals and modifications (comments, assertions, index uniqueness) between
//! the first version and the second, with a few rows written under the first.
//! For each pair:
//!
//! 1. after `plan` and `up --complete`, the catalog equals that of a fresh
//!    database synced to the second version;
//! 2. planning again produces a rollout with nothing in it;
//! 3. a row's values in fields whose definition did not change are the same
//!    before and after, and a renamed field carries its old values;
//! 4. when the rollout records itself as reversible, `rollback` after `start`
//!    restores the first version's catalog and rows.
//!
//! The generator keeps to what `plan` accepts. It never adds and removes the
//! same kind of entity in one scope (the rename guard refuses that), except for
//! one field pair defined the same apart from the name. On a `SCHEMAFULL` table
//! whose definition stays as it was, `plan` proposes that pair as a rename and
//! the test confirms it. Anywhere else the case expects `plan` to refuse it and
//! write nothing. Added fields on a table that holds records are `option<…>`,
//! since level 4 refuses a field the records do not fit. Tables are generated
//! both `SCHEMAFULL` and `SCHEMALESS`.
//!
//! The catalog compared is `INFO FOR DB`, each table's `INFO FOR TABLE` without
//! its `migrations` (the engine's history, which a fresh database lacks), and
//! SurrealKit's `__entity` schema catalog.
//!
//! `SURREALKIT_ROLLOUT_PROPTEST_CASES` sets the number of cases. The default, 12,
//! took 16.5 to 17.3 s over three runs (2026-11-23, Apple silicon, debug build,
//! the property test alone), about 1.4 s a case. Failures are shrunk. Commit a
//! minimal failing case as an ordinary test under `regressions`, written from
//! the `Case` text the failure prints. The shrinker can stop early: on the first
//! finding it stopped at three tables, and the committed case was reduced to one
//! by hand.

#![expect(clippy::unwrap_used, reason = "test helpers fail the test on any unexpected state")]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result, bail, ensure};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use serde_json::{Map, Value, json};
use surrealdb::Surreal;
use surrealdb::engine::any::{Any, connect};
use surrealdb::opt::Config as DbConfig;
use surrealdb::opt::capabilities::Capabilities;
use surrealkit::module::Module;
use surrealkit::rollout::{
	RolloutExecutionOpts, RolloutPlanOpts, RolloutUpOpts, run_baseline, run_plan, run_rollback,
	run_start, run_up,
};
use surrealkit::sync::{SyncOpts, run_sync};
use surrealkit::{RolloutSpec, TemplateVars};

const DEFAULT_CASES: u32 = 12;
const TABLES: usize = 3;
const FIELDS: usize = 4;
const INDEXES: usize = 2;
const ROWS: usize = 3;

// ---------------------------------------------------------------------------
// The generator
// ---------------------------------------------------------------------------

/// Which of the two versions an entity is in. Shrinks towards `Neither`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Presence {
	Neither,
	First,
	Second,
	Both,
}

impl Presence {
	fn in_version(self, version: usize) -> bool {
		matches!((self, version), (Self::Both, _) | (Self::First, 0) | (Self::Second, 1))
	}
}

fn presence() -> impl Strategy<Value = Presence> {
	prop_oneof![
		1 => Just(Presence::Neither),
		1 => Just(Presence::First),
		1 => Just(Presence::Second),
		3 => Just(Presence::Both),
	]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
	Int,
	Str,
	Bool,
}

#[derive(Debug, Clone)]
struct RawField {
	presence: Presence,
	kind: Kind,
	optional: bool,
	comment: [Option<u8>; 2],
	assert: [bool; 2],
}

#[derive(Debug, Clone)]
struct RawIndex {
	presence: Presence,
	field: usize,
	unique: [bool; 2],
}

#[derive(Debug, Clone)]
struct RawRow {
	seed: u16,
	none: Vec<bool>,
}

#[derive(Debug, Clone)]
struct RawTable {
	presence: Presence,
	schemafull: bool,
	comment: [Option<u8>; 2],
	rename: bool,
	fields: Vec<RawField>,
	indexes: Vec<RawIndex>,
	rows: Vec<RawRow>,
}

fn comment() -> impl Strategy<Value = Option<u8>> {
	prop_oneof![3 => Just(None), 1 => (0..2u8).prop_map(Some)]
}

fn raw_field() -> impl Strategy<Value = RawField> {
	(
		presence(),
		prop_oneof![Just(Kind::Int), Just(Kind::Str), Just(Kind::Bool)],
		any::<bool>(),
		[comment(), comment()],
		[prop::bool::weighted(0.2), prop::bool::weighted(0.2)],
	)
		.prop_map(|(presence, kind, optional, comment, assert)| RawField {
			presence,
			kind,
			optional,
			comment,
			assert,
		})
}

fn raw_index() -> impl Strategy<Value = RawIndex> {
	(presence(), 0..FIELDS, [any::<bool>(), any::<bool>()]).prop_map(|(presence, field, unique)| {
		RawIndex {
			presence,
			field,
			unique,
		}
	})
}

fn raw_table() -> impl Strategy<Value = RawTable> {
	(
		presence(),
		any::<bool>(),
		[comment(), comment()],
		prop::bool::weighted(0.4),
		prop::collection::vec(raw_field(), FIELDS),
		prop::collection::vec(raw_index(), INDEXES),
		prop::collection::vec(
			(any::<u16>(), prop::collection::vec(any::<bool>(), FIELDS)).prop_map(
				|(seed, none)| RawRow {
					seed,
					none,
				},
			),
			0..=ROWS,
		),
	)
		.prop_map(|(presence, schemafull, comment, rename, fields, indexes, rows)| RawTable {
			presence,
			schemafull,
			comment,
			rename,
			fields,
			indexes,
			rows,
		})
}

fn raw_case() -> impl Strategy<Value = Vec<RawTable>> {
	prop::collection::vec(raw_table(), TABLES)
}

// ---------------------------------------------------------------------------
// A case: two versions and the rows written under the first
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct Field {
	kind: Kind,
	optional: bool,
	comment: Option<u8>,
	assert: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Index {
	field: usize,
	unique: bool,
}

#[derive(Debug, Clone, Default)]
struct Table {
	schemafull: bool,
	comment: Option<u8>,
	fields: BTreeMap<usize, Field>,
	indexes: BTreeMap<usize, Index>,
}

/// One version of the schema: table slot to table.
type Version = BTreeMap<usize, Table>;

/// What a test runs: each version's files, and the statements that write the
/// rows under the first.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Case {
	first: Vec<(String, String)>,
	second: Vec<(String, String)>,
	rows: String,
	/// The pair holds a rename `plan` must refuse: on a table that is not
	/// `SCHEMAFULL`, or whose own definition changes too.
	refused: bool,
}

impl fmt::Display for Case {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		for (label, files) in [("first", &self.first), ("second", &self.second)] {
			writeln!(f, "-- {label} version")?;
			for (name, sql) in files {
				writeln!(f, "-- {name}\n{sql}")?;
			}
		}
		writeln!(f, "-- rows\n{}-- refused: {}", self.rows, self.refused)
	}
}

/// Turn raw choices into a pair `plan` accepts. Every rule here is one the
/// product documents; see the module comment.
fn normalize(raw: &[RawTable]) -> Case {
	let mut raw = raw.to_vec();

	// Adding one table and removing another is refused as a possible rename.
	let adds = raw.iter().any(|t| t.presence == Presence::Second);
	let removes = raw.iter().any(|t| t.presence == Presence::First);
	if adds && removes {
		for table in &mut raw {
			if table.presence == Presence::First {
				table.presence = Presence::Both;
			}
		}
	}

	let mut renamed: BTreeMap<usize, (usize, usize)> = BTreeMap::new();
	let mut refused = false;
	for (slot, table) in raw.iter_mut().enumerate() {
		if table.presence != Presence::Both {
			continue;
		}
		let removed: Vec<usize> =
			(0..FIELDS).filter(|&f| table.fields[f].presence == Presence::First).collect();
		let added: Vec<usize> =
			(0..FIELDS).filter(|&f| table.fields[f].presence == Presence::Second).collect();
		if !removed.is_empty() && !added.is_empty() {
			if table.rename {
				// One pair defined the same apart from the name: a rename.
				let (from, to) = (removed[0], added[0]);
				let source = table.fields[from].clone();
				let target = &mut table.fields[to];
				target.kind = source.kind;
				target.optional = true;
				target.comment[1] = source.comment[0];
				target.assert[1] = source.assert[0];
				table.fields[from].optional = true;
				for &f in &removed[1..] {
					table.fields[f].presence = Presence::Both;
				}
				for &f in &added[1..] {
					table.fields[f].presence = Presence::Neither;
				}
				renamed.insert(slot, (from, to));
				// `plan` proposes a rename only on a SCHEMAFULL table whose
				// definition stays as it was, and refuses the pair elsewhere.
				refused |= !table.schemafull || table.comment[0] != table.comment[1];
			} else {
				for &f in &removed {
					table.fields[f].presence = Presence::Both;
				}
			}
		}
		// A field added to a table with records must accept NONE.
		for field in &mut table.fields {
			if field.presence == Presence::Second {
				field.optional = true;
			}
		}
	}

	let mut versions: [Version; 2] = [Version::new(), Version::new()];
	for (slot, table) in raw.iter().enumerate() {
		let pair = renamed.get(&slot).copied();
		let mut index_presence: Vec<[bool; 2]> = table
			.indexes
			.iter()
			.map(|index| {
				let on_rename = pair.is_some_and(|(a, b)| index.field == a || index.field == b);
				[0, 1].map(|v| {
					table.presence.in_version(v)
						&& index.presence.in_version(v)
						&& table.fields[index.field].presence.in_version(v)
						&& !on_rename
				})
			})
			.collect();
		// Adding one index and removing another on one table is refused too.
		let index_adds = index_presence.iter().any(|p| !p[0] && p[1]);
		let index_removes = index_presence.iter().any(|p| p[0] && !p[1]);
		if table.presence == Presence::Both && index_adds && index_removes {
			for p in &mut index_presence {
				if !p[0] {
					p[1] = false;
				}
			}
		}

		for (v, version) in versions.iter_mut().enumerate() {
			if !table.presence.in_version(v) {
				continue;
			}
			let fields = table
				.fields
				.iter()
				.enumerate()
				.filter(|(_, field)| field.presence.in_version(v))
				.map(|(f, field)| {
					(
						f,
						Field {
							kind: field.kind,
							optional: field.optional,
							comment: field.comment[v],
							assert: field.assert[v],
						},
					)
				})
				.collect();
			let indexes = table
				.indexes
				.iter()
				.enumerate()
				.filter(|(i, _)| index_presence[*i][v])
				.map(|(i, index)| {
					(
						i,
						Index {
							field: index.field,
							unique: index.unique[v] && table.fields[index.field].kind != Kind::Bool,
						},
					)
				})
				.collect();
			version.insert(
				slot,
				Table {
					schemafull: table.schemafull,
					comment: table.comment[v],
					fields,
					indexes,
				},
			);
		}
	}

	let mut rows = String::new();
	for (slot, table) in raw.iter().enumerate() {
		let Some(first) = versions[0].get(&slot) else {
			continue;
		};
		let unique_fields: BTreeSet<usize> = versions
			.iter()
			.filter_map(|version| version.get(&slot))
			.flat_map(|t| t.indexes.values().filter(|i| i.unique).map(|i| i.field))
			.collect();
		for (r, row) in table.rows.iter().enumerate() {
			let mut content = Map::new();
			for (&f, field) in &first.fields {
				if field.optional && row.none[f] && !unique_fields.contains(&f) {
					continue;
				}
				// Distinct across rows, so a unique index always builds, and
				// inside every assertion the generator writes.
				let value = match field.kind {
					Kind::Int => json!(r as u64 * 100_000 + u64::from(row.seed)),
					Kind::Str => json!(format!("r{r}s{}", row.seed)),
					Kind::Bool => json!(row.seed % 2 == 0),
				};
				content.insert(format!("f{f}"), value);
			}
			if !first.schemafull {
				content.insert("x".into(), json!(r));
			}
			rows.push_str(&format!("CREATE t{slot}:r{r} CONTENT {};\n", Value::Object(content)));
		}
	}

	Case {
		first: render(&versions[0]),
		second: render(&versions[1]),
		rows,
		refused,
	}
}

fn render(version: &Version) -> Vec<(String, String)> {
	let comment = |c: Option<u8>| c.map(|c| format!(" COMMENT 'c{c}'")).unwrap_or_default();
	version
		.iter()
		.map(|(slot, table)| {
			let mode = if table.schemafull {
				"SCHEMAFULL"
			} else {
				"SCHEMALESS"
			};
			let mut sql = format!("DEFINE TABLE t{slot} {mode}{};\n", comment(table.comment));
			for (f, field) in &table.fields {
				let base = match field.kind {
					Kind::Int => "int",
					Kind::Str => "string",
					Kind::Bool => "bool",
				};
				let ty = if field.optional {
					format!("option<{base}>")
				} else {
					base.to_string()
				};
				let assert = if field.assert {
					let check = match field.kind {
						Kind::Int => "$value >= 0",
						Kind::Str => "string::len($value) > 0",
						Kind::Bool => "$value IN [true, false]",
					};
					if field.optional {
						format!(" ASSERT $value = NONE OR {check}")
					} else {
						format!(" ASSERT {check}")
					}
				} else {
					String::new()
				};
				sql.push_str(&format!(
					"DEFINE FIELD f{f} ON t{slot} TYPE {ty}{assert}{};\n",
					comment(field.comment)
				));
			}
			for (i, index) in &table.indexes {
				let unique = if index.unique {
					" UNIQUE"
				} else {
					""
				};
				sql.push_str(&format!(
					"DEFINE INDEX t{slot}_i{i} ON t{slot} FIELDS f{}{unique};\n",
					index.field
				));
			}
			(format!("t{slot}.surql"), sql)
		})
		.collect()
}

// ---------------------------------------------------------------------------
// The harness
// ---------------------------------------------------------------------------

async fn mem_db() -> Surreal<Any> {
	let db = connect(("mem://", DbConfig::new().capabilities(Capabilities::all())))
		.await
		.expect("connect");
	db.use_ns("props").use_db("props").await.expect("use");
	db
}

struct Project {
	_tmp: tempfile::TempDir,
	root: PathBuf,
}

impl Project {
	fn new(files: &[(String, String)]) -> Self {
		let tmp = tempfile::TempDir::new().expect("tempdir");
		let root = tmp.path().join("database");
		fs::create_dir_all(root.join("schema")).expect("schema dir");
		let project = Self {
			_tmp: tmp,
			root,
		};
		project.write(files);
		project
	}

	fn folder(&self) -> String {
		self.root.to_string_lossy().into_owned()
	}

	/// Make the schema folder exactly `files`.
	fn write(&self, files: &[(String, String)]) {
		let schema = self.root.join("schema");
		for entry in fs::read_dir(&schema).unwrap() {
			fs::remove_file(entry.unwrap().path()).unwrap();
		}
		for (name, sql) in files {
			fs::write(schema.join(name), sql).unwrap();
		}
	}

	async fn sync(&self, db: &Surreal<Any>) -> Result<()> {
		run_sync(
			db,
			SyncOpts {
				folder: self.folder(),
				module: Module::default_module(),
				prune: true,
				// Either version may define nothing.
				allow_empty_prune: true,
				fail_fast: true,
				..SyncOpts::default()
			},
		)
		.await
		.map(|_| ())
		.context("sync")
	}

	/// Plan, and confirm every rename it proposes, as an author who read the
	/// copy would. Returns the rollout id and the renames as (table, from, to).
	async fn plan(&self, name: &str) -> Result<(String, Vec<(String, String, String)>)> {
		let planned = run_plan(
			&self.folder(),
			RolloutPlanOpts {
				name: Some(name.to_string()),
				dry_run: false,
				allow_modified: true,
			},
		)
		.await
		.context("plan")?;
		let suffix = format!("__{name}.toml");
		let path = fs::read_dir(self.root.join("rollouts"))?
			.filter_map(Result::ok)
			.map(|e| e.path())
			.find(|p| p.to_string_lossy().ends_with(&suffix))
			.context("no manifest")?;
		let id = path.file_stem().unwrap().to_string_lossy().into_owned();
		if !planned.renames.is_empty() {
			let raw = fs::read_to_string(&path)?;
			fs::write(&path, raw.replace("confirmed = false", "confirmed = true"))?;
		}
		let renames = planned
			.renames
			.iter()
			.map(|r| (r.scope.clone().unwrap_or_default(), r.from.clone(), r.to.clone()))
			.collect();
		Ok((id, renames))
	}

	fn manifest(&self, id: &str) -> Result<RolloutSpec> {
		let raw = fs::read_to_string(self.root.join("rollouts").join(format!("{id}.toml")))?;
		Ok(toml::from_str(&raw)?)
	}
}

async fn info(db: &Surreal<Any>, sql: &str) -> Result<Value> {
	let mut res = db.query(sql).await?.check()?;
	let value: Option<Value> = res.take(0)?;
	Ok(value.unwrap_or_default())
}

/// The user's tables, by name.
async fn tables(db: &Surreal<Any>) -> Result<Vec<String>> {
	let db_info = info(db, "INFO FOR DB;").await?;
	let mut names: Vec<String> = db_info["tables"]
		.as_object()
		.map(|t| t.keys().filter(|k| !k.starts_with("__")).cloned().collect())
		.unwrap_or_default();
	names.sort();
	Ok(names)
}

/// What a database defines: `INFO FOR DB` without SurrealKit's own tables, each
/// user table's `INFO FOR TABLE`, and SurrealKit's schema catalog.
async fn catalog(db: &Surreal<Any>) -> Result<Value> {
	let mut db_info = info(db, "INFO FOR DB;").await?;
	if let Some(tables) = db_info.get_mut("tables").and_then(Value::as_object_mut) {
		tables.retain(|name, _| !name.starts_with("__"));
	}
	let mut per_table = Map::new();
	for table in tables(db).await? {
		let mut table_info = info(db, &format!("INFO FOR TABLE {table};")).await?;
		// `migrations` is the engine's history of the table, not what it
		// defines: a counter of every schema change, and an entry for each
		// level-4 `REMOVE FIELD` that clears values ("remove f2 after 2"). A
		// fresh database has neither, so it is left out.
		if let Some(table_info) = table_info.as_object_mut() {
			table_info.remove("migrations");
		}
		per_table.insert(table.clone(), table_info);
	}
	let mut res =
		db.query("SELECT key, val FROM __entity WHERE ns = 'schema' ORDER BY key;").await?;
	let entities: Vec<Value> = res.take(0)?;
	Ok(json!({ "db": db_info, "tables": per_table, "entities": entities }))
}

/// Every row of every user table, by table and record id.
async fn rows(db: &Surreal<Any>) -> Result<BTreeMap<String, BTreeMap<String, Value>>> {
	let mut all = BTreeMap::new();
	for table in tables(db).await? {
		let mut res = db.query(format!("SELECT * FROM {table};")).await?;
		let rows: Vec<Value> = res.take(0)?;
		let by_id = rows.into_iter().map(|row| (row["id"].to_string(), row)).collect();
		all.insert(table, by_id);
	}
	Ok(all)
}

/// Field definitions that are the same in both catalogs, by table.
fn unchanged_fields(first: &Value, second: &Value) -> BTreeMap<String, BTreeSet<String>> {
	let mut unchanged = BTreeMap::new();
	let Some(tables) = first["tables"].as_object() else {
		return unchanged;
	};
	for (table, info) in tables {
		let Some(fields) = info["fields"].as_object() else {
			continue;
		};
		let same: BTreeSet<String> = fields
			.iter()
			.filter(|(name, def)| second["tables"][table]["fields"].get(*name) == Some(def))
			.map(|(name, _)| name.clone())
			.collect();
		if second["tables"].get(table).is_some() {
			unchanged.insert(table.clone(), same);
		}
	}
	unchanged
}

async fn reversibility(db: &Surreal<Any>, id: &str) -> Result<Option<String>> {
	let mut res = db
		.query("SELECT VALUE reversibility FROM type::record('__rollout', $id);")
		.bind(("id", id.to_string()))
		.await?;
	let rows: Vec<String> = res.take(0)?;
	Ok(rows.into_iter().next())
}

/// Run every property against one case.
async fn check(case: &Case) -> Result<()> {
	let project = Project::new(&case.first);
	let upgraded = mem_db().await;
	let rolled_back = mem_db().await;
	for db in [&upgraded, &rolled_back] {
		project.sync(db).await?;
		run_baseline(db, &project.folder(), &Module::default_module()).await.context("baseline")?;
		db.query(&case.rows).await?.check().context("writing the rows")?;
	}
	let first_catalog = catalog(&rolled_back).await?;
	let first_rows = rows(&rolled_back).await?;

	project.write(&case.second);
	if case.refused {
		let Err(err) = project.plan("next").await else {
			bail!("plan accepted a rename it must refuse");
		};
		ensure!(
			format!("{err:#}").contains("cannot tell apart from a rename"),
			"plan refused the rename for another reason: {err:#}"
		);
		ensure!(
			!project.root.join("rollouts").exists()
				|| fs::read_dir(project.root.join("rollouts"))?.next().is_none(),
			"a refused plan wrote a manifest"
		);
		return Ok(());
	}
	let (id, renames) = project.plan("next").await?;

	// 1. `up --complete` reaches the catalog a fresh sync of the second version has.
	run_up(
		&upgraded,
		&project.folder(),
		RolloutUpOpts {
			complete_newest: true,
			..RolloutUpOpts::default()
		},
		&TemplateVars::default(),
	)
	.await
	.context("up --complete")?;
	let fresh_project = Project::new(&case.second);
	let fresh = mem_db().await;
	fresh_project.sync(&fresh).await?;
	let second_catalog = catalog(&fresh).await?;
	let upgraded_catalog = catalog(&upgraded).await?;
	ensure!(
		upgraded_catalog == second_catalog,
		"property 1: after up --complete the catalog is\n{upgraded_catalog:#}\nbut a fresh sync \
		 of the second version has\n{second_catalog:#}"
	);

	// 2. Planning again finds nothing to do.
	let (again, again_renames) = project.plan("again").await?;
	let spec = project.manifest(&again)?;
	ensure!(
		spec.steps.is_empty() && again_renames.is_empty(),
		"property 2: planning again produced steps {:?} and renames {again_renames:?}",
		spec.steps
	);

	// 3. Unchanged fields keep their values; a renamed field carries them.
	let after = rows(&upgraded).await?;
	for (table, fields) in unchanged_fields(&first_catalog, &second_catalog) {
		let before = &first_rows[&table];
		let now = after.get(&table).context("table gone")?;
		ensure!(
			before.keys().eq(now.keys()),
			"property 3: {table} had rows {:?} and has {:?}",
			before.keys().collect::<Vec<_>>(),
			now.keys().collect::<Vec<_>>()
		);
		let mut kept: Vec<(String, String)> =
			fields.iter().map(|f| (f.clone(), f.clone())).collect();
		kept.extend(
			renames
				.iter()
				.filter(|(t, _, _)| *t == table)
				.map(|(_, f, to)| (f.clone(), to.clone())),
		);
		kept.push(("x".into(), "x".into()));
		for (rid, row) in before {
			for (from, to) in &kept {
				ensure!(
					row.get(from) == now[rid].get(to),
					"property 3: {rid}.{from} was {:?} and {rid}.{to} is {:?}",
					row.get(from),
					now[rid].get(to)
				);
			}
		}
	}

	// 4. A rollout that records itself as reversible rolls back to the first
	//    version, catalog and rows.
	run_start(
		&rolled_back,
		&project.folder(),
		RolloutExecutionOpts::new(Some(id.clone())),
		&TemplateVars::default(),
	)
	.await
	.context("start")?;
	match reversibility(&rolled_back, &id).await?.as_deref() {
		Some("full" | "definition_only") => {}
		Some("irreversible") => return Ok(()),
		other => bail!("rollout records reversibility {other:?}"),
	}
	run_rollback(
		&rolled_back,
		&project.folder(),
		RolloutExecutionOpts::new(Some(id)),
		&TemplateVars::default(),
	)
	.await
	.context("rollback")?;
	let restored_catalog = catalog(&rolled_back).await?;
	ensure!(
		restored_catalog == first_catalog,
		"property 4: after rollback the catalog is\n{restored_catalog:#}\nbut the first version \
		 had\n{first_catalog:#}"
	);
	let restored_rows = rows(&rolled_back).await?;
	ensure!(
		restored_rows == first_rows,
		"property 4: after rollback the rows are\n{restored_rows:#?}\nbut were\n{first_rows:#?}"
	);
	Ok(())
}

fn cases() -> u32 {
	std::env::var("SURREALKIT_ROLLOUT_PROPTEST_CASES")
		.ok()
		.and_then(|raw| raw.parse().ok())
		.unwrap_or(DEFAULT_CASES)
}

#[test]
fn rollouts_hold_their_properties_over_generated_schema_pairs() {
	let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
	let mut runner = TestRunner::new(Config {
		cases: cases(),
		// A failure is committed by hand as a regression below, not to a file
		// the runner writes beside this one.
		failure_persistence: None,
		..Config::default()
	});
	let result = runner.run(&raw_case(), |raw| {
		let case = normalize(&raw);
		runtime
			.block_on(check(&case))
			.map_err(|err| TestCaseError::fail(format!("{err:#}\n{case}")))
	});
	if let Err(err) = result {
		panic!("{err}");
	}
}

/// The normalizer is pure, so its rules are checked without an engine.
#[test]
fn generated_pairs_keep_to_what_plan_accepts() {
	let mut runner = TestRunner::new(Config {
		cases: 256,
		failure_persistence: None,
		..Config::default()
	});
	runner
		.run(&raw_case(), |raw| {
			let case = normalize(&raw);
			let names = |files: &[(String, String)]| -> BTreeSet<String> {
				files.iter().map(|(n, _)| n.clone()).collect()
			};
			let (first, second) = (names(&case.first), names(&case.second));
			prop_assert!(
				first.is_subset(&second) || second.is_subset(&first),
				"a table added and another removed:\n{case}"
			);
			Ok(())
		})
		.unwrap();
}

mod regressions {
	//! Minimal failing cases the property test has found, kept as ordinary tests.

	use super::{Case, check};

	fn person(mode: &str, field: &str) -> Vec<(String, String)> {
		vec![(
			"t0.surql".to_string(),
			format!("DEFINE TABLE t0 {mode};\nDEFINE FIELD {field} ON t0 TYPE option<int>;\n"),
		)]
	}

	/// Property 4 found this on 2026-11-23, reduced by hand to one table from
	/// the three-table case the shrinker stopped at. `plan` proposed renaming
	/// `f3` to `f2` on a schemaless table, the rollout recorded itself as
	/// reversible, and after `rollback` the record still held the copied
	/// `f2: 3`, since `REMOVE FIELD` leaves values on a schemaless table. `plan`
	/// now proposes no rename there and refuses the pair.
	#[tokio::test(flavor = "multi_thread")]
	async fn a_rename_on_a_schemaless_table_is_refused_rather_than_left_behind_by_rollback() {
		check(&Case {
			first: person("SCHEMALESS", "f3"),
			second: person("SCHEMALESS", "f2"),
			rows: "CREATE t0:r0 CONTENT { f3: 3 };\n".into(),
			refused: true,
		})
		.await
		.unwrap();
	}

	/// The same rename on a `SCHEMAFULL` table, where level 4 clears the copies
	/// on rollback, holds every property.
	#[tokio::test(flavor = "multi_thread")]
	async fn the_same_rename_on_a_schemafull_table_holds_every_property() {
		check(&Case {
			first: person("SCHEMAFULL", "f3"),
			second: person("SCHEMAFULL", "f2"),
			rows: "CREATE t0:r0 CONTENT { f3: 3 };\n".into(),
			refused: false,
		})
		.await
		.unwrap();
	}
}
