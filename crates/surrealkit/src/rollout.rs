use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb_types::SurrealValue;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;

use crate::core::{exec_surql, sha256_hex};
use crate::schema_state::{
	CATALOG_SNAPSHOT_PATH, CatalogDiff, CatalogEntity, CatalogSnapshot, EntityKey, FileDiff,
	ROLLOUTS_DIR, SchemaFile, build_catalog_snapshot, collect_schema_files, diff_catalog,
	diff_schema, ensure_local_state_dirs, hash_schema_snapshot, load_catalog_snapshot,
	load_schema_snapshot, render_remove_sql, save_catalog_snapshot, save_schema_snapshot,
	snapshot_from_files,
};
use crate::setup::run_setup;

#[derive(Debug, Clone)]
pub struct RolloutPlanOpts {
	pub name: Option<String>,
	pub dry_run: bool,
}

#[derive(Debug, Clone)]
pub struct RolloutExecutionOpts {
	pub selector: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RolloutPhase {
	Start,
	Complete,
	Rollback,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RolloutStepKind {
	ApplySchema,
	RunSql,
	AssertSql,
	RemoveEntities,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RolloutStatus {
	Planned,
	RunningStart,
	ReadyToComplete,
	RunningComplete,
	Completed,
	RunningRollback,
	RolledBack,
	Failed,
}

impl RolloutStatus {
	fn as_str(&self) -> &'static str {
		match self {
			Self::Planned => "planned",
			Self::RunningStart => "running_start",
			Self::ReadyToComplete => "ready_to_complete",
			Self::RunningComplete => "running_complete",
			Self::Completed => "completed",
			Self::RunningRollback => "running_rollback",
			Self::RolledBack => "rolled_back",
			Self::Failed => "failed",
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RolloutSpec {
	pub id: String,
	pub name: String,
	pub source_schema_hash: String,
	pub target_schema_hash: String,
	pub compatibility: String,
	#[serde(default)]
	pub renames: Vec<RolloutRename>,
	#[serde(default)]
	pub steps: Vec<RolloutStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RolloutRename {
	pub kind: String,
	pub scope: Option<String>,
	pub from: String,
	pub to: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RolloutStep {
	pub id: String,
	pub phase: RolloutPhase,
	pub kind: RolloutStepKind,
	#[serde(default)]
	pub files: Vec<String>,
	pub sql: Option<String>,
	pub expect: Option<String>,
	#[serde(default)]
	pub entities: Vec<EntityKey>,
	pub idempotent: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct LoadedRolloutSpec {
	pub path: PathBuf,
	pub checksum: String,
	pub spec: RolloutSpec,
}

#[expect(dead_code)]
#[derive(Debug, Clone)]
pub struct ManagedEntityRecord {
	pub entity: CatalogEntity,
	pub active_rollout_id: Option<String>,
	pub state: String,
}

pub async fn run_baseline(db: &Surreal<Any>) -> Result<()> {
	run_setup(db).await?;
	ensure_local_state_dirs()?;
	if rollout_rows_exist(db).await? {
		bail!("rollout state already exists; baseline can only be run once");
	}

	let files = collect_schema_files()?;
	let schema_snapshot = snapshot_from_files(&files);
	let catalog_snapshot = build_catalog_snapshot(&files)?;

	replace_managed_entities(db, &catalog_snapshot.entities, None, "active").await?;
	replace_sync_hashes(db, &files).await?;
	save_schema_snapshot(&schema_snapshot)?;
	save_catalog_snapshot(&catalog_snapshot)?;

	println!(
		"Seeded managed entity baseline with {} schema file(s) and {} managed object(s).",
		files.len(),
		catalog_snapshot.entities.len()
	);
	Ok(())
}

pub async fn run_plan(opts: RolloutPlanOpts) -> Result<()> {
	ensure_local_state_dirs()?;
	let files = collect_schema_files()?;
	let old_schema = load_schema_snapshot()?;
	let old_catalog = load_catalog_snapshot()?;
	let new_schema = snapshot_from_files(&files);
	let new_catalog = build_catalog_snapshot(&files)?;
	let file_diff = diff_schema(&old_schema, &new_schema);
	let catalog_diff = diff_catalog(&old_catalog, &new_catalog);

	validate_autoplan(&catalog_diff)?;

	let name = opts.name.unwrap_or_else(|| "schema_rollout".to_string());
	let slug = slugify(&name);
	let ts = OffsetDateTime::now_utc()
		.format(&format_description!("[year][month][day][hour][minute][second]"))?;
	let rollout_id = format!("{ts}__{slug}");
	let path = Path::new(ROLLOUTS_DIR).join(format!("{rollout_id}.toml"));

	let spec = build_rollout_spec(
		&rollout_id,
		&name,
		&files,
		&file_diff,
		&catalog_diff,
		&old_schema,
		&new_schema,
	)?;
	let raw = toml::to_string_pretty(&spec).context("serializing rollout spec")?;

	if opts.dry_run {
		println!("Pending rollout plan:");
		println!(
			"  files: +{} ~{} -{}",
			file_diff.added.len(),
			file_diff.modified.len(),
			file_diff.removed.len()
		);
		println!(
			"  entities: +{} ~{} -{}",
			catalog_diff.added.len(),
			catalog_diff.modified.len(),
			catalog_diff.removed.len()
		);
		println!("  would create: {}", path.display());
		return Ok(());
	}

	fs::write(&path, raw).with_context(|| format!("writing rollout file {}", path.display()))?;
	save_schema_snapshot(&new_schema)?;
	save_catalog_snapshot(&new_catalog)?;

	println!("Generated rollout manifest {}", path.display());
	println!("Updated {}", CATALOG_SNAPSHOT_PATH);
	Ok(())
}

pub async fn run_lint(opts: RolloutExecutionOpts) -> Result<()> {
	ensure_local_state_dirs()?;
	let rollout = load_rollout_spec(resolve_rollout_path(opts.selector.as_deref())?)?;
	validate_rollout_spec(&rollout.spec)?;
	let files = collect_schema_files()?;
	let current_hash = hash_schema_snapshot(&snapshot_from_files(&files))?;
	if current_hash != rollout.spec.target_schema_hash {
		bail!(
			"target schema hash mismatch for '{}': manifest={}, current={}",
			rollout.spec.id,
			rollout.spec.target_schema_hash,
			current_hash
		);
	}
	println!("Rollout {} is valid (checksum {}).", rollout.spec.id, rollout.checksum);
	Ok(())
}

pub async fn run_status(db: &Surreal<Any>, selector: Option<String>) -> Result<()> {
	run_setup(db).await?;
	let mut query =
		"SELECT id, name, status, started_at, completed_at, last_error FROM _surrealkit_rollout"
			.to_string();
	if selector.is_some() {
		query.push_str(" WHERE id = $id");
	}
	query.push_str(" ORDER BY started_at DESC;");

	let mut req = db.query(query);
	if let Some(id) = selector {
		req = req.bind(("id", id));
	}
	let mut resp = req.await?;
	let rows: Vec<Value> = resp.take(0)?;
	if rows.is_empty() {
		println!("No rollout records found.");
		return Ok(());
	}

	for row in rows {
		let id = string_field(&row, "id").unwrap_or_else(|| "<unknown>".to_string());
		let name = string_field(&row, "name").unwrap_or_else(|| "<unnamed>".to_string());
		let status = string_field(&row, "status").unwrap_or_else(|| "<unknown>".to_string());
		println!("{} [{}] {}", id, status, name);
		if let Some(started_at) = string_field(&row, "started_at") {
			println!("  started_at: {}", started_at);
		}
		if let Some(completed_at) = string_field(&row, "completed_at") {
			println!("  completed_at: {}", completed_at);
		}
		if let Some(last_error) = string_field(&row, "last_error") {
			println!("  last_error: {}", last_error);
		}

		let mut step_resp = db
			.query(
				"SELECT step_id, phase, kind, status, error FROM _surrealkit_rollout_step \
				 WHERE rollout_id = $rollout_id ORDER BY started_at, step_id;",
			)
			.bind(("rollout_id", id.clone()))
			.await?;
		let steps: Vec<Value> = step_resp.take(0)?;
		for step in steps {
			let step_id = string_field(&step, "step_id").unwrap_or_else(|| "<step>".to_string());
			let phase = string_field(&step, "phase").unwrap_or_else(|| "?".to_string());
			let kind = string_field(&step, "kind").unwrap_or_else(|| "?".to_string());
			let status = string_field(&step, "status").unwrap_or_else(|| "?".to_string());
			println!("  - {} [{}:{}] {}", step_id, phase, kind, status);
			if let Some(err) = string_field(&step, "error") {
				println!("    error: {}", err);
			}
		}
	}
	Ok(())
}

pub async fn run_start(db: &Surreal<Any>, opts: RolloutExecutionOpts) -> Result<()> {
	run_setup(db).await?;
	ensure_local_state_dirs()?;
	let rollout = load_rollout_spec(resolve_rollout_path(opts.selector.as_deref())?)?;
	validate_rollout_spec(&rollout.spec)?;
	let files = collect_schema_files()?;
	let target_schema = snapshot_from_files(&files);
	let target_hash = hash_schema_snapshot(&target_schema)?;
	if target_hash != rollout.spec.target_schema_hash {
		bail!(
			"target schema hash mismatch for '{}': manifest={}, current={}",
			rollout.spec.id,
			rollout.spec.target_schema_hash,
			target_hash
		);
	}
	let target_catalog = build_catalog_snapshot(&files)?;
	let source_entities = load_managed_entities(db).await?;
	let source_catalog = CatalogSnapshot {
		version: 2,
		entities: source_entities.iter().map(|row| row.entity.clone()).collect(),
	};

	acquire_lock(db, "global").await?;
	let result = async {
		ensure_no_conflicting_active_rollout(db, &rollout.spec.id).await?;
		let record = load_rollout_record(db, &rollout.spec.id).await?;
		match record.as_ref().and_then(|row| string_field(row, "status")).as_deref() {
			Some("completed") => bail!("rollout '{}' is already completed", rollout.spec.id),
			Some("rolled_back") => {
				bail!("rollout '{}' has already been rolled back", rollout.spec.id)
			}
			_ => {}
		}

		if let Some(ref row) = record {
			verify_rollout_record_matches(row, &rollout)?;
		} else {
			create_rollout_record(
				db,
				&rollout,
				&source_catalog.entities,
				&target_catalog.entities,
				RolloutStatus::Planned,
			)
			.await?;
		}

		set_rollout_status(db, &rollout.spec.id, RolloutStatus::RunningStart, None, None).await?;
		if let Err(err) = execute_phase(db, &rollout, RolloutPhase::Start).await {
			set_rollout_status(
				db,
				&rollout.spec.id,
				RolloutStatus::Failed,
				Some(&format!("{err:#}")),
				None,
			)
			.await?;
			return Err(err);
		}
		set_rollout_status(db, &rollout.spec.id, RolloutStatus::ReadyToComplete, None, None)
			.await?;
		println!("Rollout {} is ready to complete.", rollout.spec.id);
		Ok(())
	}
	.await;
	let release = release_lock(db, "global").await;
	match (result, release) {
		(Err(err), _) => Err(err),
		(Ok(_), Err(err)) => Err(err),
		(Ok(value), Ok(())) => Ok(value),
	}
}

pub async fn run_complete(db: &Surreal<Any>, opts: RolloutExecutionOpts) -> Result<()> {
	run_setup(db).await?;
	let rollout = load_rollout_spec(resolve_rollout_path(opts.selector.as_deref())?)?;
	validate_rollout_spec(&rollout.spec)?;
	acquire_lock(db, "global").await?;
	let result = async {
		let row = load_rollout_record(db, &rollout.spec.id)
			.await?
			.ok_or_else(|| anyhow!("rollout '{}' has not been started", rollout.spec.id))?;
		verify_rollout_record_matches(&row, &rollout)?;
		match string_field(&row, "status").as_deref() {
			Some("ready_to_complete") | Some("running_complete") | Some("failed") => {}
			Some(other) => {
				bail!("rollout '{}' is not ready to complete (status={})", rollout.spec.id, other)
			}
			None => bail!("rollout '{}' has no status", rollout.spec.id),
		}

		set_rollout_status(db, &rollout.spec.id, RolloutStatus::RunningComplete, None, None)
			.await?;
		if let Err(err) = execute_phase(db, &rollout, RolloutPhase::Complete).await {
			set_rollout_status(
				db,
				&rollout.spec.id,
				RolloutStatus::Failed,
				Some(&format!("{err:#}")),
				None,
			)
			.await?;
			return Err(err);
		}

		let target_entities = deserialize_entities_field(&row, "target_entities")?;
		replace_managed_entities(db, &target_entities, None, "active").await?;
		set_rollout_status(
			db,
			&rollout.spec.id,
			RolloutStatus::Completed,
			None,
			Some(OffsetDateTime::now_utc().format(&Rfc3339)?),
		)
		.await?;
		println!("Completed rollout {}.", rollout.spec.id);
		Ok(())
	}
	.await;
	let release = release_lock(db, "global").await;
	match (result, release) {
		(Err(err), _) => Err(err),
		(Ok(_), Err(err)) => Err(err),
		(Ok(value), Ok(())) => Ok(value),
	}
}

pub async fn run_rollback(db: &Surreal<Any>, opts: RolloutExecutionOpts) -> Result<()> {
	run_setup(db).await?;
	let rollout = load_rollout_spec(resolve_rollout_path(opts.selector.as_deref())?)?;
	validate_rollout_spec(&rollout.spec)?;
	acquire_lock(db, "global").await?;
	let result = async {
		let row = load_rollout_record(db, &rollout.spec.id)
			.await?
			.ok_or_else(|| anyhow!("rollout '{}' has not been started", rollout.spec.id))?;
		verify_rollout_record_matches(&row, &rollout)?;
		match string_field(&row, "status").as_deref() {
			Some("completed") => bail!("rollout '{}' is already completed", rollout.spec.id),
			Some("rolled_back") => {
				println!("Rollout {} is already rolled back.", rollout.spec.id);
				return Ok(());
			}
			_ => {}
		}

		set_rollout_status(db, &rollout.spec.id, RolloutStatus::RunningRollback, None, None)
			.await?;
		if let Err(err) = execute_phase(db, &rollout, RolloutPhase::Rollback).await {
			set_rollout_status(
				db,
				&rollout.spec.id,
				RolloutStatus::Failed,
				Some(&format!("{err:#}")),
				None,
			)
			.await?;
			return Err(err);
		}
		let source_entities = deserialize_entities_field(&row, "source_entities")?;
		replace_managed_entities(db, &source_entities, None, "active").await?;
		set_rollout_status(
			db,
			&rollout.spec.id,
			RolloutStatus::RolledBack,
			None,
			Some(OffsetDateTime::now_utc().format(&Rfc3339)?),
		)
		.await?;
		println!("Rolled back rollout {}.", rollout.spec.id);
		Ok(())
	}
	.await;
	let release = release_lock(db, "global").await;
	match (result, release) {
		(Err(err), _) => Err(err),
		(Ok(_), Err(err)) => Err(err),
		(Ok(value), Ok(())) => Ok(value),
	}
}

pub async fn load_active_rollout_id(db: &Surreal<Any>) -> Result<Option<String>> {
	let mut resp = db
		.query(
			"SELECT id, status FROM _surrealkit_rollout \
			 WHERE status INSIDE ['planned', 'running_start', 'ready_to_complete', 'running_complete', 'running_rollback', 'failed'] \
			 ORDER BY started_at DESC LIMIT 1;",
		)
		.await?;
	let row: Option<Value> = resp.take(0)?;
	Ok(row.and_then(|value| string_field(&value, "id")))
}

pub async fn load_managed_entities(db: &Surreal<Any>) -> Result<Vec<ManagedEntityRecord>> {
	let mut resp = db
		.query(
			"SELECT kind, scope, name, source_path, statement_hash, file_hash, active_rollout_id, state \
			 FROM _surrealkit_managed_entity;",
		)
		.await?;
	let rows: Vec<Value> = resp.take(0)?;
	let mut out = Vec::with_capacity(rows.len());
	for row in rows {
		let kind = string_field_req(&row, "kind")?;
		let name = string_field_req(&row, "name")?;
		let source_path = string_field_req(&row, "source_path")?;
		let statement_hash = string_field_req(&row, "statement_hash")?;
		let file_hash = string_field_req(&row, "file_hash")?;
		let scope = row.get("scope").and_then(|value| value.as_str()).map(str::to_string);
		let active_rollout_id =
			row.get("active_rollout_id").and_then(|value| value.as_str()).map(str::to_string);
		let state = string_field(&row, "state").unwrap_or_else(|| "active".to_string());
		out.push(ManagedEntityRecord {
			entity: CatalogEntity {
				kind,
				scope,
				name,
				source_path,
				statement_hash,
				file_hash,
			},
			active_rollout_id,
			state,
		});
	}
	out.sort_by(|a, b| a.entity.cmp(&b.entity));
	Ok(out)
}

pub async fn upsert_managed_entities(
	db: &Surreal<Any>,
	entities: &[CatalogEntity],
	active_rollout_id: Option<&str>,
	state: &str,
) -> Result<()> {
	for entity in entities {
		db.query(
			"DELETE _surrealkit_managed_entity \
			 WHERE kind = $kind AND scope = $scope AND name = $name; \
			 CREATE _surrealkit_managed_entity CONTENT { \
			 	kind: $kind, \
			 	scope: $scope, \
			 	name: $name, \
			 	source_path: $source_path, \
			 	statement_hash: $statement_hash, \
			 	file_hash: $file_hash, \
			 	active_rollout_id: $active_rollout_id, \
			 	state: $state, \
			 	updated_at: time::now() \
			 };",
		)
		.bind(("kind", entity.kind.clone()))
		.bind(("scope", entity.scope.clone()))
		.bind(("name", entity.name.clone()))
		.bind(("source_path", entity.source_path.clone()))
		.bind(("statement_hash", entity.statement_hash.clone()))
		.bind(("file_hash", entity.file_hash.clone()))
		.bind(("active_rollout_id", active_rollout_id.map(str::to_string)))
		.bind(("state", state.to_string()))
		.await?
		.check()?;
	}
	Ok(())
}

pub async fn delete_managed_entities(db: &Surreal<Any>, entities: &[EntityKey]) -> Result<()> {
	for entity in entities {
		db.query(
			"DELETE _surrealkit_managed_entity \
			 WHERE kind = $kind AND scope = $scope AND name = $name;",
		)
		.bind(("kind", entity.kind.clone()))
		.bind(("scope", entity.scope.clone()))
		.bind(("name", entity.name.clone()))
		.await?
		.check()?;
	}
	Ok(())
}

pub async fn replace_managed_entities(
	db: &Surreal<Any>,
	entities: &[CatalogEntity],
	active_rollout_id: Option<&str>,
	state: &str,
) -> Result<()> {
	db.query("DELETE _surrealkit_managed_entity;").await?.check()?;
	upsert_managed_entities(db, entities, active_rollout_id, state).await
}

pub async fn replace_sync_hashes(db: &Surreal<Any>, files: &[SchemaFile]) -> Result<()> {
	db.query("DELETE _surrealkit_sync;").await?.check()?;
	for file in files {
		db.query(
			"CREATE _surrealkit_sync CONTENT { path: $path, hash: $hash, synced_at: time::now() };",
		)
		.bind(("path", file.path.clone()))
		.bind(("hash", file.hash.clone()))
		.await?
		.check()?;
	}
	Ok(())
}

pub async fn delete_sync_hashes(db: &Surreal<Any>, paths: &[String]) -> Result<()> {
	for path in paths {
		db.query("DELETE _surrealkit_sync WHERE path = $path;")
			.bind(("path", path.clone()))
			.await?
			.check()?;
	}
	Ok(())
}

fn build_rollout_spec(
	rollout_id: &str,
	name: &str,
	files: &[SchemaFile],
	file_diff: &FileDiff,
	catalog_diff: &CatalogDiff,
	old_schema: &crate::schema_state::SchemaSnapshot,
	new_schema: &crate::schema_state::SchemaSnapshot,
) -> Result<RolloutSpec> {
	let changed_paths = changed_files(files, file_diff);
	let mut steps = Vec::new();
	if !changed_paths.is_empty() {
		steps.push(RolloutStep {
			id: "apply_expand_schema".to_string(),
			phase: RolloutPhase::Start,
			kind: RolloutStepKind::ApplySchema,
			files: changed_paths,
			sql: None,
			expect: None,
			entities: Vec::new(),
			idempotent: None,
		});
	}

	let added_entities: Vec<EntityKey> =
		catalog_diff.added.iter().map(CatalogEntity::key).collect();
	if !added_entities.is_empty() {
		steps.push(RolloutStep {
			id: "rollback_expand_schema".to_string(),
			phase: RolloutPhase::Rollback,
			kind: RolloutStepKind::RemoveEntities,
			files: Vec::new(),
			sql: None,
			expect: None,
			entities: added_entities,
			idempotent: None,
		});
	}

	let removed_entities: Vec<EntityKey> =
		catalog_diff.removed.iter().map(CatalogEntity::key).collect();
	if !removed_entities.is_empty() {
		steps.push(RolloutStep {
			id: "remove_legacy_entities".to_string(),
			phase: RolloutPhase::Complete,
			kind: RolloutStepKind::RemoveEntities,
			files: Vec::new(),
			sql: None,
			expect: None,
			entities: removed_entities,
			idempotent: None,
		});
	}

	Ok(RolloutSpec {
		id: rollout_id.to_string(),
		name: name.to_string(),
		source_schema_hash: hash_schema_snapshot(old_schema)?,
		target_schema_hash: hash_schema_snapshot(new_schema)?,
		compatibility: "phased".to_string(),
		renames: Vec::new(),
		steps,
	})
}

fn validate_autoplan(diff: &CatalogDiff) -> Result<()> {
	if !diff.modified.is_empty() {
		let names = diff
			.modified
			.iter()
			.map(|change| format!("{}:{}", change.old.kind, change.old.name))
			.collect::<Vec<_>>()
			.join(", ");
		bail!(
			"automatic rollout planning refuses modified managed entities: {}. \
Author a manual rollout manifest for non-additive changes.",
			names
		);
	}

	let removed_by_scope: BTreeSet<(String, Option<String>)> =
		diff.removed.iter().map(|entity| (entity.kind.clone(), entity.scope.clone())).collect();
	let added_by_scope: BTreeSet<(String, Option<String>)> =
		diff.added.iter().map(|entity| (entity.kind.clone(), entity.scope.clone())).collect();

	if removed_by_scope.intersection(&added_by_scope).next().is_some() {
		bail!(
			"automatic rollout planning detected add/remove changes in the same scope. \
Author a manual rollout manifest with explicit renames/backfill steps."
		);
	}

	Ok(())
}

fn changed_files(files: &[SchemaFile], diff: &FileDiff) -> Vec<String> {
	let changed: BTreeSet<&str> =
		diff.added.iter().chain(diff.modified.iter()).map(String::as_str).collect();
	let mut out: Vec<String> = files
		.iter()
		.filter(|file| changed.contains(file.path.as_str()))
		.map(|file| file.path.clone())
		.collect();
	out.sort();
	out
}

fn load_rollout_spec(path: PathBuf) -> Result<LoadedRolloutSpec> {
	let raw = fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
	let spec: RolloutSpec =
		toml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))?;
	Ok(LoadedRolloutSpec {
		path,
		checksum: sha256_hex(raw.as_bytes()),
		spec,
	})
}

fn resolve_rollout_path(selector: Option<&str>) -> Result<PathBuf> {
	let selector = selector.ok_or_else(|| anyhow!("rollout id or path is required"))?;
	let path = Path::new(selector);
	if path.exists() {
		return Ok(path.to_path_buf());
	}
	let direct = Path::new(ROLLOUTS_DIR).join(selector);
	if direct.exists() {
		return Ok(direct);
	}
	let with_ext = Path::new(ROLLOUTS_DIR).join(format!("{selector}.toml"));
	if with_ext.exists() {
		return Ok(with_ext);
	}
	bail!("unable to find rollout '{}'", selector)
}

fn validate_rollout_spec(spec: &RolloutSpec) -> Result<()> {
	if spec.id.trim().is_empty() {
		bail!("rollout id is required");
	}
	if spec.name.trim().is_empty() {
		bail!("rollout name is required");
	}
	if spec.compatibility.trim().is_empty() {
		bail!("compatibility is required");
	}

	let mut step_ids = BTreeSet::new();
	for step in &spec.steps {
		if !step_ids.insert(step.id.clone()) {
			bail!("duplicate rollout step id '{}'", step.id);
		}
		match step.kind {
			RolloutStepKind::ApplySchema => {
				if step.files.is_empty() {
					bail!("apply_schema step '{}' requires files", step.id);
				}
			}
			RolloutStepKind::RunSql => {
				if step.sql.as_deref().unwrap_or("").trim().is_empty() {
					bail!("run_sql step '{}' requires sql", step.id);
				}
				if step.idempotent != Some(true) {
					bail!("run_sql step '{}' must declare idempotent = true", step.id);
				}
			}
			RolloutStepKind::AssertSql => {
				if step.sql.as_deref().unwrap_or("").trim().is_empty() {
					bail!("assert_sql step '{}' requires sql", step.id);
				}
				if step.expect.as_deref().unwrap_or("").trim().is_empty() {
					bail!("assert_sql step '{}' requires expect", step.id);
				}
			}
			RolloutStepKind::RemoveEntities => {
				if step.entities.is_empty() {
					bail!("remove_entities step '{}' requires entities", step.id);
				}
			}
		}
	}
	Ok(())
}

async fn execute_phase(
	db: &Surreal<Any>,
	rollout: &LoadedRolloutSpec,
	phase: RolloutPhase,
) -> Result<()> {
	for step in rollout.spec.steps.iter().filter(|step| step.phase == phase) {
		if step_already_completed(db, &rollout.spec.id, &step.id).await? {
			continue;
		}

		record_step_start(db, &rollout.spec.id, step).await?;
		let result = execute_step(db, step).await;
		match result {
			Ok(()) => record_step_complete(db, &rollout.spec.id, step).await?,
			Err(err) => {
				record_step_failure(db, &rollout.spec.id, step, &format!("{err:#}")).await?;
				return Err(err);
			}
		}
	}
	Ok(())
}

async fn execute_step(db: &Surreal<Any>, step: &RolloutStep) -> Result<()> {
	match step.kind {
		RolloutStepKind::ApplySchema => {
			for file in &step.files {
				let sql = fs::read_to_string(file).with_context(|| format!("reading {}", file))?;
				exec_surql(db, &sql).await?;
			}
			Ok(())
		}
		RolloutStepKind::RunSql => {
			let sql = step.sql.as_deref().ok_or_else(|| anyhow!("missing sql"))?;
			exec_surql(db, sql).await
		}
		RolloutStepKind::AssertSql => {
			let sql = step.sql.as_deref().ok_or_else(|| anyhow!("missing sql"))?;
			let expect = step.expect.as_deref().ok_or_else(|| anyhow!("missing expect"))?;
			let actual = execute_sql_value(db, sql).await?;
			if value_to_expect_string(&actual) != expect.trim() {
				bail!(
					"assert step '{}' failed: expected {}, got {}",
					step.id,
					expect,
					value_to_expect_string(&actual)
				);
			}
			Ok(())
		}
		RolloutStepKind::RemoveEntities => {
			let sql = render_remove_sql(&step.entities, true)?.join("\n");
			if sql.trim().is_empty() {
				return Ok(());
			}
			exec_surql(db, &sql).await
		}
	}
}

async fn execute_sql_value(db: &Surreal<Any>, sql: &str) -> Result<Value> {
	let mut response = db.query(sql).await?.check()?;
	let raw: surrealdb_types::Value = response.take(0)?;
	Ok(Value::from_value(raw).unwrap_or(Value::Null))
}

fn value_to_expect_string(value: &Value) -> String {
	match value {
		Value::Null => "null".to_string(),
		Value::Bool(v) => v.to_string(),
		Value::Number(v) => v.to_string(),
		Value::String(v) => v.clone(),
		other => other.to_string(),
	}
}

async fn rollout_rows_exist(db: &Surreal<Any>) -> Result<bool> {
	let mut resp = db.query("SELECT id FROM _surrealkit_rollout LIMIT 1;").await?;
	let row: Option<Value> = resp.take(0)?;
	Ok(row.is_some())
}

async fn ensure_no_conflicting_active_rollout(db: &Surreal<Any>, rollout_id: &str) -> Result<()> {
	if let Some(active_id) = load_active_rollout_id(db).await?
		&& active_id != rollout_id {
			bail!("rollout '{}' cannot start while rollout '{}' is active", rollout_id, active_id);
		}
	Ok(())
}

async fn create_rollout_record(
	db: &Surreal<Any>,
	rollout: &LoadedRolloutSpec,
	source_entities: &[CatalogEntity],
	target_entities: &[CatalogEntity],
	status: RolloutStatus,
) -> Result<()> {
	let started_at = OffsetDateTime::now_utc().format(&Rfc3339)?;
	db.query(
		"DELETE _surrealkit_rollout WHERE id = $id; \
		 CREATE _surrealkit_rollout CONTENT { \
		 	id: $id, \
		 	name: $name, \
		 	manifest_path: $manifest_path, \
		 	manifest_checksum: $manifest_checksum, \
		 	source_schema_hash: $source_schema_hash, \
		 	target_schema_hash: $target_schema_hash, \
		 	status: $status, \
		 	source_entities: $source_entities, \
		 	target_entities: $target_entities, \
		 	started_at: $started_at, \
		 	updated_at: time::now(), \
		 	last_error: NONE \
		 };",
	)
	.bind(("id", rollout.spec.id.clone()))
	.bind(("name", rollout.spec.name.clone()))
	.bind(("manifest_path", rollout.path.to_string_lossy().to_string()))
	.bind(("manifest_checksum", rollout.checksum.clone()))
	.bind(("source_schema_hash", rollout.spec.source_schema_hash.clone()))
	.bind(("target_schema_hash", rollout.spec.target_schema_hash.clone()))
	.bind(("status", status.as_str().to_string()))
	.bind(("source_entities", serde_json::to_value(source_entities)?))
	.bind(("target_entities", serde_json::to_value(target_entities)?))
	.bind(("started_at", started_at))
	.await?
	.check()?;
	Ok(())
}

async fn load_rollout_record(db: &Surreal<Any>, rollout_id: &str) -> Result<Option<Value>> {
	let mut resp = db
		.query("SELECT * FROM _surrealkit_rollout WHERE id = $id LIMIT 1;")
		.bind(("id", rollout_id.to_string()))
		.await?;
	let row: Option<Value> = resp.take(0)?;
	Ok(row)
}

fn verify_rollout_record_matches(row: &Value, rollout: &LoadedRolloutSpec) -> Result<()> {
	let checksum = string_field_req(row, "manifest_checksum")?;
	if checksum != rollout.checksum {
		bail!(
			"manifest checksum mismatch for '{}': db={}, file={}",
			rollout.spec.id,
			checksum,
			rollout.checksum
		);
	}
	let source = string_field_req(row, "source_schema_hash")?;
	let target = string_field_req(row, "target_schema_hash")?;
	if source != rollout.spec.source_schema_hash || target != rollout.spec.target_schema_hash {
		bail!("schema hash mismatch for rollout '{}'", rollout.spec.id);
	}
	Ok(())
}

fn deserialize_entities_field(row: &Value, key: &str) -> Result<Vec<CatalogEntity>> {
	let value =
		row.get(key).cloned().ok_or_else(|| anyhow!("missing '{}' on rollout record", key))?;
	serde_json::from_value(value).with_context(|| format!("parsing {}", key))
}

async fn set_rollout_status(
	db: &Surreal<Any>,
	rollout_id: &str,
	status: RolloutStatus,
	last_error: Option<&str>,
	completed_at: Option<String>,
) -> Result<()> {
	db.query(
		"UPDATE _surrealkit_rollout SET \
		 	status = $status, \
		 	last_error = $last_error, \
		 	completed_at = $completed_at, \
		 	updated_at = time::now() \
		 WHERE id = $id;",
	)
	.bind(("id", rollout_id.to_string()))
	.bind(("status", status.as_str().to_string()))
	.bind(("last_error", last_error.map(str::to_string)))
	.bind(("completed_at", completed_at))
	.await?
	.check()?;
	Ok(())
}

async fn step_already_completed(
	db: &Surreal<Any>,
	rollout_id: &str,
	step_id: &str,
) -> Result<bool> {
	let mut resp = db
		.query(
			"SELECT status FROM _surrealkit_rollout_step \
			 WHERE rollout_id = $rollout_id AND step_id = $step_id LIMIT 1;",
		)
		.bind(("rollout_id", rollout_id.to_string()))
		.bind(("step_id", step_id.to_string()))
		.await?;
	let row: Option<Value> = resp.take(0)?;
	Ok(matches!(
		row.as_ref().and_then(|value| string_field(value, "status")).as_deref(),
		Some("completed")
	))
}

async fn record_step_start(db: &Surreal<Any>, rollout_id: &str, step: &RolloutStep) -> Result<()> {
	db.query(
		"DELETE _surrealkit_rollout_step WHERE rollout_id = $rollout_id AND step_id = $step_id; \
		 CREATE _surrealkit_rollout_step CONTENT { \
		 	rollout_id: $rollout_id, \
		 	step_id: $step_id, \
		 	phase: $phase, \
		 	kind: $kind, \
		 	checksum: $checksum, \
		 	status: 'running', \
		 	started_at: time::now(), \
		 	finished_at: NONE, \
		 	error: NONE \
		 };",
	)
	.bind(("rollout_id", rollout_id.to_string()))
	.bind(("step_id", step.id.clone()))
	.bind(("phase", format!("{:?}", step.phase).to_ascii_lowercase()))
	.bind(("kind", format!("{:?}", step.kind).to_ascii_lowercase()))
	.bind(("checksum", step_checksum(step)?))
	.await?
	.check()?;
	Ok(())
}

async fn record_step_complete(
	db: &Surreal<Any>,
	rollout_id: &str,
	step: &RolloutStep,
) -> Result<()> {
	db.query(
		"UPDATE _surrealkit_rollout_step SET \
		 	status = 'completed', \
		 	finished_at = time::now(), \
		 	error = NONE \
		 WHERE rollout_id = $rollout_id AND step_id = $step_id;",
	)
	.bind(("rollout_id", rollout_id.to_string()))
	.bind(("step_id", step.id.clone()))
	.await?
	.check()?;
	Ok(())
}

async fn record_step_failure(
	db: &Surreal<Any>,
	rollout_id: &str,
	step: &RolloutStep,
	error: &str,
) -> Result<()> {
	db.query(
		"UPDATE _surrealkit_rollout_step SET \
		 	status = 'failed', \
		 	finished_at = time::now(), \
		 	error = $error \
		 WHERE rollout_id = $rollout_id AND step_id = $step_id;",
	)
	.bind(("rollout_id", rollout_id.to_string()))
	.bind(("step_id", step.id.clone()))
	.bind(("error", error.to_string()))
	.await?
	.check()?;
	Ok(())
}

fn step_checksum(step: &RolloutStep) -> Result<String> {
	let raw = serde_json::to_vec(step).context("serializing rollout step")?;
	Ok(sha256_hex(&raw))
}

pub async fn acquire_lock(db: &Surreal<Any>, lock_key: &str) -> Result<()> {
	let owner = std::env::var("SURREALKIT_OWNER").unwrap_or_else(|_| "surrealkit".to_string());
	db.query(
		"CREATE ONLY _surrealkit_lock:global CONTENT { \
		 	key: $key, \
		 	owner: $owner, \
		 	created_at: time::now() \
		 };",
	)
	.bind(("key", lock_key.to_string()))
	.bind(("owner", owner))
	.await?
	.check()?;
	Ok(())
}

pub async fn release_lock(db: &Surreal<Any>, _lock_key: &str) -> Result<()> {
	db.query("DELETE _surrealkit_lock:global;").await?.check()?;
	Ok(())
}

fn slugify(input: &str) -> String {
	let mut out = String::new();
	let mut prev_dash = false;
	for ch in input.chars() {
		let c = ch.to_ascii_lowercase();
		if c.is_ascii_alphanumeric() {
			out.push(c);
			prev_dash = false;
		} else if !prev_dash {
			out.push('_');
			prev_dash = true;
		}
	}
	let trimmed = out.trim_matches('_');
	if trimmed.is_empty() {
		"schema_rollout".to_string()
	} else {
		trimmed.to_string()
	}
}

fn string_field(row: &Value, key: &str) -> Option<String> {
	row.get(key).and_then(|value| value.as_str()).map(str::to_string)
}

fn string_field_req(row: &Value, key: &str) -> Result<String> {
	string_field(row, key).ok_or_else(|| anyhow!("missing '{}' in database row", key))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::schema_state::{CatalogChange, SchemaSnapshot, SchemaSnapshotEntry};

	#[test]
	fn plan_rejects_modified_entities() {
		let diff = CatalogDiff {
			added: Vec::new(),
			removed: Vec::new(),
			modified: vec![CatalogChange {
				old: CatalogEntity {
					kind: "field".to_string(),
					scope: Some("person".to_string()),
					name: "name".to_string(),
					source_path: "database/schema/person.surql".to_string(),
					statement_hash: "a".to_string(),
					file_hash: "fa".to_string(),
				},
				new: CatalogEntity {
					kind: "field".to_string(),
					scope: Some("person".to_string()),
					name: "name".to_string(),
					source_path: "database/schema/person.surql".to_string(),
					statement_hash: "b".to_string(),
					file_hash: "fb".to_string(),
				},
			}],
		};

		let err = validate_autoplan(&diff).expect_err("should reject modified entities");
		assert!(err.to_string().contains("refuses modified"));
	}

	#[test]
	fn build_plan_creates_add_and_remove_steps() {
		let files = vec![SchemaFile {
			path: "database/schema/customer.surql".to_string(),
			sql: "DEFINE TABLE customer SCHEMAFULL;".to_string(),
			hash: "file-a".to_string(),
		}];
		let spec = build_rollout_spec(
			"20260302153045__customer",
			"customer",
			&files,
			&FileDiff {
				added: vec!["database/schema/customer.surql".to_string()],
				modified: Vec::new(),
				removed: Vec::new(),
			},
			&CatalogDiff {
				added: vec![CatalogEntity {
					kind: "table".to_string(),
					scope: None,
					name: "customer".to_string(),
					source_path: "database/schema/customer.surql".to_string(),
					statement_hash: "stmt".to_string(),
					file_hash: "file-a".to_string(),
				}],
				removed: vec![CatalogEntity {
					kind: "field".to_string(),
					scope: Some("person".to_string()),
					name: "nickname".to_string(),
					source_path: "database/schema/person.surql".to_string(),
					statement_hash: "old".to_string(),
					file_hash: "file-old".to_string(),
				}],
				modified: Vec::new(),
			},
			&SchemaSnapshot {
				version: 1,
				files: vec![SchemaSnapshotEntry {
					path: "database/schema/person.surql".to_string(),
					hash: "old".to_string(),
				}],
			},
			&SchemaSnapshot {
				version: 1,
				files: vec![SchemaSnapshotEntry {
					path: "database/schema/customer.surql".to_string(),
					hash: "new".to_string(),
				}],
			},
		)
		.expect("build rollout");

		assert_eq!(spec.steps.len(), 3);
		assert!(
			spec.steps.iter().any(|step| step.phase == RolloutPhase::Start
				&& step.kind == RolloutStepKind::ApplySchema)
		);
		assert!(spec.steps.iter().any(|step| {
			step.phase == RolloutPhase::Rollback && step.kind == RolloutStepKind::RemoveEntities
		}));
		assert!(spec.steps.iter().any(|step| {
			step.phase == RolloutPhase::Complete && step.kind == RolloutStepKind::RemoveEntities
		}));
	}

	#[test]
	fn rollout_lint_rejects_non_idempotent_run_sql() {
		let spec = RolloutSpec {
			id: "a".to_string(),
			name: "a".to_string(),
			source_schema_hash: "1".to_string(),
			target_schema_hash: "2".to_string(),
			compatibility: "phased".to_string(),
			renames: Vec::new(),
			steps: vec![RolloutStep {
				id: "step".to_string(),
				phase: RolloutPhase::Start,
				kind: RolloutStepKind::RunSql,
				files: Vec::new(),
				sql: Some("UPDATE person SET name = 'a';".to_string()),
				expect: None,
				entities: Vec::new(),
				idempotent: Some(false),
			}],
		};

		let err = validate_rollout_spec(&spec).expect_err("must reject non-idempotent run_sql");
		assert!(err.to_string().contains("idempotent = true"));
	}
}
