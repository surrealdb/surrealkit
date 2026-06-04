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

use crate::constants::{catalog_snapshot_path, rollouts_dir};
use crate::core::{exec_surql, sha256_hex};
use crate::schema_state::{
	CatalogDiff, CatalogEntity, CatalogSnapshot, EntityKey, EntityKind, FileDiff, SchemaFile,
	build_catalog_snapshot, collect_schema_files, diff_catalog, diff_schema,
	ensure_local_state_dirs, ensure_overwrite, hash_schema_snapshot, load_catalog_snapshot,
	load_schema_snapshot, render_remove_sql, save_catalog_snapshot, save_schema_snapshot,
	snapshot_from_files,
};
use crate::setup::run_setup;
use crate::variables::TemplateVars;

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

/// The migration strategy for a rollout — how its `start` and `complete` phases
/// relate.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RolloutCompatibility {
	/// Expand/contract. The `start` phase is non-destructive (it adds or updates
	/// schema); the `complete` phase performs the destructive changes (it removes
	/// entities that are no longer needed). `rollback` undoes the `start` phase.
	/// This lets old and new application code run side-by-side between `start` and
	/// `complete`. Serializes as `"phased"`.
	#[default]
	Phased,
}

/// What a rollout step does. Each variant carries exactly the data it needs, so
/// invalid combinations (e.g. an assertion with no expected value, or schema SQL
/// mixed with a file list) cannot be represented. Construct steps with the
/// [`RolloutStep`] constructors rather than building this directly.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RolloutAction {
	/// Apply inline SurrealQL DDL. `DEFINE` statements are made idempotent
	/// (`OVERWRITE` is injected) so the step is always safe to retry.
	ApplySchema { sql: String },
	/// Read DDL from `.surql` files on disk and apply them. Used by the CLI
	/// `rollout plan`/`start` workflow; in code prefer [`RolloutAction::ApplySchema`]
	/// with inline SQL.
	ApplyFiles { files: Vec<String> },
	/// Execute SurrealQL that mutates data (e.g. a backfill). The SQL must be safe
	/// to re-run: on retry the step executes again from scratch.
	RunSql { sql: String },
	/// Run a query and assert its stringified output equals `expect`; fails the
	/// rollout otherwise.
	AssertSql { sql: String, expect: String },
	/// Issue `REMOVE … IF EXISTS` for named database objects (tables, fields,
	/// indexes, …).
	RemoveEntities { entities: Vec<EntityKey> },
}

impl RolloutAction {
	/// The persisted discriminator string for this action (e.g. `"apply_schema"`).
	fn kind_str(&self) -> &'static str {
		match self {
			Self::ApplySchema { .. } => "apply_schema",
			Self::ApplyFiles { .. } => "apply_files",
			Self::RunSql { .. } => "run_sql",
			Self::AssertSql { .. } => "assert_sql",
			Self::RemoveEntities { .. } => "remove_entities",
		}
	}
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

/// A complete rollout definition: an identity, a migration strategy, and the
/// ordered steps to run across its `start`, `complete`, and `rollback` phases.
///
/// Prefer [`RolloutSpec::builder`] over constructing this directly — it defaults
/// the bookkeeping fields (`source_schema_hash`, `target_schema_hash`, `renames`)
/// that only the CLI's filesystem drift-detection uses.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RolloutSpec {
	/// Stable, unique identifier stored in the database. Must be identical across
	/// the `start`, `complete`, and `rollback` calls for one rollout (e.g. a
	/// timestamp-prefixed migration name).
	pub id: String,
	/// Human-readable name shown in status output.
	pub name: String,
	/// Schema hash before this rollout. Set by the CLI filesystem workflow; left
	/// empty for code-driven rollouts.
	#[serde(default)]
	pub source_schema_hash: String,
	/// Desired schema hash after this rollout. When non-empty it is verified
	/// against the supplied target files before `start`. Left empty for code-driven
	/// rollouts.
	#[serde(default)]
	pub target_schema_hash: String,
	/// The migration strategy. See [`RolloutCompatibility`].
	#[serde(default)]
	pub compatibility: RolloutCompatibility,
	/// Reserved for future rename hints; currently unused by execution.
	#[serde(default)]
	pub renames: Vec<RolloutRename>,
	/// The steps to execute, grouped by [`RolloutPhase`].
	#[serde(default)]
	pub steps: Vec<RolloutStep>,
}

/// Reserved rename hint. Currently inert — carried for forward compatibility but
/// not consumed by rollout execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RolloutRename {
	pub kind: String,
	pub scope: Option<String>,
	pub from: String,
	pub to: String,
}

/// One step in a rollout. Build steps with the constructors
/// ([`RolloutStep::apply_schema`], [`RolloutStep::run_sql`],
/// [`RolloutStep::assert_sql`], [`RolloutStep::remove_entities`]) so the action
/// and its data always match.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RolloutStep {
	/// Stable identifier, unique within the rollout. Used to track per-step
	/// execution state so a re-run skips already-completed steps.
	pub id: String,
	/// Which phase runs this step: `Start`, `Complete`, or `Rollback`.
	pub phase: RolloutPhase,
	/// What the step does and the data it needs.
	#[serde(flatten)]
	pub action: RolloutAction,
}

impl RolloutStep {
	/// Apply inline SurrealQL DDL during `phase`.
	pub fn apply_schema(
		id: impl Into<String>,
		phase: RolloutPhase,
		sql: impl Into<String>,
	) -> Self {
		Self {
			id: id.into(),
			phase,
			action: RolloutAction::ApplySchema {
				sql: sql.into(),
			},
		}
	}

	/// Apply DDL read from `.surql` files on disk during `phase`. Used by the CLI;
	/// in code prefer [`RolloutStep::apply_schema`].
	pub fn apply_files(id: impl Into<String>, phase: RolloutPhase, files: Vec<String>) -> Self {
		Self {
			id: id.into(),
			phase,
			action: RolloutAction::ApplyFiles {
				files,
			},
		}
	}

	/// Execute data-mutation SQL during `phase`. The SQL must be safe to re-run.
	pub fn run_sql(id: impl Into<String>, phase: RolloutPhase, sql: impl Into<String>) -> Self {
		Self {
			id: id.into(),
			phase,
			action: RolloutAction::RunSql {
				sql: sql.into(),
			},
		}
	}

	/// Assert a query's stringified output equals `expect` during `phase`.
	pub fn assert_sql(
		id: impl Into<String>,
		phase: RolloutPhase,
		sql: impl Into<String>,
		expect: impl Into<String>,
	) -> Self {
		Self {
			id: id.into(),
			phase,
			action: RolloutAction::AssertSql {
				sql: sql.into(),
				expect: expect.into(),
			},
		}
	}

	/// Remove named database objects during `phase`.
	pub fn remove_entities(
		id: impl Into<String>,
		phase: RolloutPhase,
		entities: Vec<EntityKey>,
	) -> Self {
		Self {
			id: id.into(),
			phase,
			action: RolloutAction::RemoveEntities {
				entities,
			},
		}
	}
}

impl RolloutSpec {
	/// Start building a code-driven rollout with the given stable `id`.
	///
	/// Defaults `name` to `id` and `compatibility` to [`RolloutCompatibility::Phased`].
	/// The CLI-only bookkeeping fields (`source_schema_hash`, `target_schema_hash`,
	/// `renames`) are left empty — they are not needed for code-driven rollouts.
	pub fn builder(id: impl Into<String>) -> RolloutSpecBuilder {
		RolloutSpecBuilder {
			id: id.into(),
			name: None,
			compatibility: RolloutCompatibility::default(),
			steps: Vec::new(),
		}
	}
}

/// Builder for [`RolloutSpec`]. Create one with [`RolloutSpec::builder`].
#[derive(Debug, Clone)]
pub struct RolloutSpecBuilder {
	id: String,
	name: Option<String>,
	compatibility: RolloutCompatibility,
	steps: Vec<RolloutStep>,
}

impl RolloutSpecBuilder {
	/// Set the human-readable name (defaults to the id).
	pub fn name(mut self, name: impl Into<String>) -> Self {
		self.name = Some(name.into());
		self
	}

	/// Set the migration strategy (defaults to [`RolloutCompatibility::Phased`]).
	pub fn compatibility(mut self, compatibility: RolloutCompatibility) -> Self {
		self.compatibility = compatibility;
		self
	}

	/// Append a step. Build steps with the [`RolloutStep`] constructors.
	pub fn step(mut self, step: RolloutStep) -> Self {
		self.steps.push(step);
		self
	}

	/// Append several steps.
	pub fn steps(mut self, steps: impl IntoIterator<Item = RolloutStep>) -> Self {
		self.steps.extend(steps);
		self
	}

	/// Finish building the [`RolloutSpec`].
	pub fn build(self) -> RolloutSpec {
		let name = self.name.unwrap_or_else(|| self.id.clone());
		RolloutSpec {
			id: self.id,
			name,
			source_schema_hash: String::new(),
			target_schema_hash: String::new(),
			compatibility: self.compatibility,
			renames: Vec::new(),
			steps: self.steps,
		}
	}
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

pub async fn run_baseline(db: &Surreal<Any>, folder: &str) -> Result<()> {
	run_setup(db, folder).await?;
	ensure_local_state_dirs(folder)?;
	if rollout_rows_exist(db).await? {
		bail!("rollout state already exists; baseline can only be run once");
	}

	let files = collect_schema_files(folder)?;
	let schema_snapshot = snapshot_from_files(&files);
	let catalog_snapshot = build_catalog_snapshot(&files, false)?;

	replace_managed_entities(db, &catalog_snapshot.entities, None, "active").await?;
	replace_sync_hashes(db, &files).await?;
	save_schema_snapshot(folder, &schema_snapshot)?;
	save_catalog_snapshot(folder, &catalog_snapshot)?;

	println!(
		"Seeded managed entity baseline with {} schema file(s) and {} managed object(s).",
		files.len(),
		catalog_snapshot.entities.len()
	);
	Ok(())
}

pub async fn run_plan(folder: &str, opts: RolloutPlanOpts) -> Result<()> {
	ensure_local_state_dirs(folder)?;
	let files = collect_schema_files(folder)?;
	let old_schema = load_schema_snapshot(folder)?;
	let old_catalog = load_catalog_snapshot(folder)?;
	let new_schema = snapshot_from_files(&files);
	let new_catalog = build_catalog_snapshot(&files, false)?;
	let file_diff = diff_schema(&old_schema, &new_schema);
	let catalog_diff = diff_catalog(&old_catalog, &new_catalog);

	validate_autoplan(&catalog_diff)?;

	let name = opts.name.unwrap_or_else(|| "schema_rollout".to_string());
	let slug = slugify(&name);
	let ts = OffsetDateTime::now_utc()
		.format(&format_description!("[year][month][day][hour][minute][second]"))?;
	let rollout_id = format!("{ts}__{slug}");
	let path = rollouts_dir(folder).join(format!("{rollout_id}.toml"));

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
	save_schema_snapshot(folder, &new_schema)?;
	save_catalog_snapshot(folder, &new_catalog)?;

	println!("Generated rollout manifest {}", path.display());
	println!("Updated {}", catalog_snapshot_path(folder).display());
	Ok(())
}

pub async fn run_lint(folder: &str, opts: RolloutExecutionOpts) -> Result<()> {
	ensure_local_state_dirs(folder)?;
	let rollout = load_rollout_spec(resolve_rollout_path(folder, opts.selector.as_deref())?)?;
	validate_rollout_spec(&rollout.spec)?;
	let files = collect_schema_files(folder)?;
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

pub async fn run_status(db: &Surreal<Any>, folder: &str, selector: Option<String>) -> Result<()> {
	run_setup(db, folder).await?;
	let mut query =
		"SELECT id, name, status, started_at, completed_at, last_error, steps FROM __rollout"
			.to_string();
	if selector.is_some() {
		query.push_str(" WHERE record::id(id) = $id");
	}
	query.push_str(" ORDER BY started_at DESC;");

	let mut req = db.query(query);
	if let Some(id) = selector {
		req = req.bind(("id", id));
	}
	let mut resp = req.await?;
	let raw_rows: Vec<surrealdb_types::Value> = resp.take(0)?;
	let rows: Vec<Value> =
		raw_rows.into_iter().map(|v| Value::from_value(v).unwrap_or(Value::Null)).collect();
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

		let steps = row.get("steps").and_then(|v| v.as_array()).cloned().unwrap_or_default();
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

pub async fn run_start(
	db: &Surreal<Any>,
	folder: &str,
	opts: RolloutExecutionOpts,
	vars: &TemplateVars,
) -> Result<()> {
	run_setup(db, folder).await?;
	ensure_local_state_dirs(folder)?;
	let rollout = load_rollout_spec(resolve_rollout_path(folder, opts.selector.as_deref())?)?;
	validate_rollout_spec(&rollout.spec)?;
	let files = collect_schema_files(folder)?;
	let target_hash = hash_schema_snapshot(&snapshot_from_files(&files))?;
	if target_hash != rollout.spec.target_schema_hash {
		bail!(
			"target schema hash mismatch for '{}': manifest={}, current={}",
			rollout.spec.id,
			rollout.spec.target_schema_hash,
			target_hash
		);
	}
	let target_catalog = build_catalog_snapshot(&files, false)?;
	let source_entities = load_managed_entities(db).await?;
	let source_catalog = CatalogSnapshot {
		version: 2,
		entities: source_entities.into_iter().map(|r| r.entity).collect(),
		operations: Vec::new(),
	};
	start_inner(db, &rollout, &source_catalog, &target_catalog, vars).await
}

/// Runs the start phase of a rollout defined entirely in code.
///
/// `spec` describes the rollout steps. `target_files` is the desired schema state;
/// it is used to build the entity catalog and verify `spec.target_schema_hash`
/// when that field is non-empty.
///
/// `ApplySchema` steps in `spec` should carry their SurrealQL in the `sql` field
/// rather than in `files`, since no filesystem is read during execution.
///
/// `vars` is applied to step SQL (`ApplySchema`, `RunSql`, `AssertSql`) before execution.
/// Pass `&TemplateVars::default()` if no substitution is needed.
pub async fn run_start_with_spec(
	db: &Surreal<Any>,
	folder: &str,
	spec: &RolloutSpec,
	target_files: &[crate::sync::EmbeddedSchemaFile],
	vars: &TemplateVars,
) -> Result<()> {
	run_setup(db, folder).await?;
	validate_rollout_spec(spec)?;
	let schema_files = embedded_to_schema_files(target_files);
	if !spec.target_schema_hash.is_empty() {
		let hash = hash_schema_snapshot(&snapshot_from_files(&schema_files))?;
		if hash != spec.target_schema_hash {
			bail!(
				"target schema hash mismatch for '{}': spec={}, files={}",
				spec.id,
				spec.target_schema_hash,
				hash
			);
		}
	}
	let target_catalog = build_catalog_snapshot(&schema_files, false)?;
	let source_entities = load_managed_entities(db).await?;
	let source_catalog = CatalogSnapshot {
		version: 2,
		entities: source_entities.into_iter().map(|r| r.entity).collect(),
		operations: Vec::new(),
	};
	start_inner(db, &make_loaded_spec(spec), &source_catalog, &target_catalog, vars).await
}

async fn start_inner(
	db: &Surreal<Any>,
	rollout: &LoadedRolloutSpec,
	source_catalog: &CatalogSnapshot,
	target_catalog: &CatalogSnapshot,
	vars: &TemplateVars,
) -> Result<()> {
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
			verify_rollout_record_matches(row, rollout)?;
		} else {
			create_rollout_record(
				db,
				rollout,
				&source_catalog.entities,
				&target_catalog.entities,
				RolloutStatus::Planned,
			)
			.await?;
		}
		set_rollout_status(db, &rollout.spec.id, RolloutStatus::RunningStart, None, None).await?;
		if let Err(err) = execute_phase(db, rollout, RolloutPhase::Start, vars).await {
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

pub async fn run_complete(
	db: &Surreal<Any>,
	folder: &str,
	opts: RolloutExecutionOpts,
	vars: &TemplateVars,
) -> Result<()> {
	run_setup(db, folder).await?;
	let rollout = load_rollout_spec(resolve_rollout_path(folder, opts.selector.as_deref())?)?;
	validate_rollout_spec(&rollout.spec)?;
	complete_inner(db, &rollout, vars).await
}

/// Runs the complete phase of a rollout defined entirely in code.
///
/// The `spec` must be identical to the one passed to [`run_start_with_spec`].
/// `vars` is applied to step SQL before execution; pass `&TemplateVars::default()`
/// if no substitution is needed.
pub async fn run_complete_with_spec(
	db: &Surreal<Any>,
	folder: &str,
	spec: &RolloutSpec,
	vars: &TemplateVars,
) -> Result<()> {
	run_setup(db, folder).await?;
	validate_rollout_spec(spec)?;
	complete_inner(db, &make_loaded_spec(spec), vars).await
}

async fn complete_inner(
	db: &Surreal<Any>,
	rollout: &LoadedRolloutSpec,
	vars: &TemplateVars,
) -> Result<()> {
	acquire_lock(db, "global").await?;
	let result = async {
		let row = load_rollout_record(db, &rollout.spec.id)
			.await?
			.ok_or_else(|| anyhow!("rollout '{}' has not been started", rollout.spec.id))?;
		verify_rollout_record_matches(&row, rollout)?;
		match string_field(&row, "status").as_deref() {
			Some("ready_to_complete") | Some("running_complete") | Some("failed") => {}
			Some(other) => {
				bail!("rollout '{}' is not ready to complete (status={})", rollout.spec.id, other)
			}
			None => bail!("rollout '{}' has no status", rollout.spec.id),
		}
		set_rollout_status(db, &rollout.spec.id, RolloutStatus::RunningComplete, None, None)
			.await?;
		if let Err(err) = execute_phase(db, rollout, RolloutPhase::Complete, vars).await {
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

pub async fn run_rollback(
	db: &Surreal<Any>,
	folder: &str,
	opts: RolloutExecutionOpts,
	vars: &TemplateVars,
) -> Result<()> {
	run_setup(db, folder).await?;
	let rollout = load_rollout_spec(resolve_rollout_path(folder, opts.selector.as_deref())?)?;
	validate_rollout_spec(&rollout.spec)?;
	rollback_inner(db, &rollout, vars).await
}

/// Runs the rollback phase of a rollout defined entirely in code.
///
/// The `spec` must be identical to the one passed to [`run_start_with_spec`].
/// `vars` is applied to step SQL before execution; pass `&TemplateVars::default()`
/// if no substitution is needed.
pub async fn run_rollback_with_spec(
	db: &Surreal<Any>,
	folder: &str,
	spec: &RolloutSpec,
	vars: &TemplateVars,
) -> Result<()> {
	run_setup(db, folder).await?;
	validate_rollout_spec(spec)?;
	rollback_inner(db, &make_loaded_spec(spec), vars).await
}

async fn rollback_inner(
	db: &Surreal<Any>,
	rollout: &LoadedRolloutSpec,
	vars: &TemplateVars,
) -> Result<()> {
	acquire_lock(db, "global").await?;
	let result = async {
		let row = load_rollout_record(db, &rollout.spec.id)
			.await?
			.ok_or_else(|| anyhow!("rollout '{}' has not been started", rollout.spec.id))?;
		verify_rollout_record_matches(&row, rollout)?;
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
		if let Err(err) = execute_phase(db, rollout, RolloutPhase::Rollback, vars).await {
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

/// Heal a rollout left in an intermediate state without re-running SQL steps.
pub async fn run_repair(db: &Surreal<Any>, folder: &str, opts: RolloutExecutionOpts) -> Result<()> {
	run_setup(db, folder).await?;
	let rollout = load_rollout_spec(resolve_rollout_path(folder, opts.selector.as_deref())?)?;
	validate_rollout_spec(&rollout.spec)?;
	repair_inner(db, &rollout).await
}

async fn repair_inner(db: &Surreal<Any>, rollout: &LoadedRolloutSpec) -> Result<()> {
	acquire_lock(db, "global").await?;
	let result = async {
		let row = load_rollout_record(db, &rollout.spec.id)
			.await?
			.ok_or_else(|| anyhow!("rollout '{}' has no __rollout record", rollout.spec.id))?;
		verify_rollout_record_matches(&row, rollout)?;
		let status = string_field(&row, "status").unwrap_or_default();
		match status.as_str() {
			"running_complete" => {
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
				println!(
					"Repaired rollout {}: running_complete → completed.",
					rollout.spec.id
				);
			}
			"running_rollback" => {
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
				println!(
					"Repaired rollout {}: running_rollback → rolled_back.",
					rollout.spec.id
				);
			}
			"running_start" => {
				set_rollout_status(
					db,
					&rollout.spec.id,
					RolloutStatus::Failed,
					Some(
						"repair: rollout was killed mid-start; re-run `rollout start` (idempotent) or `rollout rollback`",
					),
					None,
				)
				.await?;
				println!(
					"Repaired rollout {}: running_start → failed (re-run start or rollback).",
					rollout.spec.id
				);
			}
			"completed" | "rolled_back" => {
				println!("Rollout {} is already in a terminal state ({}); nothing to repair.", rollout.spec.id, status);
			}
			other => bail!(
				"rollout '{}' is not in a repairable state (status={})",
				rollout.spec.id,
				other
			),
		}
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

/// Force a stuck or failed rollout to the terminal `rolled_back` state by id,
/// without running any rollback SQL and without needing the original spec.
///
/// Use this as a last-resort recovery when a rollout is wedged in `failed`,
/// `running_start`, `ready_to_complete`, `running_complete`, or `running_rollback`
/// and is blocking new rollouts (only one may be active at a time). It does **not**
/// undo any schema changes the rollout already applied — reconcile those with a
/// fresh sync or a new rollout afterwards. Already-terminal rollouts are a no-op.
pub async fn run_abandon_rollout(db: &Surreal<Any>, rollout_id: &str) -> Result<()> {
	acquire_lock(db, "global").await?;
	let result = async {
		let row = load_rollout_record(db, rollout_id)
			.await?
			.ok_or_else(|| anyhow!("rollout '{}' has no __rollout record", rollout_id))?;
		match string_field(&row, "status").as_deref() {
			Some("completed") => {
				bail!("rollout '{}' is already completed; nothing to abandon", rollout_id)
			}
			Some("rolled_back") => {
				println!("Rollout {} is already rolled back.", rollout_id);
				return Ok(());
			}
			_ => {}
		}
		set_rollout_status(
			db,
			rollout_id,
			RolloutStatus::RolledBack,
			Some("abandoned: force-transitioned to rolled_back; schema changes were not reverted"),
			Some(OffsetDateTime::now_utc().format(&Rfc3339)?),
		)
		.await?;
		println!("Abandoned rollout {} (forced → rolled_back).", rollout_id);
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
			"SELECT id, status, started_at FROM __rollout \
			 WHERE status INSIDE ['planned', 'running_start', 'ready_to_complete', 'running_complete', 'running_rollback', 'failed'] \
			 ORDER BY started_at DESC LIMIT 1;",
		)
		.await?;
	let raw: Option<surrealdb_types::Value> = resp.take(0)?;
	let row = raw.map(|v| Value::from_value(v).unwrap_or(Value::Null));
	Ok(row.and_then(|value| string_field(&value, "id")))
}

pub async fn load_managed_entities(db: &Surreal<Any>) -> Result<Vec<ManagedEntityRecord>> {
	let mut resp = db.query("SELECT key, val FROM __entity WHERE ns = 'schema';").await?;
	let rows: Vec<Value> = resp.take(0)?;
	let mut out = Vec::with_capacity(rows.len());
	for row in rows {
		let key = string_field_req(&row, "key")?;
		let val = row.get("val").cloned().unwrap_or(Value::Null);

		// key format: "kind:scope:name" (scope may be empty)
		let parts: Vec<&str> = key.splitn(3, ':').collect();
		if parts.len() < 3 {
			continue;
		}
		let kind = EntityKind::from_storage(parts[0]);
		let scope = if parts[1].is_empty() {
			None
		} else {
			Some(parts[1].to_string())
		};
		let name = parts[2].to_string();

		let source_path =
			val.get("source_path").and_then(|v| v.as_str()).unwrap_or_default().to_string();
		let statement_hash =
			val.get("statement_hash").and_then(|v| v.as_str()).unwrap_or_default().to_string();
		let file_hash =
			val.get("file_hash").and_then(|v| v.as_str()).unwrap_or_default().to_string();
		let active_rollout_id =
			val.get("active_rollout_id").and_then(|v| v.as_str()).map(str::to_string);
		let state = val.get("state").and_then(|v| v.as_str()).unwrap_or("active").to_string();

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

// All entity catalog writes go through a single bound query so a rollout with
// N managed entities is N HTTP round-trips → 1. This fixes a hang against
// SurrealDB Cloud where the per-entity loop in `complete` would stall the
// final `__rollout` status flip (issue #55).
fn entities_payload(entities: &[CatalogEntity]) -> Vec<Value> {
	entities
		.iter()
		.map(|e| {
			serde_json::json!({
				"key": entity_key_string(&e.kind, e.scope.as_deref(), &e.name),
				"source_path": e.source_path,
				"statement_hash": e.statement_hash,
				"file_hash": e.file_hash,
			})
		})
		.collect()
}

fn entity_keys_payload(entities: &[EntityKey]) -> Vec<String> {
	entities.iter().map(|e| entity_key_string(&e.kind, e.scope.as_deref(), &e.name)).collect()
}

pub async fn upsert_managed_entities(
	db: &Surreal<Any>,
	entities: &[CatalogEntity],
	active_rollout_id: Option<&str>,
	state: &str,
) -> Result<()> {
	if entities.is_empty() {
		return Ok(());
	}
	db.query(
		"FOR $e IN $entities { \
		 	DELETE __entity WHERE ns = 'schema' AND key = $e.key; \
		 	CREATE __entity CONTENT { \
		 		ns: 'schema', \
		 		key: $e.key, \
		 		val: { \
		 			source_path: $e.source_path, \
		 			statement_hash: $e.statement_hash, \
		 			file_hash: $e.file_hash, \
		 			active_rollout_id: $active_rollout_id, \
		 			state: $state \
		 		}, \
		 		updated_at: time::now() \
		 	}; \
		 };",
	)
	.bind(("entities", entities_payload(entities)))
	.bind(("active_rollout_id", active_rollout_id.map(str::to_string)))
	.bind(("state", state.to_string()))
	.await?
	.check()?;
	Ok(())
}

fn entity_key_string(kind: &EntityKind, scope: Option<&str>, name: &str) -> String {
	format!("{}:{}:{}", kind, scope.unwrap_or(""), name)
}

pub async fn delete_managed_entities(db: &Surreal<Any>, entities: &[EntityKey]) -> Result<()> {
	if entities.is_empty() {
		return Ok(());
	}
	db.query("DELETE __entity WHERE ns = 'schema' AND key INSIDE $keys;")
		.bind(("keys", entity_keys_payload(entities)))
		.await?
		.check()?;
	Ok(())
}

pub async fn replace_managed_entities(
	db: &Surreal<Any>,
	entities: &[CatalogEntity],
	active_rollout_id: Option<&str>,
	state: &str,
) -> Result<()> {
	db.query(
		"DELETE __entity WHERE ns = 'schema'; \
		 FOR $e IN $entities { \
		 	CREATE __entity CONTENT { \
		 		ns: 'schema', \
		 		key: $e.key, \
		 		val: { \
		 			source_path: $e.source_path, \
		 			statement_hash: $e.statement_hash, \
		 			file_hash: $e.file_hash, \
		 			active_rollout_id: $active_rollout_id, \
		 			state: $state \
		 		}, \
		 		updated_at: time::now() \
		 	}; \
		 };",
	)
	.bind(("entities", entities_payload(entities)))
	.bind(("active_rollout_id", active_rollout_id.map(str::to_string)))
	.bind(("state", state.to_string()))
	.await?
	.check()?;
	Ok(())
}

pub async fn replace_sync_hashes(db: &Surreal<Any>, files: &[SchemaFile]) -> Result<()> {
	db.query("DELETE __entity WHERE ns = 'sync';").await?.check()?;
	for file in files {
		db.query(
			"CREATE __entity CONTENT { ns: 'sync', key: $path, val: { hash: $hash }, updated_at: time::now() };",
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
		db.query("DELETE __entity WHERE ns = 'sync' AND key = $path;")
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
		steps.push(RolloutStep::apply_files(
			"apply_expand_schema",
			RolloutPhase::Start,
			changed_paths,
		));
	}

	let added_entities: Vec<EntityKey> =
		catalog_diff.added.iter().map(CatalogEntity::key).collect();
	if !added_entities.is_empty() {
		steps.push(RolloutStep::remove_entities(
			"rollback_expand_schema",
			RolloutPhase::Rollback,
			added_entities,
		));
	}

	let removed_entities: Vec<EntityKey> =
		catalog_diff.removed.iter().map(CatalogEntity::key).collect();
	if !removed_entities.is_empty() {
		steps.push(RolloutStep::remove_entities(
			"remove_legacy_entities",
			RolloutPhase::Complete,
			removed_entities,
		));
	}

	Ok(RolloutSpec {
		id: rollout_id.to_string(),
		name: name.to_string(),
		source_schema_hash: hash_schema_snapshot(old_schema)?,
		target_schema_hash: hash_schema_snapshot(new_schema)?,
		compatibility: RolloutCompatibility::Phased,
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

	let removed_by_scope: BTreeSet<(EntityKind, Option<String>)> =
		diff.removed.iter().map(|entity| (entity.kind.clone(), entity.scope.clone())).collect();
	let added_by_scope: BTreeSet<(EntityKind, Option<String>)> =
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

fn make_loaded_spec(spec: &RolloutSpec) -> LoadedRolloutSpec {
	let checksum = sha256_hex(toml::to_string_pretty(spec).unwrap_or_default().as_bytes());
	LoadedRolloutSpec {
		path: PathBuf::from(format!("embedded:{}", spec.id)),
		checksum,
		spec: spec.clone(),
	}
}

fn embedded_to_schema_files(files: &[crate::sync::EmbeddedSchemaFile]) -> Vec<SchemaFile> {
	files
		.iter()
		.map(|f| SchemaFile {
			path: f.path.to_string(),
			sql: f.sql.to_string(),
			hash: sha256_hex(f.sql.as_bytes()),
		})
		.collect()
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

fn resolve_rollout_path(folder: &str, selector: Option<&str>) -> Result<PathBuf> {
	let selector = selector.ok_or_else(|| anyhow!("rollout id or path is required"))?;
	let path = Path::new(selector);
	if path.exists() {
		return Ok(path.to_path_buf());
	}
	let rd = rollouts_dir(folder);
	let direct = rd.join(selector);
	if direct.exists() {
		return Ok(direct);
	}
	let with_ext = rd.join(format!("{selector}.toml"));
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

	let mut step_ids = BTreeSet::new();
	for step in &spec.steps {
		if !step_ids.insert(step.id.clone()) {
			bail!("duplicate rollout step id '{}'", step.id);
		}
		// The type system guarantees each action carries the right shape (e.g.
		// assert_sql always has both sql and expect). We only check for empty
		// payloads that compile but make no sense at runtime.
		match &step.action {
			RolloutAction::ApplySchema {
				sql,
			} => {
				if sql.trim().is_empty() {
					bail!("apply_schema step '{}' requires non-empty sql", step.id);
				}
			}
			RolloutAction::ApplyFiles {
				files,
			} => {
				if files.is_empty() {
					bail!("apply_files step '{}' requires at least one file", step.id);
				}
			}
			RolloutAction::RunSql {
				sql,
			} => {
				if sql.trim().is_empty() {
					bail!("run_sql step '{}' requires non-empty sql", step.id);
				}
			}
			RolloutAction::AssertSql {
				sql,
				expect,
			} => {
				if sql.trim().is_empty() {
					bail!("assert_sql step '{}' requires non-empty sql", step.id);
				}
				if expect.trim().is_empty() {
					bail!("assert_sql step '{}' requires a non-empty expect", step.id);
				}
			}
			RolloutAction::RemoveEntities {
				entities,
			} => {
				if entities.is_empty() {
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
	vars: &TemplateVars,
) -> Result<()> {
	for step in rollout.spec.steps.iter().filter(|step| step.phase == phase) {
		if step_already_completed(db, &rollout.spec.id, &step.id).await? {
			continue;
		}

		record_step_start(db, &rollout.spec.id, step).await?;
		let result = execute_step(db, step, vars).await;
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

async fn execute_step(db: &Surreal<Any>, step: &RolloutStep, vars: &TemplateVars) -> Result<()> {
	match &step.action {
		RolloutAction::ApplySchema {
			sql,
		} => {
			let substituted = vars.apply(sql)?;
			exec_surql(db, &ensure_overwrite(&substituted)).await
		}
		RolloutAction::ApplyFiles {
			files,
		} => {
			for file in files {
				let raw = fs::read_to_string(file).with_context(|| format!("reading {}", file))?;
				let substituted = vars
					.apply(&raw)
					.with_context(|| format!("applying template variables in {}", file))?;
				exec_surql(db, &ensure_overwrite(&substituted)).await?;
			}
			Ok(())
		}
		RolloutAction::RunSql {
			sql,
		} => {
			let substituted = vars.apply(sql)?;
			exec_surql(db, &substituted).await
		}
		RolloutAction::AssertSql {
			sql,
			expect,
		} => {
			let substituted = vars.apply(sql)?;
			let actual = execute_sql_value(db, &substituted).await?;
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
		RolloutAction::RemoveEntities {
			entities,
		} => {
			let sql = render_remove_sql(entities, true)?.join("\n");
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
	let mut resp = db.query("SELECT id FROM __rollout LIMIT 1;").await?;
	let row: Option<Value> = resp.take(0)?;
	Ok(row.is_some())
}

async fn ensure_no_conflicting_active_rollout(db: &Surreal<Any>, rollout_id: &str) -> Result<()> {
	if let Some(active_id) = load_active_rollout_id(db).await?
		&& active_id != rollout_id
	{
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
		"DELETE __rollout WHERE record::id(id) = $id; \
		 CREATE __rollout CONTENT { \
		 	id: $id, \
		 	name: $name, \
		 	manifest_path: $manifest_path, \
		 	manifest_checksum: $manifest_checksum, \
		 	source_schema_hash: $source_schema_hash, \
		 	target_schema_hash: $target_schema_hash, \
		 	status: $status, \
		 	source_entities: $source_entities, \
		 	target_entities: $target_entities, \
		 	started_at: <datetime> $started_at, \
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
		.query("SELECT * FROM __rollout WHERE record::id(id) = $id LIMIT 1;")
		.bind(("id", rollout_id.to_string()))
		.await?;
	let raw: Option<surrealdb_types::Value> = resp.take(0)?;
	Ok(raw.map(|v| Value::from_value(v).unwrap_or(Value::Null)))
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
		"UPDATE __rollout SET \
		 	status = $status, \
		 	last_error = $last_error, \
		 	completed_at = IF $completed_at THEN <datetime> $completed_at ELSE NONE END, \
		 	updated_at = time::now() \
		 WHERE record::id(id) = $id;",
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
	let row = load_rollout_record(db, rollout_id).await?;
	let Some(row) = row else {
		return Ok(false);
	};
	let steps = row.get("steps").and_then(|v| v.as_array());
	Ok(steps
		.map(|arr| {
			arr.iter().any(|s| {
				s.get("step_id").and_then(|v| v.as_str()) == Some(step_id)
					&& s.get("status").and_then(|v| v.as_str()) == Some("completed")
			})
		})
		.unwrap_or(false))
}

async fn record_step_start(db: &Surreal<Any>, rollout_id: &str, step: &RolloutStep) -> Result<()> {
	let new_step = serde_json::json!({
		"step_id": step.id,
		"phase": format!("{:?}", step.phase).to_ascii_lowercase(),
		"kind": step.action.kind_str(),
		"checksum": step_checksum(step)?,
		"status": "running",
		"error": null
	});
	// Load, remove any existing entry for this step, append, write back
	let row = load_rollout_record(db, rollout_id)
		.await?
		.ok_or_else(|| anyhow!("rollout '{}' not found", rollout_id))?;
	let mut steps: Vec<Value> =
		row.get("steps").and_then(|v| v.as_array()).cloned().unwrap_or_default();
	steps.retain(|s| s.get("step_id").and_then(|v| v.as_str()) != Some(&step.id));
	steps.push(new_step);
	db.query(
		"UPDATE __rollout SET steps = $steps, updated_at = time::now() \
		 WHERE record::id(id) = $id;",
	)
	.bind(("id", rollout_id.to_string()))
	.bind(("steps", steps))
	.await?
	.check()?;
	Ok(())
}

async fn record_step_complete(
	db: &Surreal<Any>,
	rollout_id: &str,
	step: &RolloutStep,
) -> Result<()> {
	update_step_status(db, rollout_id, &step.id, "completed", None).await
}

async fn record_step_failure(
	db: &Surreal<Any>,
	rollout_id: &str,
	step: &RolloutStep,
	error: &str,
) -> Result<()> {
	update_step_status(db, rollout_id, &step.id, "failed", Some(error)).await
}

async fn update_step_status(
	db: &Surreal<Any>,
	rollout_id: &str,
	step_id: &str,
	status: &str,
	error: Option<&str>,
) -> Result<()> {
	// Load, patch in Rust, write back — avoids complex inline array mutation
	let row = load_rollout_record(db, rollout_id)
		.await?
		.ok_or_else(|| anyhow!("rollout '{}' not found", rollout_id))?;
	let mut steps: Vec<Value> =
		row.get("steps").and_then(|v| v.as_array()).cloned().unwrap_or_default();
	for s in &mut steps {
		if s.get("step_id").and_then(|v| v.as_str()) == Some(step_id)
			&& let Some(obj) = s.as_object_mut()
		{
			obj.insert("status".into(), Value::String(status.to_string()));
			obj.insert(
				"error".into(),
				error.map(|e| Value::String(e.to_string())).unwrap_or(Value::Null),
			);
		}
	}
	db.query(
		"UPDATE __rollout SET steps = $steps, updated_at = time::now() \
		 WHERE record::id(id) = $id;",
	)
	.bind(("id", rollout_id.to_string()))
	.bind(("steps", steps))
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
		"DELETE __entity WHERE ns = 'lock' AND key = $key; \
		 CREATE __entity CONTENT { \
		 	ns: 'lock', \
		 	key: $key, \
		 	val: { owner: $owner }, \
		 	updated_at: time::now() \
		 };",
	)
	.bind(("key", lock_key.to_string()))
	.bind(("owner", owner))
	.await?
	.check()?;
	Ok(())
}

pub async fn release_lock(db: &Surreal<Any>, lock_key: &str) -> Result<()> {
	db.query("DELETE __entity WHERE ns = 'lock' AND key = $key;")
		.bind(("key", lock_key.to_string()))
		.await?
		.check()?;
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
					kind: EntityKind::Field,
					scope: Some("person".to_string()),
					name: "name".to_string(),
					source_path: "database/schema/person.surql".to_string(),
					statement_hash: "a".to_string(),
					file_hash: "fa".to_string(),
				},
				new: CatalogEntity {
					kind: EntityKind::Field,
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
					kind: EntityKind::Table,
					scope: None,
					name: "customer".to_string(),
					source_path: "database/schema/customer.surql".to_string(),
					statement_hash: "stmt".to_string(),
					file_hash: "file-a".to_string(),
				}],
				removed: vec![CatalogEntity {
					kind: EntityKind::Field,
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
		// The expand phase reads changed files from disk → ApplyFiles.
		assert!(spec.steps.iter().any(|step| {
			step.phase == RolloutPhase::Start
				&& matches!(step.action, RolloutAction::ApplyFiles { .. })
		}));
		assert!(spec.steps.iter().any(|step| {
			step.phase == RolloutPhase::Rollback
				&& matches!(step.action, RolloutAction::RemoveEntities { .. })
		}));
		assert!(spec.steps.iter().any(|step| {
			step.phase == RolloutPhase::Complete
				&& matches!(step.action, RolloutAction::RemoveEntities { .. })
		}));
	}

	#[test]
	fn rollout_lint_rejects_empty_run_sql() {
		// The action enum makes mismatched shapes unrepresentable; validation only
		// guards against empty payloads that still compile.
		let spec = RolloutSpec::builder("a")
			.step(RolloutStep::run_sql("step", RolloutPhase::Start, "   "))
			.build();
		let err = validate_rollout_spec(&spec).expect_err("must reject empty run_sql");
		assert!(err.to_string().contains("non-empty sql"));
	}

	async fn connect_mem_db() -> Surreal<Any> {
		use surrealdb::engine::any::connect;
		use surrealdb::opt::Config;
		use surrealdb::opt::capabilities::Capabilities;

		let config = Config::new().capabilities(Capabilities::all());
		let db = connect(("mem://", config)).await.expect("connect mem://");
		db.use_ns("surrealkit_test").use_db("rollout_test").await.expect("use_ns/use_db");
		db.query(crate::scaffold::DEFAULT_SETUP)
			.await
			.expect("setup schema")
			.check()
			.expect("setup schema check");
		db
	}

	fn sample_loaded_spec(id: &str) -> LoadedRolloutSpec {
		LoadedRolloutSpec {
			path: PathBuf::from(format!("database/rollouts/{id}.toml")),
			checksum: "sum".to_string(),
			spec: RolloutSpec {
				id: id.to_string(),
				name: "test".to_string(),
				source_schema_hash: "src".to_string(),
				target_schema_hash: "tgt".to_string(),
				compatibility: RolloutCompatibility::Phased,
				renames: Vec::new(),
				steps: Vec::new(),
			},
		}
	}

	async fn load_single_row(db: &Surreal<Any>) -> Value {
		let mut resp =
			db.query("SELECT * FROM __rollout LIMIT 1;").await.expect("select __rollout");
		let rows: Vec<Value> = resp.take(0).expect("take rows");
		rows.into_iter().next().expect("one row exists")
	}

	// Regression: CREATE __rollout used to bind started_at as a plain RFC3339 string,
	// which the SCHEMAFULL `datetime` field rejected. Keep the SQL-side `<datetime>`
	// cast so string bindings coerce server-side.
	#[tokio::test]
	async fn create_rollout_record_accepts_rfc3339_started_at() {
		let db = connect_mem_db().await;
		let loaded = sample_loaded_spec("20260417181055__initial_schema");

		create_rollout_record(&db, &loaded, &[], &[], RolloutStatus::Planned)
			.await
			.expect("create_rollout_record should coerce started_at string to datetime");

		let row = load_single_row(&db).await;
		let started = row
			.get("started_at")
			.and_then(|v| v.as_str())
			.expect("started_at is serialized as a datetime string");
		time::OffsetDateTime::parse(started, &Rfc3339)
			.expect("started_at should round-trip through RFC3339");
		assert_eq!(row.get("status").and_then(|v| v.as_str()), Some("planned"));
	}

	// Regression: set_rollout_status bound completed_at as Option<String>, which the
	// SCHEMAFULL `option<datetime>` field rejected for the Some(rfc3339) case. The
	// SQL-side `IF $completed_at THEN <datetime> $completed_at ELSE NONE END`
	// pattern must accept both Some(rfc3339) and None.
	#[tokio::test]
	async fn set_rollout_status_accepts_rfc3339_completed_at() {
		let db = connect_mem_db().await;
		let loaded = sample_loaded_spec("20260417181055__complete_path");
		create_rollout_record(&db, &loaded, &[], &[], RolloutStatus::RunningComplete)
			.await
			.expect("seed rollout record");

		let completed_at = OffsetDateTime::now_utc().format(&Rfc3339).expect("format rfc3339");
		set_rollout_status(
			&db,
			&loaded.spec.id,
			RolloutStatus::Completed,
			None,
			Some(completed_at),
		)
		.await
		.expect("set_rollout_status should coerce completed_at string to datetime");

		let row = load_single_row(&db).await;
		let completed = row
			.get("completed_at")
			.and_then(|v| v.as_str())
			.expect("completed_at is serialized as a datetime string");
		time::OffsetDateTime::parse(completed, &Rfc3339)
			.expect("completed_at should round-trip through RFC3339");
	}

	// Regression: `completed_at = None` must clear the field to NONE rather than
	// failing the `option<datetime>` coercion with an empty/null placeholder. Also
	// verifies the UPDATE's WHERE clause matched (status transitions planned →
	// running_start), which would silently no-op if the id lookup is broken.
	#[tokio::test]
	async fn set_rollout_status_accepts_none_completed_at() {
		let db = connect_mem_db().await;
		let loaded = sample_loaded_spec("20260417181055__running_path");
		create_rollout_record(&db, &loaded, &[], &[], RolloutStatus::Planned)
			.await
			.expect("seed rollout record");

		set_rollout_status(&db, &loaded.spec.id, RolloutStatus::RunningStart, None, None)
			.await
			.expect("set_rollout_status with None completed_at should succeed");

		let row = load_single_row(&db).await;
		assert!(
			row.get("completed_at").is_none_or(Value::is_null),
			"completed_at should be NONE/null, got {:?}",
			row.get("completed_at")
		);
		assert_eq!(row.get("status").and_then(|v| v.as_str()), Some("running_start"));
	}

	// Regression: `WHERE id = $id` against __rollout silently matched zero rows
	// because the record id is a Thing (`__rollout:…`) and the bound string isn't
	// auto-coerced. `load_rollout_record` must find the row it just created.
	#[tokio::test]
	async fn load_rollout_record_finds_created_row() {
		let db = connect_mem_db().await;
		let loaded = sample_loaded_spec("20260417181055__lookup");
		create_rollout_record(&db, &loaded, &[], &[], RolloutStatus::Planned)
			.await
			.expect("seed rollout record");

		let row = load_rollout_record(&db, &loaded.spec.id)
			.await
			.expect("load_rollout_record query")
			.expect("row must be found by rollout id");
		assert_eq!(row.get("status").and_then(|v| v.as_str()), Some("planned"));
	}

	// Regression: run_status used to call resp.take::<Vec<serde_json::Value>>(0) which
	// panics over HTTP/CBOR when rows contain SurrealDB datetime values (started_at,
	// completed_at). The fix deserialises via surrealdb_types::Value first and then
	// converts, matching the pattern already used in execute_sql_value.
	//
	// This test exercises the exact SELECT used by run_status against a completed rollout
	// record that has both started_at and completed_at populated.
	#[tokio::test]
	async fn run_status_select_does_not_panic_with_datetime_fields() {
		let db = connect_mem_db().await;
		let loaded = sample_loaded_spec("20260420101627__initial_schema");

		create_rollout_record(&db, &loaded, &[], &[], RolloutStatus::Planned)
			.await
			.expect("create rollout record");

		let completed_at = OffsetDateTime::now_utc().format(&Rfc3339).expect("format rfc3339");
		set_rollout_status(
			&db,
			&loaded.spec.id,
			RolloutStatus::Completed,
			None,
			Some(completed_at),
		)
		.await
		.expect("set completed status");

		// Replicate the exact query run_status issues, including the datetime fields.
		let mut resp = db
			.query(
				"SELECT id, name, status, started_at, completed_at, last_error, steps \
				 FROM __rollout WHERE record::id(id) = $id ORDER BY started_at DESC;",
			)
			.bind(("id", loaded.spec.id.clone()))
			.await
			.expect("query");

		// Fixed deserialization path (must not panic).
		let raw_rows: Vec<surrealdb_types::Value> = resp.take(0).expect("take raw rows");
		let rows: Vec<Value> =
			raw_rows.into_iter().map(|v| Value::from_value(v).unwrap_or(Value::Null)).collect();

		assert_eq!(rows.len(), 1, "expected one rollout row");
		let row = &rows[0];
		assert_eq!(row.get("status").and_then(|v| v.as_str()), Some("completed"));
		assert!(
			row.get("started_at").is_some(),
			"started_at must survive the surrealdb_types→serde_json conversion"
		);
		assert!(
			row.get("completed_at").is_some(),
			"completed_at must survive the surrealdb_types→serde_json conversion"
		);
	}

	// Regression: load_active_rollout_id selected started_at (a datetime field) into
	// Option<serde_json::Value>, which can panic over HTTP/CBOR. Same root cause as
	// run_status; verify the fixed deserialization path returns the correct id.
	#[tokio::test]
	async fn load_active_rollout_id_does_not_panic_with_datetime_field() {
		let db = connect_mem_db().await;
		let loaded = sample_loaded_spec("20260420101627__active_id_test");

		create_rollout_record(&db, &loaded, &[], &[], RolloutStatus::RunningStart)
			.await
			.expect("create rollout record");

		let active =
			load_active_rollout_id(&db).await.expect("load_active_rollout_id must not fail");
		// The id field is a SurrealDB Thing serialised as a string; the full record ID
		// (table prefix included) is what the function returns.
		assert!(active.is_some(), "should find the active rollout");
	}

	fn sample_entity(name: &str) -> CatalogEntity {
		CatalogEntity {
			kind: EntityKind::Field,
			scope: Some("person".to_string()),
			name: name.to_string(),
			source_path: format!("database/schema/{name}.surql"),
			statement_hash: format!("stmt-{name}"),
			file_hash: format!("file-{name}"),
		}
	}

	async fn entity_row_count(db: &Surreal<Any>) -> usize {
		let mut resp = db
			.query("SELECT count() AS c FROM __entity WHERE ns = 'schema' GROUP ALL;")
			.await
			.expect("count __entity");
		let rows: Vec<Value> = resp.take(0).expect("take count rows");
		rows.first().and_then(|v| v.get("c")).and_then(|v| v.as_u64()).unwrap_or(0) as usize
	}

	// Regression for issue #55: complete used to do one HTTP round-trip per
	// managed entity, which hung indefinitely against SurrealDB Cloud once the
	// rollout grew past a handful of DEFINE statements. The batched form must
	// land every entity in a single query and survive a re-run (idempotent).
	#[tokio::test]
	async fn replace_managed_entities_batched_writes_all_entities() {
		let db = connect_mem_db().await;
		let entities: Vec<CatalogEntity> =
			(0..25).map(|i| sample_entity(&format!("col_{i:02}"))).collect();

		replace_managed_entities(&db, &entities, Some("r-1"), "active")
			.await
			.expect("first batched replace");
		assert_eq!(entity_row_count(&db).await, 25, "all entities land on first call");

		// Re-running should still be idempotent — delete-all + recreate via the
		// FOR loop should produce the same row count, not duplicates.
		replace_managed_entities(&db, &entities, Some("r-1"), "active")
			.await
			.expect("second batched replace");
		assert_eq!(entity_row_count(&db).await, 25, "no duplicates on re-run");

		// Empty replacement clears the schema namespace.
		replace_managed_entities(&db, &[], None, "active").await.expect("empty replace");
		assert_eq!(entity_row_count(&db).await, 0, "empty entities clears ns=schema");
	}

	// Issue #55: when `complete` hangs after executing all SQL steps,
	// `__rollout.status` stays at `running_complete`. `run_repair` heals the
	// metadata transition (replace_managed_entities + status flip) without
	// re-running any SQL.
	#[tokio::test]
	async fn repair_heals_running_complete() {
		let db = connect_mem_db().await;
		let loaded = sample_loaded_spec("20260522000000__repair_complete_path");
		let target_entities = vec![sample_entity("a"), sample_entity("b")];

		create_rollout_record(&db, &loaded, &[], &target_entities, RolloutStatus::RunningComplete)
			.await
			.expect("seed rollout record");

		repair_inner(&db, &loaded).await.expect("repair_inner should succeed");

		let row = load_single_row(&db).await;
		assert_eq!(row.get("status").and_then(|v| v.as_str()), Some("completed"));
		assert!(
			row.get("completed_at").and_then(|v| v.as_str()).is_some(),
			"completed_at populated after repair",
		);
		assert_eq!(entity_row_count(&db).await, 2, "target_entities materialised");
	}

	// Repair on a `running_rollback` rollout flips it to `rolled_back` and
	// restores `source_entities`.
	#[tokio::test]
	async fn repair_heals_running_rollback() {
		let db = connect_mem_db().await;
		let loaded = sample_loaded_spec("20260522000001__repair_rollback_path");
		let source_entities = vec![sample_entity("old_a")];

		create_rollout_record(
			&db,
			&loaded,
			&source_entities,
			&[sample_entity("new_a")],
			RolloutStatus::RunningRollback,
		)
		.await
		.expect("seed rollout record");

		repair_inner(&db, &loaded).await.expect("repair_inner should succeed");

		let row = load_single_row(&db).await;
		assert_eq!(row.get("status").and_then(|v| v.as_str()), Some("rolled_back"));
		assert_eq!(entity_row_count(&db).await, 1, "source_entities materialised");
	}

	// Repair refuses to touch rollouts that are not in an intermediate state
	// so the user can't accidentally clobber a clean record.
	#[tokio::test]
	async fn repair_refuses_planned_rollout() {
		let db = connect_mem_db().await;
		let loaded = sample_loaded_spec("20260522000002__repair_planned_rejected");
		create_rollout_record(&db, &loaded, &[], &[], RolloutStatus::Planned)
			.await
			.expect("seed rollout record");

		let err = repair_inner(&db, &loaded).await.expect_err("planned is not repairable");
		assert!(err.to_string().contains("not in a repairable state"), "got: {err}",);
	}
}
