use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

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
use crate::module::{Module, Partition};
use crate::schema_state::{
	CatalogDiff, CatalogEntity, CatalogSnapshot, EntityKey, EntityKind, FileDiff, SchemaFile,
	build_catalog_snapshot, canonicalise_recorded_path, collect_schema_files, diff_catalog,
	diff_schema, ensure_local_state_dirs, ensure_overwrite, hash_schema_snapshot,
	load_catalog_snapshot, load_schema_snapshot, render_remove_sql, save_catalog_snapshot,
	save_schema_snapshot, snapshot_from_files, strip_folder_prefix, verify_schema_hash,
};
use crate::setup::run_setup;
use crate::variables::TemplateVars;

#[derive(Debug, Clone)]
#[doc(hidden)]
pub struct RolloutPlanOpts {
	pub name: Option<String>,
	pub dry_run: bool,
	/// Plan changes to entities that already exist, not just additions and
	/// removals. Off by default: rollback restores the previous *definition*, so
	/// the operator has to decide whether that is a real undo for this change.
	pub allow_modified: bool,
}

#[derive(Debug, Clone)]
#[doc(hidden)]
pub struct RolloutExecutionOpts {
	pub selector: Option<String>,
	/// Deadline for a single step's SQL. `None` waits indefinitely, which stays
	/// the default because a legitimate index build can take hours.
	pub query_timeout: Option<Duration>,
}

impl RolloutExecutionOpts {
	/// Select a rollout by id, with no step deadline.
	pub fn new(selector: Option<String>) -> Self {
		Self {
			selector,
			query_timeout: None,
		}
	}
}

/// Everything step execution needs beyond the database handle.
///
/// Bundled rather than passed as three more parameters because `folder` and
/// `query_timeout` have to reach `execute_step` through the same three
/// `start`/`complete`/`rollback` call chains that already thread `vars`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StepContext<'a> {
	pub(crate) vars: &'a TemplateVars,
	/// Project folder, for resolving an `apply_files` step's recorded paths.
	/// `None` for the embedded/library path, which reads no files.
	pub(crate) folder: Option<&'a str>,
	pub(crate) query_timeout: Option<Duration>,
}

impl<'a> StepContext<'a> {
	pub(crate) fn new(
		vars: &'a TemplateVars,
		folder: Option<&'a str>,
		query_timeout: Option<Duration>,
	) -> Self {
		Self {
			vars,
			folder,
			query_timeout,
		}
	}
}

/// Which phase of a rollout a step belongs to.
///
/// A rollout runs in two forward phases plus an undo phase:
/// - `Start` — the non-destructive expand phase (add/modify schema).
/// - `Complete` — the destructive contract phase (remove what's no longer needed).
/// - `Rollback` — undo the `Start` phase.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RolloutPhase {
	Start,
	Complete,
	Rollback,
}

impl RolloutPhase {
	/// The persisted spelling of this phase (e.g. `"start"`).
	pub fn as_str(&self) -> &'static str {
		match self {
			Self::Start => "start",
			Self::Complete => "complete",
			Self::Rollback => "rollback",
		}
	}
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
	ApplySchema {
		sql: String,
	},
	/// Read DDL from `.surql` files on disk and apply them. Used by the CLI
	/// `rollout plan`/`start` workflow; in code prefer [`RolloutAction::ApplySchema`]
	/// with inline SQL.
	ApplyFiles {
		files: Vec<String>,
	},
	/// Execute SurrealQL that mutates data (e.g. a backfill). The SQL must be safe
	/// to re-run: on retry the step executes again from scratch.
	RunSql {
		sql: String,
	},
	/// Run a query and assert its stringified output equals `expect`; fails the
	/// rollout otherwise.
	AssertSql {
		sql: String,
		expect: String,
	},
	/// Issue `REMOVE … IF EXISTS` for named database objects (tables, fields,
	/// indexes, …).
	RemoveEntities {
		entities: Vec<EntityKey>,
	},
	/// Re-apply the definitions these entities had before the rollout's expand
	/// phase, undoing a modification.
	///
	/// The statement text is not in the manifest: a catalog snapshot records only
	/// a `statement_hash`, never the SQL. `start` reads the live definitions with
	/// `INFO FOR DB` / `INFO FOR TABLE` and stores them on the `__rollout` record,
	/// so the text always describes the database this rollout actually ran
	/// against.
	///
	/// This restores *definitions*, not data. Tightening an `ASSERT` and rolling
	/// back leaves the loosened constraint, which is correct; narrowing a `TYPE`
	/// and rolling back restores the wider type but not any value coerced on the
	/// way in.
	RestoreDefinitions {
		entities: Vec<EntityKey>,
	},
}

impl RolloutAction {
	/// The persisted discriminator string for this action (e.g. `"apply_schema"`).
	fn kind_str(&self) -> &'static str {
		match self {
			Self::ApplySchema {
				..
			} => "apply_schema",
			Self::ApplyFiles {
				..
			} => "apply_files",
			Self::RunSql {
				..
			} => "run_sql",
			Self::AssertSql {
				..
			} => "assert_sql",
			Self::RemoveEntities {
				..
			} => "remove_entities",
			Self::RestoreDefinitions {
				..
			} => "restore_definitions",
		}
	}
}

/// The lifecycle state of a rollout.
///
/// Happy path:
/// `Planned → RunningStart → ReadyToComplete → RunningComplete → Completed`.
///
/// Rollback path: from `ReadyToComplete` (or a recovered state)
/// `→ RunningRollback → RolledBack`.
///
/// `Completed` and `RolledBack` are terminal (see [`RolloutStatus::is_terminal`]).
/// `Failed` and the three `Running*` states are intermediate/stuck states left by
/// an interrupted run; recover them with the CLI `repair` command or
/// [`Rollout::abandon`]. Only one rollout may be in a non-terminal state at a time.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
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
	/// The persisted lowercase string for this status (e.g. `"running_start"`).
	pub fn as_str(&self) -> &'static str {
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

	/// Parse a persisted status string, or `None` if unrecognized.
	pub fn from_storage(s: &str) -> Option<Self> {
		Some(match s {
			"planned" => Self::Planned,
			"running_start" => Self::RunningStart,
			"ready_to_complete" => Self::ReadyToComplete,
			"running_complete" => Self::RunningComplete,
			"completed" => Self::Completed,
			"running_rollback" => Self::RunningRollback,
			"rolled_back" => Self::RolledBack,
			"failed" => Self::Failed,
			_ => return None,
		})
	}

	/// Whether this is a terminal state (`completed` or `rolled_back`).
	pub fn is_terminal(&self) -> bool {
		matches!(self, Self::Completed | Self::RolledBack)
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
	/// The schema module this rollout belongs to. Defaults to the unnamed default
	/// module, so manifests written before v1 load unchanged.
	///
	/// `skip_serializing_if` is not cosmetic: `manifest_checksum` is computed from
	/// the serialized spec, so emitting this field for a default-module rollout
	/// would change the checksum of every existing in-flight rollout.
	#[serde(default = "default_module_name", skip_serializing_if = "is_default_module")]
	pub module: String,
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

fn default_module_name() -> String {
	Module::DEFAULT_NAME.to_string()
}

fn is_default_module(name: &String) -> bool {
	name == Module::DEFAULT_NAME
}

impl RolloutSpec {
	/// The validated schema module this rollout targets.
	pub fn module(&self) -> Result<Module> {
		Module::new(self.module.clone())
	}
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

	/// Restore the definitions these entities had before the expand phase.
	pub fn restore_definitions(
		id: impl Into<String>,
		phase: RolloutPhase,
		entities: Vec<EntityKey>,
	) -> Self {
		Self {
			id: id.into(),
			phase,
			action: RolloutAction::RestoreDefinitions {
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
			module: default_module_name(),
			name,
			source_schema_hash: String::new(),
			target_schema_hash: String::new(),
			compatibility: self.compatibility,
			renames: Vec::new(),
			steps: self.steps,
		}
	}
}

/// A code-driven rollout: a [`RolloutSpec`] plus the desired end-state schema, with
/// methods to drive its lifecycle.
///
/// This is the primary entry point for running rollouts from your own code (as
/// opposed to the CLI's filesystem workflow). Construct one with [`Rollout::new`]
/// and call [`Rollout::start`] / [`Rollout::complete`] / [`Rollout::rollback`].
///
/// Only one rollout may be active (in any non-terminal state) at a time. If a
/// rollout becomes wedged, recover it with [`Rollout::abandon`].
///
/// ```no_run
/// # use surrealkit::{Rollout, RolloutSpec, RolloutStep, RolloutPhase, EmbeddedSchemaFile, Surreal, engine::any::Any};
/// # async fn run(db: &Surreal<Any>) -> anyhow::Result<()> {
/// static TARGET: &[EmbeddedSchemaFile] = &[];
/// let spec = RolloutSpec::builder("20260604__add_account")
///     .step(RolloutStep::apply_schema("create", RolloutPhase::Start, "DEFINE TABLE account SCHEMAFULL;"))
///     .build();
/// let rollout = Rollout::new(spec, TARGET);
/// rollout.start(db).await?;
/// rollout.complete(db).await?;
/// # Ok(()) }
/// ```
#[derive(Debug, Clone)]
pub struct Rollout<'a> {
	spec: RolloutSpec,
	target_files: &'a [crate::sync::EmbeddedSchemaFile],
	vars: TemplateVars,
	folder: Option<String>,
}

impl<'a> Rollout<'a> {
	/// Create a code-driven rollout.
	///
	/// `target_files` is the desired schema after the rollout completes; it is used
	/// to compute the managed-entity catalog recorded on complete/rollback. Pass
	/// `&[]` when the rollout's steps fully describe the entity changes.
	pub fn new(spec: RolloutSpec, target_files: &'a [crate::sync::EmbeddedSchemaFile]) -> Self {
		Self {
			spec,
			target_files,
			vars: TemplateVars::default(),
			folder: None,
		}
	}

	/// Opt into the filesystem-backed workflow, scaffolding `<folder>/setup.surql`
	/// when it is missing.
	///
	/// Without this a rollout is purely in-database and writes nothing to disk.
	/// Before v1 the folder defaulted to `./database` unconditionally, so running a
	/// code-driven rollout from a library created `./database/setup.surql` in the
	/// caller's working directory.
	pub fn folder(mut self, folder: impl Into<String>) -> Self {
		self.folder = Some(folder.into());
		self
	}

	/// Set template variables applied to step SQL before execution.
	pub fn vars(mut self, vars: TemplateVars) -> Self {
		self.vars = vars;
		self
	}

	/// The spec this rollout will run.
	pub fn spec(&self) -> &RolloutSpec {
		&self.spec
	}

	/// Run the `start` (expand) phase. Errors if another rollout is already active,
	/// if this rollout is already in a terminal state, or if a step fails (the
	/// rollout is left in the `failed` state).
	pub async fn start(&self, db: &Surreal<Any>) -> Result<()> {
		run_start_with_spec(db, self.folder.as_deref(), &self.spec, self.target_files, &self.vars)
			.await
	}

	/// Run the `complete` (contract) phase, applying destructive changes and marking
	/// the rollout completed.
	pub async fn complete(&self, db: &Surreal<Any>) -> Result<()> {
		run_complete_with_spec(db, self.folder.as_deref(), &self.spec, &self.vars).await
	}

	/// Run the `rollback` phase, undoing the `start` phase and marking the rollout
	/// rolled back.
	pub async fn rollback(&self, db: &Surreal<Any>) -> Result<()> {
		run_rollback_with_spec(db, self.folder.as_deref(), &self.spec, &self.vars).await
	}

	/// Fetch this rollout's current status, or `None` if it has never been started.
	pub async fn status(&self, db: &Surreal<Any>) -> Result<Option<RolloutStatusReport>> {
		load_rollout_status_report(db, &self.spec.id).await
	}

	/// Force a stuck or failed rollout (by id) to the terminal `rolled_back` state
	/// without running rollback SQL. See [`run_abandon_rollout`] for the full
	/// semantics — this does not revert applied schema changes.
	pub async fn abandon(db: &Surreal<Any>, rollout_id: &str) -> Result<()> {
		run_abandon_rollout(db, &Module::default_module(), rollout_id).await
	}
}

/// The recorded state of one step within a rollout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RolloutStepStatus {
	pub step_id: String,
	pub phase: String,
	pub kind: String,
	pub status: String,
	pub error: Option<String>,
}

/// A structured snapshot of a rollout's recorded execution state, returned by
/// [`Rollout::status`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RolloutStatusReport {
	pub id: String,
	pub name: String,
	/// The parsed status, or `None` if the stored value is unrecognized.
	pub status: Option<RolloutStatus>,
	pub started_at: Option<String>,
	pub completed_at: Option<String>,
	pub last_error: Option<String>,
	pub steps: Vec<RolloutStepStatus>,
}

#[derive(Debug, Clone)]
#[doc(hidden)]
pub struct LoadedRolloutSpec {
	pub path: PathBuf,
	pub checksum: String,
	pub spec: RolloutSpec,
}

#[derive(Debug, Clone)]
#[doc(hidden)]
pub struct ManagedEntityRecord {
	pub entity: CatalogEntity,
	pub active_rollout_id: Option<String>,
	pub state: String,
}

#[doc(hidden)]
pub async fn run_baseline(db: &Surreal<Any>, folder: &str, module: &Module) -> Result<()> {
	run_setup(db, folder).await?;
	ensure_local_state_dirs(folder)?;
	if rollout_rows_exist(db).await? {
		bail!("rollout state already exists; baseline can only be run once");
	}

	let files = collect_schema_files(folder)?;
	let schema_snapshot = snapshot_from_files(&files);
	let catalog_snapshot = build_catalog_snapshot(&files, false)?;

	replace_managed_entities(db, module, &catalog_snapshot.entities, None, "active").await?;
	replace_sync_hashes(db, module, &files).await?;
	save_schema_snapshot(folder, &schema_snapshot)?;
	save_catalog_snapshot(folder, &catalog_snapshot)?;

	log::info!(
		"Seeded managed entity baseline with {} schema file(s) and {} managed object(s).",
		files.len(),
		catalog_snapshot.entities.len()
	);
	Ok(())
}

#[doc(hidden)]
pub async fn run_plan(folder: &str, opts: RolloutPlanOpts) -> Result<()> {
	ensure_local_state_dirs(folder)?;
	let files = collect_schema_files(folder)?;
	let old_schema = load_schema_snapshot(folder)?;
	let old_catalog = load_catalog_snapshot(folder)?;
	let new_schema = snapshot_from_files(&files);
	let new_catalog = build_catalog_snapshot(&files, false)?;
	let file_diff = diff_schema(&old_schema, &new_schema);
	let catalog_diff = diff_catalog(&old_catalog, &new_catalog);

	validate_autoplan(&catalog_diff, opts.allow_modified)?;

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
		log::info!("Pending rollout plan:");
		log::info!(
			"  files: +{} ~{} -{}",
			file_diff.added.len(),
			file_diff.modified.len(),
			file_diff.removed.len()
		);
		log::info!(
			"  entities: +{} ~{} -{}",
			catalog_diff.added.len(),
			catalog_diff.modified.len(),
			catalog_diff.removed.len()
		);
		log::info!("  would create: {}", path.display());
		return Ok(());
	}

	fs::write(&path, raw).with_context(|| format!("writing rollout file {}", path.display()))?;
	// Snapshots are written here, next to the manifest, because `plan` is the only
	// command in the lifecycle that runs where the repository is. `complete` runs
	// on the server, typically inside a container whose filesystem nobody commits,
	// so writing them there would advance state the developer never sees and leave
	// the next `plan` diffing from a stale snapshot.
	//
	// The cost is that abandoning a plan leaves the snapshots ahead of the
	// database. Reverting the manifest and the snapshots together (they are all
	// tracked files) is the fix, and is why they are written as a pair.
	save_schema_snapshot(folder, &new_schema)?;
	save_catalog_snapshot(folder, &new_catalog)?;

	log::info!("Generated rollout manifest {}", path.display());
	log::info!("Updated {}", catalog_snapshot_path(folder).display());
	log::info!("Commit the manifest and the snapshots together; reverting means reverting both.");
	Ok(())
}

/// Rewrite a loaded manifest's recorded file paths onto this project's canonical
/// keys, returning the legacy prefixes they carried.
///
/// Doing it once, here, is what lets hash verification and file resolution share
/// a single answer. Resolving each path independently at execution time meant
/// stripping leading segments until something happened to exist, which for a
/// module file could land on the same-named file in the default module and apply
/// the wrong DDL.
///
/// A path that matches nothing is left as recorded, so the step still fails with
/// the path the manifest actually named.
fn canonicalise_manifest_paths(spec: &mut RolloutSpec, files: &[SchemaFile]) -> Vec<String> {
	let canonical: Vec<String> = files.iter().map(|file| file.path.clone()).collect();
	let mut prefixes: Vec<String> = Vec::new();

	for step in &mut spec.steps {
		if let RolloutAction::ApplyFiles {
			files,
		} = &mut step.action
		{
			for recorded in files.iter_mut() {
				let Some((key, prefix)) = canonicalise_recorded_path(recorded, &canonical) else {
					continue;
				};
				if !prefix.is_empty() && !prefixes.contains(&prefix) {
					prefixes.push(prefix);
				} else if prefix.is_empty() && !prefixes.iter().any(|p| p.is_empty()) {
					// The filesystem root is a prefix like any other.
					prefixes.push(String::new());
				}
				*recorded = key;
			}
		}
	}
	prefixes
}

#[doc(hidden)]
pub async fn run_lint(folder: &str, opts: RolloutExecutionOpts) -> Result<()> {
	ensure_local_state_dirs(folder)?;
	let mut rollout = load_rollout_spec(resolve_rollout_path(folder, opts.selector.as_deref())?)?;
	validate_rollout_spec(&rollout.spec)?;
	let files = collect_schema_files(folder)?;
	let legacy_prefixes = canonicalise_manifest_paths(&mut rollout.spec, &files);
	verify_schema_hash(
		&snapshot_from_files(&files),
		folder,
		&rollout.spec.target_schema_hash,
		&rollout.spec.id,
		&legacy_prefixes,
	)?;
	log::info!("Rollout {} is valid (checksum {}).", rollout.spec.id, rollout.checksum);
	Ok(())
}

/// Load a single rollout's recorded state as a structured report (used by
/// [`Rollout::status`]). Returns `None` if no record exists for `rollout_id`.
async fn load_rollout_status_report(
	db: &Surreal<Any>,
	rollout_id: &str,
) -> Result<Option<RolloutStatusReport>> {
	let Some(row) = load_rollout_record(db, rollout_id).await? else {
		return Ok(None);
	};
	let steps = row
		.get("steps")
		.and_then(|v| v.as_array())
		.cloned()
		.unwrap_or_default()
		.iter()
		.map(|step| RolloutStepStatus {
			step_id: string_field(step, "step_id").unwrap_or_default(),
			phase: string_field(step, "phase").unwrap_or_default(),
			kind: string_field(step, "kind").unwrap_or_default(),
			status: string_field(step, "status").unwrap_or_default(),
			error: string_field(step, "error"),
		})
		.collect();
	Ok(Some(RolloutStatusReport {
		id: string_field(&row, "id").unwrap_or_else(|| rollout_id.to_string()),
		name: string_field(&row, "name").unwrap_or_default(),
		status: string_field(&row, "status").as_deref().and_then(RolloutStatus::from_storage),
		started_at: string_field(&row, "started_at"),
		completed_at: string_field(&row, "completed_at"),
		last_error: string_field(&row, "last_error"),
		steps,
	}))
}

#[doc(hidden)]
pub async fn run_status(db: &Surreal<Any>, folder: &str, selector: Option<String>) -> Result<()> {
	run_setup(db, folder).await?;
	let mut query = "SELECT id, name, status, started_at, completed_at, last_error, \
	                 reversibility, steps FROM __rollout"
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
		log::info!("No rollout records found.");
		return Ok(());
	}

	for row in rows {
		let id = string_field(&row, "id").unwrap_or_else(|| "<unknown>".to_string());
		let name = string_field(&row, "name").unwrap_or_else(|| "<unnamed>".to_string());
		let status = string_field(&row, "status").unwrap_or_else(|| "<unknown>".to_string());
		log::info!("{} [{}] {}", id, status, name);
		if let Some(started_at) = string_field(&row, "started_at") {
			log::info!("  started_at: {}", started_at);
		}
		if let Some(completed_at) = string_field(&row, "completed_at") {
			log::info!("  completed_at: {}", completed_at);
		}
		if let Some(last_error) = string_field(&row, "last_error") {
			log::info!("  last_error: {}", last_error);
		}
		if string_field(&row, "reversibility").as_deref() == Some("definition_only") {
			log::info!(
				"  reversibility: definition_only (rollback restores the previous \
				 definitions, not data written under the new ones)"
			);
		}

		let steps = row.get("steps").and_then(|v| v.as_array()).cloned().unwrap_or_default();
		for step in steps {
			let step_id = string_field(&step, "step_id").unwrap_or_else(|| "<step>".to_string());
			let phase = string_field(&step, "phase").unwrap_or_else(|| "?".to_string());
			let kind = string_field(&step, "kind").unwrap_or_else(|| "?".to_string());
			let status = string_field(&step, "status").unwrap_or_else(|| "?".to_string());
			log::info!("  - {} [{}:{}] {}", step_id, phase, kind, status);
			if let Some(err) = string_field(&step, "error") {
				log::info!("    error: {}", err);
			}
		}
	}
	Ok(())
}

#[doc(hidden)]
pub async fn run_start(
	db: &Surreal<Any>,
	folder: &str,
	opts: RolloutExecutionOpts,
	vars: &TemplateVars,
) -> Result<()> {
	run_setup(db, folder).await?;
	ensure_local_state_dirs(folder)?;
	let mut rollout = load_rollout_spec(resolve_rollout_path(folder, opts.selector.as_deref())?)?;
	validate_rollout_spec(&rollout.spec)?;
	let files = collect_schema_files(folder)?;
	let legacy_prefixes = canonicalise_manifest_paths(&mut rollout.spec, &files);
	verify_schema_hash(
		&snapshot_from_files(&files),
		folder,
		&rollout.spec.target_schema_hash,
		&rollout.spec.id,
		&legacy_prefixes,
	)?;
	let target_catalog = build_catalog_snapshot(&files, false)?;
	let source_entities = load_managed_entities(db, &rollout.spec.module()?, Some(folder)).await?;
	let source_catalog = CatalogSnapshot {
		version: 2,
		entities: source_entities.into_iter().map(|r| r.entity).collect(),
		operations: Vec::new(),
	};
	let ctx = StepContext::new(vars, Some(folder), opts.query_timeout);
	start_inner(db, &rollout, &source_catalog, &target_catalog, &ctx).await
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
pub(crate) async fn run_start_with_spec(
	db: &Surreal<Any>,
	folder: Option<&str>,
	spec: &RolloutSpec,
	target_files: &[crate::sync::EmbeddedSchemaFile],
	vars: &TemplateVars,
) -> Result<()> {
	// `Some` selects the CLI's filesystem workflow, which scaffolds
	// `<folder>/setup.surql` when missing. Code-driven rollouts pass `None` so
	// nothing is written into the caller's working directory.
	match folder {
		Some(folder) => run_setup(db, folder).await?,
		None => crate::setup::run_setup_embedded(db).await?,
	}
	validate_rollout_spec(spec)?;
	let schema_files = embedded_to_schema_files(target_files);

	// Owned so the recorded paths can be canonicalised the way the CLI does. The
	// caller's spec is left untouched.
	let mut spec = spec.clone();
	let legacy_prefixes = canonicalise_manifest_paths(&mut spec, &schema_files);
	let spec = &spec;

	if !spec.target_schema_hash.is_empty() {
		// Same verification the CLI performs, including the pre-1.0.0-beta.2
		// fallback. An embedded caller shipping a manifest that CI planned has the
		// same foreign-prefix problem, and had no way through it while this path
		// compared hashes directly.
		verify_schema_hash(
			&snapshot_from_files(&schema_files),
			folder.unwrap_or(crate::constants::DEFAULT_ROOT_DIR),
			&spec.target_schema_hash,
			&spec.id,
			&legacy_prefixes,
		)?;
	}
	let target_catalog = build_catalog_snapshot(&schema_files, false)?;
	let source_entities = load_managed_entities(db, &spec.module()?, folder).await?;
	let source_catalog = CatalogSnapshot {
		version: 2,
		entities: source_entities.into_iter().map(|r| r.entity).collect(),
		operations: Vec::new(),
	};
	let ctx = StepContext::new(vars, folder, None);
	start_inner(db, &make_loaded_spec(spec), &source_catalog, &target_catalog, &ctx).await
}

async fn start_inner(
	db: &Surreal<Any>,
	rollout: &LoadedRolloutSpec,
	source_catalog: &CatalogSnapshot,
	target_catalog: &CatalogSnapshot,
	ctx: &StepContext<'_>,
) -> Result<()> {
	let lock = acquire_lock(db, &rollout.spec.module()?, "global").await?;
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
		// Capture what a rollback would restore *before* anything else, including
		// before the record exists. The previous definition of a modified entity
		// lives nowhere on disk (the catalog snapshot keeps only a hash), so the
		// live database is the only source for it.
		//
		// Ordering matters more than it looks. If the capture ran after
		// `create_rollout_record` and then failed, the record would exist with no
		// definitions, the operator's re-run would see a resume and skip the
		// capture, and `rollout rollback` would be permanently unavailable for that
		// rollout with no way to recover it. Capturing first means a failure here
		// leaves no record at all, so re-running `start` is still a first run.
		let restorable: Vec<EntityKey> = rollout
			.spec
			.steps
			.iter()
			.filter_map(|step| match &step.action {
				RolloutAction::RestoreDefinitions {
					entities,
				} => Some(entities.clone()),
				_ => None,
			})
			.flatten()
			.collect();

		match record.as_ref() {
			// A resume. The opening run already captured; re-reading now would pick
			// up whatever the expand phase wrote and overwrite the originals.
			Some(row) => {
				verify_rollout_record_matches(row, rollout)?;
				log::debug!(
					"rollout '{}' is resuming; keeping the rollback definitions the opening \
					 run captured",
					rollout.spec.id
				);
			}
			None => {
				let captured = if restorable.is_empty() {
					BTreeMap::new()
				} else {
					capture_definitions(db, &restorable).await?
				};
				let missing: Vec<String> = restorable
					.iter()
					.map(|e| entity_key_string(&e.kind, e.scope.as_deref(), &e.name))
					.filter(|key| !captured.contains_key(key))
					.collect();
				// Say this now, not at rollback time. A modified entity is one the
				// snapshot says already existed, so finding it absent in the live
				// database means real drift, and that rollback cannot restore it.
				if !missing.is_empty() {
					log::warn!(
						"no live definition found for {}, so `rollout rollback` will not be \
						 able to restore {}. The catalog snapshot says they already existed, \
						 so this database has drifted from it -- check that it is the one you \
						 meant, and that the schema was ever applied here.",
						missing.join(", "),
						if missing.len() == 1 {
							"it"
						} else {
							"them"
						}
					);
				}
				if !restorable.is_empty() {
					log::info!(
						"captured {} definition(s) for rollback of {} modified entit{}",
						captured.len(),
						restorable.len(),
						if restorable.len() == 1 {
							"y"
						} else {
							"ies"
						}
					);
				}
				create_rollout_record(
					db,
					rollout,
					&source_catalog.entities,
					&target_catalog.entities,
					RolloutStatus::Planned,
					&captured,
				)
				.await?;
			}
		}

		set_rollout_status(db, &rollout.spec.id, RolloutStatus::RunningStart, None, None).await?;
		if let Err(err) = execute_phase(db, rollout, RolloutPhase::Start, ctx).await {
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
		log::info!("Rollout {} is ready to complete.", rollout.spec.id);
		Ok(())
	}
	.await;
	let release = release_lock(db, &lock).await;
	match (result, release) {
		(Err(err), _) => Err(err),
		(Ok(_), Err(err)) => Err(err),
		(Ok(value), Ok(())) => Ok(value),
	}
}

#[doc(hidden)]
pub async fn run_complete(
	db: &Surreal<Any>,
	folder: &str,
	opts: RolloutExecutionOpts,
	vars: &TemplateVars,
) -> Result<()> {
	run_setup(db, folder).await?;
	let mut rollout = load_rollout_spec(resolve_rollout_path(folder, opts.selector.as_deref())?)?;
	validate_rollout_spec(&rollout.spec)?;
	// Same rewrite as `start`: a manifest planned elsewhere records foreign paths,
	// and `complete` / `rollback` execute steps too.
	canonicalise_manifest_paths(&mut rollout.spec, &collect_schema_files(folder)?);
	let ctx = StepContext::new(vars, Some(folder), opts.query_timeout);
	complete_inner(db, &rollout, &ctx).await
}

/// Runs the complete phase of a rollout defined entirely in code.
///
/// The `spec` must be identical to the one passed to [`run_start_with_spec`].
/// `vars` is applied to step SQL before execution; pass `&TemplateVars::default()`
/// if no substitution is needed.
pub(crate) async fn run_complete_with_spec(
	db: &Surreal<Any>,
	folder: Option<&str>,
	spec: &RolloutSpec,
	vars: &TemplateVars,
) -> Result<()> {
	// `Some` selects the CLI's filesystem workflow, which scaffolds
	// `<folder>/setup.surql` when missing. Code-driven rollouts pass `None` so
	// nothing is written into the caller's working directory.
	match folder {
		Some(folder) => run_setup(db, folder).await?,
		None => crate::setup::run_setup_embedded(db).await?,
	}
	validate_rollout_spec(spec)?;
	let ctx = StepContext::new(vars, folder, None);
	complete_inner(db, &make_loaded_spec(spec), &ctx).await
}

async fn complete_inner(
	db: &Surreal<Any>,
	rollout: &LoadedRolloutSpec,
	ctx: &StepContext<'_>,
) -> Result<()> {
	let lock = acquire_lock(db, &rollout.spec.module()?, "global").await?;
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
		if let Err(err) = execute_phase(db, rollout, RolloutPhase::Complete, ctx).await {
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
		replace_managed_entities(db, &rollout.spec.module()?, &target_entities, None, "active")
			.await?;
		set_rollout_status(
			db,
			&rollout.spec.id,
			RolloutStatus::Completed,
			None,
			Some(OffsetDateTime::now_utc().format(&Rfc3339)?),
		)
		.await?;
		log::info!("Completed rollout {}.", rollout.spec.id);
		Ok(())
	}
	.await;
	let release = release_lock(db, &lock).await;
	match (result, release) {
		(Err(err), _) => Err(err),
		(Ok(_), Err(err)) => Err(err),
		(Ok(value), Ok(())) => Ok(value),
	}
}

#[doc(hidden)]
pub async fn run_rollback(
	db: &Surreal<Any>,
	folder: &str,
	opts: RolloutExecutionOpts,
	vars: &TemplateVars,
) -> Result<()> {
	run_setup(db, folder).await?;
	let mut rollout = load_rollout_spec(resolve_rollout_path(folder, opts.selector.as_deref())?)?;
	validate_rollout_spec(&rollout.spec)?;
	// Same rewrite as `start`: a manifest planned elsewhere records foreign paths,
	// and `complete` / `rollback` execute steps too.
	canonicalise_manifest_paths(&mut rollout.spec, &collect_schema_files(folder)?);
	let ctx = StepContext::new(vars, Some(folder), opts.query_timeout);
	rollback_inner(db, &rollout, &ctx).await
}

/// Runs the rollback phase of a rollout defined entirely in code.
///
/// The `spec` must be identical to the one passed to [`run_start_with_spec`].
/// `vars` is applied to step SQL before execution; pass `&TemplateVars::default()`
/// if no substitution is needed.
pub(crate) async fn run_rollback_with_spec(
	db: &Surreal<Any>,
	folder: Option<&str>,
	spec: &RolloutSpec,
	vars: &TemplateVars,
) -> Result<()> {
	// `Some` selects the CLI's filesystem workflow, which scaffolds
	// `<folder>/setup.surql` when missing. Code-driven rollouts pass `None` so
	// nothing is written into the caller's working directory.
	match folder {
		Some(folder) => run_setup(db, folder).await?,
		None => crate::setup::run_setup_embedded(db).await?,
	}
	validate_rollout_spec(spec)?;
	let ctx = StepContext::new(vars, folder, None);
	rollback_inner(db, &make_loaded_spec(spec), &ctx).await
}

async fn rollback_inner(
	db: &Surreal<Any>,
	rollout: &LoadedRolloutSpec,
	ctx: &StepContext<'_>,
) -> Result<()> {
	let lock = acquire_lock(db, &rollout.spec.module()?, "global").await?;
	let result = async {
		let row = load_rollout_record(db, &rollout.spec.id)
			.await?
			.ok_or_else(|| anyhow!("rollout '{}' has not been started", rollout.spec.id))?;
		verify_rollout_record_matches(&row, rollout)?;
		match string_field(&row, "status").as_deref() {
			Some("completed") => bail!("rollout '{}' is already completed", rollout.spec.id),
			Some("rolled_back") => {
				log::info!("Rollout {} is already rolled back.", rollout.spec.id);
				return Ok(());
			}
			_ => {}
		}
		set_rollout_status(db, &rollout.spec.id, RolloutStatus::RunningRollback, None, None)
			.await?;
		if let Err(err) = execute_phase(db, rollout, RolloutPhase::Rollback, ctx).await {
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
		replace_managed_entities(db, &rollout.spec.module()?, &source_entities, None, "active")
			.await?;
		set_rollout_status(
			db,
			&rollout.spec.id,
			RolloutStatus::RolledBack,
			None,
			Some(OffsetDateTime::now_utc().format(&Rfc3339)?),
		)
		.await?;
		log::info!("Rolled back rollout {}.", rollout.spec.id);
		Ok(())
	}
	.await;
	let release = release_lock(db, &lock).await;
	match (result, release) {
		(Err(err), _) => Err(err),
		(Ok(_), Err(err)) => Err(err),
		(Ok(value), Ok(())) => Ok(value),
	}
}

/// Heal a rollout left in an intermediate state without re-running SQL steps.
#[doc(hidden)]
pub async fn run_repair(db: &Surreal<Any>, folder: &str, opts: RolloutExecutionOpts) -> Result<()> {
	run_setup(db, folder).await?;
	let rollout = load_rollout_spec(resolve_rollout_path(folder, opts.selector.as_deref())?)?;
	validate_rollout_spec(&rollout.spec)?;
	repair_inner(db, &rollout).await
}

async fn repair_inner(db: &Surreal<Any>, rollout: &LoadedRolloutSpec) -> Result<()> {
	let lock = acquire_lock(db, &rollout.spec.module()?, "global").await?;
	let result = async {
		let row = load_rollout_record(db, &rollout.spec.id)
			.await?
			.ok_or_else(|| anyhow!("rollout '{}' has no __rollout record", rollout.spec.id))?;
		verify_rollout_record_matches(&row, rollout)?;
		let status = string_field(&row, "status").unwrap_or_default();
		match status.as_str() {
			"running_complete" => {
				let target_entities = deserialize_entities_field(&row, "target_entities")?;
				replace_managed_entities(db, &rollout.spec.module()?, &target_entities, None, "active")
					.await?;
				set_rollout_status(
					db,
					&rollout.spec.id,
					RolloutStatus::Completed,
					None,
					Some(OffsetDateTime::now_utc().format(&Rfc3339)?),
				)
				.await?;
				log::info!(
					"Repaired rollout {}: running_complete → completed.",
					rollout.spec.id
				);
			}
			"running_rollback" => {
				let source_entities = deserialize_entities_field(&row, "source_entities")?;
				replace_managed_entities(db, &rollout.spec.module()?, &source_entities, None, "active")
					.await?;
				set_rollout_status(
					db,
					&rollout.spec.id,
					RolloutStatus::RolledBack,
					None,
					Some(OffsetDateTime::now_utc().format(&Rfc3339)?),
				)
				.await?;
				log::info!(
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
				log::info!(
					"Repaired rollout {}: running_start → failed (re-run start or rollback).",
					rollout.spec.id
				);
			}
			"completed" | "rolled_back" => {
				log::info!("Rollout {} is already in a terminal state ({}); nothing to repair.", rollout.spec.id, status);
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
	let release = release_lock(db, &lock).await;
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
#[doc(hidden)]
pub async fn run_abandon_rollout(
	db: &Surreal<Any>,
	module: &Module,
	rollout_id: &str,
) -> Result<()> {
	let lock = acquire_lock(db, module, "global").await?;
	let result = async {
		let row = load_rollout_record(db, rollout_id)
			.await?
			.ok_or_else(|| anyhow!("rollout '{}' has no __rollout record", rollout_id))?;
		match string_field(&row, "status").as_deref() {
			Some("completed") => {
				bail!("rollout '{}' is already completed; nothing to abandon", rollout_id)
			}
			Some("rolled_back") => {
				log::info!("Rollout {} is already rolled back.", rollout_id);
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
		log::info!("Abandoned rollout {} (forced → rolled_back).", rollout_id);
		Ok(())
	}
	.await;
	let release = release_lock(db, &lock).await;
	match (result, release) {
		(Err(err), _) => Err(err),
		(Ok(_), Err(err)) => Err(err),
		(Ok(value), Ok(())) => Ok(value),
	}
}

pub(crate) async fn load_active_rollout_id(db: &Surreal<Any>) -> Result<Option<String>> {
	let mut resp = db
		.query(
			"SELECT record::id(id) AS rollout_id, started_at FROM __rollout \
			 WHERE status INSIDE ['planned', 'running_start', 'ready_to_complete', 'running_complete', 'running_rollback', 'failed'] \
			 ORDER BY started_at DESC LIMIT 1;",
		)
		.await?;
	let raw: Option<surrealdb_types::Value> = resp.take(0)?;
	let row = raw.map(|v| Value::from_value(v).unwrap_or(Value::Null));
	Ok(row.and_then(|value| string_field(&value, "rollout_id")))
}

pub(crate) async fn load_managed_entities(
	db: &Surreal<Any>,
	module: &Module,
	folder: Option<&str>,
) -> Result<Vec<ManagedEntityRecord>> {
	let mut resp = db
		.query("SELECT key, val FROM __entity WHERE ns = $ns;")
		.bind(("ns", module.partition(Partition::Schema)))
		.await?;
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

		// Written by an older release, `source_path` may still be
		// working-directory-relative. Normalise so it compares against the
		// folder-relative paths the catalog now produces.
		let raw_source_path = val.get("source_path").and_then(|v| v.as_str()).unwrap_or_default();
		let source_path = match folder {
			Some(folder) => strip_folder_prefix(folder, raw_source_path),
			None => raw_source_path.to_string(),
		};
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

pub(crate) async fn upsert_managed_entities(
	db: &Surreal<Any>,
	module: &Module,
	entities: &[CatalogEntity],
	active_rollout_id: Option<&str>,
	state: &str,
) -> Result<()> {
	if entities.is_empty() {
		return Ok(());
	}
	db.query(
		"FOR $e IN $entities { \
		 	DELETE __entity WHERE ns = $ns AND key = $e.key; \
		 	CREATE __entity CONTENT { \
		 		ns: $ns, \
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
	.bind(("ns", module.partition(Partition::Schema)))
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

pub(crate) async fn delete_managed_entities(
	db: &Surreal<Any>,
	module: &Module,
	entities: &[EntityKey],
) -> Result<()> {
	if entities.is_empty() {
		return Ok(());
	}
	db.query("DELETE __entity WHERE ns = $ns AND key INSIDE $keys;")
		.bind(("ns", module.partition(Partition::Schema)))
		.bind(("keys", entity_keys_payload(entities)))
		.await?
		.check()?;
	Ok(())
}

pub(crate) async fn replace_managed_entities(
	db: &Surreal<Any>,
	module: &Module,
	entities: &[CatalogEntity],
	active_rollout_id: Option<&str>,
	state: &str,
) -> Result<()> {
	// Scoped to `$ns`: unscoped, this wiped every module's catalog, not just
	// the one being rolled out.
	db.query(
		"DELETE __entity WHERE ns = $ns; \
		 FOR $e IN $entities { \
		 	CREATE __entity CONTENT { \
		 		ns: $ns, \
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
	.bind(("ns", module.partition(Partition::Schema)))
	.bind(("entities", entities_payload(entities)))
	.bind(("active_rollout_id", active_rollout_id.map(str::to_string)))
	.bind(("state", state.to_string()))
	.await?
	.check()?;
	Ok(())
}

pub(crate) async fn replace_sync_hashes(
	db: &Surreal<Any>,
	module: &Module,
	files: &[SchemaFile],
) -> Result<()> {
	let ns = module.partition(Partition::Sync);
	// Scoped to `$ns`: unscoped, this wiped every module's file hashes.
	db.query("DELETE __entity WHERE ns = $ns;").bind(("ns", ns.clone())).await?.check()?;
	for file in files {
		db.query(
			"CREATE __entity CONTENT { ns: $ns, key: $path, val: { hash: $hash }, updated_at: time::now() };",
		)
		.bind(("ns", ns.clone()))
		.bind(("path", file.path.clone()))
		.bind(("hash", file.hash.clone()))
		.await?
		.check()?;
	}
	Ok(())
}

pub(crate) async fn delete_sync_hashes(
	db: &Surreal<Any>,
	module: &Module,
	paths: &[String],
) -> Result<()> {
	for path in paths {
		db.query("DELETE __entity WHERE ns = $ns AND key = $path;")
			.bind(("ns", module.partition(Partition::Sync)))
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

	// A modified entity cannot be undone by removing it -- it existed before. The
	// rollback re-applies whatever definition `start` captured from the live
	// database.
	let modified_entities: Vec<EntityKey> =
		catalog_diff.modified.iter().map(|change| change.new.key()).collect();
	if !modified_entities.is_empty() {
		steps.push(RolloutStep::restore_definitions(
			"rollback_modified_schema",
			RolloutPhase::Rollback,
			modified_entities,
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
		module: default_module_name(),
		name: name.to_string(),
		source_schema_hash: hash_schema_snapshot(old_schema)?,
		target_schema_hash: hash_schema_snapshot(new_schema)?,
		compatibility: RolloutCompatibility::Phased,
		renames: Vec::new(),
		steps,
	})
}

/// A human-readable, scope-qualified name for a catalog entity.
///
/// The refusal used to print `field:email`, which is ambiguous the moment two
/// tables have an `email` field. Qualify it and name the file it came from.
fn describe_entity(entity: &CatalogEntity) -> String {
	let name = match &entity.scope {
		Some(scope) => format!("{}:{}.{}", entity.kind, scope, entity.name),
		None => format!("{}:{}", entity.kind, entity.name),
	};
	if entity.source_path.is_empty() {
		name
	} else {
		format!("{name} ({})", entity.source_path)
	}
}

fn validate_autoplan(diff: &CatalogDiff, allow_modified: bool) -> Result<()> {
	if !diff.modified.is_empty() && !allow_modified {
		let names = diff
			.modified
			.iter()
			.map(|change| describe_entity(&change.new))
			.collect::<Vec<_>>()
			.join("\n  - ");
		bail!(
			"rollout plan found {} modified entit{} and will not plan {} automatically \
			 without --allow-modified:\n  - {}\n\n\
			 Applying the change is safe (it re-applies the file with DEFINE ... OVERWRITE). \
			 The reason for the opt-in is rollback: undoing a modification means restoring \
			 the previous definition, which reverses the schema but not any data effect it \
			 had. That is a clean undo for ASSERT, PERMISSIONS and COMMENT changes, and only \
			 a partial one for TYPE, VALUE or DEFAULT changes.\n\n\
			 Re-run with --allow-modified once you are satisfied that is the right undo, or \
			 author a manual manifest.",
			diff.modified.len(),
			if diff.modified.len() == 1 {
				"y"
			} else {
				"ies"
			},
			if diff.modified.len() == 1 {
				"it"
			} else {
				"them"
			},
			names
		);
	}

	// An add and a remove of the same kind in the same scope cannot be told apart
	// from a rename without heuristics, and a misread rename plans as a drop of the
	// original, so this stays refused.
	//
	// It is deliberately NOT gated on --allow-modified. That flag is about changing
	// an entity that stays in place; this is about one that may be disappearing.
	// Tables, functions, params, analyzers and users all carry `scope: None`, so
	// treating an unnamed scope as "not a collision" would switch the guard off for
	// exactly the entity kinds whose removal destroys data.
	let removed_by_scope: BTreeSet<(EntityKind, Option<String>)> =
		diff.removed.iter().map(|entity| (entity.kind.clone(), entity.scope.clone())).collect();
	let added_by_scope: BTreeSet<(EntityKind, Option<String>)> =
		diff.added.iter().map(|entity| (entity.kind.clone(), entity.scope.clone())).collect();

	let colliding: Vec<&(EntityKind, Option<String>)> =
		removed_by_scope.intersection(&added_by_scope).collect();

	if !colliding.is_empty() {
		let detail = colliding
			.iter()
			.map(|(kind, scope)| {
				let scope_name = scope.as_deref().unwrap_or("<database>");
				let added: Vec<&str> = diff
					.added
					.iter()
					.filter(|e| &e.kind == kind && &e.scope == scope)
					.map(|e| e.name.as_str())
					.collect();
				let removed: Vec<&str> = diff
					.removed
					.iter()
					.filter(|e| &e.kind == kind && &e.scope == scope)
					.map(|e| e.name.as_str())
					.collect();
				format!(
					"{kind} on {scope_name}: added [{}], removed [{}]",
					added.join(", "),
					removed.join(", ")
				)
			})
			.collect::<Vec<_>>()
			.join("\n  - ");
		bail!(
			"rollout plan sees additions and removals of the same kind in one scope, which it \
			 cannot tell apart from a rename:\n  - {detail}\n\n\
			 If that is a rename, planning it automatically would drop the original and its \
			 data rather than move it, so author a manual manifest with an explicit backfill \
			 between the add and the remove. If the changes are genuinely unrelated, splitting \
			 them across two rollouts is the safest way to say so."
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
			RolloutAction::RestoreDefinitions {
				entities,
			} => {
				if entities.is_empty() {
					bail!("restore_definitions step '{}' requires entities", step.id);
				}
			}
		}
	}
	Ok(())
}

/// How often to report that a long step is still running.
///
/// Without this a step that legitimately takes minutes -- an index build over a
/// large table -- is indistinguishable from the unbounded hang that issue #55
/// reported, because nothing is printed between steps.
const STEP_HEARTBEAT: Duration = Duration::from_secs(15);

async fn execute_phase(
	db: &Surreal<Any>,
	rollout: &LoadedRolloutSpec,
	phase: RolloutPhase,
	ctx: &StepContext<'_>,
) -> Result<()> {
	let planned: Vec<&RolloutStep> =
		rollout.spec.steps.iter().filter(|step| step.phase == phase).collect();
	let total = planned.len();

	for (index, step) in planned.into_iter().enumerate() {
		if step_already_completed(db, &rollout.spec.id, &step.id).await? {
			log::info!(
				"step {}/{} '{}' ({}) already completed; skipping",
				index + 1,
				total,
				step.id,
				step.action.kind_str()
			);
			continue;
		}

		log::info!(
			"step {}/{} '{}' ({}, phase {}) starting",
			index + 1,
			total,
			step.id,
			step.action.kind_str(),
			phase.as_str()
		);
		record_step_start(db, &rollout.spec.id, step).await?;

		let started = Instant::now();
		let result = with_heartbeat(&step.id, execute_step(db, &rollout.spec.id, step, ctx)).await;
		match result {
			Ok(()) => {
				log::info!(
					"step {}/{} '{}' completed in {}ms",
					index + 1,
					total,
					step.id,
					started.elapsed().as_millis()
				);
				record_step_complete(db, &rollout.spec.id, step).await?
			}
			Err(err) => {
				record_step_failure(db, &rollout.spec.id, step, &format!("{err:#}")).await?;
				return Err(err);
			}
		}
	}
	Ok(())
}

/// Run `future`, logging every [`STEP_HEARTBEAT`] that it is still going.
async fn with_heartbeat<T>(step_id: &str, future: impl Future<Output = T>) -> T {
	let started = Instant::now();
	tokio::pin!(future);
	let mut ticker = tokio::time::interval(STEP_HEARTBEAT);
	ticker.tick().await; // the first tick completes immediately
	loop {
		tokio::select! {
			out = &mut future => return out,
			_ = ticker.tick() => log::info!(
				"  step '{}' still running ({}s)",
				step_id,
				started.elapsed().as_secs()
			),
		}
	}
}

/// Resolve an `apply_files` step's path against the project folder.
///
/// Paths are canonicalised when the manifest is loaded, so this is a plain join.
/// It deliberately does not search: a path that did not canonicalise is one this
/// project has no file for, and guessing at it risks applying a different file's
/// DDL under the name of the one that is missing.
fn resolve_step_file(folder: Option<&str>, recorded: &str) -> Result<PathBuf> {
	if let Some(folder) = folder {
		let joined = Path::new(folder).join(recorded);
		if joined.is_file() {
			return Ok(joined);
		}
		bail!(
			"apply_files step references {recorded:?}, which was not found at {}. If this \
			 manifest was planned against a different schema, re-run `surrealkit rollout plan`.",
			joined.display()
		);
	}

	let as_recorded = PathBuf::from(recorded);
	if as_recorded.is_file() {
		return Ok(as_recorded);
	}
	bail!(
		"apply_files step references {recorded:?}, which was not found. Code-driven rollouts \
		 read no files; use `RolloutAction::ApplySchema` with inline SQL instead."
	)
}

/// Read the live `DEFINE` text for `entities` straight from the database.
///
/// A catalog snapshot stores only a `statement_hash`, so the previous definition
/// of a modified entity exists nowhere on disk. `INFO FOR DB` and
/// `INFO FOR TABLE` return `name -> "DEFINE ..."` maps verbatim, which makes the
/// database itself the source of truth for what a rollback should restore.
pub(crate) async fn capture_definitions(
	db: &Surreal<Any>,
	entities: &[EntityKey],
) -> Result<BTreeMap<String, String>> {
	if entities.is_empty() {
		return Ok(BTreeMap::new());
	}

	let db_info = info_json(db, "INFO FOR DB;").await?;
	// One `INFO FOR TABLE` per distinct scope, not per entity.
	let scopes: BTreeSet<String> = entities.iter().filter_map(|e| e.scope.clone()).collect();
	let mut table_info = BTreeMap::new();
	for scope in scopes {
		let info = info_json(db, &format!("INFO FOR TABLE `{scope}`;")).await?;
		table_info.insert(scope, info);
	}

	let mut out = BTreeMap::new();
	for entity in entities {
		let section = match entity.kind {
			EntityKind::Table => Some("tables"),
			EntityKind::Function => Some("functions"),
			EntityKind::Param => Some("params"),
			EntityKind::Analyzer => Some("analyzers"),
			EntityKind::Access => Some("accesses"),
			EntityKind::User => Some("users"),
			EntityKind::Field => Some("fields"),
			EntityKind::Index => Some("indexes"),
			EntityKind::Event => Some("events"),
			_ => None,
		};
		let Some(section) = section else {
			continue;
		};
		let source = match &entity.scope {
			Some(scope) => table_info.get(scope),
			None => Some(&db_info),
		};
		let definition = source
			.and_then(|info| info.get(section))
			.and_then(|m| m.get(&entity.name))
			.and_then(|v| v.as_str());
		if let Some(definition) = definition {
			out.insert(
				entity_key_string(&entity.kind, entity.scope.as_deref(), &entity.name),
				definition.to_string(),
			);
		}
	}
	Ok(out)
}

async fn info_json(db: &Surreal<Any>, sql: &str) -> Result<Value> {
	let mut response = db.query(sql).await?.check().with_context(|| sql.to_string())?;
	let raw: surrealdb_types::Value = response.take(0)?;
	Ok(Value::from_value(raw).unwrap_or(Value::Null))
}

/// Read back the definitions `start` captured for this rollout.
async fn load_restore_definitions(
	db: &Surreal<Any>,
	rollout_id: &str,
) -> Result<BTreeMap<String, String>> {
	let mut resp = db
		.query("SELECT restore_definitions FROM __rollout WHERE record::id(id) = $id LIMIT 1;")
		.bind(("id", rollout_id.to_string()))
		.await?;
	let raw: Option<surrealdb_types::Value> = resp.take(0)?;
	let Some(row) = raw.map(|v| Value::from_value(v).unwrap_or(Value::Null)) else {
		return Ok(BTreeMap::new());
	};
	let Some(map) = row.get("restore_definitions").and_then(|v| v.as_object()) else {
		return Ok(BTreeMap::new());
	};
	Ok(map.iter().filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string()))).collect())
}

async fn execute_step(
	db: &Surreal<Any>,
	rollout_id: &str,
	step: &RolloutStep,
	ctx: &StepContext<'_>,
) -> Result<()> {
	let vars = ctx.vars;
	let run = async |sql: String| -> Result<()> {
		match ctx.query_timeout {
			None => exec_surql(db, &sql).await,
			Some(budget) => match tokio::time::timeout(budget, exec_surql(db, &sql)).await {
				Ok(result) => result,
				Err(_) => bail!(
					"step '{}' exceeded the {}s query budget. Raise it with \
					 --query-timeout-secs / SURREALDB_QUERY_TIMEOUT_SECS, or pass 0 to \
					 wait indefinitely.",
					step.id,
					budget.as_secs()
				),
			},
		}
	};

	match &step.action {
		RolloutAction::ApplySchema {
			sql,
		} => {
			let substituted = vars.apply(sql)?;
			run(ensure_overwrite(&substituted)).await
		}
		RolloutAction::ApplyFiles {
			files,
		} => {
			for file in files {
				let path = resolve_step_file(ctx.folder, file)?;
				let raw = fs::read_to_string(&path)
					.with_context(|| format!("reading {}", path.display()))?;
				let substituted = vars.apply(&raw).with_context(|| {
					format!("applying template variables in {}", path.display())
				})?;
				run(ensure_overwrite(&substituted)).await?;
			}
			Ok(())
		}
		RolloutAction::RunSql {
			sql,
		} => {
			let substituted = vars.apply(sql)?;
			run(substituted).await
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
			run(sql).await
		}
		RolloutAction::RestoreDefinitions {
			entities,
		} => {
			let captured = load_restore_definitions(db, rollout_id).await?;
			let mut statements = Vec::new();
			let mut missing = Vec::new();
			for entity in entities {
				let key = entity_key_string(&entity.kind, entity.scope.as_deref(), &entity.name);
				match captured.get(&key) {
					Some(definition) => statements.push(ensure_overwrite(definition)),
					None => missing.push(key),
				}
			}
			if !missing.is_empty() {
				bail!(
					"step '{}' has no captured definition for {}. `start` warns when an \
					 entity has no live definition to capture; there is nothing to restore \
					 for these. Recreate them by hand, or re-apply your schema files with \
					 `surrealkit sync`.",
					step.id,
					missing.join(", ")
				);
			}
			if statements.is_empty() {
				return Ok(());
			}
			run(statements.join("\n")).await
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
	restore_definitions: &BTreeMap<String, String>,
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
		 	reversibility: $reversibility, \
		 	restore_definitions: $restore_definitions, \
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
	.bind(("reversibility", reversibility(&rollout.spec).to_string()))
	.bind(("restore_definitions", serde_json::to_value(restore_definitions)?))
	.await?
	.check()?;
	Ok(())
}

/// How completely a rollback can undo this rollout.
///
/// `full` means every step is undone by removing what it added. `definition_only`
/// means at least one step modified an entity that already existed, so rollback
/// restores the previous *definition* but cannot undo a data effect it had --
/// clean for ASSERT/PERMISSIONS/COMMENT, partial for TYPE/VALUE/DEFAULT.
fn reversibility(spec: &RolloutSpec) -> &'static str {
	let restores = spec
		.steps
		.iter()
		.any(|step| matches!(step.action, RolloutAction::RestoreDefinitions { .. }));
	if restores {
		"definition_only"
	} else {
		"full"
	}
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

/// Read just the step log, rather than the whole rollout record.
///
/// `SELECT *` also returns `source_entities` and `target_entities`, which are
/// O(managed catalog). Step bookkeeping reads and writes the record three times
/// per step, so on a large schema over a high-latency link that payload dominates
/// the rollout and makes a working run look like the hang reported in issue #55.
async fn load_rollout_steps(db: &Surreal<Any>, rollout_id: &str) -> Result<Option<Vec<Value>>> {
	let mut resp = db
		.query("SELECT steps FROM __rollout WHERE record::id(id) = $id LIMIT 1;")
		.bind(("id", rollout_id.to_string()))
		.await?;
	let raw: Option<surrealdb_types::Value> = resp.take(0)?;
	let Some(row) = raw.map(|v| Value::from_value(v).unwrap_or(Value::Null)) else {
		return Ok(None);
	};
	Ok(Some(row.get("steps").and_then(|v| v.as_array()).cloned().unwrap_or_default()))
}

async fn step_already_completed(
	db: &Surreal<Any>,
	rollout_id: &str,
	step_id: &str,
) -> Result<bool> {
	let Some(steps) = load_rollout_steps(db, rollout_id).await? else {
		return Ok(false);
	};
	Ok(steps.iter().any(|s| {
		s.get("step_id").and_then(|v| v.as_str()) == Some(step_id)
			&& s.get("status").and_then(|v| v.as_str()) == Some("completed")
	}))
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
	let mut steps = load_rollout_steps(db, rollout_id)
		.await?
		.ok_or_else(|| anyhow!("rollout '{}' not found", rollout_id))?;
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
	let mut steps = load_rollout_steps(db, rollout_id)
		.await?
		.ok_or_else(|| anyhow!("rollout '{}' not found", rollout_id))?;
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

/// How long an acquired lock is honoured before another process may take it over.
///
/// A lock is only released explicitly, so without an expiry a crashed run would
/// wedge the project permanently. 15 minutes is comfortably longer than any real
/// sync or rollout phase.
const LOCK_TTL_SECS: u64 = 900;

/// Proof that this process holds a lock. Required to release it, so one process
/// cannot release another's lock.
#[derive(Debug, Clone)]
pub(crate) struct LockToken {
	/// The module-qualified `__entity.ns` partition this lock lives in.
	ns: String,
	key: String,
	owner: String,
}

/// Identifies the holder in contention messages. The pid distinguishes concurrent
/// runs that share a `SURREALKIT_OWNER` (e.g. two CI jobs).
fn lock_owner_id() -> String {
	let base = std::env::var("SURREALKIT_OWNER").unwrap_or_else(|_| "surrealkit".to_string());
	format!("{base}/{}", std::process::id())
}

/// Take the named lock, or fail if another process holds it.
///
/// This is a real mutual exclusion: the `by_ns_key` unique index rejects a second
/// holder. Expired locks -- and locks written before v1, which carry no
/// `expires_at` -- are taken over in the same statement, so a crashed run does not
/// need a manual cleanup.
pub(crate) async fn acquire_lock(
	db: &Surreal<Any>,
	module: &Module,
	lock_key: &str,
) -> Result<LockToken> {
	let owner = lock_owner_id();
	let ns = module.partition(Partition::Lock);

	// The conditional DELETE clears only an expired (or pre-v1) holder; the CREATE
	// then violates `by_ns_key` if a live holder remains, aborting the transaction
	// and leaving that holder untouched.
	let sql = format!(
		"BEGIN; \
		 DELETE __entity WHERE ns = $ns AND key = $key \
		 	AND (val.expires_at = NONE OR val.expires_at < time::now()); \
		 CREATE __entity CONTENT {{ \
		 	ns: $ns, \
		 	key: $key, \
		 	val: {{ owner: $owner, acquired_at: time::now(), expires_at: time::now() + {LOCK_TTL_SECS}s }}, \
		 	updated_at: time::now() \
		 }}; \
		 COMMIT;"
	);

	let attempt = match db
		.query(sql)
		.bind(("ns", ns.clone()))
		.bind(("key", lock_key.to_string()))
		.bind(("owner", owner.clone()))
		.await
	{
		Ok(resp) => resp.check(),
		Err(err) => Err(err),
	};

	match attempt {
		Ok(_) => Ok(LockToken {
			ns,
			key: lock_key.to_string(),
			owner,
		}),
		Err(err) => {
			// The transaction error is generic, so read the holder back to say who.
			match describe_lock_holder(db, module, lock_key).await {
				Ok(Some(holder)) => bail!(
					"another surrealkit run holds the '{lock_key}' lock ({holder}).\n\
					 Wait for it to finish, or if it crashed, clear the lock with:\n\
					 \x20   DELETE __entity WHERE ns = 'lock' AND key = '{lock_key}';\n\
					 It is taken over automatically {LOCK_TTL_SECS}s after it was acquired."
				),
				// No holder: the failure was something else (or we lost a benign race).
				_ => Err(err).with_context(|| format!("acquiring the '{lock_key}' lock")),
			}
		}
	}
}

/// Render the current holder of `lock_key` as `owner, held for Ns`, if any.
async fn describe_lock_holder(
	db: &Surreal<Any>,
	module: &Module,
	lock_key: &str,
) -> Result<Option<String>> {
	let mut resp = db
		.query(
			"SELECT val.owner AS owner, val.acquired_at AS acquired_at \
			 FROM __entity WHERE ns = $ns AND key = $key LIMIT 1;",
		)
		.bind(("ns", module.partition(Partition::Lock)))
		.bind(("key", lock_key.to_string()))
		.await?;
	let row: Option<serde_json::Value> = resp.take(0)?;
	Ok(row.map(|r| {
		let owner = r.get("owner").and_then(|v| v.as_str()).unwrap_or("unknown owner");
		match r.get("acquired_at").and_then(|v| v.as_str()) {
			Some(at) => format!("held by {owner} since {at}"),
			None => format!("held by {owner}"),
		}
	}))
}

/// Release a lock this process holds. Releasing someone else's lock is a no-op:
/// the owner check is what stops a slow run from clearing the lock a newer run
/// legitimately took over.
pub(crate) async fn release_lock(db: &Surreal<Any>, token: &LockToken) -> Result<()> {
	db.query("DELETE __entity WHERE ns = $ns AND key = $key AND val.owner = $owner;")
		.bind(("ns", token.ns.clone()))
		.bind(("key", token.key.clone()))
		.bind(("owner", token.owner.clone()))
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

		let err = validate_autoplan(&diff, false).expect_err("should reject modified entities");
		let message = err.to_string();
		assert!(
			message.contains("--allow-modified"),
			"the refusal must name the opt-in: {message}"
		);
		// The old message printed `field:name`, which is ambiguous across tables.
		assert!(
			message.contains("field:person.name"),
			"the refusal must qualify the entity by table: {message}"
		);
		assert!(
			message.contains("database/schema/person.surql"),
			"the refusal must name the source file: {message}"
		);

		// With the opt-in, the same diff plans.
		validate_autoplan(&diff, true).expect("--allow-modified should permit the change");
	}

	/// A table rename reaches the planner as "add people, remove person". Tables
	/// carry `scope: None`, so a guard that only fires on a named scope switches
	/// itself off for exactly the entity kinds whose removal destroys data: the
	/// generated manifest would define an empty `people` and then REMOVE TABLE
	/// `person` with its rows.
	#[test]
	fn plan_refuses_a_table_rename() {
		let entity = |name: &str| CatalogEntity {
			kind: EntityKind::Table,
			scope: None,
			name: name.to_string(),
			source_path: "schema/person.surql".to_string(),
			statement_hash: format!("h-{name}"),
			file_hash: "f".to_string(),
		};
		let diff = CatalogDiff {
			added: vec![entity("people")],
			removed: vec![entity("person")],
			modified: Vec::new(),
		};

		let err = validate_autoplan(&diff, false).expect_err("a table rename must be refused");
		let message = err.to_string();
		assert!(message.contains("person") && message.contains("people"), "got: {message}");

		// --allow-modified is about changing an entity that stays in place. It must
		// not wave through one that may be disappearing.
		assert!(
			validate_autoplan(&diff, true).is_err(),
			"--allow-modified must not bypass the rename guard"
		);
	}

	/// The same shape one scope down, which the guard has always caught.
	#[test]
	fn plan_refuses_a_field_rename() {
		let field = |name: &str| CatalogEntity {
			kind: EntityKind::Field,
			scope: Some("person".to_string()),
			name: name.to_string(),
			source_path: "schema/person.surql".to_string(),
			statement_hash: format!("h-{name}"),
			file_hash: "f".to_string(),
		};
		let diff = CatalogDiff {
			added: vec![field("full_name")],
			removed: vec![field("name")],
			modified: Vec::new(),
		};
		assert!(validate_autoplan(&diff, false).is_err(), "a field rename must be refused");
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

	#[tokio::test]
	async fn lock_excludes_a_second_holder() {
		// Before v1 `acquire_lock` did `DELETE` then `CREATE`, so it always
		// "succeeded" and provided no mutual exclusion at all.
		let db = connect_mem_db().await;

		let first =
			acquire_lock(&db, &Module::default_module(), "global").await.expect("first acquire");
		let err = acquire_lock(&db, &Module::default_module(), "global")
			.await
			.expect_err("second acquire must fail");
		let msg = format!("{err:#}");
		assert!(msg.contains("holds the 'global' lock"), "unexpected error: {msg}");

		// The original holder is intact.
		let holder = describe_lock_holder(&db, &Module::default_module(), "global")
			.await
			.expect("describe")
			.expect("holder");
		assert!(holder.contains(&lock_owner_id()), "holder changed: {holder}");

		release_lock(&db, &first).await.expect("release");
		let second = acquire_lock(&db, &Module::default_module(), "global")
			.await
			.expect("acquire after release");
		release_lock(&db, &second).await.expect("release second");
	}

	#[tokio::test]
	async fn releasing_someone_elses_lock_is_a_no_op() {
		let db = connect_mem_db().await;
		let real = acquire_lock(&db, &Module::default_module(), "global").await.expect("acquire");

		let forged = LockToken {
			ns: Module::default_module().partition(Partition::Lock),
			key: "global".to_string(),
			owner: "someone-else/1".to_string(),
		};
		release_lock(&db, &forged).await.expect("release call itself succeeds");

		// The genuine holder still holds it.
		assert!(
			describe_lock_holder(&db, &Module::default_module(), "global")
				.await
				.expect("describe")
				.is_some(),
			"forged release must not have cleared the lock"
		);
		assert!(
			acquire_lock(&db, &Module::default_module(), "global").await.is_err(),
			"lock must still be held"
		);

		release_lock(&db, &real).await.expect("real release");
	}

	#[tokio::test]
	async fn expired_and_pre_v1_locks_are_taken_over() {
		// Pre-v1 lock rows carry no `expires_at`. They must not wedge the project
		// forever, so they are treated as expired.
		let db = connect_mem_db().await;
		db.query(
			"CREATE __entity CONTENT { ns:'lock', key:'global', \
			 val:{ owner:'crashed-0.7-run' }, updated_at: time::now() };",
		)
		.await
		.expect("seed legacy lock")
		.check()
		.expect("seed legacy lock");

		let token = acquire_lock(&db, &Module::default_module(), "global")
			.await
			.expect("must take over a pre-v1 lock");
		let holder = describe_lock_holder(&db, &Module::default_module(), "global")
			.await
			.expect("describe")
			.expect("holder");
		assert!(holder.contains(&lock_owner_id()), "expected takeover, got: {holder}");
		release_lock(&db, &token).await.expect("release");
	}

	#[tokio::test]
	async fn distinct_modules_do_not_contend_for_the_same_lock_key() {
		// Per-module locks are what let `--all` fan out without one module's sync
		// blocking another's.
		let db = connect_mem_db().await;
		let core = Module::new("core").unwrap();
		let billing = Module::new("billing").unwrap();

		let a = acquire_lock(&db, &core, "global").await.expect("core");
		let b = acquire_lock(&db, &billing, "global").await.expect("billing must not contend");

		// But the same module still excludes itself.
		assert!(acquire_lock(&db, &core, "global").await.is_err(), "core must still exclude");

		release_lock(&db, &a).await.expect("release core");
		release_lock(&db, &b).await.expect("release billing");
	}

	#[tokio::test]
	async fn distinct_lock_keys_do_not_contend() {
		let db = connect_mem_db().await;
		let a = acquire_lock(&db, &Module::default_module(), "global").await.expect("a");
		let b = acquire_lock(&db, &Module::default_module(), "other")
			.await
			.expect("distinct key must not contend");
		release_lock(&db, &a).await.expect("release a");
		release_lock(&db, &b).await.expect("release b");
	}

	fn sample_spec(id: &str) -> RolloutSpec {
		sample_loaded_spec(id).spec
	}

	fn sample_loaded_spec(id: &str) -> LoadedRolloutSpec {
		LoadedRolloutSpec {
			path: PathBuf::from(format!("database/rollouts/{id}.toml")),
			checksum: "sum".to_string(),
			spec: RolloutSpec {
				id: id.to_string(),
				module: default_module_name(),
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

		create_rollout_record(&db, &loaded, &[], &[], RolloutStatus::Planned, &BTreeMap::new())
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
		create_rollout_record(
			&db,
			&loaded,
			&[],
			&[],
			RolloutStatus::RunningComplete,
			&BTreeMap::new(),
		)
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
		create_rollout_record(&db, &loaded, &[], &[], RolloutStatus::Planned, &BTreeMap::new())
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
		create_rollout_record(&db, &loaded, &[], &[], RolloutStatus::Planned, &BTreeMap::new())
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

		create_rollout_record(&db, &loaded, &[], &[], RolloutStatus::Planned, &BTreeMap::new())
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

	// Regression: selecting `id` returns the full SurrealDB record id
	// (`__rollout:<id>`), so a retry of the same failed rollout was mistaken for a
	// conflicting rollout. Select the raw record key and verify both same-rollout
	// resumption and different-rollout exclusion.
	#[tokio::test]
	async fn active_rollout_id_allows_same_rollout_resume() {
		let db = connect_mem_db().await;
		let loaded = sample_loaded_spec("20260420101627__active_id_test");

		create_rollout_record(
			&db,
			&loaded,
			&[],
			&[],
			RolloutStatus::RunningStart,
			&BTreeMap::new(),
		)
		.await
		.expect("create rollout record");

		let active =
			load_active_rollout_id(&db).await.expect("load_active_rollout_id must not fail");
		assert_eq!(active.as_deref(), Some(loaded.spec.id.as_str()));
		ensure_no_conflicting_active_rollout(&db, &loaded.spec.id)
			.await
			.expect("the same active rollout must be resumable");
		let err = ensure_no_conflicting_active_rollout(&db, "different_rollout")
			.await
			.expect_err("a different rollout must remain blocked");
		assert!(err.to_string().contains(&loaded.spec.id));
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

		replace_managed_entities(&db, &Module::default_module(), &entities, Some("r-1"), "active")
			.await
			.expect("first batched replace");
		assert_eq!(entity_row_count(&db).await, 25, "all entities land on first call");

		// Re-running should still be idempotent — delete-all + recreate via the
		// FOR loop should produce the same row count, not duplicates.
		replace_managed_entities(&db, &Module::default_module(), &entities, Some("r-1"), "active")
			.await
			.expect("second batched replace");
		assert_eq!(entity_row_count(&db).await, 25, "no duplicates on re-run");

		// Empty replacement clears the schema namespace.
		replace_managed_entities(&db, &Module::default_module(), &[], None, "active")
			.await
			.expect("empty replace");
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

		create_rollout_record(
			&db,
			&loaded,
			&[],
			&target_entities,
			RolloutStatus::RunningComplete,
			&BTreeMap::new(),
		)
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
			&BTreeMap::new(),
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
		create_rollout_record(&db, &loaded, &[], &[], RolloutStatus::Planned, &BTreeMap::new())
			.await
			.expect("seed rollout record");

		let err = repair_inner(&db, &loaded).await.expect_err("planned is not repairable");
		assert!(err.to_string().contains("not in a repairable state"), "got: {err}",);
	}

	/// A manifest planned elsewhere records foreign paths. They have to be mapped
	/// onto this project's canonical keys before anything executes.
	#[test]
	fn manifest_paths_are_canonicalised_against_the_project() {
		let files = vec![
			SchemaFile {
				path: "schema/014-sku.surql".to_string(),
				sql: String::new(),
				hash: "a".to_string(),
			},
			SchemaFile {
				path: "modules/billing/schema/plan.surql".to_string(),
				sql: String::new(),
				hash: "b".to_string(),
			},
		];
		let mut spec = sample_spec("20260911194216__sku_on_hand");
		spec.steps = vec![RolloutStep::apply_files(
			"apply_expand_schema",
			RolloutPhase::Start,
			vec![
				"/database/schema/014-sku.surql".to_string(),
				"/database/modules/billing/schema/plan.surql".to_string(),
			],
		)];

		let prefixes = canonicalise_manifest_paths(&mut spec, &files);
		assert_eq!(prefixes, vec!["/database".to_string()]);

		let RolloutAction::ApplyFiles {
			files: rewritten,
		} = &spec.steps[0].action
		else {
			panic!("expected an apply_files step");
		};
		assert_eq!(
			rewritten,
			&vec![
				"schema/014-sku.surql".to_string(),
				"modules/billing/schema/plan.surql".to_string(),
			],
			"each path must land on its own canonical key"
		);
	}

	/// The decoy the old stripping loop walked into: a module file and a
	/// default-module file sharing a name. Stripping leading segments until
	/// something existed resolved `modules/billing/schema/x.surql` onto
	/// `schema/x.surql` and applied the wrong DDL.
	#[test]
	fn a_module_path_does_not_collapse_onto_the_default_module() {
		let files = vec![
			SchemaFile {
				path: "schema/x.surql".to_string(),
				sql: String::new(),
				hash: "a".to_string(),
			},
			SchemaFile {
				path: "modules/billing/schema/x.surql".to_string(),
				sql: String::new(),
				hash: "b".to_string(),
			},
		];
		let mut spec = sample_spec("20260911194216__module");
		spec.steps = vec![RolloutStep::apply_files(
			"apply_expand_schema",
			RolloutPhase::Start,
			vec!["/database/modules/billing/schema/x.surql".to_string()],
		)];

		canonicalise_manifest_paths(&mut spec, &files);
		let RolloutAction::ApplyFiles {
			files: rewritten,
		} = &spec.steps[0].action
		else {
			panic!("expected an apply_files step");
		};
		assert_eq!(
			rewritten,
			&vec!["modules/billing/schema/x.surql".to_string()],
			"the longest canonical match wins, so the module file stays the module file"
		);
	}

	/// A path this project has no file for is left as recorded, so the step fails
	/// naming what the manifest actually asked for.
	#[test]
	fn an_unmatched_path_is_left_alone() {
		let files = vec![SchemaFile {
			path: "schema/a.surql".to_string(),
			sql: String::new(),
			hash: "a".to_string(),
		}];
		let mut spec = sample_spec("20260911194216__missing");
		spec.steps = vec![RolloutStep::apply_files(
			"apply_expand_schema",
			RolloutPhase::Start,
			vec!["/database/schema/gone.surql".to_string()],
		)];

		let prefixes = canonicalise_manifest_paths(&mut spec, &files);
		assert!(prefixes.is_empty(), "nothing matched, so no prefix was recovered");
		let RolloutAction::ApplyFiles {
			files: rewritten,
		} = &spec.steps[0].action
		else {
			panic!("expected an apply_files step");
		};
		assert_eq!(rewritten, &vec!["/database/schema/gone.surql".to_string()]);
	}
}
