//! The tool catalog.
//!
//! Naming: `<noun>_<verb>`, snake_case, flat. Noun first so `tools/list` sorts
//! into coherent families (`rollout_start`, `rollout_complete`, ...) and a host
//! doing prefix search finds the whole family in one hop.
//!
//! Splitting rather than merging (one `rollout` tool with an `action` enum) is
//! forced by annotations: they are per-tool and static, so a merged rollout tool
//! would have to be simultaneously `read_only_hint: true` (status, lint) and
//! `destructive_hint: true` (rollback). Whichever you picked, the host's
//! confirmation prompt would be wrong for half the actions -- and that prompt is
//! the entire reason to ship annotations.

#[cfg(feature = "analyze")]
pub mod analyze;
pub mod rollout;

use std::collections::BTreeMap;

use anyhow::{Context as _, Result, bail};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::schemars;
use rmcp::{tool, tool_router};
use serde::{Deserialize, Serialize};
use surrealdb::Surreal;
use surrealdb::engine::any::Any;

use super::context::{CallContext, WorkRequest};
use super::result::run_captured;
use super::{SurrealKitMcp, error};
use crate::config::connect;
use crate::paths::safe_join;
use crate::project::Target;
use crate::selection::Selection;

/// Connect to the one target this call operates on.
///
/// Every tool addresses a single database. Fanning a call across targets would
/// turn a partial failure into several databases left in different states, which
/// for a rollout is exactly the wedge `rollout_repair` exists to undo.
pub(super) async fn connect_single(selection: &Selection) -> Result<(Surreal<Any>, Target)> {
	let target = selection.single_target()?.clone();
	let db = connect(target.cfg())
		.await
		.with_context(|| format!("connecting to target {:?}", target.name()))?;
	Ok((db, target))
}

/// What `sync` applied, and where.
#[derive(Debug, Clone, Serialize)]
pub struct SyncOutcome {
	pub target: TargetOutcome,
	pub dry_run: bool,
	/// Whether stale database objects were removed as well as files applied.
	pub pruned: bool,
	/// Schema modules applied, in dependency order.
	pub modules: Vec<String>,
}

/// The database a tool acted on.
#[derive(Debug, Clone, Serialize)]
pub struct TargetOutcome {
	pub target: String,
	pub namespace: String,
	pub database: String,
}

impl TargetOutcome {
	pub(super) fn of(target: &Target) -> Self {
		Self {
			target: target.name().to_string(),
			namespace: target.cfg().ns().to_string(),
			database: target.cfg().db().to_string(),
		}
	}

	pub(super) fn summarize(&self, verb: &str) -> String {
		format!("{verb}: {} (ns {}, db {})", self.target, self.namespace, self.database)
	}
}

/// `project_info` takes only the shared selection parameters.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct InfoInput {
	#[serde(flatten)]
	pub work: WorkRequest,
}

/// `rollout_baseline` is one-shot and irreversible, so it carries its own gate.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct BaselineInput {
	#[serde(flatten)]
	pub work: WorkRequest,
	#[serde(default)]
	pub confirm: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SyncInput {
	#[serde(flatten)]
	pub work: WorkRequest,
	/// Report what would change without applying anything.
	#[serde(default)]
	pub dry_run: bool,
	/// Stop at the first apply error instead of continuing. Default: true.
	#[serde(default = "default_true")]
	pub fail_fast: bool,
	/// Apply changed files but leave stale entities in place. Never needs
	/// confirmation, because nothing is removed.
	#[serde(default)]
	pub no_prune: bool,
	/// Permit pruning even when the database looks shared with another project.
	#[serde(default)]
	pub allow_shared_prune: bool,
	/// Permit a prune that would remove *every* managed entity because no schema
	/// files were found. That almost always means the folder is wrong.
	#[serde(default)]
	pub allow_empty_prune: bool,
	/// Allow non-DEFINE statements (INSERT, UPDATE, ...) in schema files. Turns
	/// off catalog entity tracking; only file hashes are tracked.
	#[serde(default)]
	pub allow_all_statements: bool,
	/// Required unless `dry_run` or `no_prune` is set: sync can drop database
	/// objects that are no longer in the schema files.
	#[serde(default)]
	pub confirm: bool,
}

fn default_true() -> bool {
	true
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SeedInput {
	#[serde(flatten)]
	pub work: WorkRequest,
	/// Re-run every seed file, ignoring the `__seed` tracking table. Requires
	/// `confirm`, because re-running inserts against a populated database.
	#[serde(default)]
	pub force: bool,
	#[serde(default)]
	pub confirm: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ApplyInput {
	#[serde(flatten)]
	pub work: WorkRequest,
	/// Path to a `.surql` file, **relative to the project root**. Absolute paths
	/// and `..` are rejected.
	pub path: String,
	/// Required: the file's SurrealQL runs verbatim against the target database.
	#[serde(default)]
	pub confirm: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct TypegenInput {
	#[serde(flatten)]
	pub work: WorkRequest,
	/// Also write the JSON document to `<folder>/types/schema.json` (and
	/// TypeScript, when `[typegen] typescript` is configured). Off by default:
	/// the document is returned in the result either way.
	#[serde(default)]
	pub write: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RolloutSelectorInput {
	#[serde(flatten)]
	pub work: WorkRequest,
	/// Rollout id, or a manifest filename relative to `<folder>/rollouts`.
	pub rollout_id: String,
	#[serde(default)]
	pub confirm: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RolloutStatusInput {
	#[serde(flatten)]
	pub work: WorkRequest,
	/// A specific rollout id. Omit for every recorded rollout.
	#[serde(default)]
	pub rollout_id: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RolloutPlanInput {
	#[serde(flatten)]
	pub work: WorkRequest,
	/// Human-readable name for the rollout.
	#[serde(default)]
	pub name: Option<String>,
	/// Compute and report the plan without writing a manifest file.
	#[serde(default)]
	pub dry_run: bool,
	/// Plan changes to entities that already exist, not just additions and
	/// removals. Off by default: rollback restores the previous *definition*, so
	/// whether that is a real undo is a judgement call for this change.
	#[serde(default)]
	pub allow_modified: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct TestInput {
	#[serde(flatten)]
	pub work: WorkRequest,
	/// Run only suites whose name matches.
	#[serde(default)]
	pub suite: Option<String>,
	/// Run only cases whose name matches.
	#[serde(default)]
	pub case: Option<String>,
	/// Run only cases carrying every one of these tags.
	#[serde(default)]
	pub tags: Vec<String>,
	#[serde(default)]
	pub fail_fast: bool,
	/// Suites to run concurrently. Default: 1.
	#[serde(default)]
	pub parallel: Option<usize>,
	/// Skip the implicit setup/sync/seed that runs before the suites.
	#[serde(default)]
	pub no_setup: bool,
	#[serde(default)]
	pub no_sync: bool,
	#[serde(default)]
	pub no_seed: bool,
	#[serde(default)]
	pub timeout_ms: Option<u64>,
	/// Leave the per-suite namespace and database behind instead of dropping them.
	#[serde(default)]
	pub keep_db: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct InitInput {
	#[serde(flatten)]
	pub work: WorkRequest,
	/// Bundled template name. Omit for the default.
	#[serde(default)]
	pub template: Option<String>,
	/// Feature ids to enable. Omit to take the template's defaults.
	#[serde(default)]
	pub features: Vec<String>,
	/// Scaffold the bare project with no template features.
	#[serde(default)]
	pub minimal: bool,
	/// Overwrite files that already exist. Requires `confirm`.
	#[serde(default)]
	pub force: bool,
	#[serde(default)]
	pub confirm: bool,
}

#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct ProjectInfo {
	pub project_root: String,
	pub config_file: Option<String>,
	pub folder: String,
	/// The resolved connection. The password is never included.
	pub connection: ConnectionInfo,
	pub schema_modules: Vec<ModuleInfo>,
	pub targets: Vec<TargetInfo>,
	/// The (module x target) matrix this selection expands to.
	pub matrix: Vec<MatrixPair>,
	pub variables: BTreeMap<String, String>,
	pub typegen_typescript: Option<String>,
}

#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct ConnectionInfo {
	pub host: String,
	pub namespace: String,
	pub database: String,
	pub user: String,
	pub auth_level: String,
}

#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct ModuleInfo {
	pub name: String,
	pub depends_on: Vec<String>,
	pub schema_dir: String,
}

#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct TargetInfo {
	pub name: String,
	pub namespace: String,
	pub database: String,
	pub primary: bool,
	/// Which schema modules this target accepts. Empty means all of them.
	pub schemas: Vec<String>,
}

#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct MatrixPair {
	pub module: String,
	pub target: String,
	pub namespace: String,
	pub database: String,
}

#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct ApplyReport {
	pub path: String,
	pub bytes: usize,
	pub target: String,
	pub namespace: String,
	pub database: String,
}

#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct InitReport {
	pub folder: String,
	pub template: Option<String>,
	pub features: Vec<String>,
}

/// Reject a rollout selector that tries to escape `<folder>/rollouts`.
///
/// `rollout::resolve_rollout_path` accepts any path that exists, which is right
/// for a selector the user typed and wrong for one a model supplied: it would
/// read an arbitrary file and execute its `steps` as SurrealQL. A bare id (no
/// separator, no `.toml`) is passed through untouched; anything path-shaped must
/// resolve inside the rollouts directory.
pub(super) fn contain_rollout_selector(ctx: &CallContext, selector: &str) -> Result<String> {
	let looks_like_path =
		selector.contains('/') || selector.contains('\\') || selector.ends_with(".toml");
	if !looks_like_path {
		return Ok(selector.to_string());
	}
	let rollouts = crate::constants::rollouts_dir(&ctx.folder);
	let resolved = safe_join(&rollouts, selector)
		.with_context(|| format!("resolving rollout manifest {selector:?}"))?;
	Ok(resolved.to_string_lossy().into_owned())
}

/// Build the tool router.
///
/// `#[tool_router]` generates a private associated function, visible only inside
/// this module, so the server constructor goes through here.
pub(super) fn router() -> rmcp::handler::server::router::tool::ToolRouter<SurrealKitMcp> {
	let router = SurrealKitMcp::tool_router() + rollout::router();
	#[cfg(feature = "analyze")]
	let router = router + analyze::router();
	router
}

#[tool_router(router = tool_router)]
impl SurrealKitMcp {
	/// What this project looks like: config, schema modules, targets, and the
	/// resolved connection.
	#[tool(
		name = "project_info",
		description = "Show the resolved SurrealKit project: config file location, schema \
		               modules and their dependencies, declared database targets, the \
		               (module x target) matrix, template variables, and the connection \
		               that will be used. The password is never included. Call this first \
		               to learn which target names exist.",
		annotations(
			title = "Project info",
			read_only_hint = true,
			destructive_hint = false,
			idempotent_hint = true,
			open_world_hint = false
		)
	)]
	pub async fn project_info(&self, Parameters(input): Parameters<InfoInput>) -> CallToolResult {
		let server = self.clone();
		run_captured(
			|info: &ProjectInfo| {
				format!(
					"{} schema module(s), {} target(s), {} pair(s); folder {}",
					info.schema_modules.len(),
					info.targets.len(),
					info.matrix.len(),
					info.folder
				)
			},
			|| async move {
				let ctx = server.context(&input.work)?;
				let selection = ctx.selection(&input.work)?;

				let schema_modules = selection
					.targets()
					.first()
					.map(|t| selection.modules_for(t))
					.unwrap_or_default()
					.iter()
					.map(|m| ModuleInfo {
						name: m.name().to_string(),
						depends_on: ctx
							.project
							.schema
							.get(m.name())
							.map(|c| c.depends_on.clone())
							.unwrap_or_default(),
						schema_dir: ctx.layout(m).schema_dir().to_string_lossy().into_owned(),
					})
					.collect();

				let targets = selection
					.targets()
					.iter()
					.map(|t| TargetInfo {
						name: t.name().to_string(),
						namespace: t.cfg().ns().to_string(),
						database: t.cfg().db().to_string(),
						primary: ctx.project.target.get(t.name()).is_some_and(|c| c.primary),
						schemas: ctx
							.project
							.target
							.get(t.name())
							.and_then(|c| c.schemas.clone())
							.unwrap_or_default(),
					})
					.collect();

				let mut matrix = Vec::new();
				for target in selection.targets() {
					for module in selection.modules_for(target) {
						matrix.push(MatrixPair {
							module: module.name().to_string(),
							target: target.name().to_string(),
							namespace: target.cfg().ns().to_string(),
							database: target.cfg().db().to_string(),
						});
					}
				}

				Ok(ProjectInfo {
					project_root: ctx.root.to_string_lossy().into_owned(),
					config_file: ctx.config_path.as_ref().map(|p| p.to_string_lossy().into_owned()),
					folder: ctx.folder.clone(),
					connection: ConnectionInfo {
						host: ctx.cfg.host().to_string(),
						namespace: ctx.cfg.ns().to_string(),
						database: ctx.cfg.db().to_string(),
						user: ctx.cfg.user().to_string(),
						auth_level: format!("{:?}", ctx.cfg.auth_level()).to_lowercase(),
					},
					schema_modules,
					targets,
					matrix,
					variables: ctx.vars.vars.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
					typegen_typescript: ctx
						.typegen
						.typescript
						.as_ref()
						.map(|p| p.to_string_lossy().into_owned()),
				})
			},
		)
		.await
	}

	/// Provision SurrealKit's own metadata tables.
	#[tool(
		name = "setup",
		description = "Provision SurrealKit's metadata tables (__entity, __rollout, __seed) \
		               on the target database, and scaffold <folder>/setup.surql if it is \
		               missing. Idempotent, and run implicitly by sync and the rollout \
		               commands -- you rarely need to call it directly.",
		annotations(
			title = "Set up metadata tables",
			read_only_hint = false,
			destructive_hint = false,
			idempotent_hint = true,
			open_world_hint = true
		)
	)]
	pub async fn setup(&self, Parameters(input): Parameters<InfoInput>) -> CallToolResult {
		let server = self.clone();
		run_captured(
			|report: &TargetOutcome| report.summarize("setup"),
			|| async move {
				let ctx = server.context(&input.work)?;
				let selection = ctx.selection(&input.work)?;
				let _guard = server.lock_folder(&ctx.folder).await;
				let (db, target) = connect_single(&selection).await?;
				crate::setup::run_setup(&db, &ctx.folder).await?;
				Ok(TargetOutcome::of(&target))
			},
		)
		.await
	}

	/// Reconcile the database to the schema files.
	#[tool(
		name = "sync",
		description = "Reconcile the target database to the .surql files under \
		               <folder>/schema: apply changed files, and prune database objects \
		               that are no longer defined there. Idempotent -- re-running with \
		               unchanged files is a no-op. Because pruning DROPS objects and the \
		               data they hold, this refuses to run without confirm: true unless \
		               you pass dry_run or no_prune. Call it with dry_run first to see the \
		               plan. Watch mode is not available over MCP (a watch loop never \
		               returns); run `surrealkit sync --watch` in a terminal instead.",
		annotations(
			title = "Sync schema",
			read_only_hint = false,
			destructive_hint = true,
			idempotent_hint = true,
			open_world_hint = true
		)
	)]
	pub async fn sync(&self, Parameters(input): Parameters<SyncInput>) -> CallToolResult {
		let prunes = !input.no_prune && !input.dry_run;
		if prunes && !input.confirm {
			return error::confirmation_required(
				"sync",
				"the selected database target(s)",
				"Sync applies changed schema files and REMOVES managed database objects that \
				 are no longer defined in them.",
				"A removed table, field or index takes its data with it. SurrealKit keeps no \
				 backup and there is no undo.",
				Some(
					"call `sync` with dry_run: true to see exactly what would change, or \
					 no_prune: true to apply file changes and leave stale objects alone",
				),
			);
		}

		let server = self.clone();
		run_captured(
			|report: &SyncOutcome| {
				format!(
					"{}: {} module(s) on {} (ns {}, db {})",
					if report.dry_run {
						"sync (dry run)"
					} else {
						"sync"
					},
					report.modules.len(),
					report.target.target,
					report.target.namespace,
					report.target.database
				)
			},
			|| async move {
				let ctx = server.context(&input.work)?;
				let selection = ctx.selection(&input.work)?;
				if selection.pairs() == 0 {
					bail!(
						"refusing to sync: the selected targets accept none of the selected \
						 schema modules (applicable_pair_count=0)"
					);
				}
				let _guard = server.lock_folder(&ctx.folder).await;

				// Resolve every module's files before opening the connection, so a
				// wrong folder cannot become a prune.
				let mut sources = Vec::new();
				let target = selection.single_target()?;
				for module in selection.modules_for(target) {
					let layout = ctx.layout(&module);
					let files = crate::sync::collect_filesystem_schema_files(
						layout.folder(),
						&layout.schema_dir(),
						&module,
						input.allow_empty_prune,
					)?;
					sources.push((module, layout, files));
				}

				let (db, target) = connect_single(&selection).await?;
				let mut modules = Vec::new();
				// Modules arrive in dependency order, so a failure stops the rest:
				// applying them would build on a broken base.
				for (module, layout, files) in &sources {
					let opts = crate::sync::SyncOpts {
						watch: false,
						debounce_ms: 1000,
						dry_run: input.dry_run,
						fail_fast: input.fail_fast,
						prune: !input.no_prune,
						allow_shared_prune: input.allow_shared_prune,
						allow_empty_prune: input.allow_empty_prune,
						allow_all_statements: input.allow_all_statements,
						vars: ctx.vars.clone(),
						folder: ctx.folder.clone(),
						module: module.clone(),
						typegen_ts_out: ctx.typegen.typescript.clone(),
						typegen_ts_format: ctx.typegen.format.clone(),
					};
					crate::sync::run_sync_with_filesystem_sources(&db, opts, layout, files)
						.await
						.with_context(|| format!("syncing schema module {:?}", module.name()))?;
					modules.push(module.name().to_string());
				}

				Ok(SyncOutcome {
					target: TargetOutcome::of(&target),
					dry_run: input.dry_run,
					pruned: !input.no_prune && !input.dry_run,
					modules,
				})
			},
		)
		.await
	}

	/// Run the seed files.
	#[tool(
		name = "seed",
		description = "Run the .surql files under <folder>/seed. Each file runs only on \
		               first boot or when its content changes, tracked by hash in the \
		               __seed table, so repeated calls are no-ops. force: true re-runs \
		               every file and requires confirm: true, because seed files typically \
		               INSERT and re-running them against a populated database duplicates \
		               rows.",
		annotations(
			title = "Seed data",
			read_only_hint = false,
			destructive_hint = false,
			idempotent_hint = true,
			open_world_hint = true
		)
	)]
	pub async fn seed(&self, Parameters(input): Parameters<SeedInput>) -> CallToolResult {
		if input.force && !input.confirm {
			return error::confirmation_required(
				"seed",
				"the selected database target(s)",
				"force: true re-runs EVERY seed file, ignoring the __seed hash tracking that \
				 normally makes seeding idempotent.",
				"Seed files usually INSERT. Re-running them against a populated database \
				 duplicates rows, and SurrealKit does not de-duplicate them for you.",
				Some("call `seed` without force to run only files whose content changed"),
			);
		}

		let server = self.clone();
		run_captured(
			|report: &TargetOutcome| report.summarize("seed"),
			|| async move {
				let ctx = server.context(&input.work)?;
				let selection = ctx.selection(&input.work)?;
				let _guard = server.lock_folder(&ctx.folder).await;
				let (db, target) = connect_single(&selection).await?;
				crate::seed::Seed::from_dir(ctx.folder.clone())
					.vars(ctx.vars.clone())
					.force(input.force)
					.run(&db)
					.await?;
				Ok(TargetOutcome::of(&target))
			},
		)
		.await
	}

	/// Execute one `.surql` file.
	#[tool(
		name = "apply",
		description = "Execute a .surql file against the target database after ${VAR} \
		               substitution. The path is relative to the project root; absolute \
		               paths and '..' are rejected. This runs arbitrary SurrealQL with \
		               whatever permissions the configured user has and is tracked by \
		               nothing -- no hashing, no rollback, no entity bookkeeping -- so it \
		               always requires confirm: true. Prefer sync or a rollout for schema \
		               changes.",
		annotations(
			title = "Apply SurrealQL",
			read_only_hint = false,
			destructive_hint = true,
			idempotent_hint = false,
			open_world_hint = true
		)
	)]
	pub async fn apply(&self, Parameters(input): Parameters<ApplyInput>) -> CallToolResult {
		if !input.confirm {
			return error::confirmation_required(
				"apply",
				"the selected database target",
				&format!(
					"Executes every statement in {:?} verbatim. SurrealKit does not parse, \
					 validate or track it -- a REMOVE or DELETE in that file runs exactly as \
					 written.",
					input.path
				),
				"There is no undo and no backup. Unlike a rollout, nothing records what this \
				 changed.",
				Some("read the file first, or use `sync`/`rollout_plan` for schema changes"),
			);
		}

		let server = self.clone();
		run_captured(
			|report: &ApplyReport| {
				format!(
					"applied {} ({} bytes) to {} (ns {}, db {})",
					report.path, report.bytes, report.target, report.namespace, report.database
				)
			},
			|| async move {
				let ctx = server.context(&input.work)?;
				let selection = ctx.selection(&input.work)?;
				let path = safe_join(&ctx.root, &input.path)?;
				let raw = std::fs::read_to_string(&path)
					.with_context(|| format!("reading {}", path.display()))?;
				let sql = ctx.vars.apply(&raw)?;

				let _guard = server.lock_folder(&ctx.folder).await;
				let (db, target) = connect_single(&selection).await?;
				crate::core::exec_surql(&db, &sql).await?;
				log::info!("applied {}", path.display());

				Ok(ApplyReport {
					path: input.path.clone(),
					bytes: raw.len(),
					target: target.name().to_string(),
					namespace: target.cfg().ns().to_string(),
					database: target.cfg().db().to_string(),
				})
			},
		)
		.await
	}

	/// Introspect the live schema into a typed document.
	#[tool(
		name = "typegen",
		description = "Introspect the target database and return a typed schema document: \
		               tables with their fields, events and indexes, plus functions, params, \
		               analyzers, accesses and more. Read-only unless write: true, which \
		               also saves it to <folder>/types/schema.json and regenerates \
		               TypeScript when [typegen] typescript is configured.",
		annotations(
			title = "Generate types",
			read_only_hint = false,
			destructive_hint = false,
			idempotent_hint = true,
			open_world_hint = true
		)
	)]
	pub async fn typegen(&self, Parameters(input): Parameters<TypegenInput>) -> CallToolResult {
		let server = self.clone();
		run_captured(
			|doc: &crate::typegen::SchemaTypes| {
				format!(
					"{} table(s), {} function(s), {} param(s)",
					doc.tables.len(),
					doc.functions.len(),
					doc.params.len()
				)
			},
			|| async move {
				let ctx = server.context(&input.work)?;
				let selection = ctx.selection(&input.work)?;
				let (db, _target) = connect_single(&selection).await?;
				let doc = crate::typegen::generate(&db).await?;
				if input.write {
					let _guard = server.lock_folder(&ctx.folder).await;
					crate::typegen::run_typegen(
						&db,
						&ctx.folder,
						ctx.cfg.ns(),
						ctx.cfg.db(),
						crate::typegen::TypegenOpts {
							out: None,
							// Never true: on stdio, stdout is the JSON-RPC channel.
							stdout: false,
							pretty: true,
							ts_out: ctx.typegen.typescript.clone(),
							ts_format: ctx.typegen.format.clone(),
						},
					)
					.await?;
				}
				Ok(doc)
			},
		)
		.await
	}

	/// Run the test suites.
	#[tool(
		name = "test",
		description = "Run the test suites under <folder>/tests/suites. Each suite gets an \
		               isolated namespace and database, created and dropped around the run. \
		               Failing cases are reported in the result rather than as an error, so \
		               read cases_failed and the per-assertion messages. Requires a server \
		               endpoint with root or namespace auth: embedded engines and \
		               database-scoped users cannot create the per-suite database.",
		annotations(
			title = "Run tests",
			read_only_hint = false,
			destructive_hint = true,
			idempotent_hint = true,
			open_world_hint = true
		)
	)]
	pub async fn test(&self, Parameters(input): Parameters<TestInput>) -> CallToolResult {
		let server = self.clone();
		run_captured(
			|report: &crate::tester::RunReport| {
				format!(
					"{}/{} case(s) passed across {} suite(s) in {}ms",
					report.cases_passed,
					report.cases_total,
					report.suites_total,
					report.duration_ms
				)
			},
			|| async move {
				let ctx = server.context(&input.work)?;
				let mut overrides = server.config().overrides.clone();
				overrides.folder = Some(ctx.folder.clone());
				let opts = crate::tester::TestOpts {
					suite: input.suite.clone(),
					case: input.case.clone(),
					tags: input.tags.clone(),
					fail_fast: input.fail_fast,
					parallel: input.parallel.unwrap_or(1).max(1),
					// The report is the tool result; writing it to a path as well
					// would be a filesystem-write primitive with no benefit.
					json_out: None,
					no_setup: input.no_setup,
					no_sync: input.no_sync,
					no_seed: input.no_seed,
					// Resolved from tests/config.toml and the environment, never
					// from tool input: the harness issues HTTP requests to it.
					base_url: None,
					timeout_ms: input.timeout_ms,
					keep_db: input.keep_db,
				};
				let _guard = server.lock_folder(&ctx.folder).await;
				crate::tester::run_test_report(None, opts, ctx.vars.clone(), &overrides).await
			},
		)
		.await
	}

	/// Scaffold a project.
	#[tool(
		name = "init",
		description = "Scaffold a SurrealKit project from a bundled template: creates \
		               surrealkit.toml and the <folder> tree (schema, rollouts, snapshots, \
		               seed, tests). Skips files that already exist unless force is set, \
		               which requires confirm: true. Always non-interactive. External \
		               templates (--from <git-url>) are not available over MCP; use the CLI.",
		annotations(
			title = "Scaffold project",
			read_only_hint = false,
			destructive_hint = false,
			idempotent_hint = true,
			open_world_hint = false
		)
	)]
	pub async fn init(&self, Parameters(input): Parameters<InitInput>) -> CallToolResult {
		if input.force && !input.confirm {
			return error::confirmation_required(
				"init",
				"the project folder on disk",
				"force: true OVERWRITES files that already exist, including surrealkit.toml \
				 and any schema files sharing a name with the template's.",
				"Overwritten files are not backed up. Uncommitted work in them is lost.",
				Some("call `init` without force to skip files that already exist"),
			);
		}

		let server = self.clone();
		run_captured(
			|report: &InitReport| {
				format!(
					"scaffolded {} ({})",
					report.folder,
					if report.features.is_empty() {
						"no optional features".to_string()
					} else {
						report.features.join(", ")
					}
				)
			},
			|| async move {
				let ctx = server.context(&input.work)?;
				let _guard = server.lock_folder(&ctx.folder).await;
				crate::templates::run_init(
					&ctx.folder,
					crate::templates::InitOpts {
						template: input.template.clone(),
						// Fetching an external template would clone an arbitrary
						// git URL chosen by the caller. Left to the CLI, where the
						// URL is something the user typed.
						from: None,
						feature: input.features.clone(),
						minimal: input.minimal,
						yes: true,
						force: input.force,
					},
				)?;
				Ok(InitReport {
					folder: ctx.folder.clone(),
					template: input.template.clone(),
					features: input.features.clone(),
				})
			},
		)
		.await
	}
}
