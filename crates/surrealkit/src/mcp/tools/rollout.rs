//! The rollout family: the staged, reversible path for schema change.
//!
//! Each phase is its own tool rather than one tool with an `action` enum, because
//! MCP annotations are per-tool and static. A merged tool would have to be both
//! `read_only_hint: true` (status, lint) and `destructive_hint: true` (rollback),
//! and whichever was chosen the host's confirmation prompt would be wrong for
//! half the actions.

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::{schemars, tool, tool_router};
use serde::{Deserialize, Serialize};

use super::{
	BaselineInput, RolloutPlanInput, RolloutSelectorInput, RolloutStatusInput, TargetOutcome,
	WorkRequest, check_all_rollout_ids, connect_single, contain_rollout_selector,
};
use crate::mcp::SurrealKitMcp;
use crate::mcp::error;
use crate::mcp::result::run_captured;
use crate::rollout::{RolloutExecutionOpts, RolloutPlanOpts, RolloutUpOpts};

/// Build the rollout tool router.
pub(super) fn router() -> ToolRouter<SurrealKitMcp> {
	SurrealKitMcp::rollout_router()
}

/// A rollout phase that ran against the selected targets.
#[derive(Debug, Clone, Serialize)]
pub struct RolloutRunReport {
	pub rollout_id: String,
	pub phase: &'static str,
	#[serde(flatten)]
	pub target: TargetOutcome,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RolloutLintInput {
	#[serde(flatten)]
	pub work: WorkRequest,
	/// A rollout id, or a manifest filename relative to `<folder>/rollouts`. Omit
	/// to lint every manifest and how they chain, which suits a CI check.
	#[serde(default)]
	pub rollout_id: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RolloutUpInput {
	#[serde(flatten)]
	pub work: WorkRequest,
	/// Complete the newest pending rollout too, instead of leaving it ready to
	/// complete so the application can cut over first.
	#[serde(default)]
	pub complete: bool,
	/// The first rollout this database still needs, when its position cannot be
	/// worked out from its history.
	#[serde(default)]
	pub from: Option<String>,
	/// Report what would run, and run nothing.
	#[serde(default)]
	pub dry_run: bool,
	/// Required unless `dry_run` is set: older pending rollouts are completed,
	/// which runs their destructive phase.
	#[serde(default)]
	pub confirm: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RolloutDiscardInput {
	#[serde(flatten)]
	pub work: WorkRequest,
	/// The newest rollout's id, or its manifest filename relative to
	/// `<folder>/rollouts`.
	pub rollout_id: String,
	/// Leave the snapshots as they are, for when they have been put right by hand.
	#[serde(default)]
	pub keep_snapshots: bool,
	/// Required: the manifest and its directory are deleted.
	#[serde(default)]
	pub confirm: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RolloutFreezeInput {
	#[serde(flatten)]
	pub work: WorkRequest,
	/// The rollout's id, or its manifest filename relative to `<folder>/rollouts`.
	pub rollout_id: String,
}

/// What `rollout_lint` checked.
#[derive(Debug, Clone, Serialize)]
pub struct RolloutLintReport {
	/// The manifest linted, after containment, or `None` for all of them.
	pub manifest: Option<String>,
}

/// What `rollout_up` ran, in order.
#[derive(Debug, Clone, Serialize)]
pub struct RolloutUpReport {
	#[serde(flatten)]
	pub target: TargetOutcome,
	pub dry_run: bool,
	/// The rollouts that were pending when it began.
	pub pending: Vec<String>,
	/// The rollouts it completed.
	pub completed: Vec<String>,
	/// The newest rollout, if it was started and left for `rollout_complete`.
	pub waiting: Option<String>,
	/// Rollouts the database moved past without running, whose data steps never
	/// ran here.
	pub skipped: Vec<String>,
}

/// A manifest-only change: `freeze` or `discard`.
#[derive(Debug, Clone, Serialize)]
pub struct RolloutFileReport {
	pub rollout_id: String,
	pub action: &'static str,
}

/// A rollout manifest that was authored (or would have been).
#[derive(Debug, Clone, Serialize)]
pub struct RolloutPlanReport {
	pub rollouts_dir: String,
	pub dry_run: bool,
	pub name: Option<String>,
}

#[tool_router(router = rollout_router)]
impl SurrealKitMcp {
	#[tool(
		name = "rollout_status",
		description = "Show recorded rollout state from the __rollout table: id, name, \
		               status, timestamps, last error, and every step with its own status. \
		               Omit rollout_id for all of them. A status of running_start, \
		               running_complete or running_rollback means a run was interrupted \
		               mid-flight -- see rollout_repair.",
		annotations(
			title = "Rollout status",
			read_only_hint = false,
			destructive_hint = false,
			idempotent_hint = true,
			open_world_hint = true
		)
	)]
	pub async fn rollout_status(
		&self,
		Parameters(input): Parameters<RolloutStatusInput>,
	) -> CallToolResult {
		let server = self.clone();
		run_captured(
			|report: &TargetOutcome| report.summarize("rollout status"),
			|| async move {
				let ctx = server.context(&input.work)?;
				let selection = ctx.selection(&input.work)?;
				let (db, target) = connect_single(&selection).await?;
				crate::rollout::run_status(&db, &ctx.folder, input.rollout_id.clone()).await?;
				Ok(TargetOutcome::of(&target))
			},
		)
		.await
	}

	#[tool(
		name = "rollout_lint",
		description = "Validate rollout manifests without touching the database: parse them, \
		               check their steps, and compare the schema hash each was planned \
		               against to the current files. A mismatch means the .surql files \
		               changed since the manifest was written, so it would apply a stale \
		               plan. Omit rollout_id to lint every manifest and how they chain.",
		annotations(
			title = "Lint rollout",
			read_only_hint = true,
			destructive_hint = false,
			idempotent_hint = true,
			open_world_hint = false
		)
	)]
	pub async fn rollout_lint(
		&self,
		Parameters(input): Parameters<RolloutLintInput>,
	) -> CallToolResult {
		let server = self.clone();
		run_captured(
			|report: &RolloutLintReport| match &report.manifest {
				Some(manifest) => format!("linted {manifest}"),
				None => "linted every manifest and the chain".to_string(),
			},
			|| async move {
				let ctx = server.context(&input.work)?;
				let selector = match &input.rollout_id {
					Some(id) => Some(contain_rollout_selector(&ctx, id)?),
					None => {
						check_all_rollout_ids(&ctx)?;
						None
					}
				};
				// Lint never executes a step, so it takes no deadline.
				crate::rollout::run_lint(&ctx.folder, RolloutExecutionOpts::new(selector.clone()))
					.await?;
				Ok(RolloutLintReport {
					manifest: selector,
				})
			},
		)
		.await
	}

	#[tool(
		name = "rollout_plan",
		description = "Diff the schema files against the saved snapshots and write a new \
		               rollout manifest into <folder>/rollouts. Authoring only -- nothing is \
		               applied and no database is touched. NOT idempotent: each call writes \
		               a new timestamped manifest and updates the snapshots, so calling it \
		               twice leaves the second manifest empty. Use dry_run to see the plan \
		               without writing.",
		annotations(
			title = "Plan a rollout",
			read_only_hint = false,
			destructive_hint = false,
			idempotent_hint = false,
			open_world_hint = false
		)
	)]
	pub async fn rollout_plan(
		&self,
		Parameters(input): Parameters<RolloutPlanInput>,
	) -> CallToolResult {
		let server = self.clone();
		run_captured(
			|report: &RolloutPlanReport| {
				if report.dry_run {
					format!("planned (dry run) into {}", report.rollouts_dir)
				} else {
					format!("wrote a rollout manifest into {}", report.rollouts_dir)
				}
			},
			|| async move {
				let ctx = server.context(&input.work)?;
				let _guard = server.lock_folder(&ctx.folder).await;
				crate::rollout::run_plan(
					&ctx.folder,
					RolloutPlanOpts {
						name: input.name.clone(),
						dry_run: input.dry_run,
						allow_modified: input.allow_modified,
					},
				)
				.await?;
				Ok(RolloutPlanReport {
					rollouts_dir: crate::constants::rollouts_dir(&ctx.folder)
						.to_string_lossy()
						.into_owned(),
					dry_run: input.dry_run,
					name: input.name.clone(),
				})
			},
		)
		.await
	}

	#[tool(
		name = "rollout_baseline",
		description = "Seed managed-entity state from the current schema, once, for a \
		               project adopting rollouts on an existing database. Refuses if any \
		               rollout state already exists. Operates on one schema module at a \
		               time.",
		annotations(
			title = "Baseline rollouts",
			read_only_hint = false,
			destructive_hint = true,
			idempotent_hint = false,
			open_world_hint = true
		)
	)]
	pub async fn rollout_baseline(
		&self,
		Parameters(input): Parameters<BaselineInput>,
	) -> CallToolResult {
		if !input.confirm {
			return error::confirmation_required(
				"rollout_baseline",
				"the selected database target(s)",
				"Writes managed-entity state for every object currently defined by the schema \
				 files, declaring them the rollout system's baseline.",
				"Baseline can only run once per project: it refuses if any rollout state \
				 already exists. A baseline taken against the wrong schema or the wrong \
				 database has to be cleared out by hand before rollouts work correctly.",
				Some(
					"call `rollout_status` first -- if it shows any rollouts, this project is \
					 already baselined and you do not need this",
				),
			);
		}
		let server = self.clone();
		run_captured(
			|report: &TargetOutcome| report.summarize("rollout baseline"),
			|| async move {
				let ctx = server.context(&input.work)?;
				let selection = ctx.selection(&input.work)?;
				// Baseline records state for exactly one module; refusing here is
				// better than silently baselining the wrong one.
				let module = selection.single_module()?.clone();
				let _rollouts = server.lock_rollouts().await;
				let _guard = server.lock_folder(&ctx.folder).await;
				let (db, target) = connect_single(&selection).await?;
				crate::rollout::run_baseline(&db, &ctx.folder, &module).await?;
				Ok(TargetOutcome::of(&target))
			},
		)
		.await
	}

	#[tool(
		name = "rollout_start",
		description = "Run a rollout's additive `start` phase: new tables, new fields, \
		               backfills -- everything safe to have in place while the old shape is \
		               still being read. Safe to re-run. Leaves the rollout ready_to_complete.",
		annotations(
			title = "Start rollout",
			read_only_hint = false,
			destructive_hint = false,
			idempotent_hint = true,
			open_world_hint = true
		)
	)]
	pub async fn rollout_start(
		&self,
		Parameters(input): Parameters<RolloutSelectorInput>,
	) -> CallToolResult {
		if !input.confirm {
			return error::confirmation_required(
				"rollout_start",
				"the selected database target(s)",
				&format!(
					"Executes the `start` phase of rollout {:?}: its additive steps run \
					 against the live database.",
					input.rollout_id
				),
				"Start is additive and `rollout_rollback` undoes it, but the steps do run \
				 against real data -- a backfill over a large table is not instant and not \
				 free.",
				Some("call `rollout_lint` first to check the manifest against the current schema"),
			);
		}
		self.run_rollout_phase(input, "start").await
	}

	#[tool(
		name = "rollout_complete",
		description = "Run a rollout's destructive `complete` phase: drop the old columns, \
		               tables and indexes the start phase superseded, then promote the \
		               managed entities. This is the point of no return -- rollback is no \
		               longer available afterwards.",
		annotations(
			title = "Complete rollout",
			read_only_hint = false,
			destructive_hint = true,
			idempotent_hint = false,
			open_world_hint = true
		)
	)]
	pub async fn rollout_complete(
		&self,
		Parameters(input): Parameters<RolloutSelectorInput>,
	) -> CallToolResult {
		if !input.confirm {
			return error::confirmation_required(
				"rollout_complete",
				"the selected database target(s)",
				&format!(
					"Executes the `complete` phase of rollout {:?}: its REMOVE steps drop the \
					 database objects the start phase superseded.",
					input.rollout_id
				),
				"This is the point of no return. Once complete has run, `rollout_rollback` is \
				 no longer available and the dropped data is gone.",
				Some(
					"call `rollout_status` first and confirm the rollout is ready_to_complete \
					 and that the new shape is actually being read in production",
				),
			);
		}
		self.run_rollout_phase(input, "complete").await
	}

	#[tool(
		name = "rollout_rollback",
		description = "Undo a rollout's `start` phase, returning the database to its \
		               pre-start shape. Only available before complete has run. The rollback \
		               steps themselves are destructive: they remove what start added.",
		annotations(
			title = "Roll back rollout",
			read_only_hint = false,
			destructive_hint = true,
			idempotent_hint = false,
			open_world_hint = true
		)
	)]
	pub async fn rollout_rollback(
		&self,
		Parameters(input): Parameters<RolloutSelectorInput>,
	) -> CallToolResult {
		if !input.confirm {
			return error::confirmation_required(
				"rollout_rollback",
				"the selected database target(s)",
				&format!(
					"Executes the `rollback` phase of rollout {:?}, removing what its start \
					 phase added.",
					input.rollout_id
				),
				"Anything written into the new columns or tables since start ran is dropped \
				 with them. There is no backup.",
				Some("call `rollout_status` first to see which phase the rollout is actually in"),
			);
		}
		self.run_rollout_phase(input, "rollback").await
	}

	#[tool(
		name = "rollout_repair",
		description = "Heal a rollout stuck in running_start, running_complete or \
		               running_rollback because a run was killed mid-flight. Reconciles the \
		               recorded metadata only -- it never re-runs SQL steps, so it cannot \
		               make a partial schema change worse. Try this before rollout_abandon.",
		annotations(
			title = "Repair rollout",
			read_only_hint = false,
			destructive_hint = false,
			idempotent_hint = true,
			open_world_hint = true
		)
	)]
	pub async fn rollout_repair(
		&self,
		Parameters(input): Parameters<RolloutSelectorInput>,
	) -> CallToolResult {
		if !input.confirm {
			return error::confirmation_required(
				"rollout_repair",
				"the selected database target(s)",
				&format!(
					"Rewrites the recorded status of rollout {:?} to a terminal state. No SQL \
					 step is re-run and no schema object changes.",
					input.rollout_id
				),
				"Metadata only, so nothing in your data is at risk -- but the recorded history \
				 of this rollout changes, and a repair applied to the wrong rollout hides a \
				 genuinely interrupted one.",
				Some(
					"call `rollout_status` first to confirm it really is stuck in a running_* state",
				),
			);
		}
		self.run_rollout_phase(input, "repair").await
	}

	#[tool(
		name = "rollout_up",
		description = "Run every rollout this database has not run yet, in order. Each one \
		               is started and completed, except the newest, which is started and \
		               left ready to complete so the application can cut over first; pass \
		               complete: true to finish that one too. This is the deploy path for \
		               a database that is several rollouts behind. Completing a rollout \
		               runs its destructive phase, so this refuses without confirm: true \
		               unless dry_run is set. Call it with dry_run first to see the order.",
		annotations(
			title = "Run pending rollouts",
			read_only_hint = false,
			destructive_hint = true,
			idempotent_hint = true,
			open_world_hint = true
		)
	)]
	pub async fn rollout_up(
		&self,
		Parameters(input): Parameters<RolloutUpInput>,
	) -> CallToolResult {
		if !input.dry_run && !input.confirm {
			return error::confirmation_required(
				"rollout_up",
				"the selected database target",
				"Starts and completes every rollout this database has not run yet, in order. \
				 Completing a rollout runs its REMOVE steps, which drop the objects its start \
				 phase superseded.",
				"A completed rollout can no longer be rolled back, and the data in what it \
				 dropped is gone. The newest rollout is left ready to complete unless \
				 complete: true is passed.",
				Some(
					"call `rollout_up` with dry_run: true to see which rollouts would run, and in what order",
				),
			);
		}

		let server = self.clone();
		run_captured(
			|report: &RolloutUpReport| {
				let verb = if report.dry_run {
					"rollout up (dry run)"
				} else {
					"rollout up"
				};
				let waiting = report
					.waiting
					.as_deref()
					.map(|id| format!(", {id} waiting to complete"))
					.unwrap_or_default();
				format!(
					"{verb}: {} pending, {} completed{waiting} on {}",
					report.pending.len(),
					report.completed.len(),
					report.target.target
				)
			},
			|| async move {
				let ctx = server.context(&input.work)?;
				let selection = ctx.selection(&input.work)?;
				// `up` walks every manifest, so every manifest has to pass.
				check_all_rollout_ids(&ctx)?;
				let _rollouts = server.lock_rollouts().await;
				let _guard = server.lock_folder(&ctx.folder).await;
				let (db, target) = connect_single(&selection).await?;
				let report = crate::rollout::run_up(
					&db,
					&ctx.folder,
					RolloutUpOpts {
						complete_newest: input.complete,
						from: input.from.clone(),
						dry_run: input.dry_run,
						query_timeout: target.cfg().query_timeout,
						quiet: false,
					},
					&ctx.vars,
				)
				.await?;
				Ok(RolloutUpReport {
					target: TargetOutcome::of(&target),
					dry_run: input.dry_run,
					pending: report.pending,
					completed: report.completed,
					waiting: report.waiting,
					skipped: report.skipped,
				})
			},
		)
		.await
	}

	#[tool(
		name = "rollout_freeze",
		description = "Give a manifest planned before 1.0.0-beta.6 its own copy of the SQL it \
		               applies, under rollouts/<id>/, so it can run anywhere rather than only \
		               against the schema it was planned from. Run it where the schema folder \
		               still matches what the manifest was planned from. A manifest that is \
		               already frozen is left alone. Touches no database.",
		annotations(
			title = "Freeze rollout",
			read_only_hint = false,
			destructive_hint = false,
			idempotent_hint = true,
			open_world_hint = false
		)
	)]
	pub async fn rollout_freeze(
		&self,
		Parameters(input): Parameters<RolloutFreezeInput>,
	) -> CallToolResult {
		let server = self.clone();
		run_captured(
			|report: &RolloutFileReport| format!("froze {}", report.rollout_id),
			|| async move {
				let ctx = server.context(&input.work)?;
				let selector = contain_rollout_selector(&ctx, &input.rollout_id)?;
				let _guard = server.lock_folder(&ctx.folder).await;
				crate::rollout::run_freeze(&ctx.folder, &selector)?;
				Ok(RolloutFileReport {
					rollout_id: input.rollout_id.clone(),
					action: "freeze",
				})
			},
		)
		.await
	}

	#[tool(
		name = "rollout_discard",
		description = "Drop the newest rollout from the project, as if it had never been \
		               planned: delete its manifest and its directory, and put the snapshots \
		               back to where they were before it. Only for a rollout no database \
		               has completed. A database that has run it is left past the end of \
		               the chain and cannot be placed in it, so roll it back there first. \
		               Touches no database, so it cannot check that for you. Requires \
		               confirm: true.",
		annotations(
			title = "Discard rollout",
			read_only_hint = false,
			destructive_hint = true,
			idempotent_hint = false,
			open_world_hint = false
		)
	)]
	pub async fn rollout_discard(
		&self,
		Parameters(input): Parameters<RolloutDiscardInput>,
	) -> CallToolResult {
		if !input.confirm {
			return error::confirmation_required(
				"rollout_discard",
				"the project's rollouts/ directory on disk",
				&format!(
					"Deletes the manifest for {:?} and its rollouts/<id>/ directory, and resets \
					 snapshots/ to where they were before it was planned.",
					input.rollout_id
				),
				"Uncommitted changes to that manifest are lost. A database that has already \
				 completed this rollout is left past the end of the chain; nothing on disk \
				 records that it ran.",
				Some(
					"call `rollout_status` against each target first to confirm none has completed it",
				),
			);
		}

		let server = self.clone();
		run_captured(
			|report: &RolloutFileReport| format!("discarded {}", report.rollout_id),
			|| async move {
				let ctx = server.context(&input.work)?;
				let selector = contain_rollout_selector(&ctx, &input.rollout_id)?;
				let _guard = server.lock_folder(&ctx.folder).await;
				crate::rollout::run_discard(&ctx.folder, &selector, input.keep_snapshots)?;
				Ok(RolloutFileReport {
					rollout_id: input.rollout_id.clone(),
					action: "discard",
				})
			},
		)
		.await
	}
}

impl SurrealKitMcp {
	/// Shared body for the four phase tools.
	async fn run_rollout_phase(
		&self,
		input: RolloutSelectorInput,
		phase: &'static str,
	) -> CallToolResult {
		let server = self.clone();
		let rollout_id = input.rollout_id.clone();
		run_captured(
			move |report: &RolloutRunReport| {
				format!(
					"rollout {} {}: {} (ns {}, db {})",
					report.rollout_id,
					report.phase,
					report.target.target,
					report.target.namespace,
					report.target.database
				)
			},
			|| async move {
				let ctx = server.context(&input.work)?;
				let selection = ctx.selection(&input.work)?;
				let selector = contain_rollout_selector(&ctx, &input.rollout_id)?;
				let _rollouts = server.lock_rollouts().await;
				let _guard = server.lock_folder(&ctx.folder).await;
				let (db, target) = connect_single(&selection).await?;

				let opts = RolloutExecutionOpts {
					selector: Some(selector),
					query_timeout: target.cfg().query_timeout,
				};
				let folder = &ctx.folder;
				let vars = &ctx.vars;
				match phase {
					"start" => crate::rollout::run_start(&db, folder, opts, vars).await,
					"complete" => crate::rollout::run_complete(&db, folder, opts, vars).await,
					"rollback" => crate::rollout::run_rollback(&db, folder, opts, vars).await,
					_ => crate::rollout::run_repair(&db, folder, opts).await,
				}?;

				Ok(RolloutRunReport {
					rollout_id,
					phase,
					target: TargetOutcome::of(&target),
				})
			},
		)
		.await
	}
}
