//! `rollout up`: run every rollout a database has not run yet, in order.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use surrealdb::Surreal;
use surrealdb::engine::any::Any;

use super::chain::{self, Baseline, ChainPlan, Ledger, RolloutChain};
use super::frozen::{TargetCatalog, preflight_frozen};
use super::{
	LoadedRolloutSpec, LockKeepAlive, StepContext, acquire_lock, canonicalise_manifest_paths,
	complete_inner, live_catalog, release_lock, start_inner,
};
use crate::constants::rollouts_dir;
use crate::module::Module;
use crate::schema_state::{
	build_catalog_snapshot, collect_schema_files, ensure_local_state_dirs, hash_schema_snapshot,
	snapshot_from_files, verify_schema_hash,
};
use crate::setup::run_setup;
use crate::variables::TemplateVars;

#[derive(Debug, Clone, Default)]
#[doc(hidden)]
pub struct RolloutUpOpts {
	/// Complete the newest pending rollout too, instead of leaving it ready to
	/// complete for the application to cut over first.
	pub complete_newest: bool,
	/// Treat this rollout as the first one the database still needs, when its
	/// position cannot be worked out.
	pub from: Option<String>,
	/// Report what would run, and run nothing.
	pub dry_run: bool,
	pub query_timeout: Option<Duration>,
	/// One line per rollout instead of one per step, for replays run as setup.
	pub quiet: bool,
}

/// What [`Rollouts::up`](super::Rollouts::up) did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct UpReport {
	/// The rollouts that were pending when it began, in order.
	pub pending: Vec<String>,
	/// The rollouts it completed, in order.
	pub completed: Vec<String>,
	/// The newest rollout, if it was started (or already ready) and left for
	/// `complete` after the application cuts over.
	pub waiting: Option<String>,
	/// Rollouts the database moved past without running, whose data steps never
	/// ran here.
	pub skipped: Vec<String>,
}

fn ids(manifests: &[&LoadedRolloutSpec]) -> Vec<String> {
	manifests.iter().map(|m| m.spec.id.clone()).collect()
}

/// Apply every pending rollout in order: start and complete each, except the
/// newest, which is started and left ready to complete unless
/// `opts.complete_newest` says otherwise.
#[doc(hidden)]
pub async fn run_up(
	db: &Surreal<Any>,
	folder: &str,
	opts: RolloutUpOpts,
	vars: &TemplateVars,
) -> Result<UpReport> {
	run_setup(db, folder).await?;
	ensure_local_state_dirs(folder)?;
	let chain = RolloutChain::load(&rollouts_dir(folder))?;
	for warning in &chain.warnings {
		log::warn!("{warning}");
	}
	for m in &chain.unchained {
		log::warn!(
			"rollout '{}' has no schema hashes, so `up` cannot place it; run it on its own with \
			 `surrealkit rollout start {}`",
			m.spec.id,
			m.spec.id
		);
	}

	// Its own lock, held for the whole run, so two deploys cannot interleave.
	// Each start and complete still takes the global lock as well.
	let module = Module::default_module();
	let lock = acquire_lock(db, &module, "up").await?;
	let keep_alive = LockKeepAlive::spawn(db, &lock);
	let result = up_locked(db, folder, &chain, &opts, vars).await;
	drop(keep_alive);
	let release = release_lock(db, &lock).await;
	match (result, release) {
		(Err(err), _) => Err(err),
		(Ok(_), Err(err)) => Err(err),
		(Ok(report), Ok(())) => Ok(report),
	}
}

async fn up_locked(
	db: &Surreal<Any>,
	folder: &str,
	chain: &RolloutChain,
	opts: &RolloutUpOpts,
	vars: &TemplateVars,
) -> Result<UpReport> {
	let module = Module::default_module();
	let ledger = Ledger::load(db).await?;
	let baseline = Baseline::load(db, &module, folder).await?;
	let plan = chain::locate(chain, &ledger, &baseline, opts.from.as_deref())?;
	report_history(&plan);
	let mut report = UpReport {
		pending: ids(&plan.pending),
		skipped: ids(&plan.skipped),
		..UpReport::default()
	};
	if plan.pending.is_empty() {
		log::info!("This database is {}; every rollout has run.", plan.position.describe());
		return Ok(report);
	}
	// Running a rolled-back rollout again is a decision `up` does not make on
	// its own: it was rolled back for a reason.
	if plan.head_status.as_deref() == Some("rolled_back") {
		bail!(chain::rolled_back_message(&plan));
	}

	// Everything that can be checked without the database is checked before any
	// rollout starts, so a missing or edited frozen file in the third rollout
	// cannot leave the first two applied and the run half done.
	let disk = collect_schema_files(folder)?;
	let last = plan.pending.len() - 1;
	let mut prepared: Vec<(LoadedRolloutSpec, TargetCatalog)> =
		Vec::with_capacity(plan.pending.len());
	for (idx, m) in plan.pending.iter().enumerate() {
		let mut loaded = (*m).clone();
		let target = if loaded.spec.is_frozen() {
			preflight_frozen(&loaded)?;
			TargetCatalog::frozen(&loaded)?
		} else {
			let id = loaded.spec.id.clone();
			let freeze_advice = format!(
				"rollout '{id}' was planned before 1.0.0-beta.6, so it does not carry its SQL and can \
				 only run against the schema it was planned from. Check out the commit that planned \
				 it, run `surrealkit rollout freeze {id}` there, and commit the result."
			);
			if idx != last {
				bail!(
					"{freeze_advice} Until then it cannot run as one of several pending rollouts."
				);
			}
			let prefixes = canonicalise_manifest_paths(&mut loaded.spec, &disk);
			verify_schema_hash(
				&snapshot_from_files(&disk),
				folder,
				&loaded.spec.target_schema_hash,
				&id,
				&prefixes,
			)
			.context(freeze_advice)?;
			TargetCatalog::Full(build_catalog_snapshot(&disk, false)?)
		};
		prepared.push((loaded, target));
	}
	let newest = plan.pending[last];
	if hash_schema_snapshot(&snapshot_from_files(&disk))? != newest.spec.target_schema_hash {
		log::warn!(
			"the schema folder has changes no rollout plans yet; `up` applies only planned \
			 rollouts, up to '{}'",
			newest.spec.id
		);
	}

	log::log!(
		if opts.quiet {
			log::Level::Debug
		} else {
			log::Level::Info
		},
		"This database is {}. Pending, in order: {}",
		plan.position.describe(),
		report.pending.join(", ")
	);
	if opts.dry_run {
		for (idx, (loaded, _)) in prepared.iter().enumerate() {
			let finish = idx != last || opts.complete_newest;
			log::info!(
				"  would {} {}",
				if finish {
					"start and complete"
				} else {
					"start"
				},
				loaded.spec.id
			);
		}
		return Ok(report);
	}

	let ctx = StepContext::new(vars, Some(folder), opts.query_timeout).quiet(opts.quiet);
	for (idx, (loaded, target)) in prepared.iter().enumerate() {
		let id = loaded.spec.id.as_str();
		let row = ledger.rows.get(id);
		let status = row.map(|row| row.status.as_str());
		let failed_completing =
			status == Some("failed") && row.is_some_and(|row| row.reached_complete);
		if status == Some("running_rollback") {
			bail!(
				"rollout '{id}' was interrupted while rolling back; finish that with `surrealkit \
				 rollout rollback {id}` or `surrealkit rollout repair {id}` first"
			);
		}
		let started =
			failed_completing || matches!(status, Some("ready_to_complete" | "running_complete"));
		if !started {
			let mut source = live_catalog(db, &module, Some(folder)).await?;
			super::repair_live_catalog(&mut source, folder)?;
			start_inner(db, loaded, &source, target, &ctx).await?;
		}
		if idx != last || opts.complete_newest {
			complete_inner(db, loaded, &ctx).await?;
			report.completed.push(id.to_string());
		} else if failed_completing {
			bail!(
				"rollout '{id}' failed while completing{}. Fix the cause, then run `surrealkit rollout \
				 complete {id}` or `surrealkit rollout up --complete`.",
				row.and_then(|row| row.last_error.as_deref())
					.map(|e| format!(": {e}"))
					.unwrap_or_default()
			);
		} else {
			// Starting it just said it is ready; say what comes next, or that it was
			// already waiting.
			log::info!(
				"{}Once the application has cut over, run `surrealkit rollout complete {id}` or \
				 `surrealkit rollout up --complete`.",
				if started {
					format!("Rollout {id} was already waiting at ready_to_complete. ")
				} else {
					String::new()
				}
			);
			report.waiting = Some(id.to_string());
		}
	}
	Ok(report)
}

/// Say what a database moved past without running, and what predates it.
pub(crate) fn report_history(plan: &ChainPlan<'_>) {
	for m in &plan.skipped {
		let steps = chain::data_steps(m);
		log::warn!(
			"rollout '{}' never ran here, although this database is past it{}",
			m.spec.id,
			if steps.is_empty() {
				String::new()
			} else {
				format!(
					", so its data steps ({}) never ran either; run them by hand if they are still \
					 needed",
					steps.join(", ")
				)
			}
		);
	}
	// Routine for any database synced or baselined before its first rollout, so
	// only at debug: it would otherwise print on every status and up.
	if !plan.untracked.is_empty() {
		log::debug!(
			"{} rollout(s) predate this database's history and are not run: {}",
			plan.untracked.len(),
			ids(&plan.untracked).join(", ")
		);
	}
}
