//! The order manifests apply in, and where a database is in that order.
//!
//! `rollout plan` diffs against the snapshots the previous plan wrote, so each
//! manifest's `source_schema_hash` is the previous one's `target_schema_hash`.
//! Those hashes chain the manifests together, and the same hashes recorded on
//! `__rollout` say how far along the chain a database is. Nothing new is
//! stored: the position comes from the latest completed rollout, or, for a
//! database that has only been baselined, from the file hashes baseline wrote.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde_json::Value;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb_types::SurrealValue;

use super::{LoadedRolloutSpec, RolloutPhase, RolloutStatus, load_rollout_spec, string_field};
use crate::constants::rollouts_dir;
use crate::module::Module;
use crate::schema_state::{
	SchemaSnapshot, SchemaSnapshotEntry, hash_schema_snapshot, strip_folder_prefix,
};

/// The hash of a schema with no files, which is where the first manifest of a
/// project planned from nothing starts.
pub(crate) fn empty_schema_hash() -> String {
	hash_schema_snapshot(&SchemaSnapshot {
		version: 1,
		files: Vec::new(),
	})
	.expect("an empty snapshot serializes")
}

fn short(hash: &str) -> &str {
	&hash[..hash.len().min(12)]
}

/// The manifests in a rollouts directory, in the order they apply.
#[derive(Debug, Default)]
pub(crate) struct RolloutChain {
	pub order: Vec<LoadedRolloutSpec>,
	/// Manifests without schema hashes (written by hand or built in code), which
	/// cannot be placed. They still run one at a time with `rollout start`.
	pub unchained: Vec<LoadedRolloutSpec>,
	pub warnings: Vec<String>,
}

impl RolloutChain {
	/// Load and order every `*.toml` directly in `dir`. Subdirectories are the
	/// manifests' frozen files and are not read here.
	pub(crate) fn load(dir: &Path) -> Result<Self> {
		if !dir.is_dir() {
			return Ok(Self::default());
		}
		let mut paths: Vec<PathBuf> = fs::read_dir(dir)
			.with_context(|| format!("reading {}", dir.display()))?
			.filter_map(|entry| entry.ok().map(|entry| entry.path()))
			.filter(|path| path.is_file() && path.extension().is_some_and(|ext| ext == "toml"))
			.collect();
		paths.sort();

		let mut warnings = Vec::new();
		let mut seen: BTreeMap<String, PathBuf> = BTreeMap::new();
		let mut chained = Vec::new();
		let mut unchained = Vec::new();
		for path in paths {
			let loaded = load_rollout_spec(path.clone())?;
			let id = loaded.spec.id.clone();
			if let Some(previous) = seen.insert(id.clone(), path.clone()) {
				bail!(
					"two manifests have the rollout id '{id}': {} and {}",
					previous.display(),
					path.display()
				);
			}
			if path.file_stem().and_then(|stem| stem.to_str()) != Some(id.as_str()) {
				warnings.push(format!(
					"{} has the rollout id '{id}', which differs from its file name; its frozen \
					 files are looked for under rollouts/{id}/",
					path.display()
				));
			}
			if loaded.spec.source_schema_hash.is_empty()
				|| loaded.spec.target_schema_hash.is_empty()
			{
				unchained.push(loaded);
			} else {
				chained.push(loaded);
			}
		}
		let order = order_by_hash(chained, &mut warnings)?;
		Ok(Self {
			order,
			unchained,
			warnings,
		})
	}

	pub(crate) fn position_of(&self, id: &str) -> Option<usize> {
		self.order.iter().position(|m| m.spec.id == id)
	}
}

fn is_noop(m: &LoadedRolloutSpec) -> bool {
	m.spec.source_schema_hash == m.spec.target_schema_hash
}

/// Order manifests by following target hash to source hash.
///
/// A manifest that changes no schema (source equals target, a data-only
/// rollout) shares its source with the next one that does. Within such a group
/// the no-ops run first, in id order, and the one that changes the schema runs
/// last, so it must also be the newest. Two that change the schema from the
/// same source, or a no-op planned after the change it sits beside, are a fork:
/// two branches each planned from the same snapshots.
fn order_by_hash(
	manifests: Vec<LoadedRolloutSpec>,
	warnings: &mut Vec<String>,
) -> Result<Vec<LoadedRolloutSpec>> {
	#[derive(Default)]
	struct Group {
		noops: Vec<usize>,
		changer: Option<usize>,
	}
	let mut groups: BTreeMap<&str, Group> = BTreeMap::new();
	for (idx, m) in manifests.iter().enumerate() {
		let group = groups.entry(m.spec.source_schema_hash.as_str()).or_default();
		if is_noop(m) {
			group.noops.push(idx);
			continue;
		}
		if let Some(other) = group.changer {
			bail!(fork_message(&manifests[other], m));
		}
		group.changer = Some(idx);
	}
	for group in groups.values_mut() {
		group.noops.sort_by(|a, b| manifests[*a].spec.id.cmp(&manifests[*b].spec.id));
		if let (Some(changer), Some(&last_noop)) = (group.changer, group.noops.last())
			&& manifests[last_noop].spec.id > manifests[changer].spec.id
		{
			bail!(fork_message(&manifests[changer], &manifests[last_noop]));
		}
	}

	// A root is a source no schema change leads to.
	let reached: BTreeSet<&str> = manifests
		.iter()
		.filter(|m| !is_noop(m))
		.map(|m| m.spec.target_schema_hash.as_str())
		.collect();
	let first_id = |group: &Group| {
		group.noops.iter().chain(group.changer.iter()).map(|i| manifests[*i].spec.id.clone()).min()
	};
	let mut roots: Vec<(&str, String)> = groups
		.iter()
		.filter(|(hash, _)| !reached.contains(**hash))
		.map(|(hash, group)| (*hash, first_id(group).unwrap_or_default()))
		.collect();
	roots.sort_by(|a, b| a.1.cmp(&b.1));

	let mut segments: Vec<Vec<usize>> = Vec::new();
	let mut visited: BTreeSet<&str> = BTreeSet::new();
	for (root, _) in &roots {
		let mut segment = Vec::new();
		let mut hash = *root;
		while let Some(group) = groups.get(hash) {
			if !visited.insert(hash) {
				break;
			}
			segment.extend(group.noops.iter().copied());
			match group.changer {
				Some(changer) => {
					segment.push(changer);
					hash = manifests[changer].spec.target_schema_hash.as_str();
				}
				None => break,
			}
		}
		segments.push(segment);
	}
	let placed: usize = segments.iter().map(Vec::len).sum();
	if placed != manifests.len() {
		let stray: Vec<&str> = manifests
			.iter()
			.enumerate()
			.filter(|(idx, _)| !segments.iter().flatten().any(|i| i == idx))
			.map(|(_, m)| m.spec.id.as_str())
			.collect();
		bail!(
			"these rollouts form a loop of schema hashes and cannot be ordered: {}. Each manifest \
			 must start from the schema the one before it produced.",
			stray.join(", ")
		);
	}

	for pair in segments.windows(2) {
		let before = &manifests[*pair[0].last().expect("segments are non-empty")];
		let after = &manifests[pair[1][0]];
		if before.spec.has_legacy_paths() || after.spec.has_legacy_paths() {
			warnings.push(format!(
				"rollout '{}' does not start from where '{}' ends, but one of them was planned \
				 before 1.0.0-beta.2, whose hashes depended on the working directory; assuming they \
				 run in id order",
				after.spec.id, before.spec.id
			));
			continue;
		}
		bail!(
			"rollout '{}' ends at schema {}, but no manifest starts there; the next one, '{}', \
			 starts from {}. A manifest is missing between them, or '{}' was planned from \
			 snapshots that were reverted or merged by hand. Restore the missing manifest, or \
			 discard '{}' with `surrealkit rollout discard` and plan it again.",
			before.spec.id,
			short(&before.spec.target_schema_hash),
			after.spec.id,
			short(&after.spec.source_schema_hash),
			after.spec.id,
			after.spec.id
		);
	}

	let order: Vec<usize> = segments.into_iter().flatten().collect();
	for pair in order.windows(2) {
		if manifests[pair[0]].spec.id > manifests[pair[1]].spec.id {
			warnings.push(format!(
				"rollout '{}' runs after '{}' although its id sorts first; ids come from the \
				 planning machine's clock, and the schema hashes decide the order",
				manifests[pair[1]].spec.id, manifests[pair[0]].spec.id
			));
		}
	}
	let mut slots: Vec<Option<LoadedRolloutSpec>> = manifests.into_iter().map(Some).collect();
	Ok(order.into_iter().map(|idx| slots[idx].take().expect("each index once")).collect())
}

fn fork_message(a: &LoadedRolloutSpec, b: &LoadedRolloutSpec) -> String {
	let (first, second) = if a.spec.id <= b.spec.id {
		(a, b)
	} else {
		(b, a)
	};
	format!(
		"rollouts '{}' and '{}' were both planned from the same schema ({}), so they cannot both \
		 apply in order. That happens when two branches each ran `rollout plan`. Keep the other \
		 branch's snapshots, discard the later one with `surrealkit rollout discard {} \
		 --keep-snapshots`, and plan it again.",
		first.spec.id,
		second.spec.id,
		short(&first.spec.source_schema_hash),
		second.spec.id
	)
}

/// One `__rollout` row, as much of it as placement needs.
#[derive(Debug, Clone)]
pub(crate) struct LedgerRow {
	pub status: String,
	pub last_error: Option<String>,
	/// Whether any complete-phase step has been recorded, so a `failed` rollout
	/// is known to have failed while completing rather than while starting.
	pub reached_complete: bool,
}

/// What a database records about rollouts.
#[derive(Debug, Clone, Default)]
pub(crate) struct Ledger {
	pub rows: BTreeMap<String, LedgerRow>,
	/// The most recently completed rollout that has a target hash, and that hash.
	pub latest_completed: Option<(String, String)>,
}

impl Ledger {
	pub(crate) async fn load(db: &Surreal<Any>) -> Result<Self> {
		// The entity columns are O(catalog), so they are left out.
		let mut resp = db
			.query(
				"SELECT record::id(id) AS id, status, last_error, steps FROM __rollout; \
				 SELECT record::id(id) AS id, target_schema_hash, completed_at FROM __rollout \
				 WHERE status = 'completed' AND target_schema_hash != '' \
				 ORDER BY completed_at DESC LIMIT 1;",
			)
			.await?;
		let rows: Vec<surrealdb_types::Value> = resp.take(0)?;
		let latest: Option<surrealdb_types::Value> = resp.take(1)?;
		let mut ledger = Ledger::default();
		for row in rows {
			let row = Value::from_value(row).unwrap_or(Value::Null);
			let Some(id) = string_field(&row, "id") else {
				continue;
			};
			let reached_complete =
				row.get("steps").and_then(|v| v.as_array()).is_some_and(|steps| {
					steps.iter().any(|step| {
						step.get("phase").and_then(|v| v.as_str())
							== Some(RolloutPhase::Complete.as_str())
					})
				});
			ledger.rows.insert(
				id,
				LedgerRow {
					status: string_field(&row, "status").unwrap_or_default(),
					last_error: string_field(&row, "last_error"),
					reached_complete,
				},
			);
		}
		if let Some(latest) = latest.map(|v| Value::from_value(v).unwrap_or(Value::Null))
			&& let (Some(id), Some(hash)) =
				(string_field(&latest, "id"), string_field(&latest, "target_schema_hash"))
		{
			ledger.latest_completed = Some((id, hash));
		}
		Ok(ledger)
	}

	pub(crate) fn status(&self, id: &str) -> Option<&str> {
		self.rows.get(id).map(|row| row.status.as_str())
	}

	fn is_completed(&self, id: &str) -> bool {
		self.status(id) == Some(RolloutStatus::Completed.as_str())
	}
}

fn is_active(status: &str) -> bool {
	RolloutStatus::from_storage(status).is_some_and(|status| !status.is_terminal())
}

/// Where a database is in the chain, and how that was worked out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Position {
	/// After the most recently completed rollout.
	AfterRollout {
		id: String,
		hash: String,
	},
	/// At the schema `rollout baseline` recorded.
	Baseline {
		hash: String,
	},
	/// A database with nothing in it yet.
	Empty,
	/// Wherever `--from` said.
	From(String),
}

impl Position {
	fn hash(&self) -> Option<String> {
		match self {
			Self::AfterRollout {
				hash,
				..
			}
			| Self::Baseline {
				hash,
			} => Some(hash.clone()),
			Self::Empty => Some(empty_schema_hash()),
			Self::From(_) => None,
		}
	}

	pub(crate) fn describe(&self) -> String {
		match self {
			Self::AfterRollout {
				id,
				..
			} => format!("after rollout '{id}'"),
			Self::Baseline {
				hash,
			} => format!(
				"at the schema its file hashes record, from `rollout baseline` or `sync` (schema {})",
				short(hash)
			),
			Self::Empty => "empty".to_string(),
			Self::From(id) => format!("starting from '{id}' (--from)"),
		}
	}
}

/// What a database still has to run, and what it has already run, skipped, or
/// predates.
#[derive(Debug)]
pub(crate) struct ChainPlan<'a> {
	pub position: Position,
	pub applied: Vec<&'a LoadedRolloutSpec>,
	/// In order. A rollout already in progress, if any, comes first.
	pub pending: Vec<&'a LoadedRolloutSpec>,
	/// What the database records for the first pending rollout, if anything: an
	/// in-progress status, or `rolled_back` for one that was rolled back here and
	/// has to run again (or be discarded) before anything after it.
	pub head_status: Option<String>,
	/// Never run here, although this database has moved past them, so their
	/// `run_sql` and `assert_sql` steps never ran either.
	pub skipped: Vec<&'a LoadedRolloutSpec>,
	/// From before this database's history began.
	pub untracked: Vec<&'a LoadedRolloutSpec>,
}

/// What placement knows about the database beyond its ledger.
#[derive(Debug, Clone, Default)]
pub(crate) struct Baseline {
	/// The schema hash recomputed from the file hashes `baseline` (or `sync`)
	/// stored, if any were.
	pub sync_hash: Option<String>,
	/// No managed entities and no file hashes: a database nothing has been
	/// applied to.
	pub empty: bool,
}

impl Baseline {
	pub(crate) async fn load(db: &Surreal<Any>, module: &Module, folder: &str) -> Result<Self> {
		let stored = crate::sync::load_sync_hashes(db, module).await?;
		let entities = super::load_managed_entities(db, module, Some(folder)).await?;
		let sync_hash = if stored.is_empty() {
			None
		} else {
			let mut files: Vec<SchemaSnapshotEntry> = stored
				.into_iter()
				.map(|(path, hash)| SchemaSnapshotEntry {
					path: strip_folder_prefix(folder, &path),
					hash,
				})
				.collect();
			files.sort();
			Some(hash_schema_snapshot(&SchemaSnapshot {
				version: 1,
				files,
			})?)
		};
		Ok(Self {
			empty: sync_hash.is_none() && entities.is_empty(),
			sync_hash,
		})
	}
}

/// Work out where a database is in `chain` and what it has left to run.
pub(crate) fn locate<'a>(
	chain: &'a RolloutChain,
	ledger: &Ledger,
	baseline: &Baseline,
	from: Option<&str>,
) -> Result<ChainPlan<'a>> {
	let order = &chain.order;
	let (position, start) = match from {
		Some(id) => {
			let idx = chain.position_of(id).with_context(|| {
				format!("--from names rollout '{id}', which is not in the chain of manifests")
			})?;
			if let Some(done) = order[idx..].iter().find(|m| ledger.is_completed(&m.spec.id)) {
				bail!(
					"--from '{id}' would re-run rollout '{}', which already completed here",
					done.spec.id
				);
			}
			if baseline.empty && order[idx].spec.source_schema_hash != empty_schema_hash() {
				bail!(
					"--from '{id}' on an empty database would apply only what '{id}' changes, not \
					 the schema before it. Create the schema first (`surrealkit sync`, then \
					 `surrealkit rollout baseline`), or start from the first manifest."
				);
			}
			(Position::From(id.to_string()), idx)
		}
		None => {
			let position = if let Some((id, hash)) = &ledger.latest_completed {
				Position::AfterRollout {
					id: id.clone(),
					hash: hash.clone(),
				}
			} else if let Some(hash) = &baseline.sync_hash {
				Position::Baseline {
					hash: hash.clone(),
				}
			} else if baseline.empty {
				Position::Empty
			} else {
				bail!(
					"cannot tell which rollouts this database has run: it has managed entities but \
					 no completed rollout and no baseline. Run `surrealkit rollout baseline` if its \
					 schema matches the files, or name the first rollout it still needs with \
					 `--from <id>`."
				);
			};
			let hash = position.hash().expect("only --from has no hash");
			let start = match order.iter().position(|m| m.spec.source_schema_hash == hash) {
				Some(idx) => idx,
				None if order.last().is_none_or(|m| m.spec.target_schema_hash == hash) => {
					order.len()
				}
				None => match &position {
					Position::AfterRollout {
						id,
						..
					} if chain.position_of(id).is_some() => chain.position_of(id).expect("checked") + 1,
					Position::Empty => bail!(
						"this database is empty, and no manifest starts from an empty schema: the \
						 first rollout here was planned on top of an existing one (after `rollout \
						 baseline`). Create the schema first (`surrealkit sync`, then `surrealkit \
						 rollout baseline`), or pass `--from <id>`."
					),
					_ => bail!(
						"this database is {} (schema {}), and no manifest starts from there. Its \
						 schema was changed some other way, or the manifests it ran are not in this \
						 directory. If you know which rollout it needs next, pass `--from <id>`.",
						position.describe(),
						short(&hash)
					),
				},
			};
			(position, start)
		}
	};

	let mut plan = ChainPlan {
		position,
		applied: Vec::new(),
		pending: Vec::new(),
		head_status: None,
		skipped: Vec::new(),
		untracked: Vec::new(),
	};

	for m in &order[start..] {
		let id = m.spec.id.as_str();
		match ledger.status(id) {
			Some("completed") => {
				if let Some(first) = plan.pending.first() {
					bail!(
						"rollout '{id}' completed here before '{}', which comes earlier in the chain \
						 and has not. The history is out of order, so this database cannot be \
						 caught up automatically; check `surrealkit rollout status` and decide how \
						 to reconcile it.",
						first.spec.id
					);
				}
				plan.applied.push(m);
			}
			// Rolled back (or abandoned): the database is back where it was before it,
			// so it is pending again. Whether to run it again or discard it is the
			// operator's call, which `up` and `start` ask for.
			Some("rolled_back") => plan.pending.push(m),
			Some(status) if is_active(status) => {
				if let Some(first) = plan.pending.first() {
					bail!(
						"rollout '{id}' is in progress ({status}), but '{}' comes before it and has \
						 not run. Finish or roll back '{id}' first.",
						first.spec.id
					);
				}
				plan.pending.push(m);
			}
			_ => plan.pending.push(m),
		}
	}

	plan.head_status =
		plan.pending.first().and_then(|m| ledger.status(&m.spec.id)).map(str::to_string);

	if let Some((id, row)) = ledger.rows.iter().find(|(_, row)| is_active(&row.status))
		&& !plan.pending.iter().any(|m| m.spec.id == *id)
	{
		bail!(
			"rollout '{id}' is in progress ({}), but it is not the next rollout in the chain. \
			 Finish it with `surrealkit rollout complete {id}`, or roll it back, first.",
			row.status
		);
	}

	// Before the start: anything not completed is either from before this
	// database's history, or a rollout it moved past without running.
	let baseline_idx = baseline
		.sync_hash
		.as_deref()
		.and_then(|hash| order.iter().position(|m| m.spec.source_schema_hash == hash));
	let first_completed = order.iter().position(|m| ledger.is_completed(&m.spec.id));
	let history_start = [baseline_idx, first_completed].into_iter().flatten().min();
	for (idx, m) in order[..start].iter().enumerate() {
		if ledger.is_completed(&m.spec.id) {
			plan.applied.push(m);
		} else if history_start.is_some_and(|h| idx >= h) {
			plan.skipped.push(m);
		} else {
			plan.untracked.push(m);
		}
	}
	plan.applied.sort_by_key(|m| chain.position_of(&m.spec.id));
	Ok(plan)
}

/// Load the chain and place this database in it.
pub(crate) async fn plan_for<'a>(
	db: &Surreal<Any>,
	folder: &str,
	chain: &'a RolloutChain,
	from: Option<&str>,
) -> Result<ChainPlan<'a>> {
	let module = Module::default_module();
	let ledger = Ledger::load(db).await?;
	let baseline = Baseline::load(db, &module, folder).await?;
	locate(chain, &ledger, &baseline, from)
}

/// The names of a rollout's `run_sql` and `assert_sql` steps, for warnings about
/// rollouts that were skipped.
pub(crate) fn data_steps(m: &LoadedRolloutSpec) -> Vec<String> {
	m.spec
		.steps
		.iter()
		.filter(|step| {
			matches!(
				step.action,
				super::RolloutAction::RunSql { .. } | super::RolloutAction::AssertSql { .. }
			)
		})
		.map(|step| step.id.clone())
		.collect()
}

/// Refuse to start a frozen rollout out of order.
pub(crate) async fn require_next(
	db: &Surreal<Any>,
	folder: &str,
	rollout: &LoadedRolloutSpec,
) -> Result<()> {
	let chain = RolloutChain::load(&rollouts_dir(folder))?;
	let id = rollout.spec.id.as_str();
	if chain.position_of(id).is_none() {
		// Written by hand without hashes, or not in this folder: there is no order
		// to check it against.
		log::warn!(
			"rollout '{id}' is not part of the manifest chain in {}, so its order is not checked",
			rollouts_dir(folder).display()
		);
		return Ok(());
	}
	let plan = plan_for(db, folder, &chain, None).await?;
	if plan.applied.iter().any(|m| m.spec.id == id) {
		// `start` reports it as already completed.
		return Ok(());
	}
	match plan.pending.iter().position(|m| m.spec.id == id) {
		// Starting a rolled-back rollout by name runs it again: that is what the
		// operator asked for.
		Some(0) => Ok(()),
		Some(_) if plan.head_status.as_deref() == Some("rolled_back") => {
			bail!(rolled_back_message(&plan))
		}
		Some(idx) => bail!(
			"rollout '{id}' is not next: this database is {}, and {} must run first. Run \
			 `surrealkit rollout up` to apply them in order.",
			plan.position.describe(),
			plan.pending[..idx]
				.iter()
				.map(|m| format!("'{}'", m.spec.id))
				.collect::<Vec<_>>()
				.join(", ")
		),
		None => bail!(
			"rollout '{id}' is not pending here: this database is {}, which is already past it. \
			 Check `surrealkit rollout status`.",
			plan.position.describe()
		),
	}
}

/// What to do about a rolled-back rollout at the head of the pending list.
pub(crate) fn rolled_back_message(plan: &ChainPlan<'_>) -> String {
	let head = &plan.pending[0].spec.id;
	let later: Vec<&str> = plan.pending[1..].iter().map(|m| m.spec.id.as_str()).collect();
	let (blocked, carries_on, discard_first) = if later.is_empty() {
		(String::new(), String::new(), String::new())
	} else {
		(
			format!(
				", and the {} planned after it ({}) cannot run until it does",
				later.len(),
				later.join(", ")
			),
			", then `surrealkit rollout up` carries on with the rest".to_string(),
			format!(
				" Discard the later ones first, newest first: {}.",
				later
					.iter()
					.rev()
					.map(|id| format!("`surrealkit rollout discard {id}`"))
					.collect::<Vec<_>>()
					.join(", ")
			),
		)
	};
	format!(
		"rollout '{head}' was rolled back here, so this database is where it was before \
		 it{blocked}. Either:\n\
		 \x20 - run it again once whatever made you roll it back is fixed: `surrealkit rollout \
		 start {head}`{carries_on}; or\n\
		 \x20 - drop it from the project with `surrealkit rollout discard {head}`, which deletes \
		 its manifest and directory and puts the snapshots back to before it was planned, then \
		 plan again.{discard_first}"
	)
}

/// For a manifest without frozen files, only point out an ordering problem; the
/// disk hash check is what guards it.
pub(crate) async fn warn_if_not_next(db: &Surreal<Any>, folder: &str, rollout: &LoadedRolloutSpec) {
	if let Err(err) = require_next(db, folder, rollout).await {
		log::warn!("{err:#}");
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::rollout::{RolloutAction, RolloutSpec, RolloutStep};

	fn manifest(id: &str, source: &str, target: &str) -> LoadedRolloutSpec {
		LoadedRolloutSpec {
			path: PathBuf::from(format!("{id}.toml")),
			checksum: id.to_string(),
			spec: RolloutSpec {
				source_schema_hash: source.to_string(),
				target_schema_hash: target.to_string(),
				..RolloutSpec::builder(id)
					.step(RolloutStep::apply_files(
						"apply",
						RolloutPhase::Start,
						vec!["schema/a.surql"],
					))
					.build()
			},
			frozen_root: None,
		}
	}

	fn legacy(id: &str, source: &str, target: &str) -> LoadedRolloutSpec {
		let mut m = manifest(id, source, target);
		m.spec.steps = vec![RolloutStep::apply_files(
			"apply",
			RolloutPhase::Start,
			vec!["/database/schema/a.surql"],
		)];
		m
	}

	fn chain(manifests: Vec<LoadedRolloutSpec>) -> Result<RolloutChain> {
		let mut warnings = Vec::new();
		let order = order_by_hash(manifests, &mut warnings)?;
		Ok(RolloutChain {
			order,
			unchained: Vec::new(),
			warnings,
		})
	}

	fn ids(manifests: &[&LoadedRolloutSpec]) -> Vec<String> {
		manifests.iter().map(|m| m.spec.id.clone()).collect()
	}

	fn order_ids(chain: &RolloutChain) -> Vec<String> {
		chain.order.iter().map(|m| m.spec.id.clone()).collect()
	}

	#[test]
	fn a_linear_chain_orders_by_hash_not_by_id() {
		let c =
			chain(vec![manifest("3", "B", "C"), manifest("1", "E", "A"), manifest("2", "A", "B")])
				.unwrap();
		assert_eq!(order_ids(&c), vec!["1", "2", "3"]);
		assert!(c.warnings.is_empty());

		// Clock skew: the ids disagree with the hashes. The hashes win, with a warning.
		let c = chain(vec![manifest("9", "E", "A"), manifest("2", "A", "B")]).unwrap();
		assert_eq!(order_ids(&c), vec!["9", "2"]);
		assert_eq!(c.warnings.len(), 1, "{:?}", c.warnings);
	}

	#[test]
	fn two_changes_from_one_schema_are_a_fork() {
		let err =
			chain(vec![manifest("1", "A", "B"), manifest("2", "A", "C")]).unwrap_err().to_string();
		assert!(
			err.contains("'1' and '2'") && err.contains("rollout discard 2 --keep-snapshots"),
			"{err}"
		);
	}

	#[test]
	fn no_op_rollouts_run_before_the_change_that_shares_their_source() {
		let c =
			chain(vec![manifest("3", "A", "B"), manifest("2", "A", "A"), manifest("1", "A", "A")])
				.unwrap();
		assert_eq!(order_ids(&c), vec!["1", "2", "3"]);
		// A trailing data-only rollout after the last schema change.
		let c = chain(vec![manifest("1", "A", "B"), manifest("2", "B", "B")]).unwrap();
		assert_eq!(order_ids(&c), vec!["1", "2"]);
	}

	#[test]
	fn a_no_op_planned_after_a_change_from_the_same_schema_is_a_fork() {
		// Branch one planned A -> B; branch two, from the same snapshots, planned a
		// data-only rollout later. They merge without a git conflict.
		let err =
			chain(vec![manifest("1", "A", "B"), manifest("2", "A", "A")]).unwrap_err().to_string();
		assert!(err.contains("both planned from the same schema"), "{err}");
	}

	#[test]
	fn a_gap_between_frozen_manifests_is_an_error() {
		let err =
			chain(vec![manifest("1", "A", "B"), manifest("2", "C", "D")]).unwrap_err().to_string();
		assert!(err.contains("rollout '1' ends at schema B") && err.contains("'2'"), "{err}");
	}

	#[test]
	fn a_gap_next_to_a_legacy_manifest_is_bridged_in_id_order() {
		let c = chain(vec![legacy("1", "A", "B"), manifest("2", "C", "D")]).unwrap();
		assert_eq!(order_ids(&c), vec!["1", "2"]);
		assert!(c.warnings.iter().any(|w| w.contains("before 1.0.0-beta.2")), "{:?}", c.warnings);
	}

	#[test]
	fn a_loop_is_an_error() {
		let err =
			chain(vec![manifest("1", "A", "B"), manifest("2", "B", "A")]).unwrap_err().to_string();
		assert!(err.contains("loop"), "{err}");
	}

	#[test]
	fn an_empty_chain_is_fine() {
		assert!(chain(Vec::new()).unwrap().order.is_empty());
	}

	fn ledger(rows: &[(&str, &str)], latest: Option<(&str, &str)>) -> Ledger {
		Ledger {
			rows: rows
				.iter()
				.map(|(id, status)| {
					(
						id.to_string(),
						LedgerRow {
							status: status.to_string(),
							last_error: None,
							reached_complete: false,
						},
					)
				})
				.collect(),
			latest_completed: latest.map(|(id, hash)| (id.to_string(), hash.to_string())),
		}
	}

	fn five() -> RolloutChain {
		chain(vec![
			manifest("101", "E", "A"),
			manifest("102", "A", "B"),
			manifest("103", "B", "C"),
			manifest("104", "C", "D"),
			manifest("105", "D", "F"),
		])
		.unwrap()
	}

	fn baseline(sync_hash: Option<&str>, empty: bool) -> Baseline {
		Baseline {
			sync_hash: sync_hash.map(str::to_string),
			empty,
		}
	}

	#[test]
	fn issue_91_a_database_on_102_has_three_pending_in_order() {
		let c = five();
		let l = ledger(&[("101", "completed"), ("102", "completed")], Some(("102", "B")));
		let plan = locate(&c, &l, &baseline(None, false), None).unwrap();
		assert_eq!(ids(&plan.pending), vec!["103", "104", "105"]);
		assert_eq!(ids(&plan.applied), vec!["101", "102"]);
		assert!(plan.skipped.is_empty() && plan.untracked.is_empty());
	}

	#[test]
	fn a_baselined_database_is_placed_by_its_file_hashes() {
		let c = five();
		let plan = locate(&c, &ledger(&[], None), &baseline(Some("B"), false), None).unwrap();
		assert_eq!(ids(&plan.pending), vec!["103", "104", "105"]);
		assert_eq!(ids(&plan.untracked), vec!["101", "102"]);
	}

	#[test]
	fn an_empty_database_runs_everything_planned_from_nothing() {
		let empty = empty_schema_hash();
		let c = chain(vec![manifest("1", &empty, "A"), manifest("2", "A", "B")]).unwrap();
		let plan = locate(&c, &ledger(&[], None), &baseline(None, true), None).unwrap();
		assert_eq!(ids(&plan.pending), vec!["1", "2"]);
		assert_eq!(plan.position, Position::Empty);
	}

	#[test]
	fn an_empty_database_cannot_start_mid_chain() {
		let err = locate(&five(), &ledger(&[], None), &baseline(None, true), None)
			.unwrap_err()
			.to_string();
		assert!(err.contains("no manifest starts from an empty schema"), "{err}");
	}

	#[test]
	fn a_database_with_entities_but_no_history_cannot_be_placed() {
		let err = locate(&five(), &ledger(&[], None), &baseline(None, false), None)
			.unwrap_err()
			.to_string();
		assert!(err.contains("rollout baseline") && err.contains("--from"), "{err}");
	}

	#[test]
	fn the_latest_completed_rollout_places_the_database_even_if_its_manifest_is_gone() {
		// Old manifests pruned from the repo: 101 and 102 are gone, 102 completed.
		let c = chain(vec![manifest("103", "B", "C"), manifest("104", "C", "D")]).unwrap();
		let plan = locate(
			&c,
			&ledger(&[("102", "completed")], Some(("102", "B"))),
			&baseline(None, false),
			None,
		)
		.unwrap();
		assert_eq!(ids(&plan.pending), vec!["103", "104"]);

		let err = locate(
			&c,
			&ledger(&[("99", "completed")], Some(("99", "Z"))),
			&baseline(None, false),
			None,
		)
		.unwrap_err()
		.to_string();
		assert!(err.contains("after rollout '99'"), "{err}");
	}

	#[test]
	fn up_to_date_and_already_completed_newest() {
		let c = five();
		let l = ledger(
			&[
				("101", "completed"),
				("102", "completed"),
				("103", "completed"),
				("104", "completed"),
				("105", "completed"),
			],
			Some(("105", "F")),
		);
		let plan = locate(&c, &l, &baseline(None, false), None).unwrap();
		assert!(plan.pending.is_empty());
		assert_eq!(plan.applied.len(), 5);
	}

	#[test]
	fn rollouts_a_database_moved_past_are_skipped() {
		// The #91 reporter's database: baselined at 102, then 105 run directly by a
		// release that only checked the schema folder.
		let c = five();
		let l = ledger(&[("105", "completed")], Some(("105", "F")));
		let plan = locate(&c, &l, &baseline(Some("B"), false), None).unwrap();
		assert!(plan.pending.is_empty());
		assert_eq!(ids(&plan.skipped), vec!["103", "104"]);
		assert_eq!(ids(&plan.untracked), vec!["101", "102"]);
	}

	#[test]
	fn a_completed_rollout_after_a_pending_one_is_out_of_order() {
		let c = five();
		let l = ledger(
			&[("101", "completed"), ("102", "completed"), ("104", "completed")],
			Some(("102", "B")),
		);
		let err = locate(&c, &l, &baseline(None, false), None).unwrap_err().to_string();
		assert!(err.contains("'104' completed here before '103'"), "{err}");
	}

	#[test]
	fn a_rolled_back_rollout_is_pending_again() {
		let c = five();
		let l = ledger(
			&[("101", "completed"), ("102", "completed"), ("103", "rolled_back")],
			Some(("102", "B")),
		);
		let plan = locate(&c, &l, &baseline(None, false), None).unwrap();
		assert_eq!(ids(&plan.pending), vec!["103", "104", "105"]);
		assert_eq!(plan.head_status.as_deref(), Some("rolled_back"));
		let message = rolled_back_message(&plan);
		assert!(message.contains("rollout start 103"), "{message}");
		assert!(message.contains("rollout discard 103"), "{message}");
		assert!(message.contains("the 2 planned after it (104, 105) cannot run"), "{message}");
		assert!(
			message.contains("`surrealkit rollout discard 105`, `surrealkit rollout discard 104`"),
			"{message}"
		);

		// The newest rolled back: nothing "after it" to mention.
		let l = ledger(
			&[
				("101", "completed"),
				("102", "completed"),
				("103", "completed"),
				("104", "completed"),
				("105", "rolled_back"),
			],
			Some(("104", "D")),
		);
		let plan = locate(&c, &l, &baseline(None, false), None).unwrap();
		let message = rolled_back_message(&plan);
		assert!(!message.contains("planned after it"), "{message}");
		assert!(!message.contains("Discard the later ones"), "{message}");
	}

	#[test]
	fn an_active_rollout_must_be_the_next_one() {
		let c = five();
		let l = ledger(
			&[("101", "completed"), ("102", "completed"), ("103", "ready_to_complete")],
			Some(("102", "B")),
		);
		let plan = locate(&c, &l, &baseline(None, false), None).unwrap();
		assert_eq!(ids(&plan.pending), vec!["103", "104", "105"]);

		let l = ledger(
			&[("101", "completed"), ("102", "completed"), ("104", "failed")],
			Some(("102", "B")),
		);
		let err = locate(&c, &l, &baseline(None, false), None).unwrap_err().to_string();
		assert!(err.contains("'104' is in progress (failed)"), "{err}");

		let l = ledger(
			&[("101", "completed"), ("102", "completed"), ("elsewhere", "running_start")],
			Some(("102", "B")),
		);
		let err = locate(&c, &l, &baseline(None, false), None).unwrap_err().to_string();
		assert!(err.contains("'elsewhere' is in progress"), "{err}");
	}

	#[test]
	fn from_is_guarded() {
		let c = five();
		let l = ledger(&[("101", "completed"), ("102", "completed")], Some(("102", "B")));
		let plan = locate(&c, &l, &baseline(None, false), Some("104")).unwrap();
		assert_eq!(ids(&plan.pending), vec!["104", "105"]);
		assert_eq!(plan.position, Position::From("104".to_string()));

		let err = locate(&c, &l, &baseline(None, false), Some("102")).unwrap_err().to_string();
		assert!(err.contains("already completed"), "{err}");
		let err = locate(&c, &l, &baseline(None, false), Some("nope")).unwrap_err().to_string();
		assert!(err.contains("not in the chain"), "{err}");
		let err = locate(&c, &ledger(&[], None), &baseline(None, true), Some("103"))
			.unwrap_err()
			.to_string();
		assert!(err.contains("empty database"), "{err}");
	}

	#[test]
	fn data_steps_lists_run_and_assert_steps() {
		let mut m = manifest("1", "A", "B");
		m.spec.steps.push(RolloutStep::run_sql(
			"backfill",
			RolloutPhase::Start,
			"UPDATE t SET x = 1;",
		));
		m.spec.steps.push(RolloutStep::assert_sql("check", RolloutPhase::Start, "RETURN 1", "1"));
		assert!(matches!(m.spec.steps[0].action, RolloutAction::ApplyFiles { .. }));
		assert_eq!(data_steps(&m), vec!["backfill", "check"]);
	}
}
