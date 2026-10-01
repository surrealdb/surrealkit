//! `rollout discard`: drop the newest rollout from the project, as if it had
//! never been planned.
//!
//! Deleting a manifest by hand is not enough: `plan` also moved the snapshots
//! on to where that rollout ends, so the next plan would find nothing to do.
//! Each rollout's directory keeps the snapshots it was planned from, and
//! discarding puts them back.

use std::fs;

use anyhow::{Context, Result, bail};

use super::chain::RolloutChain;
use super::frozen::{BASE_CATALOG_SNAPSHOT, BASE_SCHEMA_SNAPSHOT};
use super::{load_rollout_spec, resolve_rollout_path};
use crate::constants::{catalog_snapshot_path, rollouts_dir, schema_snapshot_path};
use crate::schema_state::{SchemaSnapshot, ensure_local_state_dirs, hash_schema_snapshot};

/// Discard the rollout `selector` names. It must be the newest in the chain.
/// With `keep_snapshots`, the manifest and its directory are deleted and the
/// snapshots are left as they are, for when they have been put right by hand.
#[doc(hidden)]
pub fn run_discard(folder: &str, selector: &str, keep_snapshots: bool) -> Result<()> {
	ensure_local_state_dirs(folder)?;
	let path = resolve_rollout_path(folder, Some(selector))?;
	let loaded = load_rollout_spec(path.clone())?;
	let id = loaded.spec.id.clone();
	let dir = rollouts_dir(folder).join(&id);

	let restore = if keep_snapshots {
		None
	} else {
		let chain = RolloutChain::load(&rollouts_dir(folder)).with_context(|| {
			format!(
				"the chain of rollouts has to be readable to discard one safely. If it is broken \
				 because of '{id}', put `snapshots/` right by hand and run `surrealkit rollout \
				 discard {id} --keep-snapshots`"
			)
		})?;
		if let Some(idx) = chain.position_of(&id) {
			let later: Vec<&str> =
				chain.order[idx + 1..].iter().map(|m| m.spec.id.as_str()).collect();
			if !later.is_empty() {
				bail!(
					"rollouts were planned after '{id}', on top of it: {}. Discard them first, newest \
					 first.",
					later.join(", ")
				);
			}
		}
		let schema_raw = fs::read_to_string(dir.join(BASE_SCHEMA_SNAPSHOT));
		let catalog_raw = fs::read_to_string(dir.join(BASE_CATALOG_SNAPSHOT));
		let (Ok(schema_raw), Ok(catalog_raw)) = (schema_raw, catalog_raw) else {
			bail!(
				"rollout '{id}' does not keep the snapshots it was planned from (rollouts planned \
				 before 1.0.0-beta.6 do not), so discard cannot put them back. Restore \
				 `snapshots/` from the commit before '{id}' was planned, for example with `git \
				 checkout <that commit> -- {}`, then run `surrealkit rollout discard {id} \
				 --keep-snapshots`.",
				schema_snapshot_path(folder)
					.parent()
					.map(|p| p.display().to_string())
					.unwrap_or_default()
			);
		};
		let schema: SchemaSnapshot = serde_json::from_str(&schema_raw)
			.with_context(|| format!("parsing {}", dir.join(BASE_SCHEMA_SNAPSHOT).display()))?;
		if hash_schema_snapshot(&schema)? != loaded.spec.source_schema_hash {
			bail!(
				"the snapshots kept in {} do not hash to where '{id}' starts, so they are not the \
				 ones it was planned from. Put `snapshots/` right by hand and run `surrealkit \
				 rollout discard {id} --keep-snapshots`.",
				dir.display()
			);
		}
		Some((schema_raw, catalog_raw))
	};

	if let Some((schema_raw, catalog_raw)) = restore {
		fs::write(schema_snapshot_path(folder), schema_raw)
			.with_context(|| format!("writing {}", schema_snapshot_path(folder).display()))?;
		fs::write(catalog_snapshot_path(folder), catalog_raw)
			.with_context(|| format!("writing {}", catalog_snapshot_path(folder).display()))?;
		log::info!("Put the snapshots back to where they were before '{id}' was planned.");
	}
	fs::remove_file(&path).with_context(|| format!("deleting {}", path.display()))?;
	if dir.exists() {
		fs::remove_dir_all(&dir).with_context(|| format!("deleting {}", dir.display()))?;
	}
	log::info!("Discarded rollout {id}. Plan again to roll its changes into a new one.");
	log::warn!(
		"Only discard a rollout that no database has completed. A database that has run it is now \
		 past the end of the chain and cannot be placed in it; roll it back there first."
	);
	Ok(())
}
