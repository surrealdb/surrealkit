//! `rollout freeze`: give a manifest planned before 1.0.0-beta.6 its own copy of
//! the SQL it applies, so it can run after newer rollouts were planned.
//!
//! It has to run where the schema folder still matches what the manifest was
//! planned from, typically a checkout of the commit that planned it.

use std::collections::BTreeMap;
use std::fs;

use anyhow::{Context, Result, anyhow, bail};
use toml_edit::{ArrayOfTables, DocumentMut, Item, Table, value};

use super::frozen::write_frozen_dir;
use super::{
	FileRef, RolloutAction, canonicalise_manifest_paths, load_rollout_spec, resolve_rollout_path,
	validate_rollout_spec,
};
use crate::constants::rollouts_dir;
use crate::schema_state::{
	SchemaFile, collect_schema_files, ensure_local_state_dirs, hash_schema_snapshot,
	snapshot_from_files, verify_schema_hash,
};

/// Freeze the rollout `selector` names. A manifest that is already frozen is
/// left alone.
#[doc(hidden)]
pub fn run_freeze(folder: &str, selector: &str) -> Result<()> {
	ensure_local_state_dirs(folder)?;
	let path = resolve_rollout_path(folder, Some(selector))?;
	let loaded = load_rollout_spec(path.clone())?;
	validate_rollout_spec(&loaded.spec)?;
	let id = loaded.spec.id.clone();
	if loaded.spec.is_frozen() {
		log::info!("Rollout {id} already carries its SQL; nothing to freeze.");
		return Ok(());
	}

	let files = collect_schema_files(folder)?;
	let mut spec = loaded.spec;
	let prefixes = canonicalise_manifest_paths(&mut spec, &files);
	verify_schema_hash(
		&snapshot_from_files(&files),
		folder,
		&spec.target_schema_hash,
		&id,
		&prefixes,
	)
	.context(
		"`rollout freeze` copies the schema folder as it is now, so it must match what the \
			 manifest was planned from. Check out the commit that planned it and freeze there.",
	)?;

	let by_path: BTreeMap<&str, &SchemaFile> = files.iter().map(|f| (f.path.as_str(), f)).collect();
	let mut frozen: BTreeMap<&str, &SchemaFile> = BTreeMap::new();
	for file in spec.file_refs() {
		let key = file.path();
		let schema_file = by_path.get(key).ok_or_else(|| {
			anyhow!("rollout '{id}' applies {key}, which is not in the schema folder to copy")
		})?;
		frozen.insert(key, schema_file);
	}

	let raw = fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
	let mut doc: DocumentMut =
		raw.parse().with_context(|| format!("parsing {}", path.display()))?;
	rewrite_files(&mut doc, &spec, &frozen)?;
	// Manifests from before 1.0.0-beta.2 recorded a hash of working-directory
	// paths. The verification above accepted it, and the canonical one replaces
	// it, so the manifest chains with the ones planned after it.
	let canonical = hash_schema_snapshot(&snapshot_from_files(&files))?;
	if spec.target_schema_hash != canonical {
		doc["target_schema_hash"] = value(canonical);
	}

	let rollouts = rollouts_dir(folder);
	let to_write: Vec<&SchemaFile> = frozen.values().copied().collect();
	write_frozen_dir(&rollouts, &id, &to_write)?;
	if let Err(err) = fs::write(&path, doc.to_string()) {
		let _ = fs::remove_dir_all(rollouts.join(&id));
		return Err(err).with_context(|| format!("writing {}", path.display()));
	}
	log::info!(
		"Froze {} file(s) for rollout {id} into {}.",
		to_write.len(),
		rollouts.join(&id).display()
	);
	log::warn!(
		"Freezing rewrote {}, which changes its checksum. A rollout that has started somewhere \
		 but not completed would no longer match its record there, so finish those first. Commit \
		 the manifest and its directory together.",
		path.display()
	);
	Ok(())
}

/// Replace every `apply_files` step's `files` with frozen entries, in place, so
/// comments and every other step stay exactly as they were.
fn rewrite_files(
	doc: &mut DocumentMut,
	spec: &super::RolloutSpec,
	frozen: &BTreeMap<&str, &SchemaFile>,
) -> Result<()> {
	let steps = doc
		.get_mut("steps")
		.ok_or_else(|| anyhow!("the manifest has apply_files steps but no `steps`"))?;
	let tables: Vec<&mut Table> = match steps {
		Item::ArrayOfTables(array) => array.iter_mut().collect(),
		Item::Value(toml_edit::Value::Array(array)) => {
			bail!(
				"this manifest writes its steps as an inline array (`steps = [...]`, {} entries). \
				 `rollout freeze` rewrites `[[steps]]` tables, the form `rollout plan` writes; \
				 convert it to that form first.",
				array.len()
			)
		}
		_ => bail!("`steps` in the manifest is not a list of steps"),
	};
	if tables.len() != spec.steps.len() {
		bail!("the manifest's steps do not match what it parsed to; rewrite it by hand");
	}
	for (table, step) in tables.into_iter().zip(&spec.steps) {
		let RolloutAction::ApplyFiles {
			files,
		} = &step.action
		else {
			continue;
		};
		let mut entries = ArrayOfTables::new();
		for file in files {
			let FileRef::Path(path) = file else {
				continue;
			};
			let schema_file = frozen[path.as_str()];
			let mut entry = Table::new();
			entry.insert("path", value(schema_file.path.clone()));
			entry.insert("hash", value(schema_file.hash.clone()));
			entries.push(entry);
		}
		table.insert("files", Item::ArrayOfTables(entries));
	}
	Ok(())
}
