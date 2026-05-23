use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use crate::constants::{
	named_rollouts_dir, named_schema_dir, named_seed_dir, named_state_dir, rollouts_dir,
	schema_dir, seed_dir, state_dir,
};

pub struct MigrateOpts {
	/// Name to give the new named schema (e.g. "main")
	pub schema_name: String,
	/// Root database folder (e.g. "./database")
	pub folder: String,
	/// Print what would happen without touching any files
	pub dry_run: bool,
}

struct Move {
	from: PathBuf,
	to: PathBuf,
	description: String,
}

pub fn run_migrate(opts: &MigrateOpts) -> Result<()> {
	let folder = &opts.folder;
	let name = &opts.schema_name;
	let dry_run = opts.dry_run;

	validate_source(folder, name)?;

	let moves = plan_moves(folder, name);

	if moves.is_empty() {
		println!("Nothing to migrate — no flat schema, seed, rollout, or snapshot files found.");
		return Ok(());
	}

	println!(
		"{}Migrating flat schema layout to named schema '{name}' in {folder}/\n",
		if dry_run { "[dry-run] " } else { "" }
	);

	for m in &moves {
		println!("  {} → {}", m.from.display(), m.to.display());
		println!("    ({})", m.description);
	}

	if dry_run {
		println!("\n[dry-run] No files were moved. Re-run without --dry-run to apply.");
		return Ok(());
	}

	for m in &moves {
		if let Some(parent) = m.to.parent() {
			fs::create_dir_all(parent)
				.with_context(|| format!("creating parent directory {}", parent.display()))?;
		}
		fs::rename(&m.from, &m.to).with_context(|| {
			format!("moving {} → {}", m.from.display(), m.to.display())
		})?;
	}

	// schema/ was fully relocated to schemas/<name>/ — remove it.
	let legacy_schema = schema_dir(folder);
	if legacy_schema.exists() {
		fs::remove_dir_all(&legacy_schema).with_context(|| {
			format!("removing legacy {}", legacy_schema.display())
		})?;
		println!("\n  Removed {}/", legacy_schema.display());
	}

	println!();
	println!("Migration complete.");
	println!();
	println!("Next steps:");
	println!(
		"  1. Add the following to surrealkit.toml (replacing ns/db with your actual values):\n"
	);
	println!("       [schema.{name}]");
	println!("       ns = \"<your-namespace>\"");
	println!("       db = \"<your-database>\"\n");
	println!("  2. Run `surrealkit sync --schema {name}` to verify the migration.");

	Ok(())
}

fn validate_source(folder: &str, name: &str) -> Result<()> {
	let src_schema = schema_dir(folder);
	let src_seed = seed_dir(folder);
	let src_rollouts = rollouts_dir(folder);
	let src_state = state_dir(folder);

	let has_schema = src_schema.exists();
	let has_seed = src_seed.exists() && has_non_schema_subdir(&src_seed, name);
	let has_rollouts = has_flat_manifests(&src_rollouts);
	let has_state = has_flat_snapshots(&src_state);

	if !has_schema && !has_seed && !has_rollouts && !has_state {
		bail!(
			"Nothing to migrate.\n\
			 Expected one or more of:\n\
			 \t{}/schema/    (SQL files)\n\
			 \t{}/seed/      (flat seed files or seed.surql)\n\
			 \t{}/rollouts/  (*.toml manifest files at root)\n\
			 \t{}/snapshots/ (*_snapshot.json files at root)",
			folder,
			folder,
			folder,
			folder
		);
	}

	// Make sure the destination named schema dir does not already exist.
	let dest_schema = named_schema_dir(folder, name);
	if dest_schema.exists() {
		bail!(
			"Destination schema directory already exists: {}\n\
			 Choose a different schema name or remove the existing directory first.",
			dest_schema.display()
		);
	}

	Ok(())
}

/// Returns true if `seed/` contains items that are NOT the named schema subdir
/// (i.e. flat seed content still exists).
fn has_non_schema_subdir(seed_root: &Path, schema_name: &str) -> bool {
	let Ok(entries) = fs::read_dir(seed_root) else {
		return false;
	};
	for entry in entries.flatten() {
		if entry.file_name() != schema_name {
			return true;
		}
	}
	false
}

/// Returns true if `rollouts/` has any .toml files directly inside (not in
/// a named subdirectory).
fn has_flat_manifests(rollouts: &Path) -> bool {
	if !rollouts.exists() {
		return false;
	}
	let Ok(entries) = fs::read_dir(rollouts) else {
		return false;
	};
	entries.flatten().any(|e| {
		e.path().is_file()
			&& e.path().extension().and_then(|s| s.to_str()) == Some("toml")
	})
}

/// Returns true if `snapshots/` has any *_snapshot.json files at the root.
fn has_flat_snapshots(state: &Path) -> bool {
	if !state.exists() {
		return false;
	}
	let Ok(entries) = fs::read_dir(state) else {
		return false;
	};
	entries.flatten().any(|e| {
		e.path().is_file()
			&& e.file_name()
				.to_string_lossy()
				.ends_with("_snapshot.json")
	})
}

fn plan_moves(folder: &str, name: &str) -> Vec<Move> {
	let mut moves = Vec::new();

	// 1. schema/ → schemas/<name>/
	let src_schema = schema_dir(folder);
	if src_schema.exists() {
		let dest_schema = named_schema_dir(folder, name);
		collect_files(&src_schema, &src_schema, &dest_schema, &mut moves, "schema SQL");
	}

	// 2. seed/ flat files and seed.surql → seed/<name>/
	let src_seed = seed_dir(folder);
	if src_seed.exists() {
		let dest_seed = named_seed_dir(folder, name);
		collect_flat_seed_files(&src_seed, &dest_seed, &mut moves);
	}

	// 3. rollouts/*.toml → rollouts/<name>/
	let src_rollouts = rollouts_dir(folder);
	if src_rollouts.exists() {
		let dest_rollouts = named_rollouts_dir(folder, name);
		collect_files_matching(&src_rollouts, &dest_rollouts, "toml", &mut moves, "rollout manifest");
	}

	// 4. snapshots/*_snapshot.json → snapshots/<name>/
	let src_state = state_dir(folder);
	if src_state.exists() {
		let dest_state = named_state_dir(folder, name);
		collect_files_matching(&src_state, &dest_state, "json", &mut moves, "plan snapshot");
	}

	moves
}

/// Recursively collect all files under `src_root` and map them to `dest_root`.
fn collect_files(
	current: &Path,
	src_root: &Path,
	dest_root: &Path,
	moves: &mut Vec<Move>,
	kind: &str,
) {
	let Ok(entries) = fs::read_dir(current) else {
		return;
	};
	for entry in entries.flatten() {
		let path = entry.path();
		if path.is_dir() {
			collect_files(&path, src_root, dest_root, moves, kind);
		} else if path.is_file() {
			let rel = path.strip_prefix(src_root).unwrap_or(&path);
			moves.push(Move {
				from: path.clone(),
				to: dest_root.join(rel),
				description: kind.to_string(),
			});
		}
	}
}

/// Collect top-level files inside `seed/` (including seed.surql from the parent)
/// that don't look like they already belong to a named schema subdir.
fn collect_flat_seed_files(src_seed: &Path, dest_seed: &Path, moves: &mut Vec<Move>) {
	let Ok(entries) = fs::read_dir(src_seed) else {
		return;
	};
	for entry in entries.flatten() {
		let path = entry.path();
		// Only move flat files at the seed/ root, not existing named subdirs.
		if path.is_file() {
			let fname = path.file_name().unwrap_or_default().to_string_lossy();
			moves.push(Move {
				from: path.clone(),
				to: dest_seed.join(fname.as_ref()),
				description: "seed file".to_string(),
			});
		}
	}
}

/// Collect files with a given extension directly inside `src` (not recursive).
fn collect_files_matching(
	src: &Path,
	dest: &Path,
	ext: &str,
	moves: &mut Vec<Move>,
	kind: &str,
) {
	let Ok(entries) = fs::read_dir(src) else {
		return;
	};
	for entry in entries.flatten() {
		let path = entry.path();
		if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some(ext) {
			let fname = path.file_name().unwrap_or_default().to_string_lossy();
			moves.push(Move {
				from: path.clone(),
				to: dest.join(fname.as_ref()),
				description: kind.to_string(),
			});
		}
	}
}

