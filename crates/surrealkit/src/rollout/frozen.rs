//! Frozen files: the copy of each changed schema file that `rollout plan` keeps
//! beside a manifest, so the rollout applies what was planned rather than
//! whatever the schema folder holds when it runs.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Component, Path};

use anyhow::{Context, Result, bail};

use super::{FileRef, FrozenFile, LoadedRolloutSpec, RolloutAction, RolloutPhase};
use crate::core::sha256_hex;
use crate::schema_state::{
	CatalogEntity, CatalogSnapshot, EntityKey, SchemaFile, parse_schema_statements,
};

/// Hint for a hash mismatch that is only a line-ending conversion.
const CRLF_HINT: &str = "Only its line endings differ, which is what git's `core.autocrlf` does on \
	Windows checkouts. Mark frozen files as binary so git leaves them alone, e.g. with \
	`database/rollouts/** -text` in .gitattributes, and check them out again.";

/// Check a frozen entry's shape, independent of whether its file exists.
pub(crate) fn validate_frozen_entry(step_id: &str, file: &FrozenFile) -> Result<()> {
	let path = Path::new(&file.path);
	let traverses = path.components().any(|c| !matches!(c, Component::Normal(_)));
	if file.path.trim().is_empty() || file.path.contains('\\') || path.is_absolute() || traverses {
		bail!(
			"apply_files step '{step_id}' freezes {:?}, which is not a relative path inside the \
			 project (no `..`, no leading `/`, `/` as the separator)",
			file.path
		);
	}
	let is_sha256 = file.hash.len() == 64
		&& file.hash.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
	if !is_sha256 {
		bail!(
			"apply_files step '{step_id}' records {:?} as the hash of {}; expected 64 lowercase \
			 hex characters (a sha256)",
			file.hash,
			file.path
		);
	}
	Ok(())
}

/// Read a frozen file and check it against the hash its manifest recorded.
///
/// The hash covers the raw template: template variables are applied afterwards,
/// when the step runs.
pub(crate) fn read_frozen_file(root: &Path, rollout_id: &str, file: &FrozenFile) -> Result<String> {
	let location = root.join(&file.path);
	let raw = fs::read(&location).with_context(|| {
		format!(
			"rollout '{rollout_id}' applies {}, frozen at {}, which cannot be read. Frozen files \
			 travel with their manifest: commit and ship rollouts/{rollout_id}/ alongside \
			 rollouts/{rollout_id}.toml",
			file.path,
			location.display()
		)
	})?;
	let actual = sha256_hex(&raw);
	if actual != file.hash {
		let crlf_only = sha256_hex(&strip_carriage_returns(&raw)) == file.hash;
		bail!(
			"frozen file {} of rollout '{rollout_id}' does not match its manifest: expected \
			 sha256 {}, found {actual} at {}. Frozen files are what was reviewed and planned, so \
			 they must not be edited; plan a new rollout for a new change.{}",
			file.path,
			file.hash,
			location.display(),
			if crlf_only {
				format!(" {CRLF_HINT}")
			} else {
				String::new()
			}
		);
	}
	String::from_utf8(raw).with_context(|| {
		format!("frozen file {} of rollout '{rollout_id}' is not UTF-8", file.path)
	})
}

fn strip_carriage_returns(raw: &[u8]) -> Vec<u8> {
	let mut out = Vec::with_capacity(raw.len());
	let mut bytes = raw.iter().peekable();
	while let Some(&b) = bytes.next() {
		if b == b'\r' && bytes.peek() == Some(&&b'\n') {
			continue;
		}
		out.push(b);
	}
	out
}

/// The frozen files a rollout applies, in step order, with the phase of the step
/// that applies each.
fn frozen_entries(rollout: &LoadedRolloutSpec) -> Vec<(&RolloutPhase, &FrozenFile)> {
	rollout
		.spec
		.steps
		.iter()
		.flat_map(|step| match &step.action {
			RolloutAction::ApplyFiles {
				files,
			} => files
				.iter()
				.filter_map(|file| match file {
					FileRef::Frozen(frozen) => Some((&step.phase, frozen)),
					FileRef::Path(_) => None,
				})
				.collect(),
			_ => Vec::new(),
		})
		.collect()
}

/// Where a rollout's frozen files are, or why that is not known.
fn frozen_root(rollout: &LoadedRolloutSpec) -> Result<&Path> {
	rollout.frozen_root.as_deref().with_context(|| {
		format!(
			"rollout '{}' applies frozen files, but no project folder was given to find them in. \
			 Call `.folder(...)` on the Rollout so they resolve from <folder>/rollouts/{}/",
			rollout.spec.id, rollout.spec.id
		)
	})
}

/// Check, before anything is locked or recorded, that every frozen file a
/// rollout applies is present, unchanged and readable SurrealQL. A rollout that
/// cannot finish should not get as far as starting.
pub(crate) fn preflight_frozen(rollout: &LoadedRolloutSpec) -> Result<()> {
	let entries = frozen_entries(rollout);
	if entries.is_empty() {
		return Ok(());
	}
	let root = frozen_root(rollout)?;
	for (_, frozen) in entries {
		let sql = read_frozen_file(root, &rollout.spec.id, frozen)?;
		crate::surql_scan::scan(&sql).map_err(|err| {
			anyhow::anyhow!("frozen file {} of rollout '{}': {err}", frozen.path, rollout.spec.id)
		})?;
	}
	Ok(())
}

/// Read a frozen file for execution, wherever the rollout keeps them.
pub(crate) fn read_for_step(rollout: &LoadedRolloutSpec, frozen: &FrozenFile) -> Result<String> {
	read_frozen_file(frozen_root(rollout)?, &rollout.spec.id, frozen)
}

/// The catalog a rollout leaves behind, recorded when it starts and written to
/// `__entity` when it completes.
#[derive(Debug, Clone)]
pub(crate) enum TargetCatalog {
	/// The whole catalog, from the schema folder (or a library caller's
	/// `target_files`). This is what a manifest without frozen files records: it
	/// can only run against the schema it was planned from, so the folder is its
	/// target.
	Full(CatalogSnapshot),
	/// What a frozen rollout changes: the entities in its frozen files, which it
	/// defines, and the entities its remove steps drop. Applied to the catalog the
	/// database holds when the rollout starts, it gives the catalog the rollout
	/// leaves, however far the schema folder has moved on since.
	Delta {
		upsert: Vec<CatalogEntity>,
		remove: Vec<EntityKey>,
	},
}

impl TargetCatalog {
	/// The catalog after this rollout, starting from `source`.
	pub(crate) fn resolve(&self, source: &[CatalogEntity]) -> Vec<CatalogEntity> {
		match self {
			Self::Full(snapshot) => snapshot.entities.clone(),
			Self::Delta {
				upsert,
				remove,
			} => {
				let mut source = source.to_vec();
				crate::schema_state::reconcile_garbled(
					&mut source,
					upsert,
					crate::schema_state::Unmatched::Keep,
				);
				let mut by_key: BTreeMap<EntityKey, CatalogEntity> =
					source.iter().map(|entity| (entity.key(), entity.clone())).collect();
				for key in remove {
					by_key.remove(key);
				}
				// Sorted first, so that when two files define the same key the same
				// one wins as in `catalog_snapshot_to_map`.
				let mut upsert = upsert.clone();
				upsert.sort();
				for entity in upsert {
					by_key.insert(entity.key(), entity);
				}
				by_key.into_values().collect()
			}
		}
	}

	/// The delta of a frozen rollout, read from its frozen files.
	pub(crate) fn frozen(rollout: &LoadedRolloutSpec) -> Result<Self> {
		let mut files = Vec::new();
		for (phase, frozen) in frozen_entries(rollout) {
			if *phase == RolloutPhase::Rollback {
				continue;
			}
			files.push(SchemaFile {
				path: frozen.path.clone(),
				sql: read_for_step(rollout, frozen)?,
				hash: frozen.hash.clone(),
			});
		}
		Self::delta(&rollout.spec, &files)
	}

	/// The delta of `spec`, given the contents of the files it applies.
	pub(crate) fn delta(spec: &super::RolloutSpec, files: &[SchemaFile]) -> Result<Self> {
		let mut upsert = Vec::new();
		for file in files {
			let (entities, _) = parse_schema_statements(file, false)?;
			upsert.extend(entities);
		}
		let remove = spec
			.steps
			.iter()
			.filter(|step| step.phase != RolloutPhase::Rollback)
			.flat_map(|step| match &step.action {
				RolloutAction::RemoveEntities {
					entities,
				} => entities.clone(),
				_ => Vec::new(),
			})
			.collect();
		Ok(Self::Delta {
			upsert,
			remove,
		})
	}
}

/// Where a rollout's directory keeps the snapshots `plan` started from, the
/// project's `snapshots/` as they were before this rollout was planned.
/// `rollout discard` puts them back.
pub(crate) const BASE_SCHEMA_SNAPSHOT: &str = "snapshots/schema_snapshot.json";
pub(crate) const BASE_CATALOG_SNAPSHOT: &str = "snapshots/catalog_snapshot.json";

/// Write a rollout's frozen files, and the snapshots it was planned from, to
/// `rollouts/<id>/`, all or nothing.
///
/// They go to a temporary directory first, which is renamed into place, so an
/// interrupted plan never leaves a half-written directory that looks complete.
pub(crate) fn write_frozen_dir(
	rollouts_dir: &Path,
	rollout_id: &str,
	files: &[&SchemaFile],
	extra: &[(&str, String)],
) -> Result<()> {
	let target = rollouts_dir.join(rollout_id);
	if target.exists() {
		bail!(
			"{} already exists; refusing to overwrite another rollout's frozen files",
			target.display()
		);
	}
	let staging = rollouts_dir.join(format!(".{rollout_id}.tmp"));
	if staging.exists() {
		fs::remove_dir_all(&staging).with_context(|| format!("clearing {}", staging.display()))?;
	}
	let written = (|| -> Result<()> {
		for file in files {
			let path = staging.join(&file.path);
			if let Some(parent) = path.parent() {
				fs::create_dir_all(parent)
					.with_context(|| format!("creating {}", parent.display()))?;
			}
			fs::write(&path, file.sql.as_bytes())
				.with_context(|| format!("writing {}", path.display()))?;
		}
		for (rel, body) in extra {
			let path = staging.join(rel);
			if let Some(parent) = path.parent() {
				fs::create_dir_all(parent)
					.with_context(|| format!("creating {}", parent.display()))?;
			}
			fs::write(&path, body).with_context(|| format!("writing {}", path.display()))?;
		}
		fs::create_dir_all(&staging).with_context(|| format!("creating {}", staging.display()))?;
		fs::rename(&staging, &target)
			.with_context(|| format!("moving {} to {}", staging.display(), target.display()))
	})();
	if written.is_err() {
		let _ = fs::remove_dir_all(&staging);
	}
	written
}

#[cfg(test)]
mod tests {
	use test_case::test_case;

	use super::*;
	use crate::schema_state::EntityKind;

	fn frozen(path: &str, hash: &str) -> FrozenFile {
		FrozenFile {
			path: path.to_string(),
			hash: hash.to_string(),
		}
	}

	const HASH: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

	#[test_case("schema/a.surql", HASH, true ; "valid")]
	#[test_case("schema/sub/a.surql", HASH, true ; "nested")]
	#[test_case("/schema/a.surql", HASH, false ; "absolute")]
	#[test_case("schema/../../etc/passwd", HASH, false ; "traversal")]
	#[test_case("./schema/a.surql", HASH, false ; "dot segment")]
	#[test_case("schema\\a.surql", HASH, false ; "backslash")]
	#[test_case("", HASH, false ; "empty")]
	#[test_case("schema/a.surql", "abc", false ; "short hash")]
	#[test_case("schema/a.surql", "0123456789ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef", false ; "uppercase hash")]
	fn frozen_entries_are_validated(path: &str, hash: &str, ok: bool) {
		assert_eq!(validate_frozen_entry("s", &frozen(path, hash)).is_ok(), ok);
	}

	#[test]
	fn reading_checks_the_hash_and_explains_crlf() {
		let dir = tempfile::TempDir::new().unwrap();
		let sql = "DEFINE TABLE a;\nDEFINE TABLE b;\n";
		fs::create_dir_all(dir.path().join("schema")).unwrap();
		fs::write(dir.path().join("schema/a.surql"), sql).unwrap();
		let entry = frozen("schema/a.surql", &sha256_hex(sql.as_bytes()));
		assert_eq!(read_frozen_file(dir.path(), "r1", &entry).unwrap(), sql);

		fs::write(dir.path().join("schema/a.surql"), sql.replace('\n', "\r\n")).unwrap();
		let err = read_frozen_file(dir.path(), "r1", &entry).unwrap_err().to_string();
		assert!(
			err.contains("does not match its manifest") && err.contains("line endings"),
			"{err}"
		);

		fs::write(dir.path().join("schema/a.surql"), "DEFINE TABLE edited;").unwrap();
		let err = read_frozen_file(dir.path(), "r1", &entry).unwrap_err().to_string();
		assert!(err.contains(&format!("expected sha256 {}", entry.hash)), "{err}");
		assert!(!err.contains("line endings"), "{err}");

		let missing = frozen("schema/gone.surql", HASH);
		let err = format!("{:#}", read_frozen_file(dir.path(), "r1", &missing).unwrap_err());
		assert!(err.contains("rollouts/r1/") && err.contains("schema/gone.surql"), "{err}");
	}

	fn entity(kind: EntityKind, name: &str, path: &str, hash: &str) -> CatalogEntity {
		CatalogEntity {
			kind,
			scope: None,
			name: name.to_string(),
			source_path: path.to_string(),
			statement_hash: hash.to_string(),
			file_hash: hash.to_string(),
		}
	}

	#[test]
	fn a_delta_removes_then_upserts_and_keeps_everything_else() {
		let source = vec![
			entity(EntityKind::Table, "kept", "schema/k.surql", "1"),
			entity(EntityKind::Table, "dropped", "schema/d.surql", "1"),
			entity(EntityKind::Table, "changed", "schema/c.surql", "1"),
		];
		let delta = TargetCatalog::Delta {
			upsert: vec![
				entity(EntityKind::Table, "changed", "schema/moved.surql", "2"),
				entity(EntityKind::Table, "added", "schema/a.surql", "1"),
			],
			remove: vec![
				entity(EntityKind::Table, "dropped", "", "").key(),
				entity(EntityKind::Table, "added", "", "").key(),
			],
		};
		let names: Vec<(String, String)> =
			delta.resolve(&source).into_iter().map(|e| (e.name, e.source_path)).collect();
		assert_eq!(
			names,
			vec![
				("added".to_string(), "schema/a.surql".to_string()),
				("changed".to_string(), "schema/moved.surql".to_string()),
				("kept".to_string(), "schema/k.surql".to_string()),
			]
		);
	}

	#[test]
	fn a_full_target_ignores_the_source() {
		let full = TargetCatalog::Full(CatalogSnapshot {
			version: 2,
			entities: vec![entity(EntityKind::Table, "only", "schema/o.surql", "1")],
			operations: Vec::new(),
		});
		let out = full.resolve(&[entity(EntityKind::Table, "other", "schema/x.surql", "1")]);
		assert_eq!(out.len(), 1);
		assert_eq!(out[0].name, "only");
	}

	#[test]
	fn writing_the_frozen_dir_is_all_or_nothing() {
		let dir = tempfile::TempDir::new().unwrap();
		let a = SchemaFile {
			path: "schema/a.surql".to_string(),
			sql: "DEFINE TABLE a;".to_string(),
			hash: String::new(),
		};
		let nested = SchemaFile {
			path: "schema/sub/b.surql".to_string(),
			sql: "DEFINE TABLE b;".to_string(),
			hash: String::new(),
		};
		write_frozen_dir(dir.path(), "r1", &[&a, &nested], &[]).unwrap();
		assert_eq!(
			fs::read_to_string(dir.path().join("r1/schema/sub/b.surql")).unwrap(),
			"DEFINE TABLE b;"
		);
		assert!(!dir.path().join(".r1.tmp").exists());

		let err = write_frozen_dir(dir.path(), "r1", &[&a], &[]).unwrap_err().to_string();
		assert!(err.contains("already exists"), "{err}");

		// A rollout that changes no files still gets its directory, holding the
		// snapshots it was planned from.
		write_frozen_dir(dir.path(), "r2", &[], &[(BASE_SCHEMA_SNAPSHOT, "{}".to_string())])
			.unwrap();
		assert_eq!(
			fs::read_to_string(dir.path().join("r2").join(BASE_SCHEMA_SNAPSHOT)).unwrap(),
			"{}"
		);
	}
}
