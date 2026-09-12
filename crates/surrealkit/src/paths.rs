//! Containment for caller-supplied paths.
//!
//! A path typed on a CLI carries the user's own authority: `surrealkit apply
//! /tmp/fix.surql` is exactly what they asked for. The same string arriving as a
//! tool argument does not -- it was chosen by a model, or by whatever fed that
//! model. [`safe_join`] is the boundary between the two: it resolves a relative
//! path against a base directory and refuses anything that escapes it, including
//! by symlink.

use std::path::{Component, Path, PathBuf};

use anyhow::{Result, bail};

/// Resolve `relative` against `base`, refusing anything that escapes `base`.
///
/// Rejects absolute paths, `..` components, and -- after canonicalisation --
/// symlinks pointing outside the tree. Canonicalisation is what makes the check
/// real: a lexical check alone cannot see that `rollouts/link.toml` is a symlink
/// to `/etc/passwd`.
///
/// `base` must exist. A `relative` that does not exist yet is still resolved, so
/// this can guard a path that is about to be created: its parent chain is
/// canonicalised and the final component appended.
pub fn safe_join(base: &Path, relative: &str) -> Result<PathBuf> {
	if relative.is_empty() {
		bail!("empty path");
	}
	let rel = Path::new(relative);
	if rel.is_absolute() {
		bail!(
			"absolute paths are not accepted here: {relative:?}; \
			 give a path relative to {}",
			base.display()
		);
	}
	// Reject `..` and rooted/prefix components before touching the filesystem, so
	// a traversal attempt never gets as far as a `canonicalize` that might follow
	// a symlink out of the tree.
	for component in rel.components() {
		match component {
			Component::Normal(_) | Component::CurDir => {}
			Component::ParentDir => {
				bail!("path {relative:?} escapes {} with '..'", base.display())
			}
			Component::RootDir | Component::Prefix(_) => {
				bail!("path {relative:?} must be relative to {}", base.display())
			}
		}
	}

	let base = base
		.canonicalize()
		.map_err(|e| anyhow::anyhow!("resolving base directory {}: {e}", base.display()))?;
	let joined = base.join(rel);

	// Canonicalise as much of the path as exists, so a not-yet-created file is
	// still checked against its real parent directory.
	let (probe, tail) = match joined.canonicalize() {
		Ok(resolved) => (resolved, None),
		Err(_) => {
			let parent = joined
				.parent()
				.ok_or_else(|| anyhow::anyhow!("path {relative:?} has no parent"))?;
			let resolved = parent
				.canonicalize()
				.map_err(|e| anyhow::anyhow!("resolving {}: {e}", parent.display()))?;
			let name = joined
				.file_name()
				.ok_or_else(|| anyhow::anyhow!("path {relative:?} has no final component"))?;
			(resolved, Some(name.to_owned()))
		}
	};

	if !probe.starts_with(&base) {
		bail!(
			"path {relative:?} resolves to {}, which is outside {}",
			probe.display(),
			base.display()
		);
	}

	Ok(match tail {
		Some(name) => probe.join(name),
		None => probe,
	})
}

#[cfg(test)]
mod tests {
	use std::fs;

	use tempfile::TempDir;

	use super::*;

	fn base() -> TempDir {
		let dir = TempDir::new().expect("tempdir");
		fs::create_dir_all(dir.path().join("rollouts")).expect("mkdir");
		fs::write(dir.path().join("rollouts/ok.toml"), "id = 'x'").expect("write");
		dir
	}

	#[test]
	fn a_relative_path_inside_the_base_resolves() {
		let dir = base();
		let got = safe_join(dir.path(), "rollouts/ok.toml").expect("should resolve");
		assert!(got.ends_with("rollouts/ok.toml"), "got {}", got.display());
	}

	#[test]
	fn a_path_that_does_not_exist_yet_still_resolves() {
		// `rollout plan` names a file it is about to write.
		let dir = base();
		let got = safe_join(dir.path(), "rollouts/new.toml").expect("should resolve");
		assert!(got.ends_with("rollouts/new.toml"));
	}

	#[test]
	fn absolute_paths_are_rejected() {
		let dir = base();
		let err = safe_join(dir.path(), "/etc/passwd").unwrap_err().to_string();
		assert!(err.contains("absolute"), "got: {err}");
	}

	#[test]
	fn parent_traversal_is_rejected() {
		let dir = base();
		let err = safe_join(dir.path(), "rollouts/../../outside.toml").unwrap_err().to_string();
		assert!(err.contains(".."), "got: {err}");
	}

	#[test]
	fn a_symlink_leaving_the_base_is_rejected() {
		// The case a lexical check cannot catch.
		let dir = base();
		let outside = TempDir::new().expect("outside tempdir");
		let secret = outside.path().join("secret.toml");
		fs::write(&secret, "id = 'leaked'").expect("write");

		#[cfg(unix)]
		std::os::unix::fs::symlink(&secret, dir.path().join("rollouts/link.toml"))
			.expect("symlink");
		#[cfg(windows)]
		std::os::windows::fs::symlink_file(&secret, dir.path().join("rollouts/link.toml"))
			.expect("symlink");

		let err = safe_join(dir.path(), "rollouts/link.toml").unwrap_err().to_string();
		assert!(err.contains("outside"), "got: {err}");
	}

	#[test]
	fn an_empty_path_is_rejected() {
		let dir = base();
		assert!(safe_join(dir.path(), "").is_err());
	}

	#[test]
	fn a_leading_current_dir_is_fine() {
		let dir = base();
		assert!(safe_join(dir.path(), "./rollouts/ok.toml").is_ok());
	}
}
