use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};

use super::manifest::{Feature, TemplateManifest};
use super::source::TemplateFiles;
use surrealkit::constants::{fixtures_dir, schema_dir, seed_dir, suites_dir};

/// One file to write, with its resolved content already loaded from the source.
#[derive(Debug, Clone)]
pub struct EmitFile {
	pub dest: PathBuf,
	pub contents: String,
	pub feature_id: String,
}

/// The full set of files to write for a selection of features.
#[derive(Debug, Default)]
pub struct EmitPlan {
	pub files: Vec<EmitFile>,
}

/// Where each kind of file lands, preserving the relative subpath.
enum Dest {
	Schema,
	Seed,
	Suite,
	Fixture,
}

impl EmitPlan {
	/// Build a plan for `feature_ids` (already dependency-closed) by reading
	/// every contributed file from `source`. Detects cross-feature conflicts:
	/// two features targeting the same destination with differing content.
	pub fn build(
		folder: &str,
		manifest: &TemplateManifest,
		feature_ids: &[String],
		source: &dyn TemplateFiles,
	) -> Result<Self> {
		let mut files = Vec::new();
		for id in feature_ids {
			let feature = manifest
				.feature(id)
				.with_context(|| format!("feature '{id}' missing from manifest"))?;
			collect(folder, feature, Dest::Schema, &feature.schema, source, &mut files)?;
			collect(folder, feature, Dest::Seed, &feature.seed, source, &mut files)?;
			collect(folder, feature, Dest::Suite, &feature.suites, source, &mut files)?;
			collect(folder, feature, Dest::Fixture, &feature.fixtures, source, &mut files)?;
		}

		// Conflict detection: same dest, different content -> hard error before
		// any write, so emission stays all-or-nothing.
		let mut by_dest: HashMap<PathBuf, &EmitFile> = HashMap::new();
		for file in &files {
			if let Some(prev) = by_dest.get(&file.dest) {
				if prev.contents != file.contents {
					bail!(
						"templates conflict: features '{}' and '{}' both write {} with different content",
						prev.feature_id,
						file.feature_id,
						file.dest.display()
					);
				}
			} else {
				by_dest.insert(file.dest.clone(), file);
			}
		}

		Ok(EmitPlan {
			files,
		})
	}

	/// Write the plan. Skips files that already exist unless `force`. De-dupes
	/// identical (dest, content) pairs that two features legitimately share.
	pub fn write(&self, force: bool) -> Result<()> {
		let mut written: HashMap<&PathBuf, &str> = HashMap::new();
		for file in &self.files {
			// A duplicate already emitted this run with identical content: skip silently.
			if written.get(&file.dest).is_some_and(|c| *c == file.contents) {
				continue;
			}

			if file.dest.exists() && !force {
				println!("  skipped (exists): {}", display_rel(&file.dest));
				continue;
			}

			if let Some(parent) = file.dest.parent() {
				std::fs::create_dir_all(parent)
					.with_context(|| format!("creating {}", parent.display()))?;
			}
			std::fs::write(&file.dest, &file.contents)
				.with_context(|| format!("writing {}", file.dest.display()))?;
			println!("  + {}", display_rel(&file.dest));
			written.insert(&file.dest, &file.contents);
		}
		Ok(())
	}
}

fn collect(
	folder: &str,
	feature: &Feature,
	dest: Dest,
	rels: &[String],
	source: &dyn TemplateFiles,
	out: &mut Vec<EmitFile>,
) -> Result<()> {
	let base = match dest {
		Dest::Schema => schema_dir(folder),
		Dest::Seed => seed_dir(folder),
		Dest::Suite => suites_dir(folder),
		Dest::Fixture => fixtures_dir(folder),
	};
	let strip = match dest {
		Dest::Schema => "schema/",
		Dest::Seed => "seed/",
		Dest::Suite => "tests/suites/",
		Dest::Fixture => "tests/fixtures/",
	};
	for rel in rels {
		let contents = source.read_file(rel)?;
		// Preserve subpaths but drop the leading `schema/` / `tests/...` segment
		// so files land directly under the target dir.
		let leaf = rel.strip_prefix(strip).unwrap_or(rel);
		out.push(EmitFile {
			dest: base.join(leaf),
			contents,
			feature_id: feature.id.clone(),
		});
	}
	Ok(())
}

/// Render a path relative to the current dir when possible, for tidy output.
fn display_rel(path: &std::path::Path) -> String {
	std::env::current_dir()
		.ok()
		.and_then(|cwd| path.strip_prefix(&cwd).ok().map(|p| p.to_path_buf()))
		.unwrap_or_else(|| path.to_path_buf())
		.display()
		.to_string()
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use super::*;

	struct MapSource(HashMap<String, String>);
	impl TemplateFiles for MapSource {
		fn read_manifest(&self) -> Result<String> {
			Ok(self.0.get("template.toml").cloned().unwrap_or_default())
		}
		fn read_file(&self, rel: &str) -> Result<String> {
			self.0.get(rel).cloned().ok_or_else(|| anyhow::anyhow!("missing {rel}"))
		}
	}

	fn manifest() -> TemplateManifest {
		TemplateManifest::parse(
			r#"
schema_version = 1
name = "t"
[[features]]
id = "a"
name = "A"
schema = ["schema/org.surql"]
seed = ["seed/perms.surql"]
suites = ["tests/suites/a.toml"]
[[features]]
id = "b"
name = "B"
schema = ["schema/org.surql"]
"#,
		)
		.unwrap()
	}

	#[test]
	fn maps_files_to_destinations() {
		let mut files = HashMap::new();
		files.insert("schema/org.surql".to_string(), "DEFINE TABLE org;".to_string());
		files.insert("seed/perms.surql".to_string(), "UPSERT perm;".to_string());
		files.insert("tests/suites/a.toml".to_string(), "name='a'".to_string());
		let src = MapSource(files);
		let plan = EmitPlan::build("./database", &manifest(), &["a".to_string()], &src).unwrap();
		let dests: Vec<String> = plan.files.iter().map(|f| f.dest.display().to_string()).collect();
		assert!(dests.iter().any(|d| d.ends_with("database/schema/org.surql")));
		assert!(dests.iter().any(|d| d.ends_with("database/seed/perms.surql")));
		assert!(dests.iter().any(|d| d.ends_with("database/tests/suites/a.toml")));
	}

	#[test]
	fn identical_shared_file_is_not_a_conflict() {
		let mut files = HashMap::new();
		files.insert("schema/org.surql".to_string(), "same".to_string());
		files.insert("seed/perms.surql".to_string(), "UPSERT perm;".to_string());
		files.insert("tests/suites/a.toml".to_string(), "x".to_string());
		let src = MapSource(files);
		// a and b both contribute schema/org.surql with identical content.
		let plan = EmitPlan::build("./db", &manifest(), &["a".to_string(), "b".to_string()], &src)
			.unwrap();
		assert!(!plan.files.is_empty());
	}

	#[test]
	fn conflicting_shared_file_errors() {
		// Both features write schema/org.surql but with different content -> conflict.
		let mut files = HashMap::new();
		files.insert("schema/org.surql".to_string(), "one".to_string());
		files.insert("seed/perms.surql".to_string(), "UPSERT perm;".to_string());
		files.insert("tests/suites/a.toml".to_string(), "x".to_string());
		let src = MapSourceVarying(files);
		let err = EmitPlan::build("./db", &manifest(), &["a".to_string(), "b".to_string()], &src)
			.unwrap_err();
		assert!(err.to_string().contains("conflict"), "{err}");
	}

	// Returns a different body each read of the same key, to force a content mismatch.
	struct MapSourceVarying(HashMap<String, String>);
	impl TemplateFiles for MapSourceVarying {
		fn read_manifest(&self) -> Result<String> {
			Ok(String::new())
		}
		fn read_file(&self, rel: &str) -> Result<String> {
			use std::sync::atomic::{AtomicUsize, Ordering};
			static N: AtomicUsize = AtomicUsize::new(0);
			let base = self.0.get(rel).cloned().ok_or_else(|| anyhow::anyhow!("missing {rel}"))?;
			Ok(format!("{base}-{}", N.fetch_add(1, Ordering::SeqCst)))
		}
	}
}
