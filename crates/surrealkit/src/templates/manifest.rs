use std::collections::{BTreeSet, HashMap};

use anyhow::{Result, bail};
use serde::Deserialize;

/// The manifest schema versions this build understands. Bump when the format
/// changes in a backward-incompatible way.
pub const SUPPORTED_SCHEMA_VERSION: u32 = 1;

/// A parsed `template.toml`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateManifest {
	pub schema_version: u32,
	pub name: String,
	#[serde(default)]
	pub display_name: Option<String>,
	#[serde(default)]
	pub description: Option<String>,
	#[serde(default)]
	pub features: Vec<Feature>,
}

/// One optionally-selectable feature within a template.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Feature {
	/// Stable machine id, used by `--feature` and for dependency edges.
	pub id: String,
	/// Human label shown in the multi-select prompt.
	pub name: String,
	#[serde(default)]
	pub description: Option<String>,
	/// Whether the feature is pre-checked / included by `--yes`.
	#[serde(default)]
	pub default: bool,
	/// Other feature ids this feature depends on (closed transitively).
	#[serde(default)]
	pub requires: Vec<String>,
	/// Files copied into `<folder>/schema/`, preserving their relative subpath.
	#[serde(default)]
	pub schema: Vec<String>,
	/// Files copied into `<folder>/seed/` (reference data, run by `surrealkit seed`).
	#[serde(default)]
	pub seed: Vec<String>,
	/// Files copied into `<folder>/tests/suites/`.
	#[serde(default)]
	pub suites: Vec<String>,
	/// Files copied into `<folder>/tests/fixtures/`.
	#[serde(default)]
	pub fixtures: Vec<String>,
}

impl Feature {
	/// All source paths this feature contributes, across destinations.
	pub fn all_paths(&self) -> impl Iterator<Item = &str> {
		self.schema
			.iter()
			.chain(self.seed.iter())
			.chain(self.suites.iter())
			.chain(self.fixtures.iter())
			.map(String::as_str)
	}
}

impl TemplateManifest {
	/// Parse and validate a manifest from raw TOML.
	pub fn parse(raw: &str) -> Result<Self> {
		let manifest: TemplateManifest =
			toml::from_str(raw).map_err(|e| anyhow::anyhow!("invalid template.toml: {e}"))?;
		manifest.validate()?;
		Ok(manifest)
	}

	pub fn feature(&self, id: &str) -> Option<&Feature> {
		self.features.iter().find(|f| f.id == id)
	}

	/// Validate schema version, unique ids, resolvable `requires`, no cycles,
	/// and safe (relative, no `..`) file paths.
	fn validate(&self) -> Result<()> {
		if self.schema_version > SUPPORTED_SCHEMA_VERSION {
			bail!(
				"template '{}' requires schema_version {} but this surrealkit understands up to {}; \
                 upgrade surrealkit",
				self.name,
				self.schema_version,
				SUPPORTED_SCHEMA_VERSION
			);
		}

		let mut seen = BTreeSet::new();
		for f in &self.features {
			if !seen.insert(f.id.as_str()) {
				bail!("duplicate feature id '{}' in template '{}'", f.id, self.name);
			}
		}

		for f in &self.features {
			for dep in &f.requires {
				if self.feature(dep).is_none() {
					bail!("feature '{}' requires unknown feature '{}'", f.id, dep);
				}
			}
			for path in f.all_paths() {
				if path.is_empty() {
					bail!("feature '{}' has an empty file path", f.id);
				}
				let p = std::path::Path::new(path);
				if p.is_absolute() || path.split(['/', '\\']).any(|c| c == "..") {
					bail!(
						"feature '{}' has an unsafe file path '{}' (must be relative, no '..')",
						f.id,
						path
					);
				}
			}
		}

		self.detect_cycles()?;
		Ok(())
	}

	fn detect_cycles(&self) -> Result<()> {
		// 0 = unvisited, 1 = on stack, 2 = done
		let mut state: HashMap<&str, u8> = HashMap::new();
		for f in &self.features {
			self.visit(&f.id, &mut state)?;
		}
		Ok(())
	}

	fn visit<'a>(&'a self, id: &'a str, state: &mut HashMap<&'a str, u8>) -> Result<()> {
		match state.get(id) {
			Some(2) => return Ok(()),
			Some(1) => bail!("dependency cycle detected involving feature '{}'", id),
			_ => {}
		}
		state.insert(id, 1);
		if let Some(f) = self.feature(id) {
			for dep in &f.requires {
				self.visit(dep, state)?;
			}
		}
		state.insert(id, 2);
		Ok(())
	}

	/// Expand a selection of feature ids to include all transitive `requires`.
	/// Returns the closed set in manifest order. Errors on unknown ids.
	pub fn resolve_closure(&self, selected: &[String]) -> Result<Vec<String>> {
		let mut included: BTreeSet<String> = BTreeSet::new();
		for id in selected {
			if self.feature(id).is_none() {
				let known: Vec<&str> = self.features.iter().map(|f| f.id.as_str()).collect();
				bail!("unknown feature '{}' (available: {})", id, known.join(", "));
			}
			self.collect_requires(id, &mut included);
		}
		// Preserve manifest declaration order for stable output.
		Ok(self.features.iter().map(|f| f.id.clone()).filter(|id| included.contains(id)).collect())
	}

	fn collect_requires(&self, id: &str, acc: &mut BTreeSet<String>) {
		if !acc.insert(id.to_string()) {
			return;
		}
		if let Some(f) = self.feature(id) {
			for dep in &f.requires {
				self.collect_requires(dep, acc);
			}
		}
	}

	/// Ids of features marked `default = true`.
	pub fn default_feature_ids(&self) -> Vec<String> {
		self.features.iter().filter(|f| f.default).map(|f| f.id.clone()).collect()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	const SAMPLE: &str = r#"
schema_version = 1
name = "default"

[[features]]
id = "organizations"
name = "Organizations"
default = true
schema = ["schema/org.surql"]

[[features]]
id = "teams"
name = "Teams"
requires = ["organizations"]
schema = ["schema/team.surql"]
"#;

	#[test]
	fn parses_and_validates_sample() {
		let m = TemplateManifest::parse(SAMPLE).unwrap();
		assert_eq!(m.features.len(), 2);
		assert_eq!(m.default_feature_ids(), vec!["organizations"]);
	}

	#[test]
	fn closure_pulls_in_requires() {
		let m = TemplateManifest::parse(SAMPLE).unwrap();
		let closed = m.resolve_closure(&["teams".to_string()]).unwrap();
		assert_eq!(closed, vec!["organizations", "teams"]);
	}

	#[test]
	fn unknown_feature_errors() {
		let m = TemplateManifest::parse(SAMPLE).unwrap();
		let err = m.resolve_closure(&["nope".to_string()]).unwrap_err();
		assert!(err.to_string().contains("unknown feature 'nope'"));
		assert!(err.to_string().contains("organizations"));
	}

	#[test]
	fn rejects_future_schema_version() {
		let raw = "schema_version = 99\nname = \"x\"\n";
		let err = TemplateManifest::parse(raw).unwrap_err();
		assert!(err.to_string().contains("schema_version"));
	}

	#[test]
	fn rejects_unknown_requires() {
		let raw = r#"
schema_version = 1
name = "x"
[[features]]
id = "a"
name = "A"
requires = ["ghost"]
"#;
		let err = TemplateManifest::parse(raw).unwrap_err();
		assert!(err.to_string().contains("unknown feature 'ghost'"));
	}

	#[test]
	fn rejects_dependency_cycle() {
		let raw = r#"
schema_version = 1
name = "x"
[[features]]
id = "a"
name = "A"
requires = ["b"]
[[features]]
id = "b"
name = "B"
requires = ["a"]
"#;
		let err = TemplateManifest::parse(raw).unwrap_err();
		assert!(err.to_string().contains("cycle"));
	}

	#[test]
	fn rejects_path_traversal() {
		let raw = r#"
schema_version = 1
name = "x"
[[features]]
id = "a"
name = "A"
schema = ["../../etc/passwd"]
"#;
		let err = TemplateManifest::parse(raw).unwrap_err();
		assert!(err.to_string().contains("unsafe file path"));
	}
}
