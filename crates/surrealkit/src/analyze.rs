//! `surrealkit check` / `generate` / `watch` — static analysis of the
//! project's SurrealQL through the `surrealql-analyzer` library.
//!
//! No database is involved. The analyzer reads the schema files SurrealKit
//! already manages (every module's `schema/` directory), the other `.surql`
//! files under the project, and the SurrealQL embedded in host files
//! (`db.query("…")` in `.ts`/`.svelte`/…), and reports contract violations
//! before anything reaches an instance. `generate` emits the typed
//! TypeScript client for those embedded queries.
//!
//! The analyzer needs no config file of its own: [`workspace_config`] builds
//! its [`WorkspaceConfig`] from the project layout plus the `[analyze]`
//! section of `surrealkit.toml`, so the schema directories are never written
//! down twice.
//!
//! ```toml
//! [analyze]
//! # The SurrealDB release you deploy to. Enables the version checks
//! # (functions and syntax the release lacks or removed). Unset = latest.
//! surrealdb_version = "3.2"
//! # Where `surrealkit generate` writes the typed client.
//! out = "src/lib/db.generated.ts"
//! # Extra directories to skip (target/, node_modules/ and .git/ always are).
//! ignore = ["dist/**"]
//! strict = false
//! warnings_as_errors = false
//! [analyze.lints]
//! "7xxx" = "warn"
//! E1002 = "allow"
//! ```

use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;
use surrealql_analyzer::Project;
use surrealql_analyzer::workspace::config::WorkspaceConfig;

use crate::module::Module;
use crate::project::{CONFIG_FILE_NAME, ProjectConfig};

/// The `[analyze]` section of `surrealkit.toml`.
///
/// Every key is optional; an absent section analyzes the project's schema
/// modules against the latest SurrealDB release with the analyzer's default
/// lint levels.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnalyzeConfig {
	/// Directories excluded from analysis, as globs relative to the project
	/// root (`dist/**`). `target/**`, `node_modules/**` and `.git/**` are
	/// always excluded.
	#[serde(default)]
	pub ignore: Vec<String>,
	/// The SurrealDB release to analyze for (`"3"`, `"3.2"`, `"3.2.3"`).
	/// Selects the version-gated checks: a function the release lacks, syntax
	/// it lacks, syntax it removed. Unset means the latest release, under
	/// which no version-gated finding fires.
	pub surrealdb_version: Option<String>,
	/// Tighten otherwise-advisory checks.
	#[serde(default)]
	pub strict: bool,
	/// Report every warning as an error (a CI gate).
	#[serde(default)]
	pub warnings_as_errors: bool,
	/// Require a written reason on every inline `-- surrealql-analyzer: allow(…)`
	/// suppression.
	#[serde(default)]
	pub require_suppression_reasons: bool,
	/// Per-code lint levels: `"allow"` | `"warn"` | `"deny"`, keyed by code
	/// (`E1002`, `7002`), family wildcard (`"7xxx"`), or named lint.
	#[serde(default)]
	pub lints: BTreeMap<String, String>,
	/// Where `surrealkit generate` writes the typed client when `--out` is not
	/// given. Relative to the project root. Defaults to
	/// `surrealql-analyzer.generated.ts` at the root.
	pub out: Option<PathBuf>,
}

/// The directory analysis is rooted at: the one holding `surrealkit.toml`
/// when there is one above the working directory, else the working directory.
///
/// Every discovered path and every glob is relative to this, so a project
/// analyzes the same way from any subdirectory.
pub fn project_root() -> Result<PathBuf> {
	let cwd = std::env::current_dir().context("reading the working directory")?;
	Ok(ProjectConfig::discover(&cwd)
		.and_then(|config| config.parent().map(Path::to_path_buf))
		.unwrap_or(cwd))
}

/// The `surrealkit.toml` a watch should treat as an input, when the project
/// has one — an edit to it changes what is analyzed and how it is graded.
pub fn config_path() -> Result<Option<PathBuf>> {
	let root = project_root()?;
	let path = root.join(CONFIG_FILE_NAME);
	Ok(path.is_file().then_some(path))
}

/// The analyzer's configuration for this project: the schema globs come from
/// the module layout, everything else from `[analyze]`.
///
/// Built as the analyzer's own TOML and parsed by it, so lint levels, family
/// wildcards and version strings are read by exactly one parser.
pub fn workspace_config(
	project: &ProjectConfig,
	folder: &str,
	root: &Path,
) -> Result<WorkspaceConfig> {
	let modules: Vec<Module> = if project.schema.is_empty() {
		vec![Module::default_module()]
	} else {
		project
			.schema
			.keys()
			.map(Module::new)
			.collect::<Result<_>>()
			.context("resolving schema modules for analysis")?
	};

	let mut schema_globs = Vec::new();
	for module in &modules {
		let dir = project.layout_for(folder, module).schema_dir();
		let prefix = glob_prefix(root, &dir);
		schema_globs.push(format!("{prefix}/**/*.surql"));
		schema_globs.push(format!("{prefix}/**/*.surrealql"));
	}

	let analyze = &project.analyze;
	let mut ignore =
		vec!["target/**".to_string(), "node_modules/**".to_string(), ".git/**".to_string()];
	ignore.extend(analyze.ignore.iter().cloned());

	let mut toml = String::new();
	toml.push_str("[sources]\n");
	toml.push_str(&format!("schema = {}\n", toml_array(&schema_globs)));
	toml.push_str(&format!("ignore = {}\n", toml_array(&ignore)));
	toml.push_str("\n[analysis]\n");
	toml.push_str(&format!("strict = {}\n", analyze.strict));
	if let Some(version) = &analyze.surrealdb_version {
		toml.push_str(&format!("surrealdb_version = {}\n", toml_string(version)));
	}
	toml.push_str("\n[diagnostics]\n");
	toml.push_str(&format!("warnings_as_errors = {}\n", analyze.warnings_as_errors));
	toml.push_str(&format!(
		"require_suppression_reasons = {}\n",
		analyze.require_suppression_reasons
	));
	toml.push_str("\n[lints]\n");
	for (code, level) in &analyze.lints {
		toml.push_str(&format!("{} = {}\n", toml_string(code), toml_string(level)));
	}

	WorkspaceConfig::from_toml_str(&toml)
		.map_err(|error| anyhow::anyhow!("[analyze] in {CONFIG_FILE_NAME}: {error}"))
}

/// The analyzer's view of this project: rooted at [`project_root`], configured
/// by [`workspace_config`].
pub fn analyzer_project(project: &ProjectConfig, folder: &str) -> Result<Project> {
	let root = project_root()?;
	let config = workspace_config(project, folder, &root)?;
	Ok(Project::new(root, config))
}

/// `dir` as a forward-slash glob prefix relative to `root`, with `.` segments
/// dropped so `./database/schema` matches the `database/schema/x.surql` the
/// analyzer discovers. A directory outside the root keeps its own spelling.
fn glob_prefix(root: &Path, dir: &Path) -> String {
	let absolute = if dir.is_absolute() {
		dir.to_path_buf()
	} else {
		root.join(dir)
	};
	let relative = absolute.strip_prefix(root).unwrap_or(&absolute);
	let parts: Vec<String> = relative
		.components()
		.filter_map(|component| match component {
			Component::Normal(part) => Some(part.to_string_lossy().into_owned()),
			_ => None,
		})
		.collect();
	parts.join("/")
}

fn toml_string(value: &str) -> String {
	format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

fn toml_array(values: &[String]) -> String {
	let quoted: Vec<String> = values.iter().map(|value| toml_string(value)).collect();
	format!("[{}]", quoted.join(", "))
}

#[cfg(test)]
mod tests {
	use super::*;

	fn config(raw: &str) -> WorkspaceConfig {
		let project = ProjectConfig::parse(raw).expect("config parses");
		workspace_config(&project, "./database", Path::new("/proj"))
			.expect("analyzer config builds")
	}

	#[test]
	fn the_default_module_schema_dir_becomes_the_schema_glob() {
		let cfg = config("");
		assert_eq!(
			cfg.sources.schema,
			vec!["database/schema/**/*.surql", "database/schema/**/*.surrealql"]
		);
		assert!(cfg.sources.ignore.contains(&"node_modules/**".to_string()));
		assert_eq!(cfg.analysis.target_version(), None);
	}

	#[test]
	fn every_declared_module_contributes_its_schema_dir() {
		let cfg = config("[schema.core]\n[schema.billing]\npath = \"custom/billing\"\n");
		assert!(
			cfg.sources.schema.contains(&"database/modules/core/schema/**/*.surql".to_string())
		);
		// `[schema.x] path` is relative to the folder.
		assert!(cfg.sources.schema.contains(&"database/custom/billing/**/*.surql".to_string()));
	}

	#[test]
	fn analyze_section_maps_onto_the_analyzer_config() {
		let cfg = config(
			"[analyze]\nsurrealdb_version = \"3.2\"\nstrict = true\nwarnings_as_errors = true\nignore = [\"dist/**\"]\n[analyze.lints]\n\"7xxx\" = \"allow\"\nE1002 = \"warn\"\n",
		);
		assert!(cfg.analysis.strict);
		assert!(cfg.diagnostics.warnings_as_errors);
		assert_eq!(cfg.analysis.target_version().map(|v| v.to_string()), Some("3.2".to_string()));
		assert!(cfg.sources.ignore.contains(&"dist/**".to_string()));
		// The lint table round-trips through the analyzer's own parser: the
		// demoted 1002 comes back as a warning.
		let finding = surrealql_analyzer::diagnostics::catalog::finding(
			surrealql_analyzer::syntax::span::SourceSpan::new(
				surrealql_analyzer::syntax::source::SourceId::new("t"),
				surrealql_analyzer::syntax::span::ByteRange::new(0, 1).expect("ordered"),
			),
			1002,
			"probe",
		);
		use surrealql_analyzer::diagnostics::Severity;
		assert_eq!(
			cfg.policy().resolve_severity(finding.code(), finding.severity()),
			Some(Severity::Warning)
		);
	}

	#[test]
	fn an_unknown_analyze_key_is_rejected_like_every_other_section() {
		assert!(ProjectConfig::parse("[analyze]\nstrictness = true\n").is_err());
	}
}
