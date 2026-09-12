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
//! its `WorkspaceConfig` from the project layout plus the `[analyze]`
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
//!
//! Everything past [`AnalyzeConfig`] needs the analyzer itself, and so lives
//! behind the `analyze` feature (on by default, and implied by `cli`). The
//! config type stays unconditional: it is plain serde, and
//! [`ProjectConfig`](crate::project::ProjectConfig) carries an `[analyze]`
//! section whether or not this build can act on it.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::Deserialize;

/// The `[analyze]` section of `surrealkit.toml`.
///
/// Every key is optional; an absent section analyzes the project's schema
/// modules against the latest SurrealDB release with the analyzer's default
/// lint levels.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnalyzeConfig {
	/// Directories excluded from analysis, beyond the always-excluded
	/// `target/`, `node_modules/` and `.git/`.
	///
	/// Each entry is a **directory name**, matched against every component of
	/// a path — not a glob. `dist` and `dist/**` are the same pattern, and
	/// both skip every directory named `dist` anywhere in the tree. A
	/// path-shaped entry (`src/generated/**`) or a wildcard (`*.gen.ts`)
	/// matches nothing.
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
	/// given. Relative to the project root — the directory holding
	/// `surrealkit.toml` — so it names the same file from every working
	/// directory. Defaults to `surrealql-analyzer.generated.ts` at the root.
	pub out: Option<PathBuf>,
}

#[cfg(feature = "analyze")]
pub use analyzing::*;

#[cfg(feature = "analyze")]
mod analyzing {
	use std::path::{Component, Path, PathBuf};

	use anyhow::{Context, Result, bail};
	use surrealql_analyzer::Project;
	use surrealql_analyzer::workspace::config::WorkspaceConfig;

	use crate::module::Module;
	use crate::project::{CONFIG_FILE_NAME, ProjectConfig};

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

	/// `[analyze] out` as an absolute path.
	///
	/// The key is documented relative to the project root, and a value written
	/// in a config file has to mean the same file wherever the command runs
	/// from. `--out` is the opposite: it is typed at a shell, so it stays
	/// relative to the working directory like every other path a shell hands
	/// over.
	pub fn configured_out(project: &ProjectConfig) -> Result<Option<PathBuf>> {
		let Some(out) = project.analyze.out.as_ref() else {
			return Ok(None);
		};
		if out.is_absolute() {
			return Ok(Some(out.clone()));
		}
		Ok(Some(project_root()?.join(out)))
	}

	/// The schema modules an analysis covers, given the names from `--schema`.
	///
	/// Empty means every declared module — or the default module, in the
	/// pre-1.0 layout — which is the rule the database commands follow. A
	/// named module pulls in what it depends on: analyzing `billing` without
	/// the `core` tables it references would report every one of them as
	/// undefined.
	pub fn selected_modules(project: &ProjectConfig, selected: &[String]) -> Result<Vec<Module>> {
		let declared: Vec<String> = project.schema.keys().cloned().collect();
		let wanted: Vec<String> = if selected.is_empty() {
			if declared.is_empty() {
				vec![Module::DEFAULT_NAME.to_string()]
			} else {
				declared
			}
		} else {
			for name in selected {
				if !project.schema.contains_key(name) && name != Module::DEFAULT_NAME {
					bail!(
						"unknown schema module {name:?}; declared modules are: {}",
						if declared.is_empty() {
							"(none)".to_string()
						} else {
							declared.join(", ")
						}
					);
				}
			}
			project.module_order(selected)?
		};
		wanted
			.into_iter()
			.map(Module::new)
			.collect::<Result<_>>()
			.context("resolving schema modules for analysis")
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
		modules: &[Module],
	) -> Result<WorkspaceConfig> {
		let mut schema_globs = Vec::new();
		for module in modules {
			let dir = project.layout_for(folder, module).schema_dir();
			let prefix = glob_prefix(root, &dir, module.name())?;
			// An empty prefix means the root itself, where `/**/*.surql` would
			// match nothing.
			let base = if prefix.is_empty() {
				String::new()
			} else {
				format!("{prefix}/")
			};
			schema_globs.push(format!("{base}**/*.surql"));
			schema_globs.push(format!("{base}**/*.surrealql"));
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
	/// by [`workspace_config`], covering the modules `selected` names (empty is
	/// all of them).
	pub fn analyzer_project(
		project: &ProjectConfig,
		folder: &str,
		selected: &[String],
	) -> Result<Project> {
		let root = project_root()?;
		let modules = selected_modules(project, selected)?;
		let config = workspace_config(project, folder, &root, &modules)?;
		Ok(Project::new(root, config))
	}

	/// `dir` as a forward-slash glob prefix relative to `root`, with `.` and
	/// `..` resolved first so `./database/schema` matches the
	/// `database/schema/x.surql` the analyzer discovers.
	///
	/// The analyzer walks only `root` and matches its globs against paths
	/// relative to it, so a directory outside `root` can be neither expressed
	/// as a glob nor reached by the walk. Saying so is the point of the error:
	/// the alternative is a glob that matches nothing, and an analysis that
	/// reports every table in the project as undefined.
	fn glob_prefix(root: &Path, dir: &Path, module: &str) -> Result<String> {
		let absolute = if dir.is_absolute() {
			dir.to_path_buf()
		} else {
			root.join(dir)
		};
		let normalized = normalize(&absolute);
		let root = normalize(root);
		// One message rather than a `with_context` wrapper: the module name is
		// a detail of a sentence that has to be read whole, and anyhow would
		// print the wrapper alone on the first line and bury the rest under
		// "Caused by".
		let relative = normalized.strip_prefix(&root).map_err(|_| {
			anyhow::anyhow!(
				"schema module {module:?}: the schema directory `{}` is outside the project \
				 root `{}`; run from a directory that contains both, or keep the database \
				 folder under the one holding {CONFIG_FILE_NAME}",
				normalized.display(),
				root.display()
			)
		})?;
		// Normalization leaves only `Normal` components behind, so this filter
		// drops nothing but the empty path — the directory *is* the root.
		let parts: Vec<String> = relative
			.components()
			.filter_map(|component| match component {
				Component::Normal(part) => Some(part.to_string_lossy().into_owned()),
				_ => None,
			})
			.collect();
		Ok(parts.join("/"))
	}

	/// `path` with `.` dropped and `..` folded into the component before it,
	/// without touching the filesystem.
	///
	/// Lexical rather than [`Path::canonicalize`] because the directory need
	/// not exist yet: a module whose `schema/` has not been created is a
	/// normal state, not a reason to refuse the rest of the analysis. A `..`
	/// with nothing to fold into is kept, so [`glob_prefix`]'s `strip_prefix`
	/// still rejects a path that escapes the root.
	fn normalize(path: &Path) -> PathBuf {
		let mut out = PathBuf::new();
		for component in path.components() {
			match component {
				Component::CurDir => {}
				Component::ParentDir => {
					if matches!(out.components().next_back(), Some(Component::Normal(_))) {
						out.pop();
					} else {
						out.push("..");
					}
				}
				other => out.push(other.as_os_str()),
			}
		}
		out
	}

	/// `value` as a TOML basic string.
	///
	/// The `toml` crate's encoder rather than hand-rolled escaping: a value
	/// carrying a control character (a tab, or a newline written `"a\nb"`) has
	/// to come back out as an escape, or the document this builds is not the
	/// document the analyzer parses, and the user gets a syntax error pointing
	/// at a line of text they never wrote.
	fn toml_string(value: &str) -> String {
		toml::Value::String(value.to_string()).to_string()
	}

	fn toml_array(values: &[String]) -> String {
		let quoted: Vec<String> = values.iter().map(|value| toml_string(value)).collect();
		format!("[{}]", quoted.join(", "))
	}

	#[cfg(test)]
	mod tests {
		use super::*;

		fn config(raw: &str) -> WorkspaceConfig {
			config_for(raw, &[])
		}

		fn config_for(raw: &str, selected: &[&str]) -> WorkspaceConfig {
			let project = ProjectConfig::parse(raw).expect("config parses");
			let selected: Vec<String> = selected.iter().map(|s| (*s).to_string()).collect();
			let modules = selected_modules(&project, &selected).expect("modules resolve");
			workspace_config(&project, "./database", Path::new("/proj"), &modules)
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
		fn a_selected_module_narrows_the_schema_globs() {
			let cfg = config_for("[schema.core]\n[schema.billing]\n", &["core"]);
			assert_eq!(
				cfg.sources.schema,
				vec![
					"database/modules/core/schema/**/*.surql",
					"database/modules/core/schema/**/*.surrealql"
				],
				"--schema core must leave billing's schema out"
			);
		}

		#[test]
		fn a_selected_module_pulls_in_what_it_depends_on() {
			// Analyzing billing alone would report every core table it
			// references as undefined.
			let cfg = config_for(
				"[schema.core]\n[schema.billing]\ndepends_on = [\"core\"]\n",
				&["billing"],
			);
			assert!(
				cfg.sources.schema.contains(&"database/modules/core/schema/**/*.surql".to_string())
			);
		}

		#[test]
		fn an_unknown_selected_module_names_the_declared_ones() {
			let project = ProjectConfig::parse("[schema.core]\n").expect("config parses");
			let error = selected_modules(&project, &["nope".to_string()])
				.expect_err("an undeclared module is an error");
			let message = format!("{error:#}");
			assert!(message.contains("unknown schema module \"nope\""), "{message}");
			assert!(message.contains("core"), "{message}");
		}

		#[test]
		fn a_schema_directory_outside_the_root_is_an_error_not_a_silent_miss() {
			// `--folder ../shared` puts the schema where the analyzer neither
			// walks nor can express a glob for. It must say so, rather than
			// analyze the project against an empty schema and report every
			// table as undefined.
			let project = ProjectConfig::parse("").expect("config parses");
			let modules = selected_modules(&project, &[]).expect("modules resolve");
			let error = workspace_config(&project, "../shared", Path::new("/proj"), &modules)
				.expect_err("a schema directory outside the root is an error");
			let message = format!("{error:#}");
			assert!(message.contains("outside the project root"), "{message}");
			assert!(message.contains("/shared/schema"), "{message}");
		}

		#[test]
		fn an_absolute_schema_directory_under_the_root_is_relative_to_it() {
			let project = ProjectConfig::parse("").expect("config parses");
			let modules = selected_modules(&project, &[]).expect("modules resolve");
			let cfg = workspace_config(&project, "/proj/database", Path::new("/proj"), &modules)
				.expect("analyzer config builds");
			assert_eq!(
				cfg.sources.schema,
				vec!["database/schema/**/*.surql", "database/schema/**/*.surrealql"]
			);
		}

		#[test]
		fn a_value_with_a_control_character_survives_the_round_trip() {
			// Hand-rolled escaping emitted a literal newline here, which made
			// the generated document fail to parse and reported a line and
			// column in text the user never wrote.
			let cfg = config("[analyze]\nignore = [\"we\\nird\"]\n");
			assert!(cfg.sources.ignore.contains(&"we\nird".to_string()));
		}

		#[test]
		fn analyze_section_maps_onto_the_analyzer_config() {
			let cfg = config(
				"[analyze]\nsurrealdb_version = \"3.2\"\nstrict = true\nwarnings_as_errors = true\nignore = [\"dist/**\"]\n[analyze.lints]\n\"7xxx\" = \"allow\"\nE1002 = \"warn\"\n",
			);
			assert!(cfg.analysis.strict);
			assert!(cfg.diagnostics.warnings_as_errors);
			assert_eq!(
				cfg.analysis.target_version().map(|v| v.to_string()),
				Some("3.2".to_string())
			);
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
}
