//! A check of the whole project, from the files to the [`Report`].
//!
//! [`Prepared`] is what a check reads from `surrealkit.toml`, parsed once, and
//! [`Prepared::run`] is the one check every front end runs: the CLI, `watch` and the MCP server.
//! Nothing here reads the process working directory: every path is resolved from the `root` a
//! run is given.

use std::path::Path;

use anyhow::{Context, Result, bail};

use super::discover::{Ignore, discover};
use super::drift::{ConfigFile, DriftIgnore, Env, check_targets};
use super::engine::SchemaApply;
use super::policy::Policy;
use super::{ExtraFile, Extras, Report, RunOptions, run_with_options};
use crate::module::Module;
use crate::project::{ProjectConfig, Target};
use crate::selection::select_modules;
use crate::variables::{QueriesConfig, TemplateVars, TypegenConfig};

/// What a check of the project reads from its configuration.
#[derive(Debug, Clone)]
pub struct Prepared {
	pub project: ProjectConfig,
	/// The selected schema modules, in dependency order.
	pub modules: Vec<Module>,
	pub policy: Policy,
	pub ignore: Ignore,
	pub drift_ignore: DriftIgnore,
	/// `[typegen] experimental`: the capabilities the engine allows.
	pub experimental: Vec<String>,
	/// `[typegen] engine`.
	pub engine: Option<String>,
	/// `[typegen.queries]`: where host code is, when it is set.
	pub queries: Option<QueriesConfig>,
	pub vars: TemplateVars,
}

impl Prepared {
	/// Parse `project`'s `[analyze]` policy and ignores, and select the modules the way
	/// `compile` does (`schemas`, `all` and `no_deps` are `--schema`, `--all` and `--no-deps`).
	/// `typegen` is `[typegen]`, whose capabilities and host-code sources a check shares with
	/// `typegen`. A configuration mistake is an error.
	pub fn new(
		project: ProjectConfig,
		typegen: &TypegenConfig,
		schemas: &[String],
		all: bool,
		no_deps: bool,
		vars: TemplateVars,
	) -> Result<Self> {
		let modules = select_modules(&project, schemas, all, no_deps)?;
		let policy = Policy::from_config(&project.analyze)?;
		let ignore = Ignore::from_config(&project.analyze)?;
		let drift_ignore = DriftIgnore::from_config(&project.analyze)?;
		let queries = typegen.queries_config()?;
		Ok(Self {
			project,
			modules,
			policy,
			ignore,
			drift_ignore,
			experimental: typegen.experimental.clone(),
			engine: typegen.engine.clone(),
			queries,
			vars,
		})
	}

	/// One check of the project at `root`, with a fresh engine: discover the sources, run them
	/// through the schema backend, read the host code `[typegen.queries]` names, compare the
	/// files with each of `targets`, and return the report.
	///
	/// A relative `folder` is under `root`, as are the host-code sources and the
	/// `surrealkit.toml` that findings about a target point at (found from `root` upwards).
	/// `targets` is empty for a check that connects to nothing; each target named is
	/// connected to and read, and nothing is written.
	pub async fn run(&self, root: &Path, folder: &str, targets: &[Target]) -> Result<Report> {
		self.run_with_options(root, folder, targets, RunOptions::default()).await
	}

	/// [`run`](Self::run), with `options`: `show_suppressed` keeps what a directive silenced in
	/// the report's findings, marked as suppressed, and changes no count.
	pub async fn run_with_options(
		&self,
		root: &Path,
		folder: &str,
		targets: &[Target],
		options: RunOptions,
	) -> Result<Report> {
		let sources = discover(&self.project, folder, root, &self.modules, &self.project.analyze)?;
		if sources.schema.is_empty() && sources.queries.is_empty() {
			let dirs: Vec<String> = sources
				.schema_dirs
				.iter()
				.map(|dir| dir.strip_prefix(&sources.root).unwrap_or(dir).display().to_string())
				.collect();
			log::warn!(
				"no SurrealQL sources found under {} (schema dirs: {}); check --folder, --schema \
				 and [analyze] ignore",
				sources.root.display(),
				dirs.join(", ")
			);
		}
		let mut extras = Extras::default();
		if let Some(queries) = self.queries.as_ref().filter(|queries| !queries.sources.is_empty()) {
			#[cfg(feature = "host-queries")]
			{
				let linted = super::host::lint(root, queries)?;
				extras.files.extend(linted.files);
				extras.findings.extend(linted.findings);
				extras.host = Some(linted.counts);
			}
			#[cfg(not(feature = "host-queries"))]
			{
				let _ = queries;
				log::warn!(
					"[typegen.queries] sources is set, but this build does not read host code \
					 (the `host-queries` feature)"
				);
			}
		}
		if !targets.is_empty() {
			let config = config_file(root)?;
			let env = Env {
				project: &self.project,
				root,
				folder,
				vars: &self.vars,
				experimental: &self.experimental,
				engine: self.engine.as_deref(),
				ignore: &self.drift_ignore,
			};
			extras.targets = check_targets(&sources, targets, &config, &env).await?;
			extras.files.push(ExtraFile {
				path: config.path,
				text: config.text,
				comments: None,
			});
		}
		let mut engine = SchemaApply::new(&self.experimental).await?;
		run_with_options(&mut engine, &sources, &self.policy, &self.vars, extras, options).await
	}
}

/// `surrealkit.toml`, where the targets are declared: found from `root` upwards, as the rest of
/// SurrealKit finds it.
fn config_file(root: &Path) -> Result<ConfigFile> {
	let Some(path) = ProjectConfig::discover(root) else {
		bail!(
			"`--target` needs a surrealkit.toml: targets are declared there as `[target.<name>]`"
		);
	};
	let text =
		std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
	Ok(ConfigFile {
		path,
		text,
	})
}
