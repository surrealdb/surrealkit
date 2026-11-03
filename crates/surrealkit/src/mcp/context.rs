//! Per-call context.
//!
//! The server holds no mutable state between tool calls. Everything a call needs
//! is resolved here, from that call's arguments plus the immutable configuration
//! captured when the process started.
//!
//! The one rule that makes this work: **the process working directory is never
//! changed.** `std::env::set_current_dir` is process-global, so under concurrent
//! tool calls it would be a data race. Instead the project root is captured once
//! and every path is derived from it explicitly -- which the library already
//! supports, since `ProjectConfig::load`, `build_vars` and `load_typegen_config`
//! all accept an explicit path.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context as _, Result};
use rmcp::schemars;
use serde::Deserialize;

use crate::config::{DbCfg, DbOverrides};
use crate::constants::Layout;
use crate::module::Module;
use crate::project::{CONFIG_FILE_NAME, ProjectConfig};
use crate::selection::Selection;
use crate::variables::{TemplateVars, TypegenConfig};

/// Immutable, server-lifetime configuration.
///
/// Captured once at startup from the process working directory and the server's
/// own command-line flags. Nothing a tool call says can change it.
#[derive(Debug, Clone)]
pub struct ServerConfig {
	/// The project root. Captured once; `current_dir()` is never called again.
	pub root: PathBuf,
	/// Connection defaults from the server's own flags and environment.
	pub overrides: DbOverrides,
	/// Template variables from the server's own `--var` flags.
	pub vars: Vec<(String, String)>,
}

impl ServerConfig {
	/// Capture the server's configuration, rooting it at `root`.
	pub fn new(root: PathBuf, overrides: DbOverrides, vars: Vec<(String, String)>) -> Result<Self> {
		let root = root
			.canonicalize()
			.with_context(|| format!("resolving the project root {}", root.display()))?;
		Ok(Self {
			root,
			overrides,
			vars,
		})
	}
}

/// The parameters every tool shares.
///
/// Flattened into each tool's input schema rather than nested: models fill flat
/// fields far more reliably, and `$ref` resolution is inconsistent across hosts.
///
/// Connection details are deliberately **not** here. The only endpoint selector
/// is `target`, which names a `[target.<name>]` section in `surrealkit.toml`, so
/// the reachable set of databases is enumerated in source control and each
/// password comes from the server's own environment via `pass_env`. A tool
/// argument that could set `host`/`ns`/`db` would make every destructive
/// annotation meaningless -- the user approves "prune dev" and gets prod -- and
/// one that could set a password would turn the server into a credential proxy.
#[derive(Debug, Clone, Default, Deserialize, schemars::JsonSchema)]
pub struct WorkRequest {
	/// Schema module names from `[schema.<name>]`. Omit for every declared
	/// module, or the default module when none are declared.
	#[serde(default)]
	pub schema: Vec<String>,
	/// Database target names from `[target.<name>]`. Omit for the primary
	/// target, or the ambient connection when none are declared.
	#[serde(default)]
	pub target: Vec<String>,
	/// Select every declared schema module. Each tool still acts on one database,
	/// so this widens the modules, not the targets: with more than one target
	/// declared you must still name the one you mean.
	#[serde(default)]
	pub all: bool,
	/// Do not pull in the `depends_on` modules of those selected.
	#[serde(default)]
	pub no_deps: bool,
	/// Project folder, relative to the server's project root.
	/// Defaults to `SURREALDB_FOLDER`, else `./database`.
	#[serde(default)]
	pub folder: Option<String>,
	/// Template variables substituted into `${VAR}` placeholders.
	#[serde(default)]
	pub vars: HashMap<String, String>,
}

/// Everything one tool call needs, resolved from that call's arguments.
#[derive(Debug, Clone)]
pub struct CallContext {
	/// The project root: absolute, and the containment boundary for every path.
	pub root: PathBuf,
	/// The `surrealkit.toml` this call resolved against, if there is one.
	pub config_path: Option<PathBuf>,
	pub project: ProjectConfig,
	/// **Absolute.** Making the folder absolute is the single lever that makes
	/// the whole library working-directory independent: every `constants::*`
	/// path is `PathBuf::from(folder).join(..)`, so absolute in means absolute
	/// out, everywhere.
	pub folder: String,
	pub cfg: DbCfg,
	pub vars: TemplateVars,
	pub typegen: TypegenConfig,
}

impl CallContext {
	/// Resolve a call's context without touching the process working directory.
	pub fn resolve(server: &Arc<ServerConfig>, req: &WorkRequest) -> Result<Self> {
		// 1. Config: the nearest surrealkit.toml at or above the project root.
		//    `discover` walks up, so a server started in a subdirectory still
		//    finds its project -- unlike `build_vars`'s literal `./surrealkit.toml`,
		//    which is why the variables below come from this same parsed config
		//    rather than a second, working-directory-relative read.
		let config_path = ProjectConfig::discover(&server.root);
		let project = ProjectConfig::load(config_path.as_deref())
			.with_context(|| format!("loading {CONFIG_FILE_NAME}"))?;

		// 2. Connection: the server's own flags, with the call's folder layered on.
		let mut overrides = server.overrides.clone();
		if let Some(folder) = &req.folder {
			overrides.folder = Some(folder.clone());
		}
		let cfg = DbCfg::from_env(None, &overrides)?;

		// 3. Make the folder absolute against the project root. This is the single
		//    lever that makes the library working-directory independent: every
		//    `constants::*` path is `PathBuf::from(folder).join(..)`, so absolute
		//    in means absolute out.
		let folder = {
			let f = Path::new(cfg.folder());
			if f.is_absolute() {
				f.to_path_buf()
			} else {
				normalize(&server.root.join(f))
			}
		};
		let folder = folder.to_string_lossy().into_owned();

		// 4. Variables: config `[variables]`, then `SURREALKIT_VAR_*`, then the
		//    server's `--var` flags, then this call's `vars` -- highest last.
		let mut merged: Vec<(String, String)> = server.vars.clone();
		merged.extend(req.vars.iter().map(|(k, v)| (k.clone(), v.clone())));
		let vars = TemplateVars {
			vars: crate::variables::build_vars(&merged, config_path.as_deref())?,
		};

		let typegen = project.typegen.clone();
		Ok(Self {
			root: server.root.clone(),
			config_path,
			project,
			folder,
			cfg,
			vars,
			typegen,
		})
	}

	/// The (schema module x database target) matrix this call operates on.
	pub fn selection(&self, req: &WorkRequest) -> Result<Selection> {
		Selection::resolve(&self.project, &self.cfg, &req.schema, &req.target, req.all, req.no_deps)
	}

	/// On-disk paths for `module` within this call's folder.
	pub fn layout(&self, module: &Module) -> Layout {
		self.project.layout_for(&self.folder, module)
	}
}

/// Drop `.` components so a default folder of `./database` does not surface as
/// `<root>/./database` in tool output.
fn normalize(path: &Path) -> PathBuf {
	path.components().filter(|c| !matches!(c, std::path::Component::CurDir)).collect()
}
