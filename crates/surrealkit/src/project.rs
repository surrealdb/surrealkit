//! `surrealkit.toml` — the project's declared schema modules and database targets.
//!
//! Every section is optional. A project with no config file, or one containing
//! only `[variables]` and `[typegen]`, behaves exactly as it did before v1: a
//! single unnamed schema module against a single database taken from the
//! environment.
//!
//! ```toml
//! [variables]
//! app_name = "acme"
//!
//! [typegen]
//! typescript = "src/types"
//!
//! [schema.core]
//! # path defaults to <folder>/modules/core/schema
//!
//! [schema.billing]
//! depends_on = ["core"]
//!
//! [target.acme]
//! ns = "acme"
//! db = "prod"
//! pass_env = "ACME_DB_PASSWORD"   # never inline a password
//! ```

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::config::{AuthLevel, DbCfg};
use crate::constants::Layout;
use crate::module::Module;

/// The config file's name, looked up from the working directory upwards.
pub const CONFIG_FILE_NAME: &str = "surrealkit.toml";

/// One `[schema.<name>]` section.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaModuleConfig {
	/// Override the conventional schema directory for this module.
	pub path: Option<PathBuf>,
	/// Modules that must be applied before this one.
	#[serde(default)]
	pub depends_on: Vec<String>,
}

/// One `[target.<name>]` section.
///
/// Every connection field is optional and falls back to the ambient
/// [`DbCfg`], so a target usually only needs to name its `ns`/`db`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetConfig {
	pub host: Option<String>,
	pub ns: Option<String>,
	pub db: Option<String>,
	pub user: Option<String>,
	/// Name of an environment variable holding this target's password.
	///
	/// Passwords are never read from the config file itself — see the
	/// `pass`/`password` rejection in [`ProjectConfig::parse`].
	pub pass_env: Option<String>,
	pub auth_level: Option<String>,
	/// Restrict which schema modules apply to this target. `None` means all.
	pub schemas: Option<Vec<String>>,
	/// The target `typegen` introspects when several are selected.
	#[serde(default)]
	pub primary: bool,
}

/// A parsed `surrealkit.toml`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectConfig {
	#[serde(default)]
	pub variables: HashMap<String, String>,
	#[serde(default)]
	pub typegen: crate::variables::TypegenConfig,
	/// Declared schema modules, keyed by name.
	#[serde(default)]
	pub schema: BTreeMap<String, SchemaModuleConfig>,
	/// Declared database targets, keyed by name.
	#[serde(default)]
	pub target: BTreeMap<String, TargetConfig>,
}

impl ProjectConfig {
	/// Find `surrealkit.toml` by walking up from `start` to the filesystem root.
	///
	/// Before v1 the file was only read from the exact working directory, so
	/// running from a subdirectory of a project silently lost every variable.
	pub fn discover(start: &Path) -> Option<PathBuf> {
		let mut dir = Some(start);
		while let Some(d) = dir {
			let candidate = d.join(CONFIG_FILE_NAME);
			if candidate.is_file() {
				return Some(candidate);
			}
			dir = d.parent();
		}
		None
	}

	/// Load the config at `path`, or the nearest one above the working directory
	/// when `path` is `None`. A missing file yields the default config.
	pub fn load(path: Option<&Path>) -> Result<Self> {
		let found = match path {
			Some(p) => Some(p.to_path_buf()),
			None => {
				let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
				Self::discover(&cwd)
			}
		};
		let Some(path) = found else {
			return Ok(Self::default());
		};
		let raw = std::fs::read_to_string(&path)
			.with_context(|| format!("reading {}", path.display()))?;
		Self::parse(&raw).with_context(|| format!("parsing {}", path.display()))
	}

	/// Parse config from TOML text.
	pub fn parse(raw: &str) -> Result<Self> {
		// Reject inline credentials before serde does, so the error names the fix
		// rather than reporting an unknown field. All offenders are listed at once,
		// so a multi-tenant config is fixed in one pass.
		let inline = find_inline_password(raw);
		if !inline.is_empty() {
			let offenders = inline
				.iter()
				.map(|(key, section)| format!("  [target.{section}] {key}"))
				.collect::<Vec<_>>()
				.join("\n");
			bail!(
				"passwords must not be committed to source control, but \
				 {CONFIG_FILE_NAME} sets them inline:\n{offenders}\n\
				 Use environment indirection instead:\n\
				 \x20   pass_env = \"MY_DB_PASSWORD\""
			);
		}
		let cfg: Self = toml::from_str(raw)?;
		cfg.validate()?;
		Ok(cfg)
	}

	fn validate(&self) -> Result<()> {
		for name in self.schema.keys() {
			Module::new(name.clone())
				.with_context(|| format!("invalid [schema.{name}] section name"))?;
		}
		for (name, cfg) in &self.schema {
			for dep in &cfg.depends_on {
				if !self.schema.contains_key(dep) {
					bail!(
						"schema module {name:?} depends on {dep:?}, which is not declared \
						 in {CONFIG_FILE_NAME}"
					);
				}
				if dep == name {
					bail!("schema module {name:?} depends on itself");
				}
			}
		}
		for (name, cfg) in &self.target {
			if name.trim().is_empty() {
				bail!("[target] section has an empty name");
			}
			if let Some(schemas) = &cfg.schemas {
				for s in schemas {
					if !self.schema.contains_key(s) {
						bail!(
							"[target.{name}] lists schema module {s:?}, which is not \
							 declared in {CONFIG_FILE_NAME}"
						);
					}
				}
			}
		}
		let primaries: Vec<&String> =
			self.target.iter().filter(|(_, t)| t.primary).map(|(n, _)| n).collect();
		if primaries.len() > 1 {
			bail!("more than one target is marked primary: {primaries:?}");
		}
		// Detect cycles eagerly so it fails before any database is touched.
		self.module_order(&self.schema.keys().cloned().collect::<Vec<_>>())?;
		Ok(())
	}

	/// Whether the project declares any schema modules.
	pub fn has_modules(&self) -> bool {
		!self.schema.is_empty()
	}

	/// The modules in `selected`, plus their transitive dependencies, in an order
	/// where every module follows the ones it depends on.
	///
	/// Ties are broken alphabetically so output is deterministic. Cycles are an
	/// error naming the cycle rather than just reporting that one exists.
	pub fn module_order(&self, selected: &[String]) -> Result<Vec<String>> {
		// Expand to the dependency closure first.
		let mut wanted: HashSet<String> = HashSet::new();
		let mut stack: Vec<String> = selected.to_vec();
		while let Some(name) = stack.pop() {
			if !wanted.insert(name.clone()) {
				continue;
			}
			if let Some(cfg) = self.schema.get(&name) {
				stack.extend(cfg.depends_on.iter().cloned());
			}
		}

		let mut ordered: Vec<String> = Vec::with_capacity(wanted.len());
		let mut done: HashSet<String> = HashSet::new();
		let mut remaining: Vec<String> = wanted.into_iter().collect();
		remaining.sort();

		while !remaining.is_empty() {
			let ready: Vec<String> = remaining
				.iter()
				.filter(|n| {
					self.schema
						.get(*n)
						.map(|c| c.depends_on.iter().all(|d| done.contains(d)))
						.unwrap_or(true)
				})
				.cloned()
				.collect();

			if ready.is_empty() {
				let mut cycle = remaining.clone();
				cycle.sort();
				bail!("schema modules have a dependency cycle involving: {}", cycle.join(" -> "));
			}
			for name in &ready {
				done.insert(name.clone());
				ordered.push(name.clone());
			}
			remaining.retain(|n| !done.contains(n));
		}
		Ok(ordered)
	}

	/// Resolve a declared module to its [`Layout`], honouring a `path` override.
	pub fn layout_for(&self, folder: &str, module: &Module) -> Layout {
		match self.schema.get(module.name()).and_then(|c| c.path.clone()) {
			Some(path) => Layout::with_schema_dir(folder, module.clone(), path),
			None => Layout::new(folder, module.clone()),
		}
	}
}

/// Find `pass = ` / `password = ` keys inside `[target.*]` sections.
///
/// A line scan rather than a serde field: `deny_unknown_fields` would report
/// "unknown field `pass`", which does not tell the reader what to do instead.
fn find_inline_password(raw: &str) -> Vec<(String, String)> {
	let mut out = Vec::new();
	let mut section: Option<String> = None;
	for line in raw.lines() {
		let t = line.trim();
		if t.starts_with('[') {
			section = t
				.trim_start_matches('[')
				.trim_end_matches(']')
				.strip_prefix("target.")
				.map(|s| s.trim_matches('"').to_string());
			continue;
		}
		let Some(sec) = &section else {
			continue;
		};
		if let Some(key) = t.split('=').next() {
			let key = key.trim();
			if key == "pass" || key == "password" {
				out.push((key.to_string(), sec.clone()));
			}
		}
	}
	out
}

/// One resolved database endpoint.
///
/// A named target layered over the ambient [`DbCfg`]: unset fields inherit, so a
/// target usually only names its `ns`/`db`.
#[derive(Debug, Clone)]
pub struct Target {
	name: String,
	cfg: DbCfg,
	schemas: Option<Vec<String>>,
}

impl Target {
	/// The implicit target used when no `[target.*]` sections are declared: the
	/// ambient config exactly as before v1.
	pub fn implicit(cfg: DbCfg) -> Self {
		Self {
			name: "default".to_string(),
			cfg,
			schemas: None,
		}
	}

	/// Resolve `[target.<name>]` against the ambient `base` config.
	///
	/// A `pass_env` naming an unset variable fails here, before any connection is
	/// attempted, so a misconfigured target cannot half-apply a fan-out.
	pub fn resolve(name: &str, tc: &TargetConfig, base: &DbCfg) -> Result<Self> {
		let pass = match &tc.pass_env {
			Some(var) => Some(std::env::var(var).with_context(|| {
				format!(
					"[target.{name}] sets pass_env = {var:?}, but that environment \
					 variable is not set"
				)
			})?),
			None => None,
		};
		let auth_level = match &tc.auth_level {
			Some(raw) => Some(
				AuthLevel::parse_str(raw)
					.with_context(|| format!("[target.{name}] has an invalid auth_level"))?,
			),
			None => None,
		};
		Ok(Self {
			name: name.to_string(),
			cfg: base.overridden(
				tc.host.clone(),
				tc.ns.clone(),
				tc.db.clone(),
				tc.user.clone(),
				pass,
				auth_level,
			),
			schemas: tc.schemas.clone(),
		})
	}

	/// The target's name.
	pub fn name(&self) -> &str {
		&self.name
	}

	/// The connection config for this target.
	pub fn cfg(&self) -> &DbCfg {
		&self.cfg
	}

	/// Whether `module` applies to this target.
	pub fn allows(&self, module: &str) -> bool {
		self.schemas.as_ref().is_none_or(|s| s.iter().any(|n| n == module))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn cfg(raw: &str) -> ProjectConfig {
		ProjectConfig::parse(raw).expect("parse")
	}

	#[test]
	fn empty_config_declares_nothing() {
		let c = cfg("");
		assert!(!c.has_modules());
		assert!(c.target.is_empty());
	}

	#[test]
	fn a_pre_v1_config_still_parses() {
		// Only [variables] and [typegen] -- what 0.7 projects have.
		let c = cfg("[variables]\nAPP = \"x\"\n\n[typegen]\ntypescript = \"src/types\"\n");
		assert_eq!(c.variables.get("APP").map(String::as_str), Some("x"));
		assert!(c.typegen.typescript.is_some());
		assert!(!c.has_modules());
	}

	#[test]
	fn parses_modules_and_targets() {
		let c = cfg(r#"
[schema.core]
[schema.billing]
depends_on = ["core"]

[target.acme]
ns = "acme"
db = "prod"
primary = true
"#);
		assert!(c.has_modules());
		assert_eq!(c.schema["billing"].depends_on, vec!["core"]);
		assert_eq!(c.target["acme"].ns.as_deref(), Some("acme"));
		assert!(c.target["acme"].primary);
	}

	#[test]
	fn orders_modules_by_dependency() {
		let c = cfg("[schema.core]\n[schema.billing]\ndepends_on = [\"core\"]\n");
		assert_eq!(c.module_order(&["billing".into()]).unwrap(), vec!["core", "billing"]);
	}

	#[test]
	fn selecting_a_module_pulls_in_its_dependencies() {
		let c =
			cfg("[schema.a]\n[schema.b]\ndepends_on = [\"a\"]\n[schema.c]\ndepends_on = [\"b\"]\n");
		assert_eq!(c.module_order(&["c".into()]).unwrap(), vec!["a", "b", "c"]);
	}

	#[test]
	fn independent_modules_are_ordered_deterministically() {
		let c = cfg("[schema.zebra]\n[schema.alpha]\n[schema.middle]\n");
		let order = c.module_order(&["zebra".into(), "alpha".into(), "middle".into()]).unwrap();
		assert_eq!(order, vec!["alpha", "middle", "zebra"]);
	}

	#[test]
	fn rejects_a_dependency_cycle_and_names_it() {
		let err = ProjectConfig::parse(
			"[schema.a]\ndepends_on = [\"b\"]\n[schema.b]\ndepends_on = [\"a\"]\n",
		)
		.unwrap_err()
		.to_string();
		assert!(err.contains("cycle"), "got: {err}");
		assert!(err.contains('a') && err.contains('b'), "cycle should name members: {err}");
	}

	#[test]
	fn rejects_self_dependency() {
		assert!(ProjectConfig::parse("[schema.a]\ndepends_on = [\"a\"]\n").is_err());
	}

	#[test]
	fn rejects_unknown_dependency() {
		let err =
			ProjectConfig::parse("[schema.a]\ndepends_on = [\"nope\"]\n").unwrap_err().to_string();
		assert!(err.contains("nope"), "got: {err}");
	}

	#[test]
	fn rejects_invalid_module_name() {
		let err = ProjectConfig::parse("[schema.Billing]\n").unwrap_err().to_string();
		assert!(err.contains("Billing"), "got: {err}");
	}

	#[test]
	fn rejects_inline_password_with_an_actionable_message() {
		let err =
			ProjectConfig::parse("[target.acme]\npass = \"hunter2\"\n").unwrap_err().to_string();
		assert!(err.contains("pass_env"), "error must point at pass_env: {err}");
		assert!(!err.contains("hunter2"), "error must not echo the secret: {err}");
	}

	#[test]
	fn rejects_inline_password_named_password() {
		assert!(ProjectConfig::parse("[target.acme]\npassword = \"x\"\n").is_err());
	}

	#[test]
	fn allows_pass_env() {
		let c = cfg("[target.acme]\npass_env = \"ACME_PW\"\n");
		assert_eq!(c.target["acme"].pass_env.as_deref(), Some("ACME_PW"));
	}

	#[test]
	fn rejects_unknown_keys() {
		assert!(ProjectConfig::parse("[schema.a]\ntypo_here = 1\n").is_err());
	}

	#[test]
	fn rejects_more_than_one_primary_target() {
		let err = ProjectConfig::parse("[target.a]\nprimary = true\n[target.b]\nprimary = true\n")
			.unwrap_err()
			.to_string();
		assert!(err.contains("primary"), "got: {err}");
	}

	#[test]
	fn rejects_target_listing_an_undeclared_module() {
		let err =
			ProjectConfig::parse("[target.a]\nschemas = [\"ghost\"]\n").unwrap_err().to_string();
		assert!(err.contains("ghost"), "got: {err}");
	}

	#[test]
	fn target_schema_restriction_filters_modules() {
		let c = cfg("[schema.core]\n[schema.billing]\n[target.a]\nschemas = [\"core\"]\n");
		let base = DbCfg::from_env(None, &Default::default()).unwrap();
		let t = Target::resolve("a", &c.target["a"], &base).unwrap();
		assert!(t.allows("core"));
		assert!(!t.allows("billing"));
	}

	#[test]
	fn an_unrestricted_target_allows_every_module() {
		let c = cfg("[schema.core]\n[target.a]\n");
		let base = DbCfg::from_env(None, &Default::default()).unwrap();
		let t = Target::resolve("a", &c.target["a"], &base).unwrap();
		assert!(t.allows("core"));
		assert!(t.allows("anything"));
	}

	#[test]
	fn target_overrides_only_what_it_sets() {
		let c = cfg("[target.acme]\nns = \"acme\"\ndb = \"prod\"\n");
		let base = DbCfg::from_env(None, &Default::default()).unwrap();
		let base_host = base.host().to_string();
		let t = Target::resolve("acme", &c.target["acme"], &base).unwrap();
		assert_eq!(t.cfg().ns(), "acme");
		assert_eq!(t.cfg().db(), "prod");
		assert_eq!(t.cfg().host(), base_host, "unset fields must inherit");
	}

	#[test]
	fn unset_pass_env_fails_before_connecting() {
		let c = cfg("[target.acme]\npass_env = \"SURREALKIT_DEFINITELY_UNSET_VAR\"\n");
		let base = DbCfg::from_env(None, &Default::default()).unwrap();
		let err = Target::resolve("acme", &c.target["acme"], &base).unwrap_err().to_string();
		assert!(err.contains("SURREALKIT_DEFINITELY_UNSET_VAR"), "got: {err}");
	}

	#[test]
	fn discover_walks_upwards() {
		let tmp = tempfile::TempDir::new().unwrap();
		let nested = tmp.path().join("a").join("b");
		std::fs::create_dir_all(&nested).unwrap();
		std::fs::write(tmp.path().join(CONFIG_FILE_NAME), "[variables]\n").unwrap();
		assert_eq!(
			ProjectConfig::discover(&nested),
			Some(tmp.path().join(CONFIG_FILE_NAME)),
			"config should be found from a subdirectory"
		);
	}

	#[test]
	fn discover_returns_none_when_absent() {
		let tmp = tempfile::TempDir::new().unwrap();
		assert_eq!(ProjectConfig::discover(tmp.path()), None);
	}
}
