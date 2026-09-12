//! The (schema module x database target) matrix a single invocation operates on.
//!
//! This lives in the library rather than the binary so that every front end, the
//! CLI and the MCP server alike, resolves `--schema` / `--target` / `--all` /
//! `--no-deps` through exactly the same code. Two implementations of this matrix
//! would drift, and the failure mode is applying a module to the wrong database.
//!
//! Presentation stays with the caller: this module decides *what* to operate on,
//! never how to print it.

use anyhow::{Context, Result, bail};

use crate::config::DbCfg;
use crate::module::Module;
use crate::project::{ProjectConfig, Target};

/// The (schema module x database target) matrix one invocation operates on.
///
/// With no `[schema.*]`/`[target.*]` sections and no selection flags this is a
/// single pair -- the default module against the ambient connection -- which is
/// exactly the pre-1.0 behaviour.
#[derive(Debug)]
pub struct Selection {
	targets: Vec<Target>,
	/// Modules in dependency order. Applies to every target, then filtered by the
	/// target's own `schemas` list.
	modules: Vec<Module>,
}

impl Selection {
	pub fn resolve(
		project: &ProjectConfig,
		base: &DbCfg,
		schemas: &[String],
		targets: &[String],
		all: bool,
		no_deps: bool,
	) -> Result<Self> {
		// Modules: explicit --schema, else every declared module, else the default.
		let declared: Vec<String> = project.schema.keys().cloned().collect();
		let declared_list = if declared.is_empty() {
			"(none)".to_string()
		} else {
			declared.join(", ")
		};
		let wanted: Vec<String> = if !schemas.is_empty() {
			for name in schemas {
				if !project.schema.contains_key(name) && name != Module::DEFAULT_NAME {
					bail!("unknown schema module {name:?}; declared modules are: {declared_list}");
				}
			}
			schemas.to_vec()
		} else if all || !declared.is_empty() {
			declared
		} else {
			vec![Module::DEFAULT_NAME.to_string()]
		};

		// `--schema billing` pulls in `core` by default, like `cargo build -p`, so a
		// module is never applied before what it depends on. --no-deps opts out.
		let ordered = if no_deps {
			let mut only = wanted;
			only.sort();
			only
		} else {
			project.module_order(&wanted)?
		};
		let modules = ordered
			.into_iter()
			.map(Module::new)
			.collect::<Result<Vec<_>>>()
			.context("resolving selected schema modules")?;

		// Targets: explicit --target, else --all, else primary, else the ambient one.
		let resolved = if !targets.is_empty() {
			targets
				.iter()
				.map(|name| {
					let tc = project.target.get(name).ok_or_else(|| {
						anyhow::anyhow!(
							"unknown target {name:?}; declared targets are: {}",
							if project.target.is_empty() {
								"(none)".to_string()
							} else {
								project.target.keys().cloned().collect::<Vec<_>>().join(", ")
							}
						)
					})?;
					Target::resolve(name, tc, base)
				})
				.collect::<Result<Vec<_>>>()?
		} else if all && !project.target.is_empty() {
			project
				.target
				.iter()
				.map(|(n, tc)| Target::resolve(n, tc, base))
				.collect::<Result<Vec<_>>>()?
		} else if let Some((n, tc)) = project.target.iter().find(|(_, t)| t.primary).or_else(|| {
			// A single declared target is unambiguous without `primary`.
			(project.target.len() == 1).then(|| project.target.iter().next()).flatten()
		}) {
			vec![Target::resolve(n, tc, base)?]
		} else {
			vec![Target::implicit(base.clone())]
		};

		Ok(Self {
			targets: resolved,
			modules,
		})
	}

	pub fn targets(&self) -> &[Target] {
		&self.targets
	}

	/// The selected modules that `target` accepts, honouring its `schemas` list.
	pub fn modules_for(&self, target: &Target) -> Vec<Module> {
		self.modules.iter().filter(|m| target.allows(m.name())).cloned().collect()
	}

	pub fn pairs(&self) -> usize {
		self.targets.iter().map(|t| self.modules_for(t).len()).sum()
	}

	/// True when output should be grouped and summarised per pair.
	pub fn is_fan_out(&self) -> bool {
		self.pairs() > 1
	}

	/// The single selected module, for commands that cannot fan out.
	pub fn single_module(&self) -> Result<&Module> {
		match self.modules.as_slice() {
			[one] => Ok(one),
			other => bail!(
				"this command operates on one schema module at a time ({} selected); \
				 pass --schema <NAME>",
				other.len()
			),
		}
	}

	/// The single selected target, for commands that must not fan out.
	///
	/// Rollout execution is a locked, resumable state machine with one
	/// `__rollout` record per database. Fanning one rollout id across N targets
	/// would turn a partial failure into N databases sitting in different phases,
	/// so these commands refuse rather than loop.
	pub fn single_target(&self) -> Result<&Target> {
		match self.targets.as_slice() {
			[one] => Ok(one),
			other => bail!(
				"this command operates on one database target at a time ({} selected); \
				 pass --target <NAME>",
				other.len()
			),
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::config::DbOverrides;

	use super::*;

	fn base() -> DbCfg {
		DbCfg::from_env(None, &DbOverrides::default()).expect("base cfg")
	}

	fn project(raw: &str) -> ProjectConfig {
		ProjectConfig::parse(raw).expect("parse config")
	}

	fn resolve(
		raw: &str,
		schemas: &[&str],
		targets: &[&str],
		all: bool,
		no_deps: bool,
	) -> Selection {
		let schemas: Vec<String> = schemas.iter().map(|s| s.to_string()).collect();
		let targets: Vec<String> = targets.iter().map(|s| s.to_string()).collect();
		Selection::resolve(&project(raw), &base(), &schemas, &targets, all, no_deps)
			.expect("resolve selection")
	}

	#[test]
	fn no_config_and_no_flags_is_one_default_pair() {
		// The pre-1.0 case: exactly today's behaviour, and not fan-out formatted.
		let sel = resolve("", &[], &[], false, false);
		assert_eq!(sel.pairs(), 1);
		assert!(!sel.is_fan_out());
		assert!(sel.modules_for(&sel.targets()[0])[0].is_default());
		assert_eq!(sel.targets()[0].name(), "default");
	}

	#[test]
	fn declared_modules_are_all_selected_by_default() {
		let sel = resolve("[schema.core]\n[schema.billing]\n", &[], &[], false, false);
		assert_eq!(sel.pairs(), 2, "both modules against the ambient target");
	}

	#[test]
	fn selecting_a_module_pulls_in_its_dependencies_in_order() {
		let sel = resolve(
			"[schema.core]\n[schema.billing]\ndepends_on = [\"core\"]\n",
			&["billing"],
			&[],
			false,
			false,
		);
		let names: Vec<String> =
			sel.modules_for(&sel.targets()[0]).iter().map(|m| m.name().to_string()).collect();
		assert_eq!(names, vec!["core", "billing"], "dependency must be applied first");
	}

	#[test]
	fn no_deps_selects_only_what_was_asked_for() {
		let sel = resolve(
			"[schema.core]\n[schema.billing]\ndepends_on = [\"core\"]\n",
			&["billing"],
			&[],
			false,
			true,
		);
		assert_eq!(sel.modules_for(&sel.targets()[0]).len(), 1);
		assert_eq!(sel.modules_for(&sel.targets()[0])[0].name(), "billing");
	}

	#[test]
	fn all_expands_to_the_full_matrix() {
		let sel = resolve(
			"[schema.core]\n[schema.billing]\n[target.acme]\n[target.globex]\n",
			&[],
			&[],
			true,
			false,
		);
		assert_eq!(sel.pairs(), 4, "2 modules x 2 targets");
		assert!(sel.is_fan_out());
	}

	#[test]
	fn a_targets_schema_list_filters_the_matrix() {
		let sel = resolve(
			"[schema.core]\n[schema.billing]\n\
			 [target.acme]\n[target.warehouse]\nschemas = [\"core\"]\n",
			&[],
			&[],
			true,
			false,
		);
		// acme takes both; warehouse only core.
		assert_eq!(sel.pairs(), 3);
	}

	#[test]
	fn a_single_declared_target_is_used_without_being_marked_primary() {
		let sel = resolve("[target.only]\nns = \"x\"\n", &[], &[], false, false);
		assert_eq!(sel.targets().len(), 1);
		assert_eq!(sel.targets()[0].name(), "only");
		assert_eq!(sel.targets()[0].cfg().ns(), "x");
	}

	#[test]
	fn primary_is_chosen_when_several_targets_exist() {
		let sel = resolve("[target.a]\n[target.b]\nprimary = true\n", &[], &[], false, false);
		assert_eq!(sel.targets().len(), 1);
		assert_eq!(sel.targets()[0].name(), "b");
	}

	#[test]
	fn unknown_module_is_rejected_and_lists_the_declared_ones() {
		let err = Selection::resolve(
			&project("[schema.core]\n"),
			&base(),
			&["ghost".to_string()],
			&[],
			false,
			false,
		)
		.unwrap_err()
		.to_string();
		assert!(err.contains("ghost"), "got: {err}");
		assert!(err.contains("core"), "should list declared modules: {err}");
	}

	#[test]
	fn unknown_target_is_rejected_and_lists_the_declared_ones() {
		let err = Selection::resolve(
			&project("[target.acme]\n"),
			&base(),
			&[],
			&["ghost".to_string()],
			false,
			false,
		)
		.unwrap_err()
		.to_string();
		assert!(err.contains("ghost") && err.contains("acme"), "got: {err}");
	}

	#[test]
	fn single_module_errors_when_several_are_selected() {
		let sel = resolve("[schema.a]\n[schema.b]\n", &[], &[], false, false);
		assert!(sel.single_module().is_err(), "commands that cannot fan out must refuse");
	}

	#[test]
	fn single_target_errors_when_several_are_selected() {
		let sel = resolve("[target.a]\n[target.b]\n", &[], &[], true, false);
		assert!(sel.single_target().is_err(), "rollout commands must not fan out across targets");
	}
}
