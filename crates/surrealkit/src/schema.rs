use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::constants::{named_rollouts_dir, named_schema_dir, named_seed_dir, named_state_dir};
use crate::schema_state::SchemaWorkspace;
use crate::variables::{self, TemplateVars};

#[derive(Debug, Clone, Default, Deserialize)]
struct ProjectSchemaConfig {
	#[serde(default)]
	schema: HashMap<String, SchemaDefinition>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaDefinition {
	pub extends: Option<String>,
	pub ns: Option<String>,
	pub db: Option<String>,
	#[serde(default)]
	pub required_variables: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SchemaCatalog {
	definitions: HashMap<String, SchemaDefinition>,
}

#[derive(Debug, Clone)]
pub struct ResolvedSchema {
	pub name: String,
	pub chain: Vec<String>,
	pub ns: String,
	pub db: String,
	pub required_variables: Vec<String>,
	pub schema_dirs: Vec<PathBuf>,
	pub seed_dirs: Vec<PathBuf>,
	pub workspace: SchemaWorkspace,
}

pub fn load_schema_catalog(toml_path: Option<&Path>) -> Result<SchemaCatalog> {
	let path = toml_path.unwrap_or_else(|| Path::new("surrealkit.toml"));
	if !path.exists() {
		return Ok(SchemaCatalog {
			definitions: HashMap::new(),
		});
	}

	let raw =
		std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
	let cfg: ProjectSchemaConfig =
		toml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))?;
	Ok(SchemaCatalog {
		definitions: cfg.schema,
	})
}

impl SchemaCatalog {
	pub fn is_empty(&self) -> bool {
		self.definitions.is_empty()
	}

	pub fn resolve_concrete(
		&self,
		name: &str,
		root_folder: &str,
		vars: &TemplateVars,
	) -> Result<ResolvedSchema> {
		let merged = self.resolve_merged(name)?;
		let missing = missing_required_vars(&merged.required_variables, vars);
		if !missing.is_empty() {
			bail!("schema '{}' requires missing variable(s): {}", name, missing.join(", "));
		}
		let ns = merged
			.ns
			.as_deref()
			.ok_or_else(|| anyhow::anyhow!("schema '{}' is abstract: missing ns", name))
			.and_then(|value| render_target_template(value, vars))?;
		let db = merged
			.db
			.as_deref()
			.ok_or_else(|| anyhow::anyhow!("schema '{}' is abstract: missing db", name))
			.and_then(|value| render_target_template(value, vars))?;

		let schema_dirs = merged
			.chain
			.iter()
			.map(|schema| named_schema_dir(root_folder, schema))
			.collect::<Vec<_>>();
		let seed_dirs = merged
			.chain
			.iter()
			.map(|schema| named_seed_dir(root_folder, schema))
			.collect::<Vec<_>>();
		let workspace = SchemaWorkspace {
			root_folder: root_folder.to_string(),
			schema_dirs: schema_dirs.clone(),
			rollouts_dir: named_rollouts_dir(root_folder, name),
			state_dir: named_state_dir(root_folder, name),
			label: name.to_string(),
		};

		Ok(ResolvedSchema {
			name: name.to_string(),
			chain: merged.chain,
			ns,
			db,
			required_variables: merged.required_variables,
			schema_dirs,
			seed_dirs,
			workspace,
		})
	}

	pub fn resolve_all_concrete(
		&self,
		root_folder: &str,
		vars: &TemplateVars,
	) -> Result<Vec<ResolvedSchema>> {
		let mut names = self.definitions.keys().cloned().collect::<Vec<_>>();
		names.sort();

		let mut schemas = Vec::new();
		for name in names {
			match self.resolve_concrete(&name, root_folder, vars) {
				Ok(schema) => schemas.push(schema),
				Err(err) if is_skippable_all_schema_error(&err.to_string()) => {}
				Err(err) => return Err(err),
			}
		}

		Ok(schemas)
	}

	fn resolve_merged(&self, name: &str) -> Result<MergedSchema> {
		let mut visiting = BTreeSet::new();
		let mut chain = Vec::new();
		self.resolve_chain(name, &mut visiting, &mut chain)?;

		let mut ns = None;
		let mut db = None;
		let mut required = BTreeSet::new();
		for schema_name in &chain {
			let definition = self
				.definitions
				.get(schema_name)
				.ok_or_else(|| anyhow::anyhow!("schema '{}' was not found", schema_name))?;
			if let Some(value) = &definition.ns {
				ns = Some(value.clone());
			}
			if let Some(value) = &definition.db {
				db = Some(value.clone());
			}
			for var in &definition.required_variables {
				required.insert(var.to_ascii_uppercase());
			}
		}

		Ok(MergedSchema {
			chain,
			ns,
			db,
			required_variables: required.into_iter().collect(),
		})
	}

	fn resolve_chain(
		&self,
		name: &str,
		visiting: &mut BTreeSet<String>,
		chain: &mut Vec<String>,
	) -> Result<()> {
		let definition = self
			.definitions
			.get(name)
			.ok_or_else(|| anyhow::anyhow!("schema '{}' was not found", name))?;
		if !visiting.insert(name.to_string()) {
			bail!("schema inheritance cycle detected at '{}'", name);
		}
		if let Some(parent) = definition.extends.as_deref() {
			self.resolve_chain(parent, visiting, chain)?;
		}
		visiting.remove(name);
		chain.push(name.to_string());
		Ok(())
	}
}

#[derive(Debug, Clone)]
struct MergedSchema {
	chain: Vec<String>,
	ns: Option<String>,
	db: Option<String>,
	required_variables: Vec<String>,
}

fn missing_required_vars(required: &[String], vars: &TemplateVars) -> Vec<String> {
	required
		.iter()
		.filter(|name| !vars.vars.contains_key(&name.to_ascii_uppercase()))
		.cloned()
		.collect()
}

fn is_skippable_all_schema_error(message: &str) -> bool {
	message.contains(" is abstract: ")
		|| message.contains("requires missing variable")
		|| message.contains("template variable")
}

fn render_target_template(value: &str, vars: &TemplateVars) -> Result<String> {
	variables::apply(value, &vars.vars)
		.with_context(|| format!("rendering schema target template '{}'", value))
}

#[cfg(test)]
mod tests {
	use super::*;

	fn catalog(raw: &str) -> SchemaCatalog {
		let cfg: ProjectSchemaConfig = toml::from_str(raw).expect("parse schema config");
		SchemaCatalog {
			definitions: cfg.schema,
		}
	}

	fn vars(pairs: &[(&str, &str)]) -> TemplateVars {
		TemplateVars {
			vars: pairs
				.iter()
				.map(|(key, value)| (key.to_ascii_uppercase(), value.to_string()))
				.collect(),
		}
	}

	#[test]
	fn resolves_inheritance_order_and_target() {
		let catalog = catalog(
			r#"
[schema.base]

[schema.admin]
extends = "base"
ns = "system"
db = "main"
"#,
		);

		let resolved =
			catalog.resolve_concrete("admin", "./database", &TemplateVars::default()).unwrap();
		assert_eq!(resolved.chain, vec!["base", "admin"]);
		assert_eq!(resolved.ns, "system");
		assert_eq!(resolved.db, "main");
	}

	#[test]
	fn renders_target_templates() {
		let catalog = catalog(
			r#"
[schema.org]
ns = "org_${org_id}"
db = "main"
required_variables = ["org_id"]
"#,
		);

		let resolved =
			catalog.resolve_concrete("org", "./database", &vars(&[("org_id", "acme")])).unwrap();
		assert_eq!(resolved.ns, "org_acme");
	}

	#[test]
	fn rejects_missing_required_variables() {
		let catalog = catalog(
			r#"
[schema.org]
ns = "org_${org_id}"
db = "main"
required_variables = ["org_id"]
"#,
		);

		let err =
			catalog.resolve_concrete("org", "./database", &TemplateVars::default()).unwrap_err();
		assert!(err.to_string().contains("requires missing variable"));
	}

	#[test]
	fn rejects_cycles() {
		let catalog = catalog(
			r#"
[schema.a]
extends = "b"

[schema.b]
extends = "a"
"#,
		);

		let err =
			catalog.resolve_concrete("a", "./database", &TemplateVars::default()).unwrap_err();
		assert!(err.to_string().contains("cycle"));
	}
}
