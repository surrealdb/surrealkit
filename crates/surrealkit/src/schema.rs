use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::config::Cfg;
use crate::constants::{
	named_rollouts_dir, named_schema_dir, named_seed_dir, named_state_dir, seed_dir,
};
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

#[derive(Debug, Clone)]
pub enum SchemaTarget {
	Legacy {
		workspace: SchemaWorkspace,
		seed_dirs: Vec<PathBuf>,
		ns: String,
		db: String,
	},
	Named {
		schema: ResolvedSchema,
	},
}

impl SchemaTarget {
	pub fn label(&self) -> &str {
		match self {
			Self::Legacy {
				workspace,
				..
			} => &workspace.label,
			Self::Named {
				schema,
			} => &schema.name,
		}
	}

	pub fn ns(&self) -> &str {
		match self {
			Self::Legacy {
				ns,
				..
			} => ns,
			Self::Named {
				schema,
			} => &schema.ns,
		}
	}

	pub fn db(&self) -> &str {
		match self {
			Self::Legacy {
				db,
				..
			} => db,
			Self::Named {
				schema,
			} => &schema.db,
		}
	}

	pub fn workspace(&self) -> &SchemaWorkspace {
		match self {
			Self::Legacy {
				workspace,
				..
			} => workspace,
			Self::Named {
				schema,
			} => &schema.workspace,
		}
	}

	pub fn seed_dirs(&self) -> &[PathBuf] {
		match self {
			Self::Legacy {
				seed_dirs,
				..
			} => seed_dirs,
			Self::Named {
				schema,
			} => &schema.seed_dirs,
		}
	}

	pub fn connect_config(&self, cfg: &Cfg) -> Cfg {
		cfg.with_target(self.ns().to_string(), self.db().to_string())
	}

	pub fn is_legacy(&self) -> bool {
		matches!(self, Self::Legacy { .. })
	}

	pub fn schema_name(&self) -> Option<&str> {
		match self {
			Self::Legacy {
				..
			} => None,
			Self::Named {
				schema,
			} => Some(&schema.name),
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaResolveError {
	NotFound {
		name: String,
	},
	Cycle {
		name: String,
	},
	Abstract {
		name: String,
		missing: &'static str,
	},
	MissingVariables {
		name: String,
		variables: Vec<String>,
	},
	Template {
		name: String,
		message: String,
	},
}

impl std::fmt::Display for SchemaResolveError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::NotFound {
				name,
			} => write!(f, "schema '{}' was not found", name),
			Self::Cycle {
				name,
			} => write!(f, "schema inheritance cycle detected at '{}'", name),
			Self::Abstract {
				name,
				missing,
			} => write!(f, "schema '{}' is abstract: missing {}", name, missing),
			Self::MissingVariables {
				name,
				variables,
			} => {
				write!(
					f,
					"schema '{}' requires missing variable(s): {}",
					name,
					variables.join(", ")
				)
			}
			Self::Template {
				name,
				message,
			} => write!(f, "schema '{}' template resolution failed: {}", name, message),
		}
	}
}

impl std::error::Error for SchemaResolveError {}

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

	/// Sorted list of all schema names defined in `surrealkit.toml`.
	pub fn names(&self) -> Vec<String> {
		let mut names: Vec<String> = self.definitions.keys().cloned().collect();
		names.sort();
		names
	}

	pub fn resolve(
		&self,
		name: &str,
		root_folder: &str,
		vars: &TemplateVars,
	) -> Result<ResolvedSchema> {
		self.resolve_typed(name, root_folder, vars).map_err(|err| anyhow::anyhow!(err))
	}

	pub fn resolve_typed(
		&self,
		name: &str,
		root_folder: &str,
		vars: &TemplateVars,
	) -> std::result::Result<ResolvedSchema, SchemaResolveError> {
		let merged = self.resolve_merged(name)?;
		let missing = missing_required_vars(&merged.required_variables, vars);
		if !missing.is_empty() {
			return Err(SchemaResolveError::MissingVariables {
				name: name.to_string(),
				variables: missing,
			});
		}
		let ns = merged
			.ns
			.as_deref()
			.ok_or_else(|| SchemaResolveError::Abstract {
				name: name.to_string(),
				missing: "ns",
			})
			.and_then(|value| render_target_template(name, value, vars))?;
		let db = merged
			.db
			.as_deref()
			.ok_or_else(|| SchemaResolveError::Abstract {
				name: name.to_string(),
				missing: "db",
			})
			.and_then(|value| render_target_template(name, value, vars))?;

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

	/// Resolve all schemas, skipping abstract ones (no `ns`/`db`) but erroring
	/// on any schema whose required template variables are missing.
	pub fn resolve_all(
		&self,
		root_folder: &str,
		vars: &TemplateVars,
	) -> Result<Vec<ResolvedSchema>> {
		let mut names = self.definitions.keys().cloned().collect::<Vec<_>>();
		names.sort();

		let mut schemas = Vec::new();
		for name in names {
			match self.resolve_typed(&name, root_folder, vars) {
				Ok(schema) => schemas.push(schema),
				Err(SchemaResolveError::Abstract {
					..
				}) => {}
				Err(err) => return Err(anyhow::anyhow!(err)),
			}
		}

		Ok(schemas)
	}

	/// Like [`resolve_all`] but also silently skips template schemas whose
	/// required variables are not present in `vars`. Use when the caller
	/// supplies only a subset of vars and wants to sync whatever can be
	/// fully resolved (e.g. `--skip-template-schemas`).
	pub fn resolve_all_skip_templates(
		&self,
		root_folder: &str,
		vars: &TemplateVars,
	) -> Result<Vec<ResolvedSchema>> {
		let mut names = self.definitions.keys().cloned().collect::<Vec<_>>();
		names.sort();

		let mut schemas = Vec::new();
		for name in names {
			match self.resolve_typed(&name, root_folder, vars) {
				Ok(schema) => schemas.push(schema),
				Err(
					SchemaResolveError::Abstract {
						..
					}
					| SchemaResolveError::MissingVariables {
						..
					}
					| SchemaResolveError::Template {
						..
					},
				) => {}
				Err(err) => return Err(anyhow::anyhow!(err)),
			}
		}

		Ok(schemas)
	}

	pub fn resolve_targets(
		&self,
		schema: Option<&str>,
		skip_template_schemas: bool,
		root_folder: &str,
		vars: &TemplateVars,
		legacy_ns: &str,
		legacy_db: &str,
	) -> Result<Vec<SchemaTarget>> {
		if let Some(name) = schema {
			if self.is_empty() {
				bail!(
					"--schema '{}' was provided, but no [schema.*] entries were found in surrealkit.toml",
					name
				);
			}
			return Ok(vec![SchemaTarget::Named {
				schema: self.resolve(name, root_folder, vars)?,
			}]);
		}

		if self.is_empty() {
			return Ok(vec![legacy_target(root_folder, legacy_ns, legacy_db)]);
		}

		let schemas = if skip_template_schemas {
			self.resolve_all_skip_templates(root_folder, vars)?
		} else {
			self.resolve_all(root_folder, vars)?
		};
		Ok(schemas
			.into_iter()
			.map(|schema| SchemaTarget::Named {
				schema,
			})
			.collect())
	}

	pub fn resolve_required_target(
		&self,
		schema: Option<&str>,
		root_folder: &str,
		vars: &TemplateVars,
		legacy_ns: &str,
		legacy_db: &str,
	) -> Result<SchemaTarget> {
		if let Some(name) = schema {
			if self.is_empty() {
				bail!(
					"--schema '{}' was provided, but no [schema.*] entries were found in surrealkit.toml",
					name
				);
			}
			return Ok(SchemaTarget::Named {
				schema: self.resolve(name, root_folder, vars)?,
			});
		}

		if self.is_empty() {
			return Ok(legacy_target(root_folder, legacy_ns, legacy_db));
		}

		bail!(
			"surrealkit.toml defines named schemas; --schema <name> is required.\n\nAvailable schemas: {}",
			self.names().join(", ")
		)
	}

	fn resolve_merged(&self, name: &str) -> std::result::Result<MergedSchema, SchemaResolveError> {
		let mut visiting = BTreeSet::new();
		let mut chain = Vec::new();
		self.resolve_chain(name, &mut visiting, &mut chain)?;

		let mut ns = None;
		let mut db = None;
		let mut required = BTreeSet::new();
		for schema_name in &chain {
			let definition =
				self.definitions.get(schema_name).ok_or_else(|| SchemaResolveError::NotFound {
					name: schema_name.clone(),
				})?;
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
	) -> std::result::Result<(), SchemaResolveError> {
		let definition =
			self.definitions.get(name).ok_or_else(|| SchemaResolveError::NotFound {
				name: name.to_string(),
			})?;
		if !visiting.insert(name.to_string()) {
			return Err(SchemaResolveError::Cycle {
				name: name.to_string(),
			});
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

fn legacy_target(root_folder: &str, legacy_ns: &str, legacy_db: &str) -> SchemaTarget {
	SchemaTarget::Legacy {
		workspace: SchemaWorkspace::legacy(root_folder),
		seed_dirs: vec![seed_dir(root_folder)],
		ns: legacy_ns.to_string(),
		db: legacy_db.to_string(),
	}
}

fn render_target_template(
	name: &str,
	value: &str,
	vars: &TemplateVars,
) -> std::result::Result<String, SchemaResolveError> {
	variables::apply(value, &vars.vars)
		.with_context(|| format!("rendering schema target template '{}'", value))
		.map_err(|err| SchemaResolveError::Template {
			name: name.to_string(),
			message: format!("{err:#}"),
		})
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

		let resolved = catalog.resolve("admin", "./database", &TemplateVars::default()).unwrap();
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

		let resolved = catalog.resolve("org", "./database", &vars(&[("org_id", "acme")])).unwrap();
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

		let err = catalog.resolve("org", "./database", &TemplateVars::default()).unwrap_err();
		assert!(err.to_string().contains("requires missing variable"));
	}

	#[test]
	fn missing_variables_are_typed_resolution_errors() {
		let catalog = catalog(
			r#"
[schema.org]
ns = "org_${org_id}"
db = "main"
required_variables = ["org_id"]
"#,
		);

		let err = catalog.resolve_typed("org", "./database", &TemplateVars::default()).unwrap_err();
		assert!(matches!(err, SchemaResolveError::MissingVariables { .. }));
	}

	#[test]
	fn resolves_legacy_target_when_catalog_is_empty() {
		let catalog = catalog("");
		let targets = catalog
			.resolve_targets(
				None,
				false,
				"./database",
				&TemplateVars::default(),
				"legacy_ns",
				"legacy_db",
			)
			.unwrap();

		assert_eq!(targets.len(), 1);
		assert!(targets[0].is_legacy());
		assert_eq!(targets[0].ns(), "legacy_ns");
		assert_eq!(targets[0].db(), "legacy_db");
	}

	#[test]
	fn schema_flag_without_catalog_is_rejected() {
		let catalog = catalog("");
		let err = catalog
			.resolve_targets(
				Some("admin"),
				false,
				"./database",
				&TemplateVars::default(),
				"legacy_ns",
				"legacy_db",
			)
			.unwrap_err();

		assert!(err.to_string().contains("no [schema.*] entries"));
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

		let err = catalog.resolve("a", "./database", &TemplateVars::default()).unwrap_err();
		assert!(err.to_string().contains("cycle"));
	}
}
