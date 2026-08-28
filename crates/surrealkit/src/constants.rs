use std::path::PathBuf;

use crate::module::Module;

pub const DEFAULT_ROOT_DIR: &str = "./database";

pub fn setup_surql_path(folder: &str) -> PathBuf {
	PathBuf::from(folder).join("setup.surql")
}

pub fn schema_dir(folder: &str) -> PathBuf {
	PathBuf::from(folder).join("schema")
}

pub fn rollouts_dir(folder: &str) -> PathBuf {
	PathBuf::from(folder).join("rollouts")
}

pub fn state_dir(folder: &str) -> PathBuf {
	PathBuf::from(folder).join("snapshots")
}

pub fn schema_snapshot_path(folder: &str) -> PathBuf {
	state_dir(folder).join("schema_snapshot.json")
}

pub fn catalog_snapshot_path(folder: &str) -> PathBuf {
	state_dir(folder).join("catalog_snapshot.json")
}

pub fn tests_dir(folder: &str) -> PathBuf {
	PathBuf::from(folder).join("tests")
}

pub fn suites_dir(folder: &str) -> PathBuf {
	tests_dir(folder).join("suites")
}

pub fn fixtures_dir(folder: &str) -> PathBuf {
	tests_dir(folder).join("fixtures")
}

pub fn seed_dir(folder: &str) -> PathBuf {
	PathBuf::from(folder).join("seed")
}

pub fn seed_surql_path(folder: &str) -> PathBuf {
	seed_dir(folder).join("seed.surql")
}

pub fn types_dir(folder: &str) -> PathBuf {
	PathBuf::from(folder).join("types")
}

pub fn typegen_output_path(folder: &str) -> PathBuf {
	types_dir(folder).join("schema.json")
}

/// Resolves the on-disk paths for one schema module within a project folder.
///
/// The default module keeps the pre-v1 layout exactly, so an existing project is
/// unchanged. Named modules live under `modules/<name>/`, which keeps
/// `<folder>/schema` unambiguously the default module's -- nesting named modules
/// inside it would make the default module's recursive walk pick them up too.
///
/// | | default | `billing` |
/// |---|---|---|
/// | schema | `<folder>/schema` | `<folder>/modules/billing/schema` |
/// | rollouts | `<folder>/rollouts` | `<folder>/modules/billing/rollouts` |
/// | snapshots | `<folder>/snapshots` | `<folder>/modules/billing/snapshots` |
/// | seed | `<folder>/seed` | `<folder>/modules/billing/seed` |
///
/// `setup.surql` and `tests/` are not module-scoped: the metadata tables and the
/// test suites belong to the project, not to any one module.
#[derive(Debug, Clone)]
pub struct Layout {
	folder: String,
	module: Module,
	/// Set by `[schema.<name>] path`, overriding the conventional schema dir.
	schema_dir: Option<PathBuf>,
}

impl Layout {
	/// Paths for `module` within `folder`.
	pub fn new(folder: impl Into<String>, module: Module) -> Self {
		Self {
			folder: folder.into(),
			module,
			schema_dir: None,
		}
	}

	/// Like [`Layout::new`], but with an explicit schema directory from
	/// `[schema.<name>] path`. Relative paths resolve against the project folder.
	pub fn with_schema_dir(
		folder: impl Into<String>,
		module: Module,
		schema_dir: impl Into<PathBuf>,
	) -> Self {
		let folder = folder.into();
		let dir = schema_dir.into();
		let dir = if dir.is_absolute() {
			dir
		} else {
			PathBuf::from(&folder).join(dir)
		};
		Self {
			folder,
			module,
			schema_dir: Some(dir),
		}
	}

	/// Paths for the default module within `folder` — the pre-v1 layout.
	pub fn default_module(folder: impl Into<String>) -> Self {
		Self::new(folder, Module::default_module())
	}

	/// The project root.
	pub fn folder(&self) -> &str {
		&self.folder
	}

	/// The module these paths belong to.
	pub fn module(&self) -> &Module {
		&self.module
	}

	/// The module's root: the project folder itself for the default module,
	/// `<folder>/modules/<name>` otherwise.
	pub fn root(&self) -> PathBuf {
		if self.module.is_default() {
			PathBuf::from(&self.folder)
		} else {
			PathBuf::from(&self.folder).join("modules").join(self.module.name())
		}
	}

	/// Directory holding this module's `.surql` schema files.
	pub fn schema_dir(&self) -> PathBuf {
		self.schema_dir.clone().unwrap_or_else(|| self.root().join("schema"))
	}

	/// Directory holding this module's rollout manifests.
	pub fn rollouts_dir(&self) -> PathBuf {
		self.root().join("rollouts")
	}

	/// Directory holding this module's snapshots.
	pub fn state_dir(&self) -> PathBuf {
		self.root().join("snapshots")
	}

	/// This module's schema-file-hash snapshot.
	pub fn schema_snapshot_path(&self) -> PathBuf {
		self.state_dir().join("schema_snapshot.json")
	}

	/// This module's catalog snapshot.
	pub fn catalog_snapshot_path(&self) -> PathBuf {
		self.state_dir().join("catalog_snapshot.json")
	}

	/// Directory holding this module's seed files.
	pub fn seed_dir(&self) -> PathBuf {
		self.root().join("seed")
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default_module_layout_matches_the_pre_v1_paths() {
		// Any drift here silently re-keys an existing project's tracked files.
		let l = Layout::default_module("./database");
		assert_eq!(l.schema_dir(), schema_dir("./database"));
		assert_eq!(l.rollouts_dir(), rollouts_dir("./database"));
		assert_eq!(l.state_dir(), state_dir("./database"));
		assert_eq!(l.schema_snapshot_path(), schema_snapshot_path("./database"));
		assert_eq!(l.catalog_snapshot_path(), catalog_snapshot_path("./database"));
		assert_eq!(l.seed_dir(), seed_dir("./database"));
	}

	#[test]
	fn named_module_lives_under_modules() {
		let l = Layout::new("./database", Module::new("billing").unwrap());
		assert_eq!(l.root(), PathBuf::from("./database/modules/billing"));
		assert_eq!(l.schema_dir(), PathBuf::from("./database/modules/billing/schema"));
		assert_eq!(l.rollouts_dir(), PathBuf::from("./database/modules/billing/rollouts"));
		assert_eq!(
			l.schema_snapshot_path(),
			PathBuf::from("./database/modules/billing/snapshots/schema_snapshot.json")
		);
		assert_eq!(l.seed_dir(), PathBuf::from("./database/modules/billing/seed"));
	}

	#[test]
	fn named_module_does_not_nest_inside_the_default_schema_dir() {
		// If it did, the default module's recursive walk would also collect the
		// named module's files and then claim ownership of them.
		let default_schema = Layout::default_module("./database").schema_dir();
		let billing = Layout::new("./database", Module::new("billing").unwrap()).schema_dir();
		assert!(
			!billing.starts_with(&default_schema),
			"{billing:?} must not be inside {default_schema:?}"
		);
	}

	#[test]
	fn distinct_modules_get_distinct_directories() {
		let a = Layout::new("./database", Module::new("core").unwrap()).schema_dir();
		let b = Layout::new("./database", Module::new("billing").unwrap()).schema_dir();
		assert_ne!(a, b);
	}
}
