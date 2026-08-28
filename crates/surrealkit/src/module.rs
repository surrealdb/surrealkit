//! Schema modules: named, independently-tracked sets of schema files.
//!
//! Before v1 a project had exactly one unnamed schema (`<folder>/schema/**`)
//! reconciled against one database. A module gives that set a name so several can
//! coexist in one database without pruning each other.
//!
//! # How scoping works
//!
//! SurrealKit's metadata lives in `__entity`, whose `ns` field is a partition key
//! (`'sync' | 'schema' | 'meta' | 'lock'`) rather than a SurrealDB namespace. A
//! module qualifies that partition: the default module keeps the bare names, and a
//! named module appends `@<name>`.
//!
//! | partition | default module | module `billing` |
//! |---|---|---|
//! | file hashes | `sync` | `sync@billing` |
//! | managed entities | `schema` | `schema@billing` |
//! | locks | `lock` | `lock@billing` |
//!
//! Two properties follow, and both are the reason for this design:
//!
//! 1. **No migration.** Rows written before v1 already sit in the default module's
//!    partitions, so an upgraded binary finds them exactly where it expects. No
//!    `DEFINE FIELD`, no index change, no backfill.
//! 2. **Old binaries cannot do damage.** A 0.7 binary queries `WHERE ns = 'sync'`,
//!    so it is blind to named modules: it can neither prune their database objects
//!    nor delete their metadata rows.
//!
//! `meta` is deliberately *not* qualified — it holds database-wide facts (`shared`,
//! `owner`, `last_sync`) that belong to the database, not to any one module.

use std::fmt;

use anyhow::{Result, bail};

/// Separator between a partition and a module name. Chosen because it cannot
/// appear in a module name, so `partition@module` is unambiguous.
const MODULE_SEPARATOR: char = '@';

/// The longest permitted module name.
const MAX_MODULE_NAME_LEN: usize = 64;

/// A metadata partition within `__entity`.
///
/// [`Partition::Meta`] is database-wide and never module-qualified.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Partition {
	/// Per-file content hashes, keyed by the file's tracking path.
	Sync,
	/// The managed-entity catalog, keyed by `kind:scope:name`.
	Schema,
	/// Advisory locks.
	Lock,
	/// Database-wide metadata. Never module-qualified.
	Meta,
}

impl Partition {
	/// The unqualified partition name, as stored in `__entity.ns`.
	pub fn as_str(self) -> &'static str {
		match self {
			Self::Sync => "sync",
			Self::Schema => "schema",
			Self::Lock => "lock",
			Self::Meta => "meta",
		}
	}

	/// Whether this partition is scoped per module. `Meta` is not.
	fn is_module_scoped(self) -> bool {
		!matches!(self, Self::Meta)
	}
}

impl fmt::Display for Partition {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(self.as_str())
	}
}

/// A named schema module.
///
/// Construct with [`Module::new`], or [`Module::default`] for the implicit module
/// that owns everything written before v1.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Module(String);

impl Module {
	/// The name of the module that owns the unqualified (pre-v1) partitions.
	pub const DEFAULT_NAME: &'static str = "default";

	/// Validate and construct a module.
	///
	/// Names are lowercase `[a-z0-9]` plus `_` and `-`, must start alphanumeric,
	/// and are at most 64 characters. `default` yields [`Module::default`].
	///
	/// The charset is deliberately narrow: the name appears in `__entity.ns`
	/// (after `@`), in lock keys, and as a directory name, so anything that could
	/// need quoting or escaping in one of those is rejected up front.
	pub fn new(name: impl Into<String>) -> Result<Self> {
		let name = name.into();

		if name.is_empty() {
			bail!("schema module name is empty");
		}
		if name.len() > MAX_MODULE_NAME_LEN {
			bail!(
				"schema module name {name:?} is {} characters; the maximum is {MAX_MODULE_NAME_LEN}",
				name.len()
			);
		}
		if !name.starts_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit()) {
			bail!(
				"schema module name {name:?} must start with a lowercase letter or digit \
				 (allowed: a-z, 0-9, '_', '-')"
			);
		}
		if let Some(bad) = name
			.chars()
			.find(|c| !(c.is_ascii_lowercase() || c.is_ascii_digit() || *c == '_' || *c == '-'))
		{
			bail!(
				"schema module name {name:?} contains {bad:?}; allowed characters are \
				 a-z, 0-9, '_' and '-'"
			);
		}
		// `meta` is a database-wide partition, so a module of that name would make
		// `meta@meta` readable but `meta` ambiguous in error messages and docs.
		if name == Partition::Meta.as_str() {
			bail!("schema module name {name:?} is reserved");
		}

		Ok(Self(name))
	}

	/// The module owning the unqualified partitions — everything written before v1,
	/// and every project that has not declared modules.
	pub fn default_module() -> Self {
		Self(Self::DEFAULT_NAME.to_string())
	}

	/// Whether this is the default module.
	pub fn is_default(&self) -> bool {
		self.0 == Self::DEFAULT_NAME
	}

	/// The module's name.
	pub fn name(&self) -> &str {
		&self.0
	}

	/// The value to store in (and query from) `__entity.ns` for `partition`.
	///
	/// The default module and [`Partition::Meta`] both yield the bare partition
	/// name, which is what makes upgrading a pre-v1 database a no-op.
	pub fn partition(&self, partition: Partition) -> String {
		if self.is_default() || !partition.is_module_scoped() {
			partition.as_str().to_string()
		} else {
			format!("{}{MODULE_SEPARATOR}{}", partition.as_str(), self.0)
		}
	}
}

impl Default for Module {
	fn default() -> Self {
		Self::default_module()
	}
}

impl fmt::Display for Module {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(&self.0)
	}
}

impl std::str::FromStr for Module {
	type Err = anyhow::Error;

	fn from_str(s: &str) -> Result<Self> {
		Self::new(s)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default_module_uses_bare_partitions() {
		// This is the whole compatibility story: a pre-v1 database's rows are
		// already in these partitions.
		let m = Module::default_module();
		assert_eq!(m.partition(Partition::Sync), "sync");
		assert_eq!(m.partition(Partition::Schema), "schema");
		assert_eq!(m.partition(Partition::Lock), "lock");
		assert_eq!(m.partition(Partition::Meta), "meta");
	}

	#[test]
	fn named_module_qualifies_partitions() {
		let m = Module::new("billing").unwrap();
		assert_eq!(m.partition(Partition::Sync), "sync@billing");
		assert_eq!(m.partition(Partition::Schema), "schema@billing");
		assert_eq!(m.partition(Partition::Lock), "lock@billing");
	}

	#[test]
	fn meta_is_never_module_qualified() {
		// `meta` holds database-wide facts (shared, owner, last_sync).
		let m = Module::new("billing").unwrap();
		assert_eq!(m.partition(Partition::Meta), "meta");
	}

	#[test]
	fn explicitly_naming_default_is_the_default_module() {
		let m = Module::new("default").unwrap();
		assert!(m.is_default());
		assert_eq!(m.partition(Partition::Sync), "sync");
	}

	#[test]
	fn distinct_modules_get_distinct_partitions() {
		let a = Module::new("core").unwrap().partition(Partition::Schema);
		let b = Module::new("billing").unwrap().partition(Partition::Schema);
		let d = Module::default_module().partition(Partition::Schema);
		assert_ne!(a, b);
		assert_ne!(a, d);
		assert_ne!(b, d);
	}

	#[test]
	fn accepts_reasonable_names() {
		for name in ["core", "billing", "a", "0", "with_underscore", "with-dash", "v2", "a1_b2-c3"]
		{
			assert!(Module::new(name).is_ok(), "should accept {name:?}");
		}
	}

	#[test]
	fn rejects_names_that_would_break_partitions_or_paths() {
		for (name, why) in [
			("", "empty"),
			("Billing", "uppercase"),
			("_leading", "leading underscore"),
			("-leading", "leading dash"),
			("has space", "space"),
			("has@at", "separator character"),
			("has/slash", "path separator"),
			("has:colon", "entity-key separator"),
			("has.dot", "dot"),
			("meta", "reserved partition name"),
		] {
			assert!(Module::new(name).is_err(), "should reject {name:?} ({why})");
		}
	}

	#[test]
	fn rejects_over_long_names() {
		assert!(Module::new("a".repeat(MAX_MODULE_NAME_LEN)).is_ok());
		assert!(Module::new("a".repeat(MAX_MODULE_NAME_LEN + 1)).is_err());
	}

	#[test]
	fn error_messages_name_the_offending_input() {
		let err = Module::new("Bad Name").unwrap_err().to_string();
		assert!(err.contains("Bad Name"), "error should quote the input: {err}");
	}

	#[test]
	fn parses_from_str() {
		assert_eq!("billing".parse::<Module>().unwrap().name(), "billing");
		assert!("Bad".parse::<Module>().is_err());
	}
}
