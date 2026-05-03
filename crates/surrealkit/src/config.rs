use std::env;

use anyhow::{Context, Result};
use rust_dotenv::dotenv::DotEnv;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb::opt::auth::{Database, Namespace, Root};

use crate::core::create_surreal_client;

/// The SurrealDB authentication level to use when connecting.
#[derive(Debug, Clone, Default, PartialEq)]
pub enum AuthLevel {
	/// Sign in as a root user (default). Requires `--user` / `--pass` to be root credentials.
	#[default]
	Root,
	/// Sign in as a namespace-scoped user. Credentials must exist on the target namespace.
	Namespace,
	/// Sign in as a database-scoped user. Credentials must exist on the target database.
	Database,
}

impl AuthLevel {
	fn parse(s: &str) -> Option<Self> {
		match s.to_ascii_lowercase().as_str() {
			"root" => Some(Self::Root),
			"namespace" | "ns" => Some(Self::Namespace),
			"database" | "db" => Some(Self::Database),
			_ => None,
		}
	}
}

#[derive(Debug, Clone, Default)]
pub struct DbOverrides {
	pub host: Option<String>,
	pub ns: Option<String>,
	pub db: Option<String>,
	pub user: Option<String>,
	pub pass: Option<String>,
	pub auth_level: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DbCfg {
	host: String,
	ns: String,
	db: String,
	user: String,
	pass: String,
	pub auth_level: AuthLevel,
}

/// Resolve a config value with priority: CLI override → system env vars → .env file → default.
fn resolve(
	cli: &Option<String>,
	env_keys: &[&str],
	dotenv: Option<&DotEnv>,
	default: &str,
) -> String {
	if let Some(v) = cli {
		return v.clone();
	}
	for key in env_keys {
		if let Ok(v) = env::var(key) {
			if !v.is_empty() {
				return v;
			}
		}
	}
	if let Some(dotenv) = dotenv {
		for key in env_keys {
			if let Some(v) = dotenv.get_var(key.to_string())
				&& !v.is_empty()
			{
				return v;
			}
		}
	}
	default.to_string()
}

impl DbCfg {
	pub fn from_env(dotenv: Option<&DotEnv>, overrides: &DbOverrides) -> Result<Self> {
		let host = resolve(
			&overrides.host,
			&["SURREALDB_HOST", "DATABASE_HOST"],
			dotenv,
			"http://localhost:8000",
		);
		let db = resolve(&overrides.db, &["SURREALDB_NAME", "DATABASE_NAME"], dotenv, "test");
		let ns =
			resolve(&overrides.ns, &["SURREALDB_NAMESPACE", "DATABASE_NAMESPACE"], dotenv, "db");
		let user = resolve(&overrides.user, &["SURREALDB_USER", "DATABASE_USER"], dotenv, "root");
		let pass =
			resolve(&overrides.pass, &["SURREALDB_PASSWORD", "DATABASE_PASSWORD"], dotenv, "root");
		let auth_level_str = resolve(
			&overrides.auth_level,
			&["SURREALDB_AUTH_LEVEL", "DATABASE_AUTH_LEVEL"],
			dotenv,
			"root",
		);
		let auth_level = AuthLevel::parse(&auth_level_str).ok_or_else(|| {
			anyhow::anyhow!(
				"invalid auth level {:?}: expected root, namespace/ns, or database/db",
				auth_level_str
			)
		})?;

		Ok(Self {
			host,
			ns,
			db,
			user,
			pass,
			auth_level,
		})
	}

	pub fn host(&self) -> &str {
		&self.host
	}

	pub fn ns(&self) -> &str {
		&self.ns
	}

	pub fn db(&self) -> &str {
		&self.db
	}

	pub fn user(&self) -> &str {
		&self.user
	}

	pub fn pass(&self) -> &str {
		&self.pass
	}

	pub fn auth_level(&self) -> &AuthLevel {
		&self.auth_level
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Mutex;

	use super::*;

	/// Guards tests that mutate real SURREALDB_*/DATABASE_* env vars so they
	/// don't race against each other or against tests that expect clean env.
	static ENV_LOCK: Mutex<()> = Mutex::new(());

	unsafe fn set_env(key: &str, val: &str) {
		unsafe { env::set_var(key, val) };
	}

	unsafe fn unset_env(key: &str) {
		unsafe { env::remove_var(key) };
	}

	fn clear_db_env() {
		unsafe {
			unset_env("SURREALDB_HOST");
			unset_env("SURREALDB_NAME");
			unset_env("SURREALDB_NAMESPACE");
			unset_env("SURREALDB_USER");
			unset_env("SURREALDB_PASSWORD");
			unset_env("SURREALDB_AUTH_LEVEL");
			unset_env("DATABASE_HOST");
			unset_env("DATABASE_NAME");
			unset_env("DATABASE_NAMESPACE");
			unset_env("DATABASE_USER");
			unset_env("DATABASE_PASSWORD");
			unset_env("DATABASE_AUTH_LEVEL");
		}
	}

	// resolve() unit tests use unique key names, safe to run in parallel

	#[test]
	fn resolve_returns_default_when_nothing_set() {
		let result = resolve(&None, &["__TEST_UNSET_VAR__"], None, "fallback");
		assert_eq!(result, "fallback");
	}

	#[test]
	fn resolve_cli_override_wins() {
		unsafe { set_env("__TEST_CLI_WIN__", "from_env") };
		let result = resolve(&Some("from_cli".into()), &["__TEST_CLI_WIN__"], None, "default");
		assert_eq!(result, "from_cli");
		unsafe { unset_env("__TEST_CLI_WIN__") };
	}

	#[test]
	fn resolve_reads_system_env() {
		unsafe { set_env("__TEST_SYS_ENV__", "from_system") };
		let result = resolve(&None, &["__TEST_SYS_ENV__"], None, "default");
		assert_eq!(result, "from_system");
		unsafe { unset_env("__TEST_SYS_ENV__") };
	}

	#[test]
	fn resolve_skips_empty_env_var() {
		unsafe { set_env("__TEST_EMPTY_ENV__", "") };
		let result = resolve(&None, &["__TEST_EMPTY_ENV__"], None, "default");
		assert_eq!(result, "default");
		unsafe { unset_env("__TEST_EMPTY_ENV__") };
	}

	#[test]
	fn resolve_first_env_key_has_priority() {
		unsafe {
			set_env("__TEST_PRI_A__", "first");
			set_env("__TEST_PRI_B__", "second");
		}
		let result = resolve(&None, &["__TEST_PRI_A__", "__TEST_PRI_B__"], None, "default");
		assert_eq!(result, "first");
		unsafe {
			unset_env("__TEST_PRI_A__");
			unset_env("__TEST_PRI_B__");
		}
	}

	#[test]
	fn resolve_falls_through_to_second_env_key() {
		unsafe {
			unset_env("__TEST_FALL_A__");
			set_env("__TEST_FALL_B__", "second");
		}
		let result = resolve(&None, &["__TEST_FALL_A__", "__TEST_FALL_B__"], None, "default");
		assert_eq!(result, "second");
		unsafe { unset_env("__TEST_FALL_B__") };
	}

	// from_env tests touch real SURREALDB_* keys, must hold ENV_LOCK

	#[test]
	fn from_env_uses_defaults_with_no_overrides() {
		let _guard = ENV_LOCK.lock().unwrap();
		clear_db_env();
		let cfg = DbCfg::from_env(None, &DbOverrides::default()).unwrap();
		assert_eq!(cfg.host(), "http://localhost:8000");
		assert_eq!(cfg.db(), "test");
		assert_eq!(cfg.ns(), "db");
		assert_eq!(cfg.user(), "root");
		assert_eq!(cfg.pass(), "root");
	}

	#[test]
	fn from_env_respects_all_overrides() {
		let _guard = ENV_LOCK.lock().unwrap();
		clear_db_env();
		let overrides = DbOverrides {
			host: Some("http://custom:9000".into()),
			db: Some("mydb".into()),
			ns: Some("myns".into()),
			user: Some("admin".into()),
			pass: Some("secret".into()),
			auth_level: None,
		};
		let cfg = DbCfg::from_env(None, &overrides).unwrap();
		assert_eq!(cfg.host(), "http://custom:9000");
		assert_eq!(cfg.db(), "mydb");
		assert_eq!(cfg.ns(), "myns");
		assert_eq!(cfg.user(), "admin");
		assert_eq!(cfg.pass(), "secret");
	}

	#[test]
	fn from_env_defaults_to_root_auth_level() {
		let _guard = ENV_LOCK.lock().unwrap();
		clear_db_env();
		let cfg = DbCfg::from_env(None, &DbOverrides::default()).unwrap();
		assert_eq!(cfg.auth_level(), &AuthLevel::Root);
	}

	#[test]
	fn from_env_parses_auth_level_override() {
		let _guard = ENV_LOCK.lock().unwrap();
		clear_db_env();

		for (input, expected) in [
			("root", AuthLevel::Root),
			("ROOT", AuthLevel::Root),
			("namespace", AuthLevel::Namespace),
			("ns", AuthLevel::Namespace),
			("NS", AuthLevel::Namespace),
			("database", AuthLevel::Database),
			("db", AuthLevel::Database),
			("DB", AuthLevel::Database),
		] {
			let overrides = DbOverrides {
				auth_level: Some(input.into()),
				..Default::default()
			};
			let cfg = DbCfg::from_env(None, &overrides).unwrap();
			assert_eq!(cfg.auth_level(), &expected, "input={input}");
		}
	}

	#[test]
	fn from_env_reads_auth_level_from_env_var() {
		let _guard = ENV_LOCK.lock().unwrap();
		clear_db_env();
		unsafe { set_env("SURREALDB_AUTH_LEVEL", "namespace") };
		let cfg = DbCfg::from_env(None, &DbOverrides::default()).unwrap();
		assert_eq!(cfg.auth_level(), &AuthLevel::Namespace);
		unsafe { unset_env("SURREALDB_AUTH_LEVEL") };
	}

	#[test]
	fn from_env_rejects_unknown_auth_level() {
		let _guard = ENV_LOCK.lock().unwrap();
		clear_db_env();
		let overrides = DbOverrides {
			auth_level: Some("superadmin".into()),
			..Default::default()
		};
		let err = DbCfg::from_env(None, &overrides).unwrap_err();
		assert!(err.to_string().contains("invalid auth level"), "got: {err}");
	}

	#[test]
	fn from_env_reads_surrealdb_env_vars() {
		let _guard = ENV_LOCK.lock().unwrap();
		clear_db_env();
		unsafe {
			set_env("SURREALDB_HOST", "http://envhost:8000");
			set_env("SURREALDB_NAME", "envdb");
			set_env("SURREALDB_NAMESPACE", "envns");
			set_env("SURREALDB_USER", "envuser");
			set_env("SURREALDB_PASSWORD", "envpass");
		}

		let cfg = DbCfg::from_env(None, &DbOverrides::default()).unwrap();
		assert_eq!(cfg.host(), "http://envhost:8000");
		assert_eq!(cfg.db(), "envdb");
		assert_eq!(cfg.ns(), "envns");
		assert_eq!(cfg.user(), "envuser");
		assert_eq!(cfg.pass(), "envpass");

		clear_db_env();
	}

	#[test]
	fn cli_overrides_beat_env_vars() {
		let _guard = ENV_LOCK.lock().unwrap();
		clear_db_env();
		unsafe { set_env("SURREALDB_HOST", "http://envhost:8000") };
		let overrides = DbOverrides {
			host: Some("http://clihost:9000".into()),
			..Default::default()
		};
		let cfg = DbCfg::from_env(None, &overrides).unwrap();
		assert_eq!(cfg.host(), "http://clihost:9000");
		clear_db_env();
	}
}

pub async fn connect(cfg: &DbCfg) -> Result<Surreal<Any>> {
	let db = create_surreal_client(&cfg.host)
		.await
		.with_context(|| format!("Failed connecting to {}", cfg.host))?;

	match cfg.auth_level {
		AuthLevel::Root => {
			db.signin(Root {
				username: cfg.user.clone(),
				password: cfg.pass.clone(),
			})
			.await
			.context("root signin failed")?;
			db.use_ns(&cfg.ns)
				.use_db(&cfg.db)
				.await
				.with_context(|| format!("use_ns/use_db failed for ns={} db={}", cfg.ns, cfg.db))?;
		}
		AuthLevel::Namespace => {
			db.signin(Namespace {
				namespace: cfg.ns.clone(),
				username: cfg.user.clone(),
				password: cfg.pass.clone(),
			})
			.await
			.context("namespace signin failed")?;
			db.use_db(&cfg.db).await.with_context(|| format!("use_db failed for db={}", cfg.db))?;
		}
		AuthLevel::Database => {
			db.signin(Database {
				namespace: cfg.ns.clone(),
				database: cfg.db.clone(),
				username: cfg.user.clone(),
				password: cfg.pass.clone(),
			})
			.await
			.context("database signin failed")?;
		}
	}

	Ok(db)
}
