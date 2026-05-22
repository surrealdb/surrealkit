use std::collections::HashMap;
use std::env;
use std::path::Path;

use anyhow::{Context, Result, bail};
use rust_dotenv::dotenv::DotEnv;
use serde::Deserialize;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb::opt::auth::{Database, Namespace, Root};

use crate::constants::DEFAULT_ROOT_DIR;
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
pub struct ConfigOverrides {
	pub connection: Option<String>,
	pub host: Option<String>,
	pub ns: Option<String>,
	pub db: Option<String>,
	pub user: Option<String>,
	pub pass: Option<String>,
	pub auth_level: Option<String>,
	pub folder: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct ProjectConfig {
	#[serde(default)]
	connections: HashMap<String, ConnectionDefinition>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectionDefinition {
	host: Option<String>,
	user: Option<String>,
	#[serde(alias = "password")]
	pass: Option<String>,
	auth_level: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Cfg {
	host: String,
	ns: String,
	db: String,
	user: String,
	pass: String,
	pub auth_level: AuthLevel,
	pub folder: String,
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

fn read_project_config(toml_path: Option<&Path>) -> Result<ProjectConfig> {
	let cfg_path = toml_path.unwrap_or_else(|| Path::new("surrealkit.toml"));
	if !cfg_path.exists() {
		return Ok(ProjectConfig::default());
	}

	let raw = std::fs::read_to_string(cfg_path)
		.with_context(|| format!("reading {}", cfg_path.display()))?;
	let cfg: ProjectConfig =
		toml::from_str(&raw).with_context(|| format!("parsing {}", cfg_path.display()))?;
	Ok(cfg)
}

fn normalized_connection_env_name(connection: &str) -> Result<String> {
	let mut out = String::new();
	let mut last_was_separator = false;

	for ch in connection.chars() {
		if ch.is_ascii_alphanumeric() {
			out.push(ch.to_ascii_uppercase());
			last_was_separator = false;
		} else if !out.is_empty() && !last_was_separator {
			out.push('_');
			last_was_separator = true;
		}
	}

	while out.ends_with('_') {
		out.pop();
	}

	if out.is_empty() {
		bail!(
			"invalid connection {:?}: connection names must contain at least one ASCII letter or digit",
			connection
		);
	}

	Ok(out)
}

fn connection_env_key(connection_env_name: &str, field: &str) -> String {
	format!("SURREALDB_CONNECTION_{connection_env_name}_{field}")
}

fn read_system_env(key: &str) -> Option<String> {
	env::var(key).ok().filter(|v| !v.is_empty())
}

fn read_dotenv(dotenv: Option<&DotEnv>, key: &str) -> Option<String> {
	dotenv.and_then(|dotenv| dotenv.get_var(key.to_string()).filter(|v| !v.is_empty()))
}

fn has_connection_env(connection_env_name: &str, dotenv: Option<&DotEnv>) -> bool {
	["HOST", "USER", "PASSWORD", "AUTH_LEVEL"].iter().any(|field| {
		let key = connection_env_key(connection_env_name, field);
		read_system_env(&key).is_some() || read_dotenv(dotenv, &key).is_some()
	})
}

fn resolve_connection_value(
	cli: &Option<String>,
	connection_env_name: Option<&str>,
	connection_value: Option<&String>,
	field: &str,
	global_env_keys: &[&str],
	dotenv: Option<&DotEnv>,
	default: &str,
) -> String {
	if let Some(v) = cli {
		return v.clone();
	}

	if let Some(connection_env_name) = connection_env_name {
		let key = connection_env_key(connection_env_name, field);
		if let Some(v) = read_system_env(&key) {
			return v;
		}
		if let Some(v) = read_dotenv(dotenv, &key) {
			return v;
		}
	}

	if let Some(v) = connection_value {
		return v.clone();
	}

	resolve(&None, global_env_keys, dotenv, default)
}

impl Cfg {
	pub fn from_env(dotenv: Option<&DotEnv>, overrides: &ConfigOverrides) -> Result<Self> {
		Self::from_env_with_project_config_path(dotenv, overrides, None)
	}

	fn from_env_with_project_config_path(
		dotenv: Option<&DotEnv>,
		overrides: &ConfigOverrides,
		toml_path: Option<&Path>,
	) -> Result<Self> {
		let project_config = read_project_config(toml_path)?;
		let connection_env_name =
			overrides.connection.as_deref().map(normalized_connection_env_name).transpose()?;
		let connection = if let Some(connection_name) = overrides.connection.as_deref() {
			let connection = project_config.connections.get(connection_name);
			if connection.is_none()
				&& !connection_env_name
					.as_deref()
					.is_some_and(|env_name| has_connection_env(env_name, dotenv))
			{
				bail!(
					"connection {:?} was not found in surrealkit.toml and no SURREALDB_CONNECTION_{}_* env vars were set",
					connection_name,
					connection_env_name.as_deref().unwrap_or("UNKNOWN")
				);
			}
			connection
		} else {
			None
		};

		let host = resolve_connection_value(
			&overrides.host,
			connection_env_name.as_deref(),
			connection.and_then(|connection| connection.host.as_ref()),
			"HOST",
			&["SURREALDB_HOST", "DATABASE_HOST"],
			dotenv,
			"http://localhost:8000",
		);
		let db = resolve(&overrides.db, &["SURREALDB_NAME", "DATABASE_NAME"], dotenv, "test");
		let ns =
			resolve(&overrides.ns, &["SURREALDB_NAMESPACE", "DATABASE_NAMESPACE"], dotenv, "db");
		let user = resolve_connection_value(
			&overrides.user,
			connection_env_name.as_deref(),
			connection.and_then(|connection| connection.user.as_ref()),
			"USER",
			&["SURREALDB_USER", "DATABASE_USER"],
			dotenv,
			"root",
		);
		let pass = resolve_connection_value(
			&overrides.pass,
			connection_env_name.as_deref(),
			connection.and_then(|connection| connection.pass.as_ref()),
			"PASSWORD",
			&["SURREALDB_PASSWORD", "DATABASE_PASSWORD"],
			dotenv,
			"root",
		);
		let auth_level_str = resolve_connection_value(
			&overrides.auth_level,
			connection_env_name.as_deref(),
			connection.and_then(|connection| connection.auth_level.as_ref()),
			"AUTH_LEVEL",
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
		let folder = resolve(&None, &["SURREALDB_FOLDER"], dotenv, DEFAULT_ROOT_DIR);

		Ok(Self {
			host,
			ns,
			db,
			user,
			pass,
			auth_level,
			folder,
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

	pub fn folder(&self) -> &str {
		&self.folder
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
			unset_env("SURREALDB_CONNECTION_LOCAL_HOST");
			unset_env("SURREALDB_CONNECTION_LOCAL_USER");
			unset_env("SURREALDB_CONNECTION_LOCAL_PASSWORD");
			unset_env("SURREALDB_CONNECTION_LOCAL_AUTH_LEVEL");
			unset_env("SURREALDB_CONNECTION_LOCAL_NAME");
			unset_env("SURREALDB_CONNECTION_LOCAL_NAMESPACE");
			unset_env("SURREALDB_CONNECTION_STAGING_US_HOST");
		}
	}

	fn write_project_config(raw: &str) -> (tempfile::TempDir, std::path::PathBuf) {
		let tmp = tempfile::tempdir().unwrap();
		let path = tmp.path().join("surrealkit.toml");
		std::fs::write(&path, raw).unwrap();
		(tmp, path)
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
		let cfg = Cfg::from_env(None, &ConfigOverrides::default()).unwrap();
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
		let overrides = ConfigOverrides {
			connection: None,
			host: Some("http://custom:9000".into()),
			db: Some("mydb".into()),
			ns: Some("myns".into()),
			user: Some("admin".into()),
			pass: Some("secret".into()),
			auth_level: None,
			folder: None,
		};
		let cfg = Cfg::from_env(None, &overrides).unwrap();
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
		let cfg = Cfg::from_env(None, &ConfigOverrides::default()).unwrap();
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
			let overrides = ConfigOverrides {
				auth_level: Some(input.into()),
				..Default::default()
			};
			let cfg = Cfg::from_env(None, &overrides).unwrap();
			assert_eq!(cfg.auth_level(), &expected, "input={input}");
		}
	}

	#[test]
	fn from_env_reads_auth_level_from_env_var() {
		let _guard = ENV_LOCK.lock().unwrap();
		clear_db_env();
		unsafe { set_env("SURREALDB_AUTH_LEVEL", "namespace") };
		let cfg = Cfg::from_env(None, &ConfigOverrides::default()).unwrap();
		assert_eq!(cfg.auth_level(), &AuthLevel::Namespace);
		unsafe { unset_env("SURREALDB_AUTH_LEVEL") };
	}

	#[test]
	fn from_env_rejects_unknown_auth_level() {
		let _guard = ENV_LOCK.lock().unwrap();
		clear_db_env();
		let overrides = ConfigOverrides {
			auth_level: Some("superadmin".into()),
			..Default::default()
		};
		let err = Cfg::from_env(None, &overrides).unwrap_err();
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

		let cfg = Cfg::from_env(None, &ConfigOverrides::default()).unwrap();
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
		let overrides = ConfigOverrides {
			host: Some("http://clihost:9000".into()),
			..Default::default()
		};
		let cfg = Cfg::from_env(None, &overrides).unwrap();
		assert_eq!(cfg.host(), "http://clihost:9000");
		clear_db_env();
	}

	#[test]
	fn from_env_reads_toml_connection_values() {
		let _guard = ENV_LOCK.lock().unwrap();
		clear_db_env();
		let (_tmp, cfg_path) = write_project_config(
			r#"
[connections.local]
host = "http://tomlhost:8000"
user = "tomluser"
pass = "tomlpass"
auth_level = "namespace"
"#,
		);
		let overrides = ConfigOverrides {
			connection: Some("local".into()),
			..Default::default()
		};

		let cfg =
			Cfg::from_env_with_project_config_path(None, &overrides, Some(&cfg_path)).unwrap();

		assert_eq!(cfg.host(), "http://tomlhost:8000");
		assert_eq!(cfg.user(), "tomluser");
		assert_eq!(cfg.pass(), "tomlpass");
		assert_eq!(cfg.auth_level(), &AuthLevel::Namespace);
		assert_eq!(cfg.ns(), "db");
		assert_eq!(cfg.db(), "test");
	}

	#[test]
	fn from_env_accepts_password_alias_in_toml_connection() {
		let _guard = ENV_LOCK.lock().unwrap();
		clear_db_env();
		let (_tmp, cfg_path) = write_project_config(
			r#"
[connections.local]
password = "tomlpass"
"#,
		);
		let overrides = ConfigOverrides {
			connection: Some("local".into()),
			..Default::default()
		};

		let cfg =
			Cfg::from_env_with_project_config_path(None, &overrides, Some(&cfg_path)).unwrap();

		assert_eq!(cfg.pass(), "tomlpass");
	}

	#[test]
	fn from_env_rejects_unknown_toml_connection_fields() {
		let _guard = ENV_LOCK.lock().unwrap();
		clear_db_env();

		for field in ["ns", "namespace", "db", "database"] {
			let raw = format!(
				r#"
[connections.local]
{field} = "not_allowed"
"#
			);
			let (_tmp, cfg_path) = write_project_config(&raw);
			let overrides = ConfigOverrides {
				connection: Some("local".into()),
				..Default::default()
			};

			let err = Cfg::from_env_with_project_config_path(None, &overrides, Some(&cfg_path))
				.unwrap_err();
			let err = format!("{err:#}");

			assert!(err.contains("unknown field"), "got: {err}");
			assert!(err.contains(field));
		}
	}

	#[test]
	fn connection_specific_env_overrides_toml_connection_values() {
		let _guard = ENV_LOCK.lock().unwrap();
		clear_db_env();
		let (_tmp, cfg_path) = write_project_config(
			r#"
[connections.local]
host = "http://tomlhost:8000"
"#,
		);
		unsafe { set_env("SURREALDB_CONNECTION_LOCAL_HOST", "http://connection-env:8000") };
		let overrides = ConfigOverrides {
			connection: Some("local".into()),
			..Default::default()
		};

		let cfg =
			Cfg::from_env_with_project_config_path(None, &overrides, Some(&cfg_path)).unwrap();

		assert_eq!(cfg.host(), "http://connection-env:8000");
		clear_db_env();
	}

	#[test]
	fn connection_specific_dotenv_overrides_toml_connection_values() {
		let _guard = ENV_LOCK.lock().unwrap();
		clear_db_env();
		let (tmp, cfg_path) = write_project_config(
			r#"
[connections.local]
user = "tomluser"
"#,
		);
		std::fs::write(tmp.path().join(".env"), "SURREALDB_CONNECTION_LOCAL_USER=dotenvuser\n")
			.unwrap();
		let original_dir = env::current_dir().unwrap();
		env::set_current_dir(tmp.path()).unwrap();
		let dotenv = DotEnv::new("");
		env::set_current_dir(original_dir).unwrap();
		let overrides = ConfigOverrides {
			connection: Some("local".into()),
			..Default::default()
		};

		let cfg =
			Cfg::from_env_with_project_config_path(Some(&dotenv), &overrides, Some(&cfg_path))
				.unwrap();

		assert_eq!(cfg.user(), "dotenvuser");
	}

	#[test]
	fn cli_overrides_beat_connection_values() {
		let _guard = ENV_LOCK.lock().unwrap();
		clear_db_env();
		let (_tmp, cfg_path) = write_project_config(
			r#"
[connections.local]
host = "http://tomlhost:8000"
user = "tomluser"
"#,
		);
		let overrides = ConfigOverrides {
			connection: Some("local".into()),
			host: Some("http://clihost:8000".into()),
			user: Some("cliuser".into()),
			..Default::default()
		};

		let cfg =
			Cfg::from_env_with_project_config_path(None, &overrides, Some(&cfg_path)).unwrap();

		assert_eq!(cfg.host(), "http://clihost:8000");
		assert_eq!(cfg.user(), "cliuser");
	}

	#[test]
	fn selected_connection_values_override_global_env_values() {
		let _guard = ENV_LOCK.lock().unwrap();
		clear_db_env();
		let (_tmp, cfg_path) = write_project_config(
			r#"
[connections.local]
host = "http://tomlhost:8000"
user = "tomluser"
"#,
		);
		unsafe {
			set_env("SURREALDB_HOST", "http://global-env:8000");
			set_env("SURREALDB_USER", "globaluser");
		}
		let overrides = ConfigOverrides {
			connection: Some("local".into()),
			..Default::default()
		};

		let cfg =
			Cfg::from_env_with_project_config_path(None, &overrides, Some(&cfg_path)).unwrap();

		assert_eq!(cfg.host(), "http://tomlhost:8000");
		assert_eq!(cfg.user(), "tomluser");
		clear_db_env();
	}

	#[test]
	fn selected_connection_does_not_affect_namespace_or_database() {
		let _guard = ENV_LOCK.lock().unwrap();
		clear_db_env();
		let (_tmp, cfg_path) = write_project_config(
			r#"
[connections.local]
host = "http://tomlhost:8000"
"#,
		);
		unsafe {
			set_env("SURREALDB_NAMESPACE", "globalns");
			set_env("SURREALDB_NAME", "globaldb");
			set_env("SURREALDB_CONNECTION_LOCAL_NAMESPACE", "ignoredns");
			set_env("SURREALDB_CONNECTION_LOCAL_NAME", "ignoreddb");
		}
		let overrides = ConfigOverrides {
			connection: Some("local".into()),
			..Default::default()
		};

		let cfg =
			Cfg::from_env_with_project_config_path(None, &overrides, Some(&cfg_path)).unwrap();

		assert_eq!(cfg.ns(), "globalns");
		assert_eq!(cfg.db(), "globaldb");
		clear_db_env();
	}

	#[test]
	fn missing_selected_connection_returns_error() {
		let _guard = ENV_LOCK.lock().unwrap();
		clear_db_env();
		let (_tmp, cfg_path) = write_project_config("");
		let overrides = ConfigOverrides {
			connection: Some("missing".into()),
			..Default::default()
		};

		let err =
			Cfg::from_env_with_project_config_path(None, &overrides, Some(&cfg_path)).unwrap_err();

		assert!(err.to_string().contains("connection \"missing\" was not found"));
	}

	#[test]
	fn connection_names_normalize_for_env_keys() {
		let _guard = ENV_LOCK.lock().unwrap();
		clear_db_env();
		let (_tmp, cfg_path) = write_project_config("");
		unsafe { set_env("SURREALDB_CONNECTION_STAGING_US_HOST", "http://staging-us:8000") };
		let overrides = ConfigOverrides {
			connection: Some("staging-us".into()),
			..Default::default()
		};

		let cfg =
			Cfg::from_env_with_project_config_path(None, &overrides, Some(&cfg_path)).unwrap();

		assert_eq!(cfg.host(), "http://staging-us:8000");
		clear_db_env();
	}
}

pub async fn connect(cfg: &Cfg) -> Result<Surreal<Any>> {
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
