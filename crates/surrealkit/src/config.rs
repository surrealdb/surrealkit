use std::env;
use std::future::Future;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use rust_dotenv::dotenv::DotEnv;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb::opt::auth::{Database, Namespace, Root};

use crate::constants::DEFAULT_ROOT_DIR;
use crate::core::create_surreal_client;

/// Default deadline for connect + sign-in.
///
/// Nothing between the CLI and the database had a deadline through
/// 1.0.0-beta.1, so an endpoint that accepted the socket but never completed the
/// handshake -- a database restarting behind a load balancer, a wedged proxy --
/// blocked forever. In a deploy pipeline that surfaces as a rollout that "hangs"
/// until CI kills the job, leaving `__rollout` in an intermediate state.
pub const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 30;

/// `0` means "no deadline", which is the documented escape hatch.
fn secs_to_timeout(secs: u64) -> Option<Duration> {
	(secs > 0).then(|| Duration::from_secs(secs))
}

/// Resolve a timeout with the same CLI -> env -> `.env` -> default precedence as
/// every other setting.
fn resolve_timeout(
	override_secs: Option<u64>,
	env_key: &str,
	dotenv: Option<&DotEnv>,
	default_secs: Option<u64>,
) -> Result<Option<Duration>> {
	if let Some(secs) = override_secs {
		return Ok(secs_to_timeout(secs));
	}
	let from_env = env::var(env_key)
		.ok()
		.filter(|v| !v.is_empty())
		.or_else(|| dotenv.and_then(|d| d.get_var(env_key.to_string())).filter(|v| !v.is_empty()));
	match from_env {
		Some(raw) => {
			let secs = raw.trim().parse::<u64>().with_context(|| {
				format!("{env_key} must be a whole number of seconds, got {raw:?}")
			})?;
			Ok(secs_to_timeout(secs))
		}
		None => Ok(default_secs.and_then(secs_to_timeout)),
	}
}

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
	/// Skip authentication entirely and go straight to `use_ns`/`use_db`.
	///
	/// This is the right choice for embedded engines such as `surrealkv://`,
	/// `rocksdb://`, and `mem://`, which have no users defined on a fresh datastore.
	/// It is selected automatically when the host is an embedded endpoint; pass
	/// `--auth-level none` to force it for any endpoint.
	None,
}

impl AuthLevel {
	/// Parse an auth level from its CLI/config spelling, erroring with the accepted
	/// values rather than returning `None`.
	pub fn parse_str(s: &str) -> Result<Self> {
		Self::parse(s).ok_or_else(|| {
			anyhow::anyhow!(
				"invalid auth level {s:?}: expected root, namespace/ns, database/db, or none"
			)
		})
	}

	fn parse(s: &str) -> Option<Self> {
		match s.to_ascii_lowercase().as_str() {
			"root" => Some(Self::Root),
			"namespace" | "ns" => Some(Self::Namespace),
			"database" | "db" => Some(Self::Database),
			"none" | "no-auth" | "noauth" => Some(Self::None),
			_ => None,
		}
	}
}

/// Returns `true` for endpoints served by an in-process embedded engine, which
/// have no authentication on a fresh datastore. Matching is case-insensitive on
/// the URL scheme.
pub(crate) fn is_embedded_endpoint(host: &str) -> bool {
	let lower = host.to_ascii_lowercase();
	const EMBEDDED_SCHEMES: &[&str] = &[
		"mem://",
		"surrealkv://",
		"surrealkv+versioned://",
		"rocksdb://",
		"speedb://",
		"file://",
		"tikv://",
		"indxdb://",
	];
	EMBEDDED_SCHEMES.iter().any(|scheme| lower.starts_with(scheme))
}

#[derive(Clone, Default)]
/// CLI-supplied connection overrides, each taking priority over the environment.
pub struct DbOverrides {
	pub host: Option<String>,
	pub ns: Option<String>,
	pub db: Option<String>,
	pub user: Option<String>,
	pub pass: Option<String>,
	pub auth_level: Option<String>,
	pub folder: Option<String>,
	/// Seconds to wait for the connection and sign-in to complete. `0` disables
	/// the deadline.
	pub connect_timeout_secs: Option<u64>,
	/// Seconds to wait for a single rollout step's SQL. `0` disables the deadline.
	pub query_timeout_secs: Option<u64>,
}

#[derive(Clone)]
/// A fully resolved database connection.
///
/// Built by [`DbCfg::from_env`], which layers CLI overrides over the process
/// environment, then `.env`, then defaults.
pub struct DbCfg {
	host: String,
	ns: String,
	db: String,
	user: String,
	pass: String,
	pub auth_level: AuthLevel,
	pub folder: String,
	/// How long to wait for connect + sign-in before giving up. `None` waits
	/// forever, which is what every release through 1.0.0-beta.1 did.
	pub connect_timeout: Option<Duration>,
	/// How long to wait for a single rollout step's SQL. `None` waits forever,
	/// which stays the default: a legitimate index build can take hours.
	pub query_timeout: Option<Duration>,
}

/// Shown in place of a password so `{:?}` cannot leak one.
///
/// [`DbCfg`] and [`DbOverrides`] hold resolved credentials and are reachable from
/// other `Debug` types (`Target`, and the CLI's selection), so a derived `Debug`
/// would print passwords into any log or panic message that formatted them.
const REDACTED: &str = "<redacted>";

impl std::fmt::Debug for DbCfg {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("DbCfg")
			.field("host", &self.host)
			.field("ns", &self.ns)
			.field("db", &self.db)
			.field("user", &self.user)
			.field("pass", &REDACTED)
			.field("auth_level", &self.auth_level)
			.field("folder", &self.folder)
			.field("connect_timeout", &self.connect_timeout)
			.field("query_timeout", &self.query_timeout)
			.finish()
	}
}

impl std::fmt::Debug for DbOverrides {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("DbOverrides")
			.field("host", &self.host)
			.field("ns", &self.ns)
			.field("db", &self.db)
			.field("user", &self.user)
			.field("pass", &self.pass.as_ref().map(|_| REDACTED))
			.field("auth_level", &self.auth_level)
			.field("folder", &self.folder)
			.field("connect_timeout_secs", &self.connect_timeout_secs)
			.field("query_timeout_secs", &self.query_timeout_secs)
			.finish()
	}
}

/// The `DATABASE_*` aliases accepted before v1, paired with their replacements.
const LEGACY_ENV_ALIASES: &[(&str, &str)] = &[
	("DATABASE_HOST", "SURREALDB_HOST"),
	("DATABASE_NAME", "SURREALDB_NAME"),
	("DATABASE_NAMESPACE", "SURREALDB_NAMESPACE"),
	("DATABASE_USER", "SURREALDB_USER"),
	("DATABASE_PASSWORD", "SURREALDB_PASSWORD"),
	("DATABASE_AUTH_LEVEL", "SURREALDB_AUTH_LEVEL"),
];

/// Fail when a removed `DATABASE_*` variable is set without its replacement.
///
/// Silently ignoring it would be the worst outcome: an unrecognised
/// `DATABASE_HOST` is not an error, it falls back to `http://localhost:8000`, so
/// a deployment would quietly connect to the *wrong database* instead of failing.
fn reject_orphaned_legacy_env(dotenv: Option<&DotEnv>) -> Result<()> {
	let lookup = |key: &str| -> Option<String> {
		env::var(key)
			.ok()
			.filter(|v| !v.is_empty())
			.or_else(|| dotenv.and_then(|d| d.get_var(key.to_string())).filter(|v| !v.is_empty()))
	};

	let orphaned: Vec<String> = LEGACY_ENV_ALIASES
		.iter()
		.filter(|(legacy, modern)| lookup(legacy).is_some() && lookup(modern).is_none())
		.map(|(legacy, modern)| format!("  {legacy} -> {modern}"))
		.collect();

	if !orphaned.is_empty() {
		anyhow::bail!(
			"the DATABASE_* environment variables were removed in SurrealKit 1.0, but \
			 these are still set with no SURREALDB_* replacement:\n{}\n\
			 Rename them. They are rejected rather than ignored because ignoring them \
			 would silently fall back to the defaults and connect to the wrong database.",
			orphaned.join("\n")
		);
	}
	Ok(())
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
	/// Just the database folder, resolved exactly as [`DbCfg::from_env`]
	/// resolves it: `--folder` → `SURREALDB_FOLDER` → `.env` → `./database`.
	///
	/// The commands that only read the filesystem — `check`, `generate`,
	/// `watch` — need the folder and nothing else, and must not be held up by
	/// the rest of a connection they never open. A `[target.*]` whose
	/// `pass_env` secret is not exported, or a stale `DATABASE_*` variable,
	/// would otherwise fail a static analysis that contacts no database.
	pub fn resolve_folder(dotenv: Option<&DotEnv>, overrides: &DbOverrides) -> String {
		resolve(&overrides.folder, &["SURREALDB_FOLDER"], dotenv, DEFAULT_ROOT_DIR)
	}

	/// Resolve a connection with priority: CLI override → process environment →
	/// `.env` file → default.
	///
	/// Errors if a removed `DATABASE_*` variable is set without its `SURREALDB_*`
	/// replacement — ignoring it would silently fall back to the defaults and
	/// connect to the wrong database.
	pub fn from_env(dotenv: Option<&DotEnv>, overrides: &DbOverrides) -> Result<Self> {
		reject_orphaned_legacy_env(dotenv)?;
		let host = resolve(&overrides.host, &["SURREALDB_HOST"], dotenv, "http://localhost:8000");
		let db = resolve(&overrides.db, &["SURREALDB_NAME"], dotenv, "test");
		let ns = resolve(&overrides.ns, &["SURREALDB_NAMESPACE"], dotenv, "db");
		let user = resolve(&overrides.user, &["SURREALDB_USER"], dotenv, "root");
		let pass = resolve(&overrides.pass, &["SURREALDB_PASSWORD"], dotenv, "root");
		let auth_level_str =
			resolve(&overrides.auth_level, &["SURREALDB_AUTH_LEVEL"], dotenv, "root");
		let auth_level = AuthLevel::parse(&auth_level_str).ok_or_else(|| {
			anyhow::anyhow!(
				"invalid auth level {:?}: expected root, namespace/ns, or database/db",
				auth_level_str
			)
		})?;
		let folder = Self::resolve_folder(dotenv, overrides);
		let connect_timeout = resolve_timeout(
			overrides.connect_timeout_secs,
			"SURREALDB_CONNECT_TIMEOUT_SECS",
			dotenv,
			Some(DEFAULT_CONNECT_TIMEOUT_SECS),
		)?;
		let query_timeout = resolve_timeout(
			overrides.query_timeout_secs,
			"SURREALDB_QUERY_TIMEOUT_SECS",
			dotenv,
			None,
		)?;

		Ok(Self {
			host,
			ns,
			db,
			user,
			pass,
			auth_level,
			folder,
			connect_timeout,
			query_timeout,
		})
	}

	/// A copy of this config with the supplied fields replaced. `None` inherits,
	/// which is what lets a `[target.*]` section name only its `ns`/`db`.
	pub fn overridden(
		&self,
		host: Option<String>,
		ns: Option<String>,
		db: Option<String>,
		user: Option<String>,
		pass: Option<String>,
		auth_level: Option<AuthLevel>,
	) -> Self {
		Self {
			host: host.unwrap_or_else(|| self.host.clone()),
			ns: ns.unwrap_or_else(|| self.ns.clone()),
			db: db.unwrap_or_else(|| self.db.clone()),
			user: user.unwrap_or_else(|| self.user.clone()),
			pass: pass.unwrap_or_else(|| self.pass.clone()),
			auth_level: auth_level.unwrap_or_else(|| self.auth_level.clone()),
			folder: self.folder.clone(),
			connect_timeout: self.connect_timeout,
			query_timeout: self.query_timeout,
		}
	}

	/// A copy of this config with per-target timeout overrides applied. `None`
	/// inherits, so a `[target.*]` section need only name what differs.
	pub fn with_timeouts(
		&self,
		connect_timeout_secs: Option<u64>,
		query_timeout_secs: Option<u64>,
	) -> Self {
		let mut out = self.clone();
		if let Some(secs) = connect_timeout_secs {
			out.connect_timeout = secs_to_timeout(secs);
		}
		if let Some(secs) = query_timeout_secs {
			out.query_timeout = secs_to_timeout(secs);
		}
		out
	}

	/// The endpoint URL.
	pub fn host(&self) -> &str {
		&self.host
	}

	/// The namespace.
	pub fn ns(&self) -> &str {
		&self.ns
	}

	/// The database.
	pub fn db(&self) -> &str {
		&self.db
	}

	/// The username used to sign in.
	pub fn user(&self) -> &str {
		&self.user
	}

	/// The password used to sign in.
	pub fn pass(&self) -> &str {
		&self.pass
	}

	/// The authentication level used when connecting.
	pub fn auth_level(&self) -> &AuthLevel {
		&self.auth_level
	}

	/// The project folder holding schema, seeds and rollouts.
	pub fn folder(&self) -> &str {
		&self.folder
	}
}

/// Await `future`, failing with an actionable message if `budget` elapses first.
///
/// The deadline is enforced client-side on purpose. `surrealdb::opt::Config`'s
/// timeout knobs are server-side for remote endpoints, so they do nothing for the
/// failure that actually bites: a peer that accepts the TCP connection and then
/// never answers.
async fn with_deadline<T>(
	budget: Option<Duration>,
	what: &str,
	endpoint: &str,
	future: impl Future<Output = Result<T>>,
) -> Result<T> {
	match budget {
		None => future.await,
		Some(budget) => match tokio::time::timeout(budget, future).await {
			Ok(result) => result,
			Err(_) => bail!(
				"{what} to {endpoint} timed out after {}s. The endpoint accepted the \
				 connection but did not respond. Raise the budget with \
				 --connect-timeout-secs / SURREALDB_CONNECT_TIMEOUT_SECS, or pass 0 to \
				 wait indefinitely.",
				budget.as_secs()
			),
		},
	}
}

pub async fn connect(cfg: &DbCfg) -> Result<Surreal<Any>> {
	let db = with_deadline(cfg.connect_timeout, "connecting", &cfg.host, async {
		create_surreal_client(&cfg.host)
			.await
			.with_context(|| format!("Failed connecting to {}", cfg.host))
	})
	.await?;

	// Embedded engines have no users on a fresh datastore, so signing in would
	// fail. Auto-detect them and skip auth (unless the user forced a level).
	let auth_level = if is_embedded_endpoint(&cfg.host) {
		AuthLevel::None
	} else {
		cfg.auth_level.clone()
	};

	with_deadline(cfg.connect_timeout, "signing in", &cfg.host, async {
		match auth_level {
			AuthLevel::None => {
				db.use_ns(&cfg.ns).use_db(&cfg.db).await.with_context(|| {
					format!("use_ns/use_db failed for ns={} db={}", cfg.ns, cfg.db)
				})?;
			}
			AuthLevel::Root => {
				db.signin(Root {
					username: cfg.user.clone(),
					password: cfg.pass.clone(),
				})
				.await
				.context("root signin failed")?;
				db.use_ns(&cfg.ns).use_db(&cfg.db).await.with_context(|| {
					format!("use_ns/use_db failed for ns={} db={}", cfg.ns, cfg.db)
				})?;
			}
			AuthLevel::Namespace => {
				db.signin(Namespace {
					namespace: cfg.ns.clone(),
					username: cfg.user.clone(),
					password: cfg.pass.clone(),
				})
				.await
				.context("namespace signin failed")?;
				db.use_db(&cfg.db)
					.await
					.with_context(|| format!("use_db failed for db={}", cfg.db))?;
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
		Ok(())
	})
	.await?;

	Ok(db)
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
			unset_env("SURREALDB_FOLDER");
		}
	}

	#[test]
	fn is_embedded_endpoint_matches_local_schemes() {
		for host in [
			"mem://",
			"surrealkv://./data",
			"SURREALKV://Data",
			"surrealkv+versioned://./data",
			"rocksdb://./db",
			"speedb://./db",
			"file://./db",
			"tikv://127.0.0.1:2379",
			"indxdb://mydb",
		] {
			assert!(is_embedded_endpoint(host), "expected embedded: {host}");
		}
	}

	#[test]
	fn is_embedded_endpoint_rejects_remote_schemes() {
		for host in
			["http://localhost:8000", "https://db.example.com", "ws://localhost:8000", "wss://x"]
		{
			assert!(!is_embedded_endpoint(host), "expected remote: {host}");
		}
	}

	#[test]
	fn auth_level_parses_none_spellings() {
		assert_eq!(AuthLevel::parse("none"), Some(AuthLevel::None));
		assert_eq!(AuthLevel::parse("NONE"), Some(AuthLevel::None));
		assert_eq!(AuthLevel::parse("no-auth"), Some(AuthLevel::None));
		assert_eq!(AuthLevel::parse("noauth"), Some(AuthLevel::None));
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
		let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
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
		let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
		clear_db_env();
		let overrides = DbOverrides {
			host: Some("http://custom:9000".into()),
			db: Some("mydb".into()),
			ns: Some("myns".into()),
			user: Some("admin".into()),
			pass: Some("secret".into()),
			auth_level: None,
			folder: None,
			connect_timeout_secs: None,
			query_timeout_secs: None,
		};
		let cfg = DbCfg::from_env(None, &overrides).unwrap();
		assert_eq!(cfg.host(), "http://custom:9000");
		assert_eq!(cfg.db(), "mydb");
		assert_eq!(cfg.ns(), "myns");
		assert_eq!(cfg.user(), "admin");
		assert_eq!(cfg.pass(), "secret");
	}

	#[test]
	fn debug_never_prints_the_password() {
		let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
		clear_db_env();
		let overrides = DbOverrides {
			pass: Some("hunter2".into()),
			..Default::default()
		};
		let cfg = DbCfg::from_env(None, &overrides).unwrap();

		let rendered = format!("{cfg:?}");
		assert!(!rendered.contains("hunter2"), "DbCfg Debug leaked the password: {rendered}");
		assert!(rendered.contains(REDACTED), "password should be shown redacted: {rendered}");

		let rendered = format!("{overrides:?}");
		assert!(!rendered.contains("hunter2"), "DbOverrides Debug leaked the password: {rendered}");

		// A password that was never set must not look like one that was.
		let empty = format!("{:?}", DbOverrides::default());
		assert!(empty.contains("None"), "unset password should render as None: {empty}");
	}

	#[test]
	fn folder_override_is_honoured() {
		// Regression: `from_env` passed `&None` here instead of `overrides.folder`,
		// so `--folder` was parsed, stored, and then silently discarded.
		let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
		clear_db_env();
		let overrides = DbOverrides {
			folder: Some("./custom-db".into()),
			..Default::default()
		};
		let cfg = DbCfg::from_env(None, &overrides).unwrap();
		assert_eq!(cfg.folder(), "./custom-db");
	}

	#[test]
	fn folder_override_beats_env_var() {
		let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
		clear_db_env();
		unsafe { set_env("SURREALDB_FOLDER", "./from-env") };
		let overrides = DbOverrides {
			folder: Some("./from-cli".into()),
			..Default::default()
		};
		let cfg = DbCfg::from_env(None, &overrides).unwrap();
		assert_eq!(cfg.folder(), "./from-cli");
		clear_db_env();
	}

	#[test]
	fn folder_falls_back_to_env_then_default() {
		let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
		clear_db_env();
		unsafe { set_env("SURREALDB_FOLDER", "./from-env") };
		assert_eq!(DbCfg::from_env(None, &DbOverrides::default()).unwrap().folder(), "./from-env");

		clear_db_env();
		assert_eq!(
			DbCfg::from_env(None, &DbOverrides::default()).unwrap().folder(),
			DEFAULT_ROOT_DIR
		);
	}

	#[test]
	fn from_env_defaults_to_root_auth_level() {
		let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
		clear_db_env();
		let cfg = DbCfg::from_env(None, &DbOverrides::default()).unwrap();
		assert_eq!(cfg.auth_level(), &AuthLevel::Root);
	}

	#[test]
	fn from_env_parses_auth_level_override() {
		let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
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
		let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
		clear_db_env();
		unsafe { set_env("SURREALDB_AUTH_LEVEL", "namespace") };
		let cfg = DbCfg::from_env(None, &DbOverrides::default()).unwrap();
		assert_eq!(cfg.auth_level(), &AuthLevel::Namespace);
		unsafe { unset_env("SURREALDB_AUTH_LEVEL") };
	}

	#[test]
	fn from_env_rejects_unknown_auth_level() {
		let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
		clear_db_env();
		let overrides = DbOverrides {
			auth_level: Some("superadmin".into()),
			..Default::default()
		};
		let err = DbCfg::from_env(None, &overrides).unwrap_err();
		assert!(err.to_string().contains("invalid auth level"), "got: {err}");
	}

	#[test]
	fn orphaned_legacy_env_var_is_rejected_not_ignored() {
		// Ignoring it would silently fall back to http://localhost:8000 and connect
		// to the wrong database, which is worse than failing.
		let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
		clear_db_env();
		unsafe { set_env("DATABASE_HOST", "http://legacy:8000") };

		let err = DbCfg::from_env(None, &DbOverrides::default()).unwrap_err().to_string();
		assert!(err.contains("DATABASE_HOST"), "error should name the variable: {err}");
		assert!(err.contains("SURREALDB_HOST"), "error should name the replacement: {err}");

		clear_db_env();
	}

	#[test]
	fn legacy_var_alongside_its_replacement_is_accepted_and_ignored() {
		// Both set means the deployment has already migrated; the leftover is inert.
		let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
		clear_db_env();
		unsafe {
			set_env("DATABASE_HOST", "http://legacy:8000");
			set_env("SURREALDB_HOST", "http://modern:8000");
		}

		let cfg = DbCfg::from_env(None, &DbOverrides::default()).unwrap();
		assert_eq!(cfg.host(), "http://modern:8000");

		clear_db_env();
	}

	#[test]
	fn legacy_env_vars_no_longer_resolve_values() {
		let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
		clear_db_env();
		// Set every legacy var *and* its replacement, so the guard passes and we can
		// assert the legacy values are not the ones used.
		unsafe {
			set_env("DATABASE_NAMESPACE", "legacyns");
			set_env("SURREALDB_NAMESPACE", "modernns");
		}
		let cfg = DbCfg::from_env(None, &DbOverrides::default()).unwrap();
		assert_eq!(cfg.ns(), "modernns");
		clear_db_env();
	}

	#[test]
	fn from_env_reads_surrealdb_env_vars() {
		let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
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
		let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
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

	#[test]
	fn connect_timeout_defaults_to_thirty_seconds() {
		let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
		clear_db_env();
		unsafe { unset_env("SURREALDB_CONNECT_TIMEOUT_SECS") };
		let cfg = DbCfg::from_env(None, &DbOverrides::default()).expect("cfg");
		assert_eq!(cfg.connect_timeout, Some(Duration::from_secs(DEFAULT_CONNECT_TIMEOUT_SECS)));
		// A step budget must stay opt-in: a legitimate index build can take hours.
		assert_eq!(cfg.query_timeout, None);
	}

	#[test]
	fn connect_timeout_precedence_is_cli_then_env_then_default() {
		let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
		clear_db_env();
		unsafe { set_env("SURREALDB_CONNECT_TIMEOUT_SECS", "7") };

		let from_env = DbCfg::from_env(None, &DbOverrides::default()).expect("env cfg");
		assert_eq!(from_env.connect_timeout, Some(Duration::from_secs(7)));

		let overridden = DbCfg::from_env(
			None,
			&DbOverrides {
				connect_timeout_secs: Some(3),
				..Default::default()
			},
		)
		.expect("cli cfg");
		assert_eq!(overridden.connect_timeout, Some(Duration::from_secs(3)));

		unsafe { unset_env("SURREALDB_CONNECT_TIMEOUT_SECS") };
	}

	#[test]
	fn zero_disables_the_deadline() {
		let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
		clear_db_env();
		let cfg = DbCfg::from_env(
			None,
			&DbOverrides {
				connect_timeout_secs: Some(0),
				..Default::default()
			},
		)
		.expect("cfg");
		assert_eq!(cfg.connect_timeout, None, "0 must mean wait indefinitely");
	}

	#[test]
	fn a_non_numeric_timeout_is_rejected_rather_than_ignored() {
		let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
		clear_db_env();
		unsafe { set_env("SURREALDB_CONNECT_TIMEOUT_SECS", "thirty") };
		let err = DbCfg::from_env(None, &DbOverrides::default()).expect_err("must reject");
		assert!(err.to_string().contains("whole number of seconds"), "got: {err}");
		unsafe { unset_env("SURREALDB_CONNECT_TIMEOUT_SECS") };
	}

	#[test]
	fn per_target_timeouts_override_the_ambient_ones() {
		let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
		clear_db_env();
		let base = DbCfg::from_env(None, &DbOverrides::default()).expect("cfg");
		let target = base.with_timeouts(Some(5), Some(60));
		assert_eq!(target.connect_timeout, Some(Duration::from_secs(5)));
		assert_eq!(target.query_timeout, Some(Duration::from_secs(60)));
		// Unspecified fields inherit.
		let partial = base.with_timeouts(None, Some(60));
		assert_eq!(partial.connect_timeout, base.connect_timeout);
	}
}
