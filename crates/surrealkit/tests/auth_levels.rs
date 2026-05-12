/// Integration tests for all three SurrealDB authentication levels.
///
/// The config-parsing tests in config.rs (unit tests) cover AuthLevel::parse,
/// from_env resolution, and env-var precedence without needing a server.
///
/// The connect() tests below exercise the actual signin path. They require a
/// live SurrealDB server because the embedded mem:// engine disables auth by
/// default. Set the environment variable to opt in:
///
///   SURREALKIT_AUTH_TEST_URL=ws://localhost:8000 cargo test --test auth_levels
///
/// If the variable is not set the tests are skipped (not failed).
use std::sync::Mutex;

use surrealdb::engine::any::connect as surreal_connect;
use surrealdb::opt::Config;
use surrealdb::opt::auth::Root;
use surrealdb::opt::capabilities::Capabilities;
use surrealkit::config::{AuthLevel, DbCfg, DbOverrides};
use surrealkit::connect;
use surrealkit::tester::{TestOpts, run_test};
use surrealkit::variables::TemplateVars;

/// Serialises tests that mutate SURREALDB_AUTH_LEVEL / DATABASE_AUTH_LEVEL.
static ENV_LOCK: Mutex<()> = Mutex::new(());

const NS: &str = "surrealkit_auth_test";
const DB: &str = "surrealkit_auth_test";

/// Returns the test server URL, or None when not configured (skips the test).
fn server_url() -> Option<String> {
	std::env::var("SURREALKIT_AUTH_TEST_URL").ok().filter(|s| !s.is_empty())
}

/// Open a root connection to the test server using the raw surrealdb client.
async fn root_conn(url: &str) -> surrealdb::Surreal<surrealdb::engine::any::Any> {
	let cfg = Config::new().capabilities(Capabilities::all());
	let db = surreal_connect((url, cfg)).await.expect("connect to test server");
	db.signin(Root {
		username: "root".to_string(),
		password: "root".to_string(),
	})
	.await
	.expect("root signin on test server");
	db.use_ns(NS).use_db(DB).await.expect("use_ns/use_db");
	db
}

fn make_cfg(url: &str, auth_level: AuthLevel, user: &str, pass: &str) -> DbCfg {
	DbCfg::from_env(
		None,
		&DbOverrides {
			host: Some(url.into()),
			ns: Some(NS.into()),
			db: Some(DB.into()),
			user: Some(user.into()),
			pass: Some(pass.into()),
			auth_level: Some(
				match auth_level {
					AuthLevel::Root => "root",
					AuthLevel::Namespace => "namespace",
					AuthLevel::Database => "database",
				}
				.into(),
			),
		},
	)
	.expect("DbCfg::from_env")
}

#[test]
fn auth_level_parses_all_aliases() {
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
		let cfg = DbCfg::from_env(
			None,
			&DbOverrides {
				auth_level: Some(input.into()),
				..Default::default()
			},
		)
		.unwrap();
		assert_eq!(cfg.auth_level(), &expected, "input={input}");
	}
}

#[test]
fn auth_level_reads_from_env_var() {
	let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
	unsafe { std::env::set_var("SURREALDB_AUTH_LEVEL", "namespace") };
	let cfg = DbCfg::from_env(None, &DbOverrides::default()).unwrap();
	unsafe { std::env::remove_var("SURREALDB_AUTH_LEVEL") };
	assert_eq!(cfg.auth_level(), &AuthLevel::Namespace);
}

#[test]
fn auth_level_cli_beats_env_var() {
	let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
	unsafe { std::env::set_var("SURREALDB_AUTH_LEVEL", "namespace") };
	let cfg = DbCfg::from_env(
		None,
		&DbOverrides {
			auth_level: Some("database".into()),
			..Default::default()
		},
	)
	.unwrap();
	unsafe { std::env::remove_var("SURREALDB_AUTH_LEVEL") };
	assert_eq!(cfg.auth_level(), &AuthLevel::Database);
}

#[test]
fn auth_level_default_is_root_no_env() {
	let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
	unsafe { std::env::remove_var("SURREALDB_AUTH_LEVEL") };
	unsafe { std::env::remove_var("DATABASE_AUTH_LEVEL") };
	let cfg = DbCfg::from_env(None, &DbOverrides::default()).unwrap();
	assert_eq!(cfg.auth_level(), &AuthLevel::Root);
}

#[test]
fn auth_level_unknown_value_is_rejected() {
	let err = DbCfg::from_env(
		None,
		&DbOverrides {
			auth_level: Some("superadmin".into()),
			..Default::default()
		},
	)
	.unwrap_err();
	assert!(err.to_string().contains("invalid auth level"), "got: {err}");
}

#[tokio::test]
async fn connect_root_auth() {
	let Some(url) = server_url() else {
		eprintln!("SKIP: set SURREALKIT_AUTH_TEST_URL to run connect() auth tests");
		return;
	};

	let cfg = make_cfg(&url, AuthLevel::Root, "root", "root");
	let db = connect(&cfg).await.expect("root connect");
	db.query("SELECT 1;").await.expect("query").check().expect("check");
}

#[tokio::test]
async fn connect_namespace_auth() {
	let Some(url) = server_url() else {
		eprintln!("SKIP: set SURREALKIT_AUTH_TEST_URL to run connect() auth tests");
		return;
	};

	// Provision a namespace-scoped user via root.
	let root = root_conn(&url).await;
	root.query("DEFINE USER ns_user ON NAMESPACE PASSWORD 'ns_pass' ROLES EDITOR;")
		.await
		.expect("define ns user")
		.check()
		.expect("check");

	let cfg = make_cfg(&url, AuthLevel::Namespace, "ns_user", "ns_pass");
	let db = connect(&cfg).await.expect("namespace connect");
	db.query("SELECT 1;").await.expect("query").check().expect("check");
}

#[tokio::test]
async fn connect_database_auth() {
	let Some(url) = server_url() else {
		eprintln!("SKIP: set SURREALKIT_AUTH_TEST_URL to run connect() auth tests");
		return;
	};

	// Provision a database-scoped user via root.
	let root = root_conn(&url).await;
	root.query("DEFINE USER db_user ON DATABASE PASSWORD 'db_pass' ROLES EDITOR;")
		.await
		.expect("define db user")
		.check()
		.expect("check");

	let cfg = make_cfg(&url, AuthLevel::Database, "db_user", "db_pass");
	let db = connect(&cfg).await.expect("database connect");
	db.query("SELECT 1;").await.expect("query").check().expect("check");
}

#[tokio::test]
async fn connect_namespace_auth_wrong_password_fails() {
	let Some(url) = server_url() else {
		eprintln!("SKIP: set SURREALKIT_AUTH_TEST_URL to run connect() auth tests");
		return;
	};

	let root = root_conn(&url).await;
	root.query("DEFINE USER ns_wrong_user ON NAMESPACE PASSWORD 'correct' ROLES EDITOR;")
		.await
		.expect("define user")
		.check()
		.expect("check");

	let cfg = make_cfg(&url, AuthLevel::Namespace, "ns_wrong_user", "wrong");
	let err = connect(&cfg).await.expect_err("should fail with wrong password");
	let msg = err.to_string().to_lowercase();
	assert!(
		msg.contains("signin") || msg.contains("auth") || msg.contains("invalid"),
		"unexpected error: {err}"
	);
}

#[tokio::test]
async fn connect_database_auth_wrong_password_fails() {
	let Some(url) = server_url() else {
		eprintln!("SKIP: set SURREALKIT_AUTH_TEST_URL to run connect() auth tests");
		return;
	};

	let root = root_conn(&url).await;
	root.query("DEFINE USER db_wrong_user ON DATABASE PASSWORD 'correct' ROLES EDITOR;")
		.await
		.expect("define user")
		.check()
		.expect("check");

	let cfg = make_cfg(&url, AuthLevel::Database, "db_wrong_user", "wrong");
	let err = connect(&cfg).await.expect_err("should fail with wrong password");
	let msg = err.to_string().to_lowercase();
	assert!(
		msg.contains("signin") || msg.contains("auth") || msg.contains("invalid"),
		"unexpected error: {err}"
	);
}

#[tokio::test]
async fn test_runner_rejects_database_auth_level() {
	let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
	unsafe { std::env::remove_var("SURREALDB_AUTH_LEVEL") };
	unsafe { std::env::remove_var("DATABASE_AUTH_LEVEL") };
	let overrides = DbOverrides {
		auth_level: Some("database".into()),
		..Default::default()
	};
	let opts = TestOpts {
		suite: None,
		case: None,
		tags: Vec::new(),
		fail_fast: false,
		parallel: 1,
		json_out: None,
		no_setup: true,
		no_sync: true,
		no_seed: true,
		base_url: None,
		timeout_ms: None,
		keep_db: false,
	};
	let err = run_test(None, opts, TemplateVars::default(), &overrides)
		.await
		.expect_err("expected database auth level to be rejected");
	let msg = err.to_string();
	assert!(
		msg.contains("auth level 'root' or 'namespace'"),
		"unexpected error message: {msg}"
	);
	assert!(msg.contains("got 'database'"), "unexpected error message: {msg}");
}
