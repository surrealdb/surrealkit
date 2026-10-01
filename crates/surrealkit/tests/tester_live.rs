//! `surrealkit test` end to end against a live SurrealDB: named permission
//! rules, `record_id = "$auth"`, and suites built from replayed rollouts.
//!
//! The tester refuses embedded engines, so these need a server. They run when
//! `SURREALKIT_TEST_URL` is set (CI sets it) and pass trivially otherwise.
//! Credentials come from `SURREALKIT_TEST_USER` / `SURREALKIT_TEST_PASS`,
//! defaulting to root/secret.

#![expect(clippy::unwrap_used, reason = "test helpers fail the test on any unexpected state")]

use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;
use surrealkit::TemplateVars;
use surrealkit::config::DbOverrides;
use surrealkit::rollout::{RolloutPlanOpts, run_plan};
use surrealkit::tester::{SchemaSource, TestOpts, run_test};

fn server() -> Option<String> {
	std::env::var("SURREALKIT_TEST_URL").ok().filter(|url| !url.is_empty())
}

macro_rules! require_server {
	() => {
		match server() {
			Some(url) => url,
			None => {
				eprintln!("SKIP: set SURREALKIT_TEST_URL to run the live tester tests");
				return;
			}
		}
	};
}

struct Project {
	_tmp: tempfile::TempDir,
	root: PathBuf,
}

impl Project {
	fn new() -> Self {
		let tmp = tempfile::TempDir::new().unwrap();
		let root = tmp.path().join("database");
		for dir in ["schema", "tests/suites"] {
			fs::create_dir_all(root.join(dir)).unwrap();
		}
		Self {
			_tmp: tmp,
			root,
		}
	}

	fn write(&self, rel: &str, body: &str) {
		let path = self.root.join(rel);
		fs::create_dir_all(path.parent().unwrap()).unwrap();
		fs::write(path, body).unwrap();
	}

	async fn plan(&self, name: &str) {
		run_plan(
			&self.root.to_string_lossy(),
			RolloutPlanOpts {
				name: Some(name.to_string()),
				dry_run: false,
				allow_modified: false,
			},
		)
		.await
		.unwrap();
	}

	/// Run the suites, returning the error (if any) and the JSON report.
	async fn test(&self, url: &str, schema_from: Option<SchemaSource>) -> (Option<String>, Value) {
		let report_path = self.root.join("report.json");
		let overrides = DbOverrides {
			host: Some(url.to_string()),
			user: Some(std::env::var("SURREALKIT_TEST_USER").unwrap_or_else(|_| "root".into())),
			pass: Some(std::env::var("SURREALKIT_TEST_PASS").unwrap_or_else(|_| "secret".into())),
			auth_level: Some("root".into()),
			folder: Some(self.root.to_string_lossy().into_owned()),
			..Default::default()
		};
		let opts = TestOpts {
			suite: None,
			case: None,
			tags: Vec::new(),
			fail_fast: false,
			parallel: 1,
			json_out: Some(report_path.clone()),
			no_setup: false,
			no_sync: false,
			no_seed: true,
			base_url: None,
			timeout_ms: None,
			keep_db: false,
			schema_from,
		};
		let result = run_test(None, opts, TemplateVars::default(), &overrides).await;
		let report = fs::read_to_string(&report_path)
			.ok()
			.map(|raw| serde_json::from_str(&raw).unwrap())
			.unwrap_or(Value::Null);
		(result.err().map(|e| format!("{e:#}")), report)
	}
}

const USER_SCHEMA: &str = "DEFINE TABLE user SCHEMAFULL
	PERMISSIONS FOR select, update, delete WHERE id = $auth FOR create NONE;
DEFINE FIELD email ON user TYPE string;
DEFINE FIELD passphrase ON user TYPE string;
DEFINE ACCESS human_passphrase ON DATABASE TYPE RECORD
	SIGNUP (CREATE user SET email = $email, passphrase = crypto::argon2::generate($passphrase))
	SIGNIN (SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(passphrase, $passphrase))
	WITH JWT ALGORITHM HS512 KEY 'tester-live-only'
	DURATION FOR SESSION 1h;
DEFINE TABLE note SCHEMALESS;
";

const ACTOR: &str = r#"
[actors.record]
kind = "record"
access = "human_passphrase"
signup_params = { email = "user@example.test", passphrase = "abc" }
signin_params = { email = "user@example.test", passphrase = "abc" }
"#;

fn case<'a>(report: &'a Value, suite: &str, case: &str) -> &'a Value {
	report["suites"]
		.as_array()
		.unwrap()
		.iter()
		.filter(|s| s["suite_name"].as_str().unwrap_or_default() == suite)
		.flat_map(|s| s["cases"].as_array().unwrap())
		.find(|c| c["name"].as_str() == Some(case))
		.unwrap_or_else(|| panic!("no case '{case}' in suite '{suite}':\n{report:#}"))
}

fn assertion_names(case: &Value) -> Vec<String> {
	case["assertions"]
		.as_array()
		.unwrap()
		.iter()
		.map(|a| a["name"].as_str().unwrap().to_string())
		.collect()
}

#[tokio::test]
async fn auth_record_cases_and_named_rules() {
	let url = require_server!();
	let project = Project::new();
	project.write("schema/user.surql", USER_SCHEMA);
	project.write(
		"tests/suites/self.toml",
		&format!(
			r#"name = "user"
{ACTOR}
[[fixtures]]
sql = "CREATE user:other SET email = 'other@example.test', passphrase = 'x';"

[[cases]]
name = "user can be created and signed in and read its own record"
kind = "permissions_matrix"
actor = "record"
table = "user"
record_id = "$auth"

[[cases.rules]]
name = "reads its own record"
action = "select"
allow = true

[[cases.rules]]
name = "updates its own record"
action = "update"
allow = true

[[cases.rules]]
name = "deletes its own record"
action = "delete"
allow = true

[[cases.rules]]
action = "select"
allow = true

[[cases]]
name = "another user is off limits"
kind = "permissions_matrix"
actor = "record"
table = "user"
record_id = "other"

[[cases.rules]]
name = "cannot read another user"
action = "select"
allow = false

[[cases.rules]]
name = "cannot update another user"
action = "update"
allow = false

[[cases.rules]]
name = "cannot delete another user"
action = "delete"
allow = false

[[cases]]
name = "auth id is the signed-in record"
kind = "sql_expect"
actor = "record"
sql = "RETURN {{ me: $auth }}"
assertions = [{{ path = "me", equals_auth = "$auth.id" }}, {{ path = "me", equals_auth = "$auth" }}]

[[cases]]
name = "the record survives the in-place rules"
kind = "sql_expect"
actor = "record"
sql = "SELECT VALUE email FROM ONLY $auth"
assertions = [{{ path = ".", equals = "user@example.test" }}]
"#
		),
	);
	project.write(
		"tests/suites/mistakes.toml",
		&format!(
			r#"name = "mistakes"
{ACTOR}
[[cases]]
name = "auth in the wrong table"
kind = "permissions_matrix"
actor = "record"
table = "note"
record_id = "$auth"
[[cases.rules]]
action = "select"

[[cases]]
name = "auth for root"
kind = "permissions_matrix"
actor = "root"
table = "user"
record_id = "$auth"
[[cases.rules]]
action = "select"
"#
		),
	);

	let (err, report) = project.test(&url, None).await;
	assert_eq!(err.as_deref(), Some("2 test cases failed"), "{report:#}");

	let own = case(&report, "user", "user can be created and signed in and read its own record");
	assert_eq!(own["passed"], true, "{own:#}");
	assert_eq!(
		assertion_names(own),
		vec![
			"reads its own record",
			"updates its own record",
			"deletes its own record",
			"action:select (#4)"
		]
	);
	assert_eq!(case(&report, "user", "another user is off limits")["passed"], true, "{report:#}");
	assert_eq!(
		case(&report, "user", "auth id is the signed-in record")["passed"],
		true,
		"{report:#}"
	);
	assert_eq!(
		case(&report, "user", "the record survives the in-place rules")["passed"],
		true,
		"{report:#}"
	);

	let wrong_table = case(&report, "mistakes", "auth in the wrong table");
	assert!(
		wrong_table["message"].as_str().unwrap().contains("a record in 'user'"),
		"{wrong_table:#}"
	);
	let root = case(&report, "mistakes", "auth for root");
	assert!(root["message"].as_str().unwrap().contains("not signed in as a record"), "{root:#}");
}

#[tokio::test]
async fn a_create_rule_on_auth_is_refused_when_the_suite_loads() {
	let url = require_server!();
	let project = Project::new();
	project.write("schema/user.surql", USER_SCHEMA);
	project.write(
		"tests/suites/bad.toml",
		&format!(
			r#"{ACTOR}
[[cases]]
name = "create self"
kind = "permissions_matrix"
actor = "record"
table = "user"
record_id = "$auth"
[[cases.rules]]
action = "create"
"#
		),
	);
	let (err, _) = project.test(&url, None).await;
	assert!(err.unwrap().contains("create rule with record_id = \"$auth\""));
}

fn rollout_project() -> Project {
	let project = Project::new();
	project.write("tests/suites/smoke.toml", "name = \"smoke\"\n\n[[cases]]\nname = \"note table exists\"\nkind = \"schema_metadata\"\nactor = \"root\"\nsql = \"INFO FOR DB;\"\ncontains = [\"note\"]\n");
	project
}

#[tokio::test]
async fn suites_run_on_synced_and_replayed_databases_and_parity_holds() {
	let url = require_server!();
	let project = rollout_project();
	project.write(
		"schema/a.surql",
		"DEFINE TABLE note SCHEMAFULL;\nDEFINE FIELD body ON note TYPE string;\n",
	);
	project.plan("first").await;
	project.write(
		"schema/b.surql",
		"DEFINE TABLE tag SCHEMALESS;\nDEFINE SEQUENCE tag_no BATCH 1 START 1;\n",
	);
	project.plan("second").await;

	let (err, report) = project.test(&url, Some(SchemaSource::Both)).await;
	assert!(err.is_none(), "{err:?}\n{report:#}");
	let names: Vec<&str> = report["suites"]
		.as_array()
		.unwrap()
		.iter()
		.map(|s| s["suite_name"].as_str().unwrap())
		.collect();
	assert_eq!(names, vec!["rollout parity", "smoke [sync]", "smoke [rollouts]"]);
	let parity =
		case(&report, "rollout parity", "replaying every rollout defines what sync defines");
	assert_eq!(parity["passed"], true, "{parity:#}");
}

#[tokio::test]
async fn parity_catches_a_schema_change_no_rollout_plans() {
	let url = require_server!();
	let project = rollout_project();
	project.write("schema/a.surql", "DEFINE TABLE note SCHEMAFULL;\n");
	project.plan("first").await;
	project.write(
		"schema/a.surql",
		"DEFINE TABLE note SCHEMAFULL;\nDEFINE FIELD unplanned ON note TYPE string;\n",
	);

	let (err, report) = project.test(&url, Some(SchemaSource::Rollouts)).await;
	assert_eq!(err.as_deref(), Some("1 test cases failed"), "{report:#}");
	let parity =
		case(&report, "rollout parity", "replaying every rollout defines what sync defines");
	assert_eq!(parity["passed"], false);
	let failing: Vec<String> = parity["assertions"]
		.as_array()
		.unwrap()
		.iter()
		.filter(|a| a["passed"] == false)
		.map(|a| format!("{} {}", a["name"], a["message"]))
		.collect();
	assert!(
		failing.iter().any(|f| f.contains("fields:note.unplanned")
			&& f.contains("defined by sync but not by the rollouts")),
		"{failing:#?}"
	);
	// The suite itself still ran on the replayed database.
	assert_eq!(case(&report, "smoke", "note table exists")["passed"], true);
}

#[tokio::test]
async fn replaying_needs_a_chain_that_starts_from_nothing() {
	let url = require_server!();
	let project = rollout_project();
	project.write("schema/a.surql", "DEFINE TABLE note SCHEMAFULL;\n");
	// A baseline-style start: snapshots that already hold a schema.
	fs::create_dir_all(project.root.join("snapshots")).unwrap();
	project.plan("first").await;
	let first = only_manifest(&project.root);
	let raw = fs::read_to_string(&first).unwrap();
	let source = raw.lines().find(|l| l.starts_with("source_schema_hash")).unwrap().to_string();
	fs::write(&first, raw.replace(&source, "source_schema_hash = \"0000\"")).unwrap();

	let (err, _) = project.test(&url, Some(SchemaSource::Rollouts)).await;
	let err = err.unwrap();
	assert!(err.contains("no manifest starts from an empty schema"), "{err}");
}

fn only_manifest(root: &Path) -> PathBuf {
	fs::read_dir(root.join("rollouts"))
		.unwrap()
		.filter_map(Result::ok)
		.map(|e| e.path())
		.find(|p| p.is_file())
		.unwrap()
}
