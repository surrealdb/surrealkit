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
		self.test_with(url, schema_from, Vec::new(), None).await
	}

	/// [`Project::test`] with an `--exec` command, and a namespace of its own when `ns` is
	/// given (suite namespaces then start with `<ns>_sk_test_`).
	async fn test_with(
		&self,
		url: &str,
		schema_from: Option<SchemaSource>,
		exec: Vec<String>,
		ns: Option<&str>,
	) -> (Option<String>, Value) {
		self.test_custom(url, ns, |opts| {
			opts.schema_from = schema_from;
			opts.exec = exec;
		})
		.await
	}

	/// Run the suites with options `customize` adjusts, in a namespace of their own when `ns`
	/// is given.
	async fn test_custom(
		&self,
		url: &str,
		ns: Option<&str>,
		customize: impl FnOnce(&mut TestOpts),
	) -> (Option<String>, Value) {
		let report_path = self.root.join("report.json");
		let overrides = DbOverrides {
			host: Some(url.to_string()),
			ns: ns.map(str::to_string),
			user: Some(std::env::var("SURREALKIT_TEST_USER").unwrap_or_else(|_| "root".into())),
			pass: Some(std::env::var("SURREALKIT_TEST_PASS").unwrap_or_else(|_| "secret".into())),
			auth_level: Some("root".into()),
			folder: Some(self.root.to_string_lossy().into_owned()),
			..Default::default()
		};
		let mut opts = TestOpts {
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
			schema_from: None,
			exec: Vec::new(),
			exec_root_token: false,
		};
		customize(&mut opts);
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

/// `member` and `handle` have unique indexes, so a copy of one of their records
/// collides with the original; `plain` has none. Deleting a handle or a plain
/// record leaves a `gone` row naming the id that was deleted.
const UNIQUE_SCHEMA: &str = "DEFINE TABLE member SCHEMAFULL
	PERMISSIONS FOR select, update, delete WHERE id = $auth FOR create NONE;
DEFINE FIELD email ON member TYPE string;
DEFINE FIELD passphrase ON member TYPE string;
DEFINE INDEX member_email ON member FIELDS email UNIQUE;
DEFINE ACCESS member_passphrase ON DATABASE TYPE RECORD
	SIGNUP (CREATE member SET email = $email, passphrase = crypto::argon2::generate($passphrase))
	SIGNIN (SELECT * FROM member WHERE email = $email AND crypto::argon2::compare(passphrase, $passphrase))
	WITH JWT ALGORITHM HS512 KEY 'tester-live-only'
	DURATION FOR SESSION 1h;
DEFINE TABLE handle SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD name ON handle TYPE string;
DEFINE INDEX handle_name ON handle FIELDS name UNIQUE;
DEFINE EVENT handle_gone ON handle WHEN $event = 'DELETE' THEN (CREATE gone SET was = $before.id);
DEFINE TABLE plain SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD name ON plain TYPE string;
DEFINE EVENT plain_gone ON plain WHEN $event = 'DELETE' THEN (CREATE gone SET was = $before.id);
DEFINE TABLE gone SCHEMALESS PERMISSIONS FULL;
";

#[tokio::test]
async fn rules_on_tables_with_unique_indexes() {
	let url = require_server!();
	let project = Project::new();
	project.write("schema/unique.surql", UNIQUE_SCHEMA);
	project.write(
		"tests/suites/unique.toml",
		r#"name = "unique"

[actors.member]
kind = "record"
access = "member_passphrase"
signup_params = { email = "member@example.test", passphrase = "abc" }
signin_params = { email = "member@example.test", passphrase = "abc" }

[[fixtures]]
sql = """
CREATE member:other SET email = 'other@example.test', passphrase = 'x';
CREATE handle:taken SET name = 'taken';
CREATE plain:kept SET name = 'kept';
"""

[[cases]]
name = "another member is off limits"
kind = "permissions_matrix"
actor = "member"
table = "member"
record_id = "other"

[[cases.rules]]
name = "cannot read another member"
action = "select"
allow = false

[[cases.rules]]
name = "cannot update another member"
action = "update"
allow = false

[[cases.rules]]
name = "cannot delete another member"
action = "delete"
allow = false

[[cases.rules]]
name = "cannot create a member"
action = "create"
allow = false

[[cases]]
name = "the other member is as it was"
kind = "sql_expect"
actor = "root"
sql = "RETURN { email: (SELECT VALUE email FROM ONLY member:other), members: count(SELECT * FROM member) }"
assertions = [
  { path = "email", equals = "other@example.test" },
  { path = "members", equals = 2 },
]

[[cases]]
name = "anyone can change a handle"
kind = "permissions_matrix"
actor = "member"
table = "handle"
record_id = "taken"

[[cases.rules]]
name = "updates a handle"
action = "update"
allow = true

[[cases.rules]]
name = "deletes a handle"
action = "delete"
allow = true

[[cases]]
name = "the handle itself was changed and put back"
kind = "sql_expect"
actor = "root"
sql = """
RETURN {
  handles: (SELECT VALUE name FROM handle),
  marked: (SELECT VALUE _marker FROM ONLY handle:taken) != NONE,
  deleted: (SELECT VALUE record::id(was) FROM gone WHERE record::tb(was) = 'handle'),
}
"""
assertions = [
  { path = "handles", equals = ["taken"] },
  { path = "marked", equals = false },
  { path = "deleted", equals = ["taken"] },
]

[[cases]]
name = "a plain record is still worked on through a copy"
kind = "permissions_matrix"
actor = "member"
table = "plain"
record_id = "kept"

[[cases.rules]]
name = "updates a plain record"
action = "update"
allow = true

[[cases.rules]]
name = "deletes a plain record"
action = "delete"
allow = true

[[cases.rules]]
name = "creates a plain record"
action = "create"
allow = true

[[cases]]
name = "the plain record was never deleted"
kind = "sql_expect"
actor = "root"
sql = """
RETURN {
  plains: (SELECT VALUE name FROM plain),
  copies_deleted: count(SELECT * FROM gone WHERE record::tb(was) = 'plain') > 0,
  original_deleted: (SELECT VALUE record::id(was) FROM gone WHERE record::tb(was) = 'plain') CONTAINS 'kept',
}
"""
assertions = [
  { path = "plains", equals = ["kept"] },
  { path = "copies_deleted", equals = true },
  { path = "original_deleted", equals = false },
]

[[cases]]
name = "a create that collides in a unique index"
kind = "permissions_matrix"
actor = "member"
table = "handle"
record_id = "taken"

[[cases.rules]]
name = "expected to be denied"
action = "create"
allow = false

[[cases.rules]]
name = "expected to be allowed"
action = "create"
allow = true
"#,
	);

	let (err, report) = project.test(&url, None).await;
	// Only the colliding create case fails.
	assert_eq!(err.as_deref(), Some("1 test cases failed"), "{report:#}");
	for name in [
		"another member is off limits",
		"the other member is as it was",
		"anyone can change a handle",
		"the handle itself was changed and put back",
		"a plain record is still worked on through a copy",
		"the plain record was never deleted",
	] {
		let found = case(&report, "unique", name);
		assert_eq!(found["passed"], true, "{found:#}");
	}

	// The index refused both creates. Neither rule may pass: not the one that
	// expects a denial, which the collision used to satisfy, and not the other.
	let collides = case(&report, "unique", "a create that collides in a unique index");
	let rules = collides["assertions"].as_array().unwrap();
	assert_eq!(rules.len(), 2, "{collides:#}");
	for rule in rules {
		assert_eq!(rule["passed"], false, "{collides:#}");
		let message = rule["message"].as_str().unwrap();
		assert!(message.starts_with("inconclusive: "), "{message}");
		assert!(message.contains("unique index `handle_name`"), "{message}");
		assert!(message.contains("handle:taken"), "{message}");
	}
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

/// The suite namespaces left on the server whose name starts with `<ns>_sk_test_`.
async fn leftover_namespaces(url: &str, ns: &str) -> Vec<String> {
	let db = surrealdb::engine::any::connect(url).await.unwrap();
	db.signin(surrealdb::opt::auth::Root {
		username: std::env::var("SURREALKIT_TEST_USER").unwrap_or_else(|_| "root".into()),
		password: std::env::var("SURREALKIT_TEST_PASS").unwrap_or_else(|_| "secret".into()),
	})
	.await
	.unwrap();
	let mut response = db.query("INFO FOR ROOT").await.unwrap().check().unwrap();
	let info: Option<Value> = response.take(0).unwrap();
	let prefix = format!("{ns}_sk_test_");
	info.unwrap()["namespaces"]
		.as_object()
		.map(|names| names.keys().filter(|n| n.starts_with(&prefix)).cloned().collect())
		.unwrap_or_default()
}

fn unique_ns(label: &str) -> String {
	format!("{label}_{}", surrealdb_types::uuid::Uuid::new_v4().simple())
}

const PROBE_SCHEMA: &str = "DEFINE TABLE probe SCHEMAFULL;\nDEFINE FIELD n ON probe TYPE int;\n";

const SMOKE_SUITE: &str = r#"name = "smoke"

[[cases]]
name = "probe exists"
kind = "schema_metadata"
actor = "root"
sql = "INFO FOR DB;"
contains = ["probe"]
"#;

/// A script that records the `SURREALKIT_EXEC_*` variables it was given in its first
/// argument, then exits with its second.
#[cfg(unix)]
fn probe_script(project: &Project) -> (String, PathBuf) {
	let script = project.root.join("probe.sh");
	fs::write(&script, "#!/bin/sh\nenv | grep ^SURREALKIT_EXEC_ | sort > \"$1\"\nexit \"$2\"\n")
		.unwrap();
	(script.to_string_lossy().into_owned(), project.root.join("exec.env"))
}

#[cfg(unix)]
fn recorded(path: &Path) -> std::collections::BTreeMap<String, String> {
	fs::read_to_string(path)
		.unwrap()
		.lines()
		.filter_map(|l| l.split_once('='))
		.map(|(k, v)| (k.to_string(), v.to_string()))
		.collect()
}

#[cfg(unix)]
fn exec_case(report: &Value) -> &Value {
	report["suites"][0]["cases"]
		.as_array()
		.unwrap()
		.iter()
		.find(|c| c["kind"] == "exec")
		.unwrap_or_else(|| panic!("no exec case:\n{report:#}"))
}

/// `--exec` without suite files: one implicit suite prepares the database, the command gets
/// its coordinates, a token scoped to it (no root token) and the schema digest, and the
/// database is gone afterwards.
#[cfg(unix)]
#[tokio::test]
async fn exec_runs_against_the_prepared_database_before_teardown() {
	let url = require_server!();
	let project = Project::new();
	project.write("schema/probe.surql", PROBE_SCHEMA);
	let (script, out) = probe_script(&project);
	let ns = unique_ns("exec_ok");
	let exec = vec!["sh".into(), script, out.to_string_lossy().into_owned(), "0".into()];
	let (err, report) = project.test_with(&url, None, exec, Some(&ns)).await;
	assert_eq!(err, None, "{report:#}");
	assert_eq!(report["suites"][0]["suite_name"], "exec");
	assert_eq!(exec_case(&report)["passed"], true);

	let env = recorded(&out);
	assert_eq!(env["SURREALKIT_EXEC_ENDPOINT"], url);
	assert!(env["SURREALKIT_EXEC_NAMESPACE"].starts_with(&format!("{ns}_sk_test_")), "{env:?}");
	assert_eq!(env["SURREALKIT_EXEC_NAMESPACE"], report["suites"][0]["namespace"]);
	assert_eq!(env["SURREALKIT_EXEC_DATABASE"], report["suites"][0]["database"]);
	assert_eq!(env["SURREALKIT_EXEC_SUITE"], "exec");
	assert_eq!(env["SURREALKIT_EXEC_SCHEMA_FROM"], "sync");
	// The root actor's token is server-wide; it is passed only with --exec-root-token.
	assert_eq!(env.get("SURREALKIT_EXEC_ACTORS").map(String::as_str), Some(""), "{env:?}");
	assert!(!env.contains_key("SURREALKIT_EXEC_TOKEN_ROOT"), "{env:?}");
	assert_eq!(env["SURREALKIT_EXEC_TOKEN"].split('.').count(), 3, "a JWT: {env:?}");
	let digest = &env["SURREALKIT_EXEC_SCHEMA_DIGEST"];
	assert!(digest.starts_with("sha256:") && digest.len() == 71, "{digest}");

	// The digest is the schema the files describe: the same files compiled agree.
	#[cfg(feature = "kv-mem")]
	{
		use surrealkit::compile::{CompileModule, CompileOpts, compile};
		use surrealkit::schema_state::SchemaFile;
		let compiled = compile(
			&[CompileModule {
				name: "schema".into(),
				files: vec![SchemaFile {
					path: "schema/probe.surql".into(),
					sql: PROBE_SCHEMA.into(),
					hash: String::new(),
				}],
			}],
			&CompileOpts::default(),
		)
		.await
		.unwrap();
		assert_eq!(&compiled.document.semantic_digest, digest);
	}

	assert_eq!(leftover_namespaces(&url, &ns).await, Vec::<String>::new());
}

/// `SURREALKIT_EXEC_TOKEN` reaches the suite database and nothing else; the root token is
/// passed only on request.
#[cfg(unix)]
#[tokio::test]
async fn the_exec_token_is_scoped_to_the_suite_database() {
	let url = require_server!();
	let project = Project::new();
	project.write("schema/probe.surql", PROBE_SCHEMA);
	let (script, out) = probe_script(&project);
	let ns = unique_ns("exec_scope");
	let exec = vec!["sh".into(), script, out.to_string_lossy().into_owned(), "0".into()];
	let (err, report) = project
		.test_custom(&url, Some(&ns), |opts| {
			opts.exec = exec;
			opts.exec_root_token = true;
			opts.keep_db = true;
		})
		.await;
	assert_eq!(err, None, "{report:#}");
	let env = recorded(&out);
	assert_eq!(env["SURREALKIT_EXEC_ACTORS"], "root");
	assert_eq!(env["SURREALKIT_EXEC_TOKEN_ROOT"].split('.').count(), 3, "a JWT: {env:?}");
	let (suite_ns, suite_db) =
		(&env["SURREALKIT_EXEC_NAMESPACE"], &env["SURREALKIT_EXEC_DATABASE"]);

	let db = surrealdb::engine::any::connect(url.as_str()).await.unwrap();
	db.authenticate(env["SURREALKIT_EXEC_TOKEN"].clone()).await.unwrap();
	db.use_ns(suite_ns.as_str()).use_db(suite_db.as_str()).await.unwrap();
	db.query("CREATE probe:9 SET n = 9")
		.await
		.and_then(|r| r.check())
		.expect("the token owns the suite database");
	// 3.3 refuses the switch itself, 3.2 the first statement after it.
	let other = async {
		db.use_ns(suite_ns.as_str()).use_db("elsewhere").await?;
		db.query("CREATE probe:1 SET n = 1").await?.check()?;
		Ok::<_, surrealdb::Error>(())
	}
	.await;
	assert!(other.is_err(), "another database is out of reach: {other:?}");
	let info = db.query("INFO FOR ROOT").await.and_then(|r| r.check());
	assert!(info.is_err(), "the root level is out of reach: {info:?}");

	// --keep-db left the namespace; remove it.
	let root = surrealdb::engine::any::connect(url.as_str()).await.unwrap();
	root.signin(surrealdb::opt::auth::Root {
		username: std::env::var("SURREALKIT_TEST_USER").unwrap_or_else(|_| "root".into()),
		password: std::env::var("SURREALKIT_TEST_PASS").unwrap_or_else(|_| "secret".into()),
	})
	.await
	.unwrap();
	root.query(format!("REMOVE NAMESPACE `{suite_ns}`")).await.unwrap().check().unwrap();
	assert_eq!(leftover_namespaces(&url, &ns).await, Vec::<String>::new());
}

/// A failing command is a failed case, the run fails, and teardown still happens.
#[cfg(unix)]
#[tokio::test]
async fn a_failing_exec_fails_the_run_and_the_database_is_still_removed() {
	let url = require_server!();
	let project = Project::new();
	project.write("schema/probe.surql", PROBE_SCHEMA);
	project.write("tests/suites/smoke.toml", SMOKE_SUITE);
	let (script, out) = probe_script(&project);
	let ns = unique_ns("exec_fail");
	let exec = vec!["sh".into(), script, out.to_string_lossy().into_owned(), "3".into()];
	let (err, report) = project.test_with(&url, None, exec, Some(&ns)).await;
	assert_eq!(err.as_deref(), Some("1 test cases failed"), "{report:#}");
	assert_eq!(case(&report, "smoke", "probe exists")["passed"], true);
	let failed = exec_case(&report);
	assert_eq!(failed["passed"], false);
	assert!(failed["message"].as_str().unwrap().contains("exited with status 3"), "{failed}");
	assert_eq!(recorded(&out)["SURREALKIT_EXEC_SUITE"], "smoke");
	assert_eq!(leftover_namespaces(&url, &ns).await, Vec::<String>::new());
}

/// A suite whose preparation fails (here: a schema the server rejects) used to leave its
/// database behind; teardown no longer depends on preparation succeeding.
#[tokio::test]
async fn a_failed_preparation_still_removes_the_database() {
	let url = require_server!();
	let project = Project::new();
	project.write(
		"schema/bad.surql",
		"DEFINE TABLE probe SCHEMAFULL;\nDEFINE FIELD n ON probe TYPE nonsense<;\n",
	);
	project.write("tests/suites/smoke.toml", "name = \"smoke\"\n");
	let ns = unique_ns("prep_fail");
	let (err, _) = project.test_with(&url, None, Vec::new(), Some(&ns)).await;
	assert!(err.is_some(), "the broken schema must fail the run");
	assert_eq!(leftover_namespaces(&url, &ns).await, Vec::<String>::new());
}

/// `--schema-from` on the command line overrides a suite's own `schema_from`, as documented.
#[tokio::test]
async fn the_command_line_schema_source_overrides_the_suite() {
	let url = require_server!();
	let project = Project::new();
	project.write("schema/probe.surql", PROBE_SCHEMA);
	// No rollouts exist, so a suite that really replayed them would fail.
	project.write(
		"tests/suites/smoke.toml",
		&SMOKE_SUITE
			.replace("name = \"smoke\"\n", "name = \"smoke\"\nschema_from = \"rollouts\"\n"),
	);
	let (err, report) = project.test(&url, Some(SchemaSource::Sync)).await;
	assert_eq!(err, None, "{report:#}");
	assert_eq!(case(&report, "smoke", "probe exists")["passed"], true);
	let (err, _) = project.test(&url, None).await;
	assert!(err.unwrap().contains("rollouts"), "without the flag the suite replays rollouts");
}
