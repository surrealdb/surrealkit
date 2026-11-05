//! `surrealkit check` and `surrealkit watch` against a project on disk: the exit codes and the
//! output contract, without a database.
//!
//! The environment is cleared: these commands read no connection setting, and a variable of the
//! developer's (`SURREALDB_FOLDER` for one) must not change which directory they check.

use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tempfile::TempDir;

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn write(path: &Path, text: &str) {
	fs::create_dir_all(path.parent().expect("parent")).expect("dir");
	fs::write(path, text).expect("write");
}

/// A command for the binary in `root` with a clean environment. `RUST_MIN_STACK` is what
/// `.cargo/config.toml` sets for tests: the 4.0 engine needs more than a 2 MiB thread stack.
fn command(root: &Path, args: &[&str]) -> Command {
	let mut command = Command::new(env!("CARGO_BIN_EXE_surrealkit"));
	command
		.current_dir(root)
		.args(args)
		.env_clear()
		.env("NO_COLOR", "1")
		.env("RUST_MIN_STACK", "8388608");
	command
}

fn surrealkit(root: &Path, args: &[&str]) -> Output {
	command(root, args).output().expect("run surrealkit")
}

fn stdout(output: &Output) -> String {
	String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
	String::from_utf8_lossy(&output.stderr).into_owned()
}

fn json(output: &Output) -> serde_json::Value {
	serde_json::from_slice(&output.stdout)
		.unwrap_or_else(|err| panic!("stdout is one JSON document ({err}): {}", stdout(output)))
}

const BASE: &str = "DEFINE TABLE t SCHEMAFULL;\nDEFINE FIELD a ON t TYPE string;\n";
/// 4.0 refuses a nested field whose parent is not declared.
const BAD: &str = "DEFINE FIELD b ON t TYPE int;\nDEFINE FIELD x.y ON t TYPE string;\n";
const OPEN: &str = "DEFINE FIELD c ON t DEFAULT 'open;\n";

fn project(config: &str, schema: &[(&str, &str)]) -> TempDir {
	let dir = TempDir::new().expect("tempdir");
	if !config.is_empty() {
		write(&dir.path().join("surrealkit.toml"), config);
	}
	for (name, text) in schema {
		write(&dir.path().join("database/schema").join(name), text);
	}
	dir
}

fn broken() -> TempDir {
	project("", &[("001_base.surql", BASE), ("002_bad.surql", BAD), ("003_open.surql", OPEN)])
}

#[test]
fn a_clean_project_exits_zero_with_only_a_summary_on_stderr() {
	let dir = project("", &[("001_base.surql", BASE)]);
	let output = surrealkit(dir.path(), &["check"]);
	assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
	assert_eq!(stdout(&output), "");
	assert_eq!(stderr(&output), "checked 1 schema file and 0 query files: 0 errors, 0 warnings\n");
}

#[test]
fn findings_are_shown_for_a_person_as_json_and_as_sarif() {
	let dir = broken();
	let human = surrealkit(dir.path(), &["check"]);
	assert_eq!(human.status.code(), Some(1));
	assert_eq!(stdout(&human), "", "human output is all on stderr");
	insta::assert_snapshot!("check_human", stderr(&human));

	let as_json = surrealkit(dir.path(), &["--json", "check"]);
	assert_eq!(as_json.status.code(), Some(1));
	assert_eq!(stderr(&as_json), "", "the report is the product, not an error");
	assert_eq!(json(&as_json)["summary"]["passed"], false);
	insta::assert_snapshot!("check_json", stdout(&as_json));

	let sarif = surrealkit(dir.path(), &["check", "--format", "sarif"]);
	assert_eq!(sarif.status.code(), Some(1));
	assert_eq!(stderr(&sarif), "");
	insta::assert_snapshot!("check_sarif", stdout(&sarif).replace(VERSION, "<version>"));
}

#[test]
fn a_level_in_the_config_decides_what_a_finding_costs() {
	let schema = [("001_base.surql", BASE), ("002_bad.surql", BAD)];
	let denied = surrealkit(project("", &schema).path(), &["-j", "check"]);
	assert_eq!(denied.status.code(), Some(1));
	assert_eq!(json(&denied)["summary"]["errors"], 1);

	let allowed = project("[analyze.lints]\nK9003 = \"allow\"\n", &schema);
	let output = surrealkit(allowed.path(), &["-j", "check"]);
	assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
	let report = json(&output);
	assert_eq!(
		(report["summary"]["errors"].as_u64(), report["summary"]["allowed"].as_u64()),
		(Some(0), Some(1))
	);

	let warned = project("[analyze.lints]\nK9003 = \"warn\"\n", &schema);
	let output = surrealkit(warned.path(), &["-j", "check"]);
	assert_eq!(output.status.code(), Some(0));
	assert_eq!(json(&output)["summary"]["warnings"], 1);
	assert_eq!(json(&output)["diagnostics"][0]["severity"], "warning");

	// An entry's own level is not raised by `warnings_as_errors`.
	let named = project(
		"[analyze]\nwarnings_as_errors = true\n[analyze.lints]\n\"9xxx\" = \"warn\"\n",
		&schema,
	);
	assert_eq!(surrealkit(named.path(), &["check"]).status.code(), Some(0));
}

#[test]
fn warnings_as_errors_raises_a_default_warning() {
	// A malformed suppression is L7013, a warning by default.
	let schema = [("001_base.surql", "-- surreal: allow(*)\nDEFINE TABLE t;\n")];
	let plain = project("", &schema);
	let output = surrealkit(plain.path(), &["-j", "check"]);
	assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
	assert_eq!(json(&output)["diagnostics"][0]["code"], "L7013");

	let strict = project("[analyze]\nwarnings_as_errors = true\n", &schema);
	let output = surrealkit(strict.path(), &["-j", "check"]);
	assert_eq!(output.status.code(), Some(1));
	assert_eq!(json(&output)["diagnostics"][0]["severity"], "error");
}

#[test]
fn a_suppression_comment_silences_the_finding_it_covers() {
	let bad = "DEFINE FIELD b ON t TYPE int;\n\
		-- surreal: allow(K9003) reason=\"the parent comes in a later release\"\n\
		DEFINE FIELD x.y ON t TYPE string;\n";
	let schema = [("001_base.surql", BASE), ("002_bad.surql", bad)];
	let dir = project("[analyze]\nrequire_suppression_reason = true\n", &schema);
	let output = surrealkit(dir.path(), &["-j", "check"]);
	assert_eq!(output.status.code(), Some(0), "{}", stdout(&output));
	let report = json(&output);
	assert_eq!(report["summary"]["suppressed"], 1);
	assert_eq!(report["diagnostics"], serde_json::json!([]));
}

#[test]
fn a_missing_reason_is_l7013_when_one_is_required_and_suppresses_nothing() {
	let bad = "DEFINE FIELD b ON t TYPE int;\n-- surreal: allow(K9003)\nDEFINE FIELD x.y ON t TYPE string;\n";
	let schema = [("001_base.surql", BASE), ("002_bad.surql", bad)];
	let dir = project("[analyze]\nrequire_suppression_reason = true\n", &schema);
	let output = surrealkit(dir.path(), &["-j", "check"]);
	assert_eq!(output.status.code(), Some(1), "the K9003 under the directive is reported");
	let report = json(&output);
	assert_eq!(report["summary"]["suppressed"], 0);
	assert_eq!(report["summary"]["warnings"], 1);
	assert_eq!(report["summary"]["errors"], 1);
	let diagnostics = report["diagnostics"].as_array().unwrap();
	assert_eq!(diagnostics[0]["code"], "L7013");
	assert_eq!(diagnostics[0]["severity"], "warning");
	assert_eq!(diagnostics[1]["code"], "K9003");

	// Without the requirement the same file passes, and the directive suppresses.
	let relaxed = project("", &schema);
	let output = surrealkit(relaxed.path(), &["-j", "check"]);
	assert_eq!(output.status.code(), Some(0), "{}", stdout(&output));
	assert_eq!(json(&output)["summary"]["suppressed"], 1);
}

#[test]
fn a_missing_reason_in_a_host_file_is_l7013_when_one_is_required_and_suppresses_nothing() {
	let config = format!("{HOST_CONFIG}[analyze]\nrequire_suppression_reason = true\n");
	let dir = project(&config, &[("001_base.surql", BASE)]);
	let app = "\
export async function find(db, id) {
	// surreal: allow(K9201)
	return db.query(`SELECT * FROM t WHERE a = ${id}`);
}
";
	write(&dir.path().join("src/app.ts"), app);
	let output = surrealkit(dir.path(), &["-j", "check"]);
	assert_eq!(output.status.code(), Some(1), "{}", stdout(&output));
	let report = json(&output);
	assert_eq!(report["summary"]["suppressed"], 0);
	let codes: Vec<&str> = report["diagnostics"]
		.as_array()
		.unwrap()
		.iter()
		.filter_map(|d| d["code"].as_str())
		.collect();
	assert_eq!(codes, vec!["L7013", "K9201"]);
}

#[test]
fn a_malformed_directive_is_l7013_and_suppresses_nothing() {
	let bad = "DEFINE FIELD b ON t TYPE int;\n-- surreal: allow(7xxx)\nDEFINE FIELD x.y ON t TYPE string;\n";
	let schema = [("001_base.surql", BASE), ("002_bad.surql", bad)];
	let output = surrealkit(project("", &schema).path(), &["-j", "check"]);
	assert_eq!(output.status.code(), Some(1));
	let report = json(&output);
	assert_eq!(report["summary"]["suppressed"], 0);
	let codes: Vec<&str> = report["diagnostics"]
		.as_array()
		.unwrap()
		.iter()
		.filter_map(|d| d["code"].as_str())
		.collect();
	assert_eq!(codes, vec!["L7013", "K9003"]);
}

#[test]
fn a_bad_target_in_a_list_is_one_l7013_and_the_good_target_still_suppresses() {
	// `K9003` is kit-owned and valid, `E1002` is the engine's, and the bare `9201` is a kit target
	// that is not a rendered code.
	let bad = "DEFINE FIELD b ON t TYPE int;\n\
		-- surreal: allow(K9003, E1002, 9201)\n\
		DEFINE FIELD x.y ON t TYPE string;\n";
	let schema = [("001_base.surql", BASE), ("002_bad.surql", bad)];
	let output = surrealkit(project("", &schema).path(), &["-j", "check"]);
	assert_eq!(output.status.code(), Some(0), "{}", stdout(&output));
	let report = json(&output);
	assert_eq!(report["summary"]["suppressed"], 1, "K9003 was suppressed");
	let diagnostics = report["diagnostics"].as_array().unwrap();
	assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
	assert_eq!(diagnostics[0]["code"], "L7013");
	assert_eq!(diagnostics[0]["path"], "database/schema/002_bad.surql");
	assert_eq!(diagnostics[0]["span"]["line"], 2, "at the directive");
	let message = diagnostics[0]["message"].as_str().unwrap();
	assert!(message.contains("`9201`") && message.contains("K9201"), "{message}");
}

#[test]
fn a_bad_target_in_a_host_directive_is_one_l7013_and_the_good_target_still_suppresses() {
	let dir = project(HOST_CONFIG, &[("001_base.surql", BASE)]);
	let app = "\
export async function find(db, id) {
	// surreal: allow(E1002, K9201, K9xxx) reason=\"id is a validated integer\"
	return db.query(`SELECT * FROM t WHERE a = ${id}`);
}
";
	write(&dir.path().join("src/app.ts"), app);
	let output = surrealkit(dir.path(), &["-j", "check"]);
	assert_eq!(output.status.code(), Some(0), "{}", stdout(&output));
	let report = json(&output);
	assert_eq!(report["summary"]["suppressed"], 1, "K9201 was suppressed");
	let diagnostics = report["diagnostics"].as_array().unwrap();
	assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
	assert_eq!(diagnostics[0]["code"], "L7013");
	assert_eq!(diagnostics[0]["path"], "src/app.ts");
	assert_eq!(diagnostics[0]["span"]["line"], 2, "at the directive");
	let message = diagnostics[0]["message"].as_str().unwrap();
	assert!(message.contains("`K9xxx`") && message.contains("no wildcards"), "{message}");
}

#[test]
fn a_bad_lint_key_or_level_is_an_error_not_a_finding() {
	for config in ["[analyze.lints]\n\"E20\" = \"warn\"\n", "[analyze.lints]\nE1002 = \"loud\"\n"] {
		let dir = project(config, &[("001_base.surql", BASE)]);
		let output = surrealkit(dir.path(), &["check"]);
		assert_eq!(output.status.code(), Some(1), "{config}");
		assert_eq!(stdout(&output), "");
		assert!(stderr(&output).starts_with("error: "), "{}", stderr(&output));

		// Under `--json`, and under `--format json`, the failure is the envelope on stderr.
		for args in [&["-j", "check"][..], &["check", "--format", "json"]] {
			let output = surrealkit(dir.path(), args);
			assert_eq!(output.status.code(), Some(1));
			assert_eq!(stdout(&output), "", "{args:?}");
			let envelope: serde_json::Value =
				serde_json::from_slice(&output.stderr).expect("envelope");
			assert_eq!(envelope["exit_code"], 1);
			assert!(
				envelope["message"].as_str().unwrap().contains("[analyze.lints]"),
				"{envelope}"
			);
		}
	}
}

#[test]
fn contradictory_options_are_errors() {
	let dir = project("", &[("001_base.surql", BASE)]);
	let output = surrealkit(dir.path(), &["-j", "check", "--format", "sarif"]);
	assert_eq!(output.status.code(), Some(1));
	assert_eq!(stdout(&output), "");
	assert!(stderr(&output).contains("disagree"), "{}", stderr(&output));

	let output = surrealkit(dir.path(), &["check", "--watch", "-j"]);
	assert_eq!(output.status.code(), Some(1));
	assert!(stderr(&output).contains("takes no `--json`"), "{}", stderr(&output));

	let output = surrealkit(dir.path(), &["watch", "-j"]);
	assert_eq!(output.status.code(), Some(1));
	assert!(stderr(&output).contains("takes no `--json`"), "{}", stderr(&output));

	let output = surrealkit(dir.path(), &["check", "--watch", "--format", "sarif"]);
	assert_eq!(output.status.code(), Some(2), "clap's usage error");
}

#[test]
fn check_needs_none_of_the_environment_a_database_command_would() {
	// A declared target whose password is in an unset environment variable fails every
	// database command before it starts; a CI job that only lints must not need it.
	let config = "[target.prod]\nns = \"app\"\ndb = \"prod\"\npass_env = \"PROD_DB_PASSWORD\"\n";
	let dir = project(config, &[("001_base.surql", BASE)]);
	let output = command(dir.path(), &["-j", "check"])
		.env("SURREALDB_HOST", "http://127.0.0.1:1")
		.env("SURREALDB_NAMESPACE", "nope")
		.env("SURREALDB_FOLDER", "elsewhere")
		.env("DATABASE_HOST", "http://127.0.0.1:1")
		.output()
		.expect("run");
	assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
	assert_eq!(json(&output)["summary"]["schema_files"], 1);
	assert!(!stderr(&output).contains("PROD_DB_PASSWORD"), "{}", stderr(&output));
}

/// A declared target nothing listens on: a command that connects to it fails at once.
const UNREACHABLE: &str =
	"[target.prod]\nhost = \"ws://127.0.0.1:1\"\nns = \"app\"\ndb = \"prod\"\n";

#[test]
fn a_plain_check_connects_to_no_target_even_a_primary_one() {
	let config = format!("{UNREACHABLE}primary = true\n");
	let dir = project(&config, &[("001_base.surql", BASE)]);
	for args in [&["check"][..], &["check", "--all"]] {
		let started = Instant::now();
		let output = surrealkit(dir.path(), args);
		assert_eq!(output.status.code(), Some(0), "{args:?}: {}", stderr(&output));
		assert!(started.elapsed() < Duration::from_secs(20), "{args:?} waited on a connection");
		assert!(!stderr(&output).contains("prod"), "{args:?}: {}", stderr(&output));
	}
	let output = surrealkit(dir.path(), &["-j", "check"]);
	let summary = &json(&output)["summary"];
	assert_eq!(
		(summary["targets_checked"].as_u64(), summary["host_read"].as_bool()),
		(Some(0), Some(false))
	);
	assert!(summary["drift"].as_object().expect("an object").is_empty());
}

#[test]
fn connection_flags_still_warn_on_a_check_without_a_target() {
	let dir = project("", &[("001_base.surql", BASE)]);
	let output = surrealkit(dir.path(), &["check", "--host", "ws://127.0.0.1:1"]);
	assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
	assert!(stderr(&output).contains("never connects to a database"), "{}", stderr(&output));
}

#[test]
fn watching_a_target_is_a_usage_error() {
	let dir = project(UNREACHABLE, &[("001_base.surql", BASE)]);
	for args in [&["check", "--watch", "--target", "prod"][..], &["watch", "-t", "prod"]] {
		let output = surrealkit(dir.path(), args);
		assert_eq!(output.status.code(), Some(1), "{args:?}");
		assert_eq!(stdout(&output), "");
		assert!(
			stderr(&output).contains("`--watch` does not poll a live database; drop --target"),
			"{args:?}: {}",
			stderr(&output)
		);
	}
}

#[test]
fn an_unreachable_target_is_a_command_error_not_a_finding() {
	let dir = project(UNREACHABLE, &[("001_base.surql", BASE)]);
	let output =
		surrealkit(dir.path(), &["check", "--target", "prod", "--connect-timeout-secs", "5"]);
	assert_eq!(output.status.code(), Some(1));
	assert!(
		stderr(&output).starts_with("error: connecting to target `prod`"),
		"{}",
		stderr(&output)
	);
	assert!(!stderr(&output).contains("checked "), "no report: {}", stderr(&output));

	let output =
		surrealkit(dir.path(), &["--json", "check", "-t", "prod", "--connect-timeout-secs", "5"]);
	assert_eq!(output.status.code(), Some(1));
	assert_eq!(stdout(&output), "", "a failure is the envelope on stderr, never a report");
	assert!(stderr(&output).contains("connecting to target `prod`"), "{}", stderr(&output));
}

#[test]
fn a_target_must_be_declared_and_its_password_variable_set() {
	let dir = project(UNREACHABLE, &[("001_base.surql", BASE)]);
	let output = surrealkit(dir.path(), &["check", "--target", "nope"]);
	assert_eq!(output.status.code(), Some(1));
	assert!(stderr(&output).contains("unknown target \"nope\""), "{}", stderr(&output));

	let config = "[target.prod]\nhost = \"ws://127.0.0.1:1\"\npass_env = \"PROD_DB_PASSWORD\"\n";
	let dir = project(config, &[("001_base.surql", BASE)]);
	let output = surrealkit(dir.path(), &["check", "--target", "prod"]);
	assert_eq!(output.status.code(), Some(1));
	assert!(stderr(&output).contains("PROD_DB_PASSWORD"), "{}", stderr(&output));
}

#[test]
fn a_bad_drift_ignore_entry_is_an_error() {
	let dir = project("[analyze]\ndrift_ignore = [\"a..b\"]\n", &[("001_base.surql", BASE)]);
	let output = surrealkit(dir.path(), &["check"]);
	assert_eq!(output.status.code(), Some(1));
	assert!(stderr(&output).contains("drift_ignore"), "{}", stderr(&output));
}

fn server() -> Option<(String, String, String)> {
	let url = std::env::var("SURREALKIT_TEST_URL").ok().filter(|url| !url.is_empty())?;
	let user = std::env::var("SURREALKIT_TEST_USER").unwrap_or_else(|_| "root".into());
	let pass = std::env::var("SURREALKIT_TEST_PASS").unwrap_or_else(|_| "secret".into());
	Some((url, user, pass))
}

const PERSON: &str = "DEFINE TABLE person SCHEMAFULL;\nDEFINE FIELD name ON person TYPE string;\n\
                      DEFINE FIELD age ON person TYPE int;\nDEFINE TABLE legacy SCHEMAFULL;\n\
                      DEFINE FIELD note ON legacy TYPE string;\n";
const PERSON_NOW: &str = "DEFINE TABLE person SCHEMAFULL;\n\
                          DEFINE FIELD name ON person TYPE string;\n\
                          DEFINE FIELD age ON person TYPE int;\n";

/// The report as it is compared with the golden one: what depends on the server's engine
/// version (`K9107`, and the hint it counts) left out.
fn redacted(report: &serde_json::Value) -> String {
	let mut report = report.clone();
	let kept: Vec<serde_json::Value> = report["diagnostics"]
		.as_array()
		.expect("diagnostics")
		.iter()
		.filter(|d| d["code"] != "K9107")
		.cloned()
		.collect();
	report["diagnostics"] = kept.into();
	report["summary"]["hints"] = 0.into();
	report["summary"]["drift"]["live"] =
		(report["diagnostics"].as_array().map_or(0, Vec::len)).into();
	serde_json::to_string_pretty(&report).expect("json")
}

/// `check --target` against a live server, and a project that has drifted from it.
#[test]
#[expect(clippy::print_stderr, reason = "a skipped live test says why")]
fn drift_against_a_live_server() {
	let Some((url, user, pass)) = server() else {
		eprintln!("SKIP: set SURREALKIT_TEST_URL to run the live drift test");
		return;
	};
	let nonce = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map_or(0, |since| since.as_nanos());
	let ns = format!("kit_drift_{nonce}");
	let config = format!(
		"[target.live]\nhost = {url:?}\nns = {ns:?}\ndb = \"drift\"\nuser = {user:?}\n\
		 pass_env = \"KIT_DRIFT_PASS\"\n"
	);
	let dir = project(&config, &[("001_person.surql", PERSON)]);
	let run = |args: &[&str]| {
		command(dir.path(), args)
			.env("KIT_DRIFT_PASS", &pass)
			.env("SURREALDB_CONNECT_TIMEOUT_SECS", "30")
			.output()
			.expect("run surrealkit")
	};

	let synced = run(&["sync", "--target", "live"]);
	assert_eq!(synced.status.code(), Some(0), "{}", stderr(&synced));

	// Synced from these files: nothing differs but, across engine minors, the hint.
	let clean = run(&["-j", "check", "--target", "live"]);
	assert_eq!(clean.status.code(), Some(0), "{}{}", stderr(&clean), stdout(&clean));
	let report = json(&clean);
	assert_eq!(report["summary"]["targets_checked"], 1);
	assert_eq!(
		(report["summary"]["errors"].as_u64(), report["summary"]["warnings"].as_u64()),
		(Some(0), Some(0))
	);

	// The files drop `legacy`, which stays on the server; the server loses a field, gains an
	// unmanaged table and has a field's kind changed.
	write(&dir.path().join("database/schema/001_person.surql"), PERSON_NOW);
	let runtime = tokio::runtime::Runtime::new().expect("runtime");
	runtime.block_on(async {
		let db = surrealkit::surrealdb::engine::any::connect(url.as_str()).await.expect("connect");
		db.signin(surrealkit::surrealdb::opt::auth::Root {
			username: user.clone(),
			password: pass.clone(),
		})
		.await
		.expect("signin");
		db.use_ns(&ns).use_db("drift").await.expect("use");
		db.query(
			"REMOVE FIELD age ON person; DEFINE TABLE audit SCHEMAFULL; \
			 DEFINE FIELD OVERWRITE name ON person TYPE int;",
		)
		.await
		.and_then(|response| response.check())
		.expect("alter");
	});

	let drifted = run(&["-j", "check", "--target", "live"]);
	assert_eq!(drifted.status.code(), Some(1), "{}", stderr(&drifted));
	let report = json(&drifted);
	let codes: Vec<&str> = report["diagnostics"]
		.as_array()
		.expect("diagnostics")
		.iter()
		.map(|d| d["code"].as_str().expect("code"))
		.filter(|code| *code != "K9107")
		.collect();
	// In file order: `name` (line 2) before `age` (line 3), then the config file.
	assert_eq!(codes, ["K9104", "K9103", "K9105", "K9106"], "{report:#}");
	let at_server_engine = !report["diagnostics"]
		.as_array()
		.expect("diagnostics")
		.iter()
		.any(|d| d["code"] == "K9107");
	if at_server_engine {
		insta::assert_snapshot!("check_drift_json", redacted(&report));
	} else {
		eprintln!(
			"the server runs another engine minor: the golden report is compared at 4.0 only"
		);
	}

	let human = run(&["check", "--target", "live"]);
	let text = stderr(&human);
	assert!(
		text.contains("error[K9103]: field `person.age` is not defined on target `live`"),
		"{text}"
	);
	assert!(text.contains("compared the files with 1 target: live ("), "{text}");
	// The drift pass is read-only: another run finds the same thing.
	assert_eq!(json(&run(&["-j", "check", "--target", "live"])), report);
}

const HOST_CONFIG: &str = "[typegen.queries]\nsources = [\"src\"]\n";
const APP_TS: &str = "\
import { surql } from 'surrealdb';

export async function find(db, id) {
	await db.query(`SELECT * FROM t WHERE a = ${id}`);
	await db.query(surql`SELECT * FROM t WHERE a = ${id}`);
	await db.query('SELECT * FROM t WHERE a = $id', { id });
	// surreal: allow(K9201) reason=\"id is a validated integer\"
	return db.query(`SELECT * FROM t WHERE a = ${id}`);
}
";

#[test]
fn host_code_with_an_interpolated_query_is_k9201() {
	let dir = project(HOST_CONFIG, &[("001_base.surql", BASE)]);
	write(&dir.path().join("src/app.ts"), APP_TS);
	let human = surrealkit(dir.path(), &["check"]);
	assert_eq!(human.status.code(), Some(1), "{}", stderr(&human));
	assert_eq!(stdout(&human), "");
	insta::assert_snapshot!("check_host_human", stderr(&human));

	let as_json = surrealkit(dir.path(), &["-j", "check"]);
	let report = json(&as_json);
	let summary = &report["summary"];
	assert_eq!(
		(
			summary["host_read"].as_bool(),
			summary["host_files"].as_u64(),
			summary["host_queries"].as_u64()
		),
		(Some(true), Some(1), Some(3))
	);
	assert_eq!(summary["suppressed"], 1);
	assert_eq!(report["diagnostics"][0]["code"], "K9201");
	assert_eq!(report["diagnostics"][0]["path"], "src/app.ts");

	// A level in `[analyze.lints]` applies to it like any other code.
	write(
		&dir.path().join("surrealkit.toml"),
		&format!("{HOST_CONFIG}[analyze.lints]\nK9201 = \"warn\"\n"),
	);
	let warned = surrealkit(dir.path(), &["check"]);
	assert_eq!(warned.status.code(), Some(0), "{}", stderr(&warned));
}

#[test]
fn a_svelte_query_attribute_with_a_brace_is_k9201_at_the_brace() {
	let dir = project(HOST_CONFIG, &[("001_base.surql", BASE)]);
	let page = "\
<h1>People</h1>
<Query q=\"SELECT * FROM person WHERE name = '{name}'\" />
<Query q=\"SELECT * FROM person WHERE age > $min\" params={{ min }} />
<script>
	const all = createQuery(\"SELECT * FROM person\");
</script>
";
	write(&dir.path().join("src/Page.svelte"), page);
	let human = surrealkit(dir.path(), &["check"]);
	assert_eq!(human.status.code(), Some(1), "{}", stderr(&human));
	let text = stderr(&human);
	assert!(text.contains("error[K9201]: `${…}` interpolated into a query string"), "{text}");
	assert!(text.contains(" --> src/Page.svelte:2:46\n"), "{text}");
	let source_line = "2 | <Query q=\"SELECT * FROM person WHERE name = '{name}'\" />";
	let carets = format!("  | {}{}", " ".repeat(45), "^".repeat("{name}".len()));
	assert!(text.contains(&format!("{source_line}\n{carets}\n")), "{text}");
	assert!(text.contains("= help: pass `params` instead of interpolating"), "{text}");
	assert!(text.contains("read 1 host file holding 3 queries"), "{text}");

	let as_json = surrealkit(dir.path(), &["-j", "check"]);
	let report = json(&as_json);
	assert_eq!(report["diagnostics"].as_array().map(Vec::len), Some(1));
	assert_eq!(report["diagnostics"][0]["code"], "K9201");
	assert_eq!(report["diagnostics"][0]["path"], "src/Page.svelte");
}

#[test]
fn a_preload_second_argument_with_a_substitution_is_k9201() {
	let dir = project(HOST_CONFIG, &[("001_base.surql", BASE)]);
	let page = "\
export const load = async ({ locals: { db } }) => ({
	people: await preload(db, `SELECT * FROM person WHERE id = ${id}`),
	posts: await preload(db, \"SELECT * FROM post\"),
	swapped: await preload(`SELECT ${first}`, db),
});
";
	write(&dir.path().join("src/+page.server.ts"), page);
	let human = surrealkit(dir.path(), &["check"]);
	assert_eq!(human.status.code(), Some(1), "{}", stderr(&human));
	let text = stderr(&human);
	assert!(text.contains(" --> src/+page.server.ts:2:"), "{text}");
	assert!(text.contains("read 1 host file holding 2 queries"), "{text}");

	let report = json(&surrealkit(dir.path(), &["-j", "check"]));
	assert_eq!(report["diagnostics"].as_array().map(Vec::len), Some(1));
	assert_eq!(report["diagnostics"][0]["code"], "K9201");
	assert_eq!(report["diagnostics"][0]["path"], "src/+page.server.ts");

	// `functions` can move the argument: with `arg = 0` the first argument is the query.
	write(
		&dir.path().join("surrealkit.toml"),
		"[typegen.queries]\nsources = [\"src\"]\nfunctions = [{ name = \"preload\", arg = 0 }]\n",
	);
	let moved = surrealkit(dir.path(), &["check"]);
	assert_eq!(moved.status.code(), Some(1), "{}", stderr(&moved));
	assert!(stderr(&moved).contains(" --> src/+page.server.ts:4:"), "{}", stderr(&moved));
}

#[test]
fn without_queries_sources_no_host_code_is_read() {
	let dir = project("", &[("001_base.surql", BASE)]);
	write(&dir.path().join("src/app.ts"), APP_TS);
	let output = surrealkit(dir.path(), &["-j", "check"]);
	assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
	assert_eq!(json(&output)["summary"]["host_read"], false);
	let missing =
		project("[typegen.queries]\nsources = [\"nowhere\"]\n", &[("001_base.surql", BASE)]);
	let output = surrealkit(missing.path(), &["check"]);
	assert_eq!(output.status.code(), Some(1));
	assert!(stderr(&output).contains("does not exist"), "{}", stderr(&output));
}

#[test]
fn a_project_with_no_sources_warns_and_passes() {
	let dir = TempDir::new().expect("tempdir");
	let output = surrealkit(dir.path(), &["check"]);
	assert_eq!(output.status.code(), Some(0));
	let text = stderr(&output);
	assert!(text.contains("no SurrealQL sources found under "), "{text}");
	assert!(text.contains("check --folder, --schema and [analyze] ignore"), "{text}");
	assert!(
		text.ends_with("checked 0 schema files and 0 query files: 0 errors, 0 warnings\n"),
		"{text}"
	);
}

#[test]
fn query_files_are_counted_and_reported_as_not_analyzed() {
	let dir = project("", &[("001_base.surql", BASE)]);
	write(&dir.path().join("queries/list.surql"), "SELECT nope FROM t;\n");
	write(&dir.path().join("dist/gen.surql"), "SELECT 1;\n");
	let output = surrealkit(dir.path(), &["check"]);
	assert_eq!(output.status.code(), Some(0));
	let text = stderr(&output);
	assert!(text.contains("checked 1 schema file and 2 query files"), "{text}");
	assert!(text.contains("2 query files were found but not analyzed"), "{text}");

	write(&dir.path().join("surrealkit.toml"), "[analyze]\nignore = [\"dist\"]\n");
	let output = surrealkit(dir.path(), &["-j", "check"]);
	let report = json(&output);
	assert_eq!(report["summary"]["query_files"], 1);
	assert_eq!(report["summary"]["queries_analyzed"], false);
}

/// A running `surrealkit watch`, with what it wrote to stderr gathered on a thread.
struct Watching {
	child: std::process::Child,
	seen: Arc<Mutex<String>>,
}

impl Watching {
	fn start(root: &Path) -> Self {
		Self::start_with(root, &["watch"])
	}

	fn start_with(root: &Path, args: &[&str]) -> Self {
		let mut child = command(root, args)
			.stdout(Stdio::piped())
			.stderr(Stdio::piped())
			.spawn()
			.expect("spawn watch");
		let seen = Arc::new(Mutex::new(String::new()));
		let stderr = child.stderr.take().expect("piped stderr");
		let sink = Arc::clone(&seen);
		std::thread::spawn(move || {
			for line in BufReader::new(stderr).lines().map_while(Result::ok) {
				let mut seen = sink.lock().expect("lock");
				seen.push_str(&line);
				seen.push('\n');
			}
		});
		Self {
			child,
			seen,
		}
	}

	fn text(&self) -> String {
		self.seen.lock().expect("lock").clone()
	}

	/// Poll until `done` accepts what was written, or `limit` passes; `tick` runs on each poll.
	fn wait(&self, limit: Duration, done: impl Fn(&str) -> bool, mut tick: impl FnMut()) -> bool {
		let start = Instant::now();
		while start.elapsed() < limit {
			if done(&self.text()) {
				return true;
			}
			tick();
			std::thread::sleep(Duration::from_millis(250));
		}
		done(&self.text())
	}
}

impl Drop for Watching {
	fn drop(&mut self) {
		let _ = self.child.kill();
		let _ = self.child.wait();
	}
}

#[test]
fn watch_runs_again_when_a_schema_file_changes() {
	let dir = project("", &[("001_base.surql", BASE)]);
	let watching = Watching::start(dir.path());
	assert!(
		watching.wait(Duration::from_secs(90), |text| text.contains("0 errors"), || {}),
		"run 1 never finished: {}",
		watching.text()
	);
	let first = watching.text();
	assert!(first.starts_with("watching "), "{first}");
	assert!(first.contains("run 1 · start"), "{first}");

	// Rewritten every couple of seconds until it is seen: a file system watcher may not be
	// ready the instant the first run prints.
	let bad = dir.path().join("database/schema/002_bad.surql");
	let mut round = 0;
	let mut last = Instant::now() - Duration::from_secs(10);
	let seen = watching.wait(
		Duration::from_secs(60),
		|text| text.contains("run 2 · ") && text.contains("error[K9003]"),
		|| {
			if last.elapsed() >= Duration::from_secs(2) {
				round += 1;
				write(&bad, &format!("{BAD}-- {round}\n"));
				last = Instant::now();
			}
		},
	);
	let text = watching.text();
	assert!(seen, "run 2 never reported the error: {text}");
	assert!(text.contains("database/schema/002_bad.surql"), "the reason names the file: {text}");
	assert!(text.contains("1 error"), "{text}");
}

/// A project whose one K9003 is silenced by a directive with a reason.
fn silenced() -> TempDir {
	let bad = "DEFINE FIELD b ON t TYPE int;\n\
		-- surreal: allow(K9003) reason=\"the parent comes in a later release\"\n\
		DEFINE FIELD x.y ON t TYPE string;\n";
	project("", &[("001_base.surql", BASE), ("002_bad.surql", bad)])
}

#[test]
fn show_suppressed_shows_what_a_directive_silenced_in_every_format() {
	let dir = silenced();
	let plain = surrealkit(dir.path(), &["check"]);
	assert_eq!(plain.status.code(), Some(0), "{}", stderr(&plain));
	assert_eq!(
		stderr(&plain),
		"checked 2 schema files and 0 query files: 0 errors, 0 warnings, 1 suppressed\n"
	);

	let human = surrealkit(dir.path(), &["check", "--show-suppressed"]);
	assert_eq!(human.status.code(), Some(0), "the exit code is the same: {}", stderr(&human));
	assert_eq!(stdout(&human), "");
	assert!(stderr(&human).starts_with("suppressed[K9003]: "), "{}", stderr(&human));
	assert!(stderr(&human).ends_with("0 errors, 0 warnings, 1 suppressed\n"), "{}", stderr(&human));
	insta::assert_snapshot!("check_show_suppressed_human", stderr(&human));

	let bare = surrealkit(dir.path(), &["-j", "check"]);
	assert_eq!(json(&bare)["diagnostics"], serde_json::json!([]));
	let as_json = surrealkit(dir.path(), &["--json", "check", "--show-suppressed"]);
	assert_eq!(as_json.status.code(), Some(0), "{}", stderr(&as_json));
	assert_eq!(stderr(&as_json), "");
	let report = json(&as_json);
	assert_eq!(report["summary"], json(&bare)["summary"], "the counts do not move");
	assert_eq!(report["summary"]["suppressed"], 1);
	assert_eq!(report["diagnostics"][0]["suppressed"]["directive"]["line"], 2);
	insta::assert_snapshot!("check_show_suppressed_json", stdout(&as_json));

	let bare = surrealkit(dir.path(), &["check", "--format", "sarif"]);
	assert_eq!(json(&bare)["runs"][0]["results"], serde_json::json!([]));
	let sarif = surrealkit(dir.path(), &["check", "--format", "sarif", "--show-suppressed"]);
	assert_eq!(sarif.status.code(), Some(0), "{}", stderr(&sarif));
	assert_eq!(json(&sarif)["runs"][0]["results"][0]["suppressions"][0]["kind"], "inSource");
	insta::assert_snapshot!(
		"check_show_suppressed_sarif",
		stdout(&sarif).replace(VERSION, "<version>")
	);
}

#[test]
fn show_suppressed_changes_neither_the_exit_code_nor_the_other_findings() {
	// One K9003 is silenced and one is not: the flag adds a block and the exit code stays 1.
	let schema = [
		("001_base.surql", BASE),
		("002_bad.surql", "-- surreal: allow(K9003)\nDEFINE FIELD x.y ON t TYPE string;\n"),
		("003_bad.surql", "DEFINE FIELD p.q ON t TYPE string;\n"),
	];
	let dir = project("", &schema);
	let without = surrealkit(dir.path(), &["-j", "check"]);
	let with = surrealkit(dir.path(), &["-j", "check", "--show-suppressed"]);
	assert_eq!((without.status.code(), with.status.code()), (Some(1), Some(1)));
	assert_eq!(json(&without)["summary"], json(&with)["summary"]);
	let shown = json(&with);
	let diagnostics = shown["diagnostics"].as_array().expect("diagnostics");
	assert_eq!(diagnostics.len(), 2, "{shown}");
	let only: Vec<_> = diagnostics.iter().filter(|d| d.get("suppressed").is_none()).collect();
	assert_eq!(only.len(), 1);
	assert_eq!(json(&without)["diagnostics"], serde_json::Value::Array(vec![only[0].clone()]));
	assert_eq!(shown["summary"]["errors"], 1);
}

#[test]
fn a_host_finding_a_comment_silenced_is_shown_with_show_suppressed() {
	let dir = project(HOST_CONFIG, &[("001_base.surql", BASE)]);
	write(&dir.path().join("src/app.ts"), APP_TS);
	let human = surrealkit(dir.path(), &["check", "--show-suppressed"]);
	assert_eq!(human.status.code(), Some(1), "the unsilenced K9201 still fails it");
	let text = stderr(&human);
	assert!(text.contains("suppressed[K9201]: "), "{text}");
	assert!(text.contains("= note: suppressed by the directive at src/app.ts:7"), "{text}");
	assert!(text.contains("error[K9201]: "), "{text}");
	assert!(text.ends_with("read 1 host file holding 3 queries\n"), "{text}");

	let report = json(&surrealkit(dir.path(), &["-j", "check", "--show-suppressed"]));
	assert_eq!(report["summary"]["suppressed"], 1);
	assert_eq!(report["summary"]["errors"], 1);
	let silenced: Vec<_> = report["diagnostics"]
		.as_array()
		.expect("diagnostics")
		.iter()
		.filter(|d| d.get("suppressed").is_some())
		.collect();
	assert_eq!(silenced.len(), 1, "{report}");
	assert_eq!(silenced[0]["code"], "K9201");
	assert_eq!(silenced[0]["path"], "src/app.ts");
	assert_eq!(silenced[0]["span"]["line"], 8);
	assert_eq!(silenced[0]["suppressed"]["directive"]["line"], 7);
}

#[test]
fn watch_takes_show_suppressed() {
	let dir = silenced();
	let watching = Watching::start_with(dir.path(), &["watch", "--show-suppressed"]);
	assert!(
		watching.wait(Duration::from_secs(90), |text| text.contains("1 suppressed"), || {}),
		"run 1 never finished: {}",
		watching.text()
	);
	assert!(watching.text().contains("suppressed[K9003]"), "{}", watching.text());
}
