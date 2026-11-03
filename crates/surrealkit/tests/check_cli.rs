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
fn a_missing_reason_is_l7013_when_one_is_required_and_still_suppresses() {
	let bad = "DEFINE FIELD b ON t TYPE int;\n-- surreal: allow(K9003)\nDEFINE FIELD x.y ON t TYPE string;\n";
	let schema = [("001_base.surql", BASE), ("002_bad.surql", bad)];
	let dir = project("[analyze]\nrequire_suppression_reason = true\n", &schema);
	let output = surrealkit(dir.path(), &["-j", "check"]);
	assert_eq!(output.status.code(), Some(0));
	let report = json(&output);
	assert_eq!(report["summary"]["suppressed"], 1);
	assert_eq!(report["summary"]["warnings"], 1);
	assert_eq!(report["diagnostics"][0]["code"], "L7013");
	assert_eq!(report["diagnostics"][0]["severity"], "warning");
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
		let mut child = command(root, &["watch"])
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
