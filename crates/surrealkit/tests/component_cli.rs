//! The command line's contract with scripts and with the `surreal` launcher,
//! which runs it as `surreal kit`: `--version`, `--json`, help, and how a
//! failure reads. Nothing here connects to a database.

use std::process::{Command, Output};

use tempfile::TempDir;

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Variables to set on one run.
type Env<'a> = &'a [(&'a str, &'a str)];

fn run(args: &[&str], env: Env) -> Output {
	let dir = TempDir::new().expect("tempdir");
	let mut command = Command::new(env!("CARGO_BIN_EXE_surrealkit"));
	command.current_dir(dir.path()).args(args).env_remove("SURREAL_KIT_JSON");
	command.env_remove("SURREAL_INVOKED_AS");
	for (key, value) in env {
		command.env(key, value);
	}
	command.output().expect("run surrealkit")
}

fn text(bytes: &[u8]) -> String {
	String::from_utf8_lossy(bytes).into_owned()
}

fn json(bytes: &[u8]) -> serde_json::Value {
	serde_json::from_slice(bytes)
		.unwrap_or_else(|err| panic!("not one JSON document ({err}): {}", text(bytes)))
}

fn line() -> String {
	format!("{VERSION} for {} on {}\n", std::env::consts::OS, std::env::consts::ARCH)
}

/// `-v`, `--version` and the older `-V` print one line, the release first,
/// which is what the launcher reads an install's version from.
#[test]
fn the_version_is_one_line_led_by_the_release() {
	for flag in ["--version", "-v", "-V"] {
		let output = run(&[flag], &[]);
		assert!(output.status.success(), "{flag}");
		assert_eq!(text(&output.stdout), line(), "{flag}");
		assert!(output.stderr.is_empty(), "{flag}: {}", text(&output.stderr));
	}
}

/// With `--json`, before or after the flag, or the variable, it is one
/// document, keys in order.
#[test]
fn the_version_under_json_is_arch_os_version() {
	let cases: [(&[&str], Env); 5] = [
		(&["--version", "--json"], &[]),
		(&["--json", "--version"], &[]),
		(&["-vj"], &[]),
		(&["-j", "-V"], &[]),
		(&["--version"], &[("SURREAL_KIT_JSON", "1")]),
	];
	for (args, env) in cases {
		let output = run(args, env);
		assert!(output.status.success(), "{args:?}");
		let document = json(&output.stdout);
		let keys: Vec<&str> =
			document.as_object().expect("an object").keys().map(String::as_str).collect();
		assert_eq!(keys, ["arch", "os", "version"], "{args:?}");
		assert_eq!(document["version"], VERSION);
	}
}

/// `-v` is the version, and no longer `--verbose`.
#[test]
fn verbose_is_long_only() {
	let output = run(&["-v", "status"], &[]);
	assert!(output.status.success());
	assert_eq!(text(&output.stdout), line());
}

/// Help is a short description, the usage, then the commands: no banner.
#[test]
fn help_leads_with_the_description() {
	let output = run(&["-h"], &[]);
	assert!(output.status.success());
	let help = text(&output.stdout);
	assert!(
		help.starts_with("Schema migrations, seed data, typegen and tests\n\nUsage: surrealkit "),
		"{help}"
	);
	assert!(help.contains("-v, --version  Print version"), "{help}");
	assert!(help.contains("-j, --json"), "{help}");
	assert!(help.contains("Emit machine-readable JSON on stdout"), "{help}");
	assert!(!help.contains("-V"), "{help}");
}

/// No command prints help on stderr and exits 2.
#[test]
fn no_command_prints_help() {
	let output = run(&[], &[]);
	assert_eq!(output.status.code(), Some(2));
	assert!(output.stdout.is_empty());
	assert!(text(&output.stderr).contains("Usage: surrealkit "));
}

/// Under the launcher, help names the command as the user typed it.
#[test]
fn help_names_the_command_as_the_launcher_ran_it() {
	let output = run(&["rollout", "--help"], &[("SURREAL_INVOKED_AS", "surreal kit")]);
	assert!(output.status.success());
	assert!(
		text(&output.stdout).contains("Usage: surreal kit rollout "),
		"{}",
		text(&output.stdout)
	);
}

/// A usage error under JSON is the envelope on stderr, naming the failing
/// command's help, with nothing on stdout and exit code 2.
#[test]
fn a_usage_error_under_json_is_the_envelope() {
	let output = run(&["--json", "apply", "a.surql", "--bogus"], &[]);
	assert_eq!(output.status.code(), Some(2));
	assert!(output.stdout.is_empty(), "{}", text(&output.stdout));
	let envelope = json(&output.stderr);
	assert_eq!(envelope["kind"], "usage");
	assert_eq!(envelope["message"], "unexpected argument '--bogus' found");
	assert_eq!(envelope["command"], "surrealkit apply --help");
	assert_eq!(envelope["exit_code"], 2);
	assert_eq!(envelope["status"], serde_json::Value::Null);
}

/// Without JSON a usage error is clap's text.
#[test]
fn a_usage_error_is_clap_text() {
	let output = run(&["apply", "a.surql", "--bogus"], &[]);
	assert_eq!(output.status.code(), Some(2));
	assert!(output.stdout.is_empty());
	assert!(text(&output.stderr).starts_with("error: unexpected argument '--bogus' found"));
}

/// A command's failure is `error: <message>` and exit code 1, or under JSON
/// the envelope, with nothing on stdout either way.
#[test]
fn a_failure_is_one_line_or_the_envelope() {
	let output = run(&["--var", "NOEQUALS", "status"], &[]);
	assert_eq!(output.status.code(), Some(1));
	assert!(output.stdout.is_empty(), "{}", text(&output.stdout));
	assert_eq!(
		text(&output.stderr),
		"error: --var 'NOEQUALS' is missing '=' (expected KEY=VALUE)\n"
	);

	for (args, env) in [
		(&["--json", "--var", "NOEQUALS", "status"][..], &[][..]),
		(&["--var", "NOEQUALS", "status"], &[("SURREAL_KIT_JSON", "true")]),
	] {
		let output = run(args, env);
		assert_eq!(output.status.code(), Some(1), "{args:?}");
		assert!(output.stdout.is_empty(), "{}", text(&output.stdout));
		let envelope = json(&output.stderr);
		assert_eq!(envelope["kind"], "unknown");
		assert_eq!(envelope["message"], "--var 'NOEQUALS' is missing '=' (expected KEY=VALUE)");
		assert_eq!(envelope["exit_code"], 1);
	}
}

/// Under JSON stdout carries the command's document and nothing else, so
/// progress moves to stderr. `init` has no document: stdout stays empty.
#[test]
fn progress_moves_to_stderr_under_json() {
	let dir = TempDir::new().expect("tempdir");
	let init = |args: &[&str]| {
		Command::new(env!("CARGO_BIN_EXE_surrealkit"))
			.current_dir(dir.path())
			.args(args)
			.env_remove("SURREAL_KIT_JSON")
			.output()
			.expect("run surrealkit")
	};

	let output = init(&["--json", "init", "--minimal", "--force"]);
	assert!(output.status.success(), "{}", text(&output.stderr));
	assert!(output.stdout.is_empty(), "{}", text(&output.stdout));
	assert!(!output.stderr.is_empty());

	let output = init(&["init", "--minimal", "--force"]);
	assert!(output.status.success(), "{}", text(&output.stderr));
	assert!(!output.stdout.is_empty(), "progress stays on stdout without --json");
}
