//! The `typegen` command's TypeScript output options (#75), through the binary.

use std::fs;
use std::process::Command;

fn surrealkit(dir: &std::path::Path, args: &[&str]) -> (bool, String) {
	let output = Command::new(env!("CARGO_BIN_EXE_surrealkit"))
		.current_dir(dir)
		.args(["--host", "http://127.0.0.1:1"])
		.args(args)
		.output()
		.expect("run surrealkit");
	let text = format!(
		"{}{}",
		String::from_utf8_lossy(&output.stdout),
		String::from_utf8_lossy(&output.stderr)
	);
	(output.status.success(), text)
}

/// Run the binary against its embedded in-memory engine (a fresh, empty database
/// per invocation). Returns the exit code and the combined output.
fn surrealkit_mem(dir: &std::path::Path, args: &[&str]) -> (Option<i32>, String) {
	let output = Command::new(env!("CARGO_BIN_EXE_surrealkit"))
		.current_dir(dir)
		.args(["--host", "mem://"])
		.args(args)
		.output()
		.expect("run surrealkit");
	let text = format!(
		"{}{}",
		String::from_utf8_lossy(&output.stdout),
		String::from_utf8_lossy(&output.stderr)
	);
	(output.status.code(), text)
}

#[test]
fn typegen_check_exits_non_zero_on_drift_and_writes_nothing() {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	fs::write(tmp.path().join("surrealkit.toml"), "[typegen]\ntypescript = \"types\"\n")
		.expect("config");
	let json = tmp.path().join("database/types/schema.json");
	let ts = tmp.path().join("types/index.ts");

	let (code, text) = surrealkit_mem(tmp.path(), &["typegen", "--check"]);
	assert_eq!(code, Some(1), "{text}");
	assert!(text.contains("missing"), "{text}");
	assert!(!json.exists() && !ts.exists(), "--check must not write");

	let (code, text) = surrealkit_mem(tmp.path(), &["typegen"]);
	assert_eq!(code, Some(0), "{text}");
	let (code, text) = surrealkit_mem(tmp.path(), &["typegen", "--check"]);
	assert_eq!(code, Some(0), "an unchanged schema is up to date despite generatedAt: {text}");

	fs::write(&ts, "// edited\n").expect("edit");
	let (code, text) = surrealkit_mem(tmp.path(), &["typegen", "--check"]);
	assert_eq!(code, Some(1), "{text}");
	assert!(text.contains("stale:") && text.contains("index.ts"), "{text}");
	assert_eq!(fs::read_to_string(&ts).unwrap(), "// edited\n");
}

#[test]
fn typegen_without_generated_at_is_byte_for_byte_repeatable() {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	let json = tmp.path().join("database/types/schema.json");
	let (code, text) = surrealkit_mem(tmp.path(), &["typegen", "--no-generated-at"]);
	assert_eq!(code, Some(0), "{text}");
	let first = fs::read(&json).expect("json written");
	assert!(!String::from_utf8_lossy(&first).contains("generatedAt"));

	fs::write(tmp.path().join("surrealkit.toml"), "[typegen]\ngenerated_at = false\n")
		.expect("config");
	let (code, text) = surrealkit_mem(tmp.path(), &["typegen"]);
	assert_eq!(code, Some(0), "{text}");
	assert!(text.contains("is up to date"), "unchanged bytes are not rewritten: {text}");
	assert_eq!(fs::read(&json).unwrap(), first);
}

#[test]
fn typegen_check_and_stdout_are_exclusive() {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	let (ok, text) = surrealkit(tmp.path(), &["typegen", "--check", "--stdout"]);
	assert!(!ok);
	assert!(text.contains("cannot be used with"), "{text}");
}

#[test]
fn typegen_offers_a_typescript_flag() {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	let (_, help) = surrealkit(tmp.path(), &["typegen", "--help"]);
	assert!(help.contains("--typescript <PATH>"), "{help}");
}

#[test]
fn a_misspelt_typegen_key_fails_before_connecting() {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	fs::write(tmp.path().join("surrealkit.toml"), "[typegen]\ntypescrpt = \"types\"\n")
		.expect("config");
	let (ok, text) = surrealkit(tmp.path(), &["typegen"]);
	assert!(!ok);
	assert!(text.contains("unknown field `typescrpt`"), "{text}");
	assert!(!text.contains("127.0.0.1:1"), "the config error must come before connecting: {text}");
}

#[test]
fn typegen_connects_to_the_selected_target_not_the_ambient_one() {
	// The ambient connection is 127.0.0.1:1 (see `surrealkit`); each target names
	// its own unreachable port, so the connect error shows which one was used.
	let tmp = tempfile::TempDir::new().expect("tempdir");
	fs::write(
		tmp.path().join("surrealkit.toml"),
		"[target.acme]\nhost = \"http://127.0.0.1:2\"\n\n\
		 [target.globex]\nhost = \"http://127.0.0.1:3\"\nprimary = true\n",
	)
	.expect("config");

	let (ok, text) =
		surrealkit(tmp.path(), &["--connect-timeout-secs", "5", "typegen", "--target", "acme"]);
	assert!(!ok);
	assert!(text.contains("127.0.0.1:2"), "--target must pick the database: {text}");

	let (ok, text) = surrealkit(tmp.path(), &["--connect-timeout-secs", "5", "typegen"]);
	assert!(!ok);
	assert!(text.contains("127.0.0.1:3"), "the primary target is the default: {text}");

	let (ok, text) = surrealkit(tmp.path(), &["typegen", "--all"]);
	assert!(!ok);
	assert!(text.contains("one database target at a time"), "{text}");
}

#[test]
fn a_contradictory_typegen_config_fails_before_connecting() {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	fs::write(
		tmp.path().join("surrealkit.toml"),
		"[typegen]\ntypescript = \"types/database.ts\"\nfilename = \"other.ts\"\n",
	)
	.expect("config");
	let (ok, text) = surrealkit(tmp.path(), &["typegen"]);
	assert!(!ok);
	assert!(text.contains("already names a file"), "{text}");
	assert!(!text.contains("127.0.0.1:1"), "the config error must come before connecting: {text}");

	// A file on the command line overrides the configured name instead of
	// clashing with it.
	fs::write(
		tmp.path().join("surrealkit.toml"),
		"[typegen]\ntypescript = \"types\"\nfilename = \"other.ts\"\n",
	)
	.expect("config");
	let (ok, text) = surrealkit(tmp.path(), &["typegen", "--typescript", "types/database.ts"]);
	assert!(!ok);
	assert!(text.contains("127.0.0.1:1"), "expected to get as far as connecting: {text}");
}
