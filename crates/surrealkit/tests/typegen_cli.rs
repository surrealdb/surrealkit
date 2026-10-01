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

#[test]
fn typegen_offers_a_typescript_flag() {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	let (_, help) = surrealkit(tmp.path(), &["typegen", "--help"]);
	assert!(help.contains("--typescript <PATH>"), "{help}");
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
