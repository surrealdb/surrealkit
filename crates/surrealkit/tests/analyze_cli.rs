//! `surrealkit check` / `generate` against a project on disk: the CLI's exit
//! codes and output contract, without a database.

use std::fs;
use std::path::Path;
use std::process::Command;

use tempfile::TempDir;

fn write_project(root: &Path, query: &str, config: &str) {
	fs::create_dir_all(root.join("database/schema")).expect("schema dir");
	fs::create_dir_all(root.join("src")).expect("src dir");
	fs::write(root.join("surrealkit.toml"), config).expect("config");
	fs::write(
		root.join("database/schema/001_person.surql"),
		"DEFINE TABLE person SCHEMAFULL;\nDEFINE FIELD name ON person TYPE string;\n",
	)
	.expect("schema");
	fs::write(root.join("src/app.ts"), format!("const rows = await db.query(\"{query}\");\n"))
		.expect("host source");
}

fn surrealkit(root: &Path, args: &[&str]) -> std::process::Output {
	Command::new(env!("CARGO_BIN_EXE_surrealkit"))
		.current_dir(root)
		.args(args)
		.env("NO_COLOR", "1")
		.output()
		.expect("run surrealkit")
}

#[test]
fn check_reports_an_embedded_query_error_at_the_host_file_and_exits_non_zero() {
	let dir = TempDir::new().expect("tempdir");
	write_project(dir.path(), "SELECT nope FROM person", "");

	let output = surrealkit(dir.path(), &["check", "--json"]);
	assert!(!output.status.success(), "an unknown field is an error");
	let json: serde_json::Value =
		serde_json::from_slice(&output.stdout).expect("stdout is the JSON document");
	assert_eq!(json["summary"]["errors"], 1, "{json}");
	assert_eq!(json["diagnostics"][0]["code"], "E1002");
	assert!(
		json["diagnostics"][0]["source"].as_str().expect("source").ends_with("src/app.ts"),
		"the finding lands on the host file: {json}"
	);
}

#[test]
fn check_passes_a_valid_project_and_the_module_layout_supplies_the_schema() {
	let dir = TempDir::new().expect("tempdir");
	write_project(dir.path(), "SELECT name FROM person", "");

	let output = surrealkit(dir.path(), &["check"]);
	let stdout = String::from_utf8_lossy(&output.stdout);
	assert!(output.status.success(), "clean project: {stdout}");
	assert!(stdout.contains("0 errors"), "{stdout}");
}

#[test]
fn generate_writes_the_registry_where_the_analyze_section_says() {
	let dir = TempDir::new().expect("tempdir");
	write_project(
		dir.path(),
		"SELECT name FROM person",
		"[analyze]\nout = \"src/db.generated.ts\"\n",
	);

	let output = surrealkit(dir.path(), &["generate"]);
	assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stdout));
	let written = fs::read_to_string(dir.path().join("src/db.generated.ts")).expect("registry");
	assert!(written.contains("SELECT name FROM person"), "{written}");
}

#[test]
fn a_configured_target_version_turns_on_the_version_checks() {
	let dir = TempDir::new().expect("tempdir");
	write_project(
		dir.path(),
		"SELECT name FROM person",
		"[analyze]\nsurrealdb_version = \"3.2\"\n",
	);
	fs::write(
		dir.path().join("database/schema/002_legacy.surql"),
		"DEFINE FIELD v ON person TYPE array<float>;\nDEFINE INDEX i ON person FIELDS v MTREE DIMENSION 4;\n",
	)
	.expect("legacy index");

	let output = surrealkit(dir.path(), &["check", "--json"]);
	let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("json");
	let codes: Vec<&str> = json["diagnostics"]
		.as_array()
		.expect("array")
		.iter()
		.filter_map(|d| d["code"].as_str())
		.collect();
	assert!(codes.contains(&"E8002"), "MTREE is removed on a 3.x target: {codes:?}");
}
