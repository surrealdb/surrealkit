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

/// Run the CLI against `root` with a *clean* environment.
///
/// `env_clear` is the point of this helper: these commands resolve the
/// database folder from `SURREALDB_FOLDER`, so a developer with one exported
/// would watch these tests analyze a directory the fixture never wrote. The
/// binary is invoked by absolute path and spawns nothing, so it needs no
/// `PATH`.
fn surrealkit(root: &Path, args: &[&str]) -> std::process::Output {
	Command::new(env!("CARGO_BIN_EXE_surrealkit"))
		.current_dir(root)
		.args(args)
		.env_clear()
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
	let stderr = String::from_utf8_lossy(&output.stderr);
	assert!(output.status.success(), "clean project: {stderr}");
	// Findings and the summary go to stderr, as rustc's do, so redirecting
	// stdout cannot hide an error.
	assert!(stderr.contains("0 errors"), "{stderr}");
	assert!(
		String::from_utf8_lossy(&output.stdout).is_empty(),
		"text mode writes nothing to stdout: {:?}",
		String::from_utf8_lossy(&output.stdout)
	);
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

#[test]
fn check_needs_none_of_the_environment_a_database_command_would() {
	// A declared target whose password lives in an unset environment variable
	// used to fail the whole invocation before dispatch -- so a CI job that
	// only runs `surrealkit check --json` could not run without production
	// credentials it never uses.
	let dir = TempDir::new().expect("tempdir");
	write_project(
		dir.path(),
		"SELECT name FROM person",
		"[target.prod]\nns = \"app\"\ndb = \"prod\"\npass_env = \"PROD_DB_PASSWORD\"\n",
	);

	let output = surrealkit(dir.path(), &["check", "--json"]);
	let stderr = String::from_utf8_lossy(&output.stderr);
	assert!(output.status.success(), "a clean project checks clean: {stderr}");
	let json: serde_json::Value =
		serde_json::from_slice(&output.stdout).expect("stdout is the JSON document");
	assert_eq!(json["summary"]["errors"], 0, "{json}");
	assert!(!stderr.contains("PROD_DB_PASSWORD"), "the secret is never consulted: {stderr}");
}

#[test]
fn a_configured_out_names_the_same_file_from_every_directory() {
	// `[analyze] out` is documented relative to the project root. Resolving it
	// against the working directory instead wrote the registry somewhere else
	// -- or, more often, failed outright.
	let dir = TempDir::new().expect("tempdir");
	write_project(
		dir.path(),
		"SELECT name FROM person",
		"[analyze]\nout = \"src/db.generated.ts\"\n",
	);
	let nested = dir.path().join("src/nested");
	fs::create_dir_all(&nested).expect("nested dir");

	let output = surrealkit(&nested, &["generate"]);
	assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
	let written = fs::read_to_string(dir.path().join("src/db.generated.ts")).expect("registry");
	assert!(written.contains("SELECT name FROM person"), "{written}");
	assert!(
		!nested.join("src/db.generated.ts").exists(),
		"the registry must not land under the working directory"
	);
}

#[test]
fn an_undeclared_schema_module_is_an_error_rather_than_a_whole_project_run() {
	// `--schema` was accepted and then thrown away, so `--schema typo` quietly
	// analyzed everything. It now picks the schema directories the analysis
	// treats as schema, and a name that is not declared says so.
	//
	// What it does *not* do is hide another module's definitions: the analyzer
	// walks the whole project root, and a `DEFINE` in a file classified as a
	// query still reaches the catalog. Narrowing selects which directories are
	// schema (and so are analyzed first), not which files are read.
	let dir = TempDir::new().expect("tempdir");
	write_project(dir.path(), "SELECT name FROM person", "[schema.core]\n[schema.billing]\n");
	for module in ["core", "billing"] {
		fs::create_dir_all(dir.path().join(format!("database/modules/{module}/schema")))
			.expect("module schema dir");
	}

	let core_only = surrealkit(dir.path(), &["check", "--schema", "core"]);
	assert!(
		core_only.status.success(),
		"a declared module is analyzed: {}",
		String::from_utf8_lossy(&core_only.stderr)
	);

	let unknown = surrealkit(dir.path(), &["check", "--schema", "nope"]);
	let stderr = String::from_utf8_lossy(&unknown.stderr);
	assert!(!unknown.status.success(), "an undeclared module is an error");
	assert!(stderr.contains("unknown schema module"), "{stderr}");
	assert!(stderr.contains("billing"), "the error names what is declared: {stderr}");
}

#[test]
fn a_schema_folder_outside_the_project_root_is_named_rather_than_silently_empty() {
	// The analyzer walks the project root and matches its globs against paths
	// under it, so a `--folder` pointing outside contributes no schema at all.
	// Reporting every table as undefined is the worst possible answer.
	let dir = TempDir::new().expect("tempdir");
	write_project(dir.path(), "SELECT name FROM person", "");

	let output = surrealkit(dir.path(), &["check", "--folder", "../elsewhere"]);
	let stderr = String::from_utf8_lossy(&output.stderr);
	assert!(!output.status.success(), "an unreachable schema directory is an error");
	assert!(stderr.contains("outside the project root"), "{stderr}");
	assert!(
		!String::from_utf8_lossy(&output.stdout).contains("E1001"),
		"it must not report the schema's tables as undefined instead"
	);
}

#[test]
fn json_sources_are_project_relative_for_every_kind_of_finding() {
	// Host files reported a bare absolute path and `.surql` files a
	// `file:///abs/...` URL, so a consumer had to know which it was holding,
	// and two machines analyzing the same commit disagreed about every row.
	let dir = TempDir::new().expect("tempdir");
	write_project(dir.path(), "SELECT nope FROM person", "");
	fs::write(dir.path().join("bad.surql"), "SELECT alsonope FROM person;\n").expect("query file");

	let output = surrealkit(dir.path(), &["check", "--json"]);
	let json: serde_json::Value =
		serde_json::from_slice(&output.stdout).expect("stdout is the JSON document");
	let sources: Vec<&str> = json["diagnostics"]
		.as_array()
		.expect("array")
		.iter()
		.filter_map(|d| d["source"].as_str())
		.collect();
	assert!(sources.contains(&"src/app.ts"), "host file: {sources:?}");
	assert!(sources.contains(&"bad.surql"), "surql file: {sources:?}");
	for source in &sources {
		assert!(!source.starts_with("file://"), "no scheme: {source}");
		assert!(!source.starts_with('/'), "no absolute path: {source}");
	}
}

#[test]
fn the_dotenv_beside_surrealkit_toml_decides_the_folder_from_any_directory() {
	// `.env` was looked for in the working directory, so `SURREALDB_FOLDER` in
	// the project's own `.env` was honoured from the root and ignored from a
	// subdirectory -- two different analyses of one project.
	let dir = TempDir::new().expect("tempdir");
	write_project(dir.path(), "SELECT name FROM person", "");
	fs::create_dir_all(dir.path().join("db/schema")).expect("db dir");
	fs::rename(
		dir.path().join("database/schema/001_person.surql"),
		dir.path().join("db/schema/001_person.surql"),
	)
	.expect("move schema");
	fs::write(dir.path().join(".env"), "SURREALDB_FOLDER=./db\n").expect("dotenv");
	let nested = dir.path().join("src/nested");
	fs::create_dir_all(&nested).expect("nested dir");

	for cwd in [dir.path(), nested.as_path()] {
		let output = surrealkit(cwd, &["check"]);
		assert!(
			output.status.success(),
			"the .env-named folder must be found from {cwd:?}: {}",
			String::from_utf8_lossy(&output.stderr)
		);
	}
}

#[test]
fn flags_that_do_not_apply_are_reported_rather_than_ignored() {
	let dir = TempDir::new().expect("tempdir");
	write_project(
		dir.path(),
		"SELECT name FROM person",
		"[target.prod]\nns = \"app\"\ndb = \"prod\"\n",
	);

	let output = surrealkit(dir.path(), &["check", "--target", "prod"]);
	let stderr = String::from_utf8_lossy(&output.stderr);
	assert!(output.status.success(), "{stderr}");
	assert!(stderr.contains("--target/--all has no effect"), "{stderr}");
	assert!(stderr.contains("check"), "the message names the command: {stderr}");
}

#[test]
fn an_ignore_that_would_blank_the_schema_is_refused() {
	// `ignore` entries are directory names matched anywhere in the tree, so
	// `"schema"` skips every module's schema directory and the run reports
	// every table in the project as undefined.
	let dir = TempDir::new().expect("tempdir");
	write_project(dir.path(), "SELECT name FROM person", "[analyze]\nignore = [\"schema\"]\n");

	let output = surrealkit(dir.path(), &["check"]);
	let stderr = String::from_utf8_lossy(&output.stderr);
	assert!(!output.status.success(), "an ignore that blanks the schema is an error");
	assert!(stderr.contains("schema directory"), "{stderr}");
	assert!(!stderr.contains("E1001"), "it must not report the tables as undefined: {stderr}");
}

#[test]
fn a_misspelled_surrealdb_version_is_refused_rather_than_read_as_latest() {
	let dir = TempDir::new().expect("tempdir");
	write_project(
		dir.path(),
		"SELECT name FROM person",
		"[analyze]\nsurrealdb_version = \"3.x\"\n",
	);

	let output = surrealkit(dir.path(), &["check"]);
	let stderr = String::from_utf8_lossy(&output.stderr);
	assert!(!output.status.success(), "a version that parses as nothing is an error");
	assert!(stderr.contains("is not a release"), "{stderr}");
}

#[test]
fn a_run_that_read_nothing_says_so_and_still_exits_zero() {
	// Exit 0 on an empty run reads as "checked, all clear" in CI, which is the
	// most expensive possible way to misconfigure `--folder`.
	let dir = TempDir::new().expect("tempdir");
	fs::write(dir.path().join("surrealkit.toml"), "").expect("config");

	let output = surrealkit(dir.path(), &["check"]);
	let stderr = String::from_utf8_lossy(&output.stderr);
	assert!(output.status.success(), "an empty project is not a failure: {stderr}");
	assert!(stderr.contains("no SurrealQL sources found"), "{stderr}");
	assert!(stderr.contains("database/schema"), "it names where it looked: {stderr}");
}
