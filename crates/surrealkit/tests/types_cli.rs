//! `surrealkit types`, through the binary.
//!
//! An embedded engine that cannot type queries (SurrealDB before 4.1) makes the command's answer
//! the clear "4.1 or later" error; one that can makes it the query's types. Each test asks the
//! engine which it is and asserts that answer. Without `--target` it connects to nothing: the
//! environment is hostile on purpose, as in `compile_cli`.
#![cfg(feature = "kv-mem")]

use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

struct Run {
	code: Option<i32>,
	stdout: String,
	stderr: String,
}

fn surrealkit(dir: &Path, args: &[&str], stdin: Option<&str>) -> Run {
	let mut child = Command::new(env!("CARGO_BIN_EXE_surrealkit"))
		.current_dir(dir)
		.env("SURREALDB_HOST", "http://127.0.0.1:1")
		.env("SURREALDB_NAMESPACE", "nope")
		.env("DATABASE_HOST", "http://127.0.0.1:1")
		.args(args)
		.stdin(if stdin.is_some() {
			Stdio::piped()
		} else {
			Stdio::null()
		})
		.stdout(Stdio::piped())
		.stderr(Stdio::piped())
		.spawn()
		.expect("run surrealkit");
	if let Some(text) = stdin {
		child.stdin.take().expect("stdin").write_all(text.as_bytes()).expect("write stdin");
	}
	let output = child.wait_with_output().expect("wait");
	Run {
		code: output.status.code(),
		stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
		stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
	}
}

fn project() -> tempfile::TempDir {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	let schema = tmp.path().join("database/schema/001.surql");
	fs::create_dir_all(schema.parent().expect("parent")).expect("dir");
	fs::write(
		&schema,
		"DEFINE TABLE person SCHEMAFULL;\nDEFINE FIELD name ON person TYPE string;\n",
	)
	.expect("schema");
	tmp
}

const CANNOT: &str = "the engine cannot type queries (SurrealDB 4.1 or later is needed)";

/// Whether the embedded engine, the `surrealdb` this build links, types queries: the same
/// question `types` asks it, through the same seam.
fn the_embedded_engine_types_queries() -> bool {
	let runtime =
		tokio::runtime::Builder::new_current_thread().enable_all().build().expect("runtime");
	runtime.block_on(async {
		let db = surrealdb::engine::any::connect("mem://").await.expect("embedded engine");
		db.use_ns("probe").use_db("probe").await.expect("namespace");
		surrealkit::codegen::engine::types_document(&db, &[(None, "RETURN 1".to_string())])
			.await
			.expect("ask the engine")
			.is_some()
	})
}

/// The run got as far as the engine: the clear error on one that cannot type queries, the
/// TypeScript snippet on one that can.
fn reached_the_engine(run: &Run, can_type: bool) {
	if can_type {
		assert_eq!(run.code, Some(0), "{}", run.stderr);
		assert!(run.stdout.contains("export type "), "{}", run.stdout);
	} else {
		assert_eq!(run.code, Some(1), "{}", run.stderr);
		assert!(run.stdout.is_empty(), "{}", run.stdout);
		assert!(run.stderr.contains(&format!("error: {CANNOT}")), "{}", run.stderr);
	}
}

#[test]
fn the_engine_that_cannot_type_is_a_clear_error() {
	let tmp = project();
	let run = surrealkit(tmp.path(), &["types", "SELECT name FROM person"], None);
	reached_the_engine(&run, the_embedded_engine_types_queries());
}

#[test]
fn json_failures_are_the_error_envelope() {
	let tmp = project();
	let run = surrealkit(tmp.path(), &["--json", "types", "SELECT name FROM person"], None);
	if the_embedded_engine_types_queries() {
		// The report is the document on stdout.
		assert_eq!(run.code, Some(0), "{}", run.stderr);
		let report: serde_json::Value = serde_json::from_str(&run.stdout).expect("a report");
		assert_eq!(report["text"], "SELECT name FROM person");
		assert!(report["statements"].as_array().is_some_and(|s| !s.is_empty()), "{report}");
	} else {
		assert_eq!(run.code, Some(1), "{}", run.stderr);
		assert!(run.stdout.is_empty(), "{}", run.stdout);
		let envelope: serde_json::Value = serde_json::from_str(&run.stderr).expect("an envelope");
		assert_eq!(envelope["message"], CANNOT);
		assert_eq!(envelope["exit_code"], 1);
	}
}

#[test]
fn the_query_is_read_from_stdin() {
	let tmp = project();
	let can_type = the_embedded_engine_types_queries();
	for args in [&["types", "-"][..], &["types", "--name", "People", "-"][..]] {
		let run = surrealkit(tmp.path(), args, Some("SELECT name FROM person"));
		reached_the_engine(&run, can_type);
	}
	// No argument and nothing piped (stdin closed): an empty query, refused before the engine.
	let run = surrealkit(tmp.path(), &["types"], Some("  \n"));
	assert_eq!(run.code, Some(1), "{}", run.stderr);
	assert!(run.stderr.contains("the query is empty"), "{}", run.stderr);
}

#[test]
fn the_flags_are_checked_before_the_engine_is() {
	let tmp = project();
	let cases: [(&[&str], &str); 3] = [
		(&["types", "--param", "min", "RETURN $min"], "NAME=KIND"),
		(&["types", "--param", "min=array<", "RETURN $min"], "is not a SurrealQL kind"),
		(&["types", "--name", "2fast", "RETURN 1"], "not an identifier"),
	];
	for (args, wanted) in cases {
		let run = surrealkit(tmp.path(), args, None);
		assert_eq!(run.code, Some(1), "{args:?}: {}", run.stderr);
		assert!(run.stderr.contains(wanted), "{args:?}: {}", run.stderr);
	}
	// Valid flags get as far as the engine.
	let run = surrealkit(tmp.path(), &["types", "--param", "min=int", "RETURN $min"], None);
	reached_the_engine(&run, the_embedded_engine_types_queries());
}

#[test]
fn a_project_without_schema_files_is_refused_as_compile_refuses_it() {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	fs::create_dir_all(tmp.path().join("database/schema")).expect("dir");
	let run = surrealkit(tmp.path(), &["types", "RETURN 1"], None);
	assert_eq!(run.code, Some(1), "{}", run.stderr);
	assert!(run.stderr.contains("no .surql files"), "{}", run.stderr);
}

#[test]
fn a_target_is_a_live_database_and_is_connected_to() {
	let tmp = project();
	fs::write(
		tmp.path().join("surrealkit.toml"),
		"[target.dev]\nhost = \"ws://127.0.0.1:1\"\nns = \"a\"\ndb = \"b\"\nuser = \"root\"\n",
	)
	.expect("config");
	let run = surrealkit(tmp.path(), &["types", "-t", "dev", "RETURN 1"], None);
	assert_eq!(run.code, Some(1), "{}", run.stderr);
	assert!(!run.stderr.contains(CANNOT), "the closed port was never reached: {}", run.stderr);
}
