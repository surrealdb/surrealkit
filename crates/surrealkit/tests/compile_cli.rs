//! `surrealkit compile` and `surrealkit typegen --from`, through the binary.
//!
//! Neither command connects to anything, so these run without a server. The environment is
//! deliberately hostile: `SURREALDB_HOST` points at a closed port and a removed `DATABASE_*`
//! variable is set, which makes every connecting command fail before it starts. Compile must
//! not notice.
#![cfg(feature = "kv-mem")]

use std::fs;
use std::path::Path;
use std::process::Command;

fn surrealkit(dir: &Path, args: &[&str]) -> (Option<i32>, String) {
	let output = Command::new(env!("CARGO_BIN_EXE_surrealkit"))
		.current_dir(dir)
		.env("SURREALDB_HOST", "http://127.0.0.1:1")
		.env("SURREALDB_NAMESPACE", "nope")
		.env("DATABASE_HOST", "http://127.0.0.1:1")
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

fn write(path: &Path, text: &str) {
	fs::create_dir_all(path.parent().expect("parent")).expect("dir");
	fs::write(path, text).expect("write");
}

/// Two modules where order matters: a FLEXIBLE field fails unless its table already exists,
/// and `billing` sorts before `core`, so only `depends_on` gets them right.
fn write_project(root: &Path, depends: bool) {
	write(
		&root.join("database/modules/core/schema/001_core.surql"),
		"DEFINE TABLE account SCHEMAFULL;\nDEFINE FIELD name ON account TYPE string;\n",
	);
	write(
		&root.join("database/modules/billing/schema/001_billing.surql"),
		"DEFINE FIELD meta ON account TYPE object FLEXIBLE;\nDEFINE FUNCTION fn::total($a: int) -> int { RETURN $a; };\n",
	);
	let deps = if depends {
		"depends_on = [\"core\"]\n"
	} else {
		""
	};
	write(
		&root.join("surrealkit.toml"),
		&format!(
			"[schema.core]\n[schema.billing]\n{deps}\n[typegen]\nschema_version = 2\njson = \"out/schema.json\"\n"
		),
	);
}

#[test]
fn compile_applies_modules_in_dependency_order_without_connecting() {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	write_project(tmp.path(), true);
	let (code, text) = surrealkit(tmp.path(), &["compile"]);
	assert_eq!(code, Some(0), "{text}");
	let json = fs::read_to_string(tmp.path().join("out/schema.json")).expect("schema.json");
	let doc: serde_json::Value = serde_json::from_str(&json).expect("json");
	assert_eq!(doc["version"], 2);
	assert_eq!(doc["provenance"]["engine"]["mode"], "embedded");
	assert_eq!(doc["provenance"]["source"]["kind"], "compile");
	assert_eq!(doc["provenance"]["source"]["modules"], serde_json::json!(["billing", "core"]));
	assert_eq!(
		doc["provenance"]["source"]["files"][0]["path"],
		"modules/billing/schema/001_billing.surql"
	);
	assert_eq!(doc["provenance"]["capabilities"]["network"], false);
	let account =
		doc["tables"].as_array().unwrap().iter().find(|t| t["name"] == "account").unwrap();
	assert_eq!(account["fields"].as_array().unwrap().len(), 2);
	assert_eq!(doc["functions"][0]["name"], "fn::total");

	// Without the dependency, `billing` is applied first and its FLEXIBLE field fails.
	let tmp = tempfile::TempDir::new().expect("tempdir");
	write_project(tmp.path(), false);
	let (code, text) = surrealkit(tmp.path(), &["compile"]);
	assert_eq!(code, Some(1), "{text}");
	assert!(text.contains("001_billing.surql"), "{text}");
}

#[test]
fn compile_check_reports_drift_and_writes_nothing() {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	write_project(tmp.path(), true);
	let json = tmp.path().join("out/schema.json");
	let (code, text) = surrealkit(tmp.path(), &["compile", "--check"]);
	assert_eq!(code, Some(1), "{text}");
	assert!(text.contains("missing"), "{text}");
	assert!(!json.exists());

	let (code, text) = surrealkit(tmp.path(), &["compile"]);
	assert_eq!(code, Some(0), "{text}");
	let first = fs::read(&json).expect("written");
	let (code, text) = surrealkit(tmp.path(), &["compile", "--check"]);
	assert_eq!(code, Some(0), "{text}");
	let (code, text) = surrealkit(tmp.path(), &["compile"]);
	assert_eq!(code, Some(0), "{text}");
	assert_eq!(fs::read(&json).expect("rewritten"), first, "byte-for-byte repeatable");

	write(
		&tmp.path().join("database/modules/core/schema/002_more.surql"),
		"DEFINE TABLE invoice SCHEMAFULL;\n",
	);
	let (code, text) = surrealkit(tmp.path(), &["compile", "--check"]);
	assert_eq!(code, Some(1), "{text}");
	assert!(text.contains("stale:"), "{text}");
	assert_eq!(fs::read(&json).expect("untouched"), first);
}

#[test]
fn compile_selects_modules_and_writes_typescript_when_configured() {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	write_project(tmp.path(), true);
	write(
		&tmp.path().join("surrealkit.toml"),
		"[schema.core]\n[schema.billing]\ndepends_on = [\"core\"]\n\n[typegen]\nschema_version = 2\njson = \
		 \"out/schema.json\"\ntypescript = \"out/types\"\n",
	);
	// `--schema core` compiles core alone; billing depends on core, not the reverse.
	let (code, text) = surrealkit(tmp.path(), &["--schema", "core", "compile", "--stdout"]);
	assert_eq!(code, Some(0), "{text}");
	assert!(text.contains("\"version\": 2"), "{text}");
	assert!(!text.contains("fn::total"), "billing was not selected: {text}");
	let ts = fs::read_to_string(tmp.path().join("out/types/index.ts")).expect("index.ts");
	assert!(ts.contains("export interface Account"), "{ts}");
}

#[test]
fn compile_refuses_what_sync_refuses_and_unknown_capabilities() {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	write(&tmp.path().join("database/schema/001.surql"), "CREATE account SET name = 'x';\n");
	let (code, text) = surrealkit(tmp.path(), &["compile"]);
	assert_eq!(code, Some(1), "{text}");
	assert!(text.contains("non-DEFINE"), "{text}");

	let tmp = tempfile::TempDir::new().expect("tempdir");
	write(&tmp.path().join("database/schema/001.surql"), "DEFINE TABLE a;\n");
	let (code, text) = surrealkit(tmp.path(), &["compile", "--allow-experimental", "warp"]);
	assert_eq!(code, Some(1), "{text}");
	assert!(text.contains("unknown experimental capability"), "{text}");

	let tmp = tempfile::TempDir::new().expect("tempdir");
	fs::create_dir_all(tmp.path().join("database/schema")).expect("dir");
	let (code, text) = surrealkit(tmp.path(), &["compile"]);
	assert_eq!(code, Some(1), "{text}");
	assert!(text.contains("no .surql files"), "{text}");
}

#[test]
fn compile_takes_experimental_capabilities_from_config_and_flag() {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	write(
		&tmp.path().join("database/schema/001.surql"),
		"DEFINE BUCKET avatars BACKEND \"memory\";\nDEFINE TABLE a SCHEMAFULL;\nDEFINE FIELD f ON a TYPE option<file<avatars>>;\n",
	);
	let (code, text) = surrealkit(tmp.path(), &["compile", "--stdout"]);
	assert_eq!(code, Some(1), "DEFINE BUCKET needs the files capability: {text}");
	let (code, text) =
		surrealkit(tmp.path(), &["compile", "--stdout", "--allow-experimental", "files"]);
	assert_eq!(code, Some(0), "{text}");
	assert!(text.contains("\"avatars\""), "{text}");
	write(&tmp.path().join("surrealkit.toml"), "[typegen]\nexperimental = [\"files\"]\n");
	let (code, text) = surrealkit(tmp.path(), &["compile", "--stdout"]);
	assert_eq!(code, Some(0), "{text}");
}

/// `[typegen.emit.<name>]` from `surrealkit.toml`: compile runs the emitter over the
/// document it writes, and `compile --check` covers the emitter's files.
#[cfg(unix)]
#[test]
fn compile_runs_configured_emitters() {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	write_project(tmp.path(), true);
	let script = tmp.path().join("emit.sh");
	write(
		&script,
		"#!/bin/sh\ncat > /dev/null\nprintf '{\"protocol\":1,\"files\":[{\"path\":\"tables.txt\",\"contents\":\"%s\"}]}' \"$1\"\n",
	);
	let mut perms = fs::metadata(&script).unwrap().permissions();
	std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o755);
	fs::set_permissions(&script, perms).unwrap();
	write(
		&tmp.path().join("surrealkit.toml"),
		&format!(
			"[schema.core]\n[schema.billing]\ndepends_on = [\"core\"]\n\n[typegen]\nschema_version = 2\njson = \
			 \"out/schema.json\"\n\n[typegen.emit.demo]\ncommand = [\"sh\", {:?}, \"hello world\"]\nout = \
			 \"out/demo\"\n",
			script.to_string_lossy()
		),
	);
	let (code, text) = surrealkit(tmp.path(), &["compile"]);
	assert_eq!(code, Some(0), "{text}");
	assert_eq!(fs::read_to_string(tmp.path().join("out/demo/tables.txt")).unwrap(), "hello world");
	assert!(tmp.path().join("out/demo/.surrealkit-emit.json").is_file());
	let (code, text) = surrealkit(tmp.path(), &["compile", "--check"]);
	assert_eq!(code, Some(0), "{text}");
	fs::write(tmp.path().join("out/demo/tables.txt"), "edited").unwrap();
	let (code, text) = surrealkit(tmp.path(), &["compile", "--check"]);
	assert_eq!(code, Some(1), "{text}");
	assert!(text.contains("tables.txt"), "{text}");
}

#[test]
fn typegen_from_reads_a_document_and_refuses_an_edited_one() {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	write_project(tmp.path(), true);
	let (code, text) = surrealkit(tmp.path(), &["compile"]);
	assert_eq!(code, Some(0), "{text}");
	let source = tmp.path().join("out/schema.json");

	// Rewrites the same document, byte for byte, without connecting.
	let (code, text) = surrealkit(
		tmp.path(),
		&["typegen", "--from", "out/schema.json", "--out", "copy/schema.json"],
	);
	assert_eq!(code, Some(0), "{text}");
	assert_eq!(
		fs::read(tmp.path().join("copy/schema.json")).expect("copy"),
		fs::read(&source).expect("source")
	);
	let (code, text) = surrealkit(
		tmp.path(),
		&["typegen", "--from", "out/schema.json", "--out", "copy/schema.json", "--check"],
	);
	assert_eq!(code, Some(0), "{text}");

	// The TypeScript interfaces come from v1 only.
	let (code, text) = surrealkit(
		tmp.path(),
		&[
			"typegen",
			"--from",
			"out/schema.json",
			"--out",
			"copy/schema.json",
			"--typescript",
			"ts",
		],
	);
	assert_eq!(code, Some(1), "{text}");
	assert!(text.contains("schema document v1"), "{text}");

	// A hand edit no longer matches the digest.
	let edited = fs::read_to_string(&source).unwrap().replacen("\"account\"", "\"accounts\"", 1);
	fs::write(tmp.path().join("edited.json"), edited).unwrap();
	let (code, text) =
		surrealkit(tmp.path(), &["typegen", "--from", "edited.json", "--out", "x.json"]);
	assert_eq!(code, Some(1), "{text}");
	assert!(text.contains("semanticDigest"), "{text}");

	// Not a schema document at all.
	fs::write(tmp.path().join("other.json"), "{\"version\": 7}").unwrap();
	let (code, text) = surrealkit(tmp.path(), &["typegen", "--from", "other.json"]);
	assert_eq!(code, Some(1), "{text}");
	assert!(text.contains("not a schema document"), "{text}");
}

#[test]
fn typegen_from_a_v1_document_reproduces_the_v1_outputs() {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	write(
		&tmp.path().join("database/schema/001.surql"),
		"DEFINE TABLE a SCHEMAFULL;\nDEFINE FIELD n ON a TYPE int;\n",
	);
	write(
		&tmp.path().join("surrealkit.toml"),
		"[typegen]\ntypescript = \"live/types\"\njson = \"live/schema.json\"\n",
	);
	// A live v1 document: compile writes v2, so use typegen against an embedded engine that
	// sync populated.
	let output = Command::new(env!("CARGO_BIN_EXE_surrealkit"))
		.current_dir(tmp.path())
		.args(["--host", "mem://", "sync"])
		.output()
		.expect("sync");
	assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
	let live_ts = fs::read_to_string(tmp.path().join("live/types/index.ts")).expect("live ts");
	let (code, text) = surrealkit(
		tmp.path(),
		&[
			"typegen",
			"--from",
			"live/schema.json",
			"--out",
			"from/schema.json",
			"--typescript",
			"from/types",
		],
	);
	assert_eq!(code, Some(0), "{text}");
	assert_eq!(fs::read_to_string(tmp.path().join("from/types/index.ts")).unwrap(), live_ts);
	assert_eq!(
		fs::read(tmp.path().join("from/schema.json")).unwrap(),
		fs::read(tmp.path().join("live/schema.json")).unwrap()
	);
}

/// Template variables come from `surrealkit.toml` and `--var`, never from `SURREALKIT_VAR_*`
/// in the environment, so the same checkout compiles to the same document anywhere.
#[test]
fn compile_ignores_template_variables_from_the_environment() {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	write(
		&tmp.path().join("database/schema/001_app.surql"),
		"DEFINE TABLE t SCHEMAFULL COMMENT '${NOTE}';\n",
	);
	write(
		&tmp.path().join("surrealkit.toml"),
		"[variables]\nnote = \"from_toml\"\n\n[typegen]\nschema_version = 2\njson = \"out/schema.json\"\n",
	);
	let compile = |args: &[&str]| {
		let output = Command::new(env!("CARGO_BIN_EXE_surrealkit"))
			.current_dir(tmp.path())
			.env("SURREALKIT_VAR_NOTE", "from_env")
			.args(args)
			.output()
			.expect("run surrealkit");
		assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
		let json = fs::read_to_string(tmp.path().join("out/schema.json")).expect("schema.json");
		let doc: serde_json::Value = serde_json::from_str(&json).expect("json");
		doc["tables"][0]["comment"].as_str().expect("a comment").to_string()
	};
	assert_eq!(compile(&["compile"]), "from_toml");
	assert_eq!(compile(&["compile", "--var", "NOTE=from_flag"]), "from_flag");
}

/// `sync` and `typegen` write `[typegen] json` in the configured version (1 unless set), so
/// compile, which writes v2, refuses that path until the project says v2; --out and --stdout
/// write elsewhere.
#[test]
fn compile_writes_the_configured_json_only_as_v2() {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	write_project(tmp.path(), true);
	write(
		&tmp.path().join("surrealkit.toml"),
		"[schema.core]\n[schema.billing]\ndepends_on = [\"core\"]\n\n[typegen]\njson = \
		 \"out/schema.json\"\n",
	);
	let (code, text) = surrealkit(tmp.path(), &["compile"]);
	assert_eq!(code, Some(1), "{text}");
	assert!(text.contains("schema_version = 2"), "{text}");
	assert!(!tmp.path().join("out/schema.json").exists());
	let (code, text) = surrealkit(tmp.path(), &["compile", "--stdout"]);
	assert_eq!(code, Some(0), "{text}");
	let (code, text) = surrealkit(tmp.path(), &["compile", "--out", "v2/schema.json"]);
	assert_eq!(code, Some(0), "{text}");
	assert!(tmp.path().join("v2/schema.json").exists());
}

/// `[typegen] engine` names the minor the project deploys to; a different minor than the
/// embedded engine's is an error unless acknowledged.
#[test]
fn compile_reports_a_different_engine_minor() {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	write(&tmp.path().join("database/schema/001_app.surql"), "DEFINE TABLE t;\n");
	write(&tmp.path().join("surrealkit.toml"), "[typegen]\nengine = \"3.1\"\n");
	let (code, text) = surrealkit(tmp.path(), &["compile"]);
	assert_eq!(code, Some(1), "{text}");
	assert!(text.contains("compile-engine-mismatch"), "{text}");
	write(&tmp.path().join("surrealkit.toml"), "[typegen]\nengine = \"three\"\n");
	let (code, text) = surrealkit(tmp.path(), &["compile"]);
	assert_eq!(code, Some(1), "{text}");
	assert!(text.contains("not an engine minor"), "{text}");
}

/// A broken emitter is never a green `--check`: not through `compile --lenient`, whose
/// leniency covers diagnostics only, and not through a non-strict `typegen --from`.
#[test]
fn check_fails_when_an_emitter_cannot_run() {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	write(&tmp.path().join("database/schema/001_app.surql"), "DEFINE TABLE t;\n");
	write(
		&tmp.path().join("surrealkit.toml"),
		"[typegen]\nschema_version = 2\njson = \"out/schema.json\"\n",
	);
	let (code, text) = surrealkit(tmp.path(), &["compile"]);
	assert_eq!(code, Some(0), "{text}");
	write(
		&tmp.path().join("surrealkit.toml"),
		"[typegen]\nschema_version = 2\njson = \"out/schema.json\"\n\n[typegen.emit.broken]\ncommand = \"false\"\nout = \"gen\"\n",
	);
	for args in [
		&["compile", "--lenient", "--check"][..],
		&["compile", "--check"][..],
		&["typegen", "--from", "out/schema.json", "--check"][..],
	] {
		let (code, text) = surrealkit(tmp.path(), args);
		assert_eq!(code, Some(1), "{args:?}: {text}");
		assert!(text.contains("[typegen.emit.broken]"), "{args:?}: {text}");
		assert!(!text.contains("up to date"), "{args:?}: {text}");
		assert!(!tmp.path().join("gen").exists(), "{args:?}");
	}
	// Writing, too: compile --lenient does not skip a broken emitter.
	let (code, text) = surrealkit(tmp.path(), &["compile", "--lenient"]);
	assert_eq!(code, Some(1), "{text}");
}

/// `[typegen] acknowledge` lets compile pass an error code it names, with a reason; an empty
/// reason is refused.
#[test]
fn compile_passes_acknowledged_error_codes() {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	write(
		&tmp.path().join("database/schema/001_app.surql"),
		"DEFINE PARAM $started VALUE time::now();\n",
	);
	write(
		&tmp.path().join("surrealkit.toml"),
		"[typegen]\nschema_version = 2\njson = \"out/schema.json\"\n",
	);
	let (code, text) = surrealkit(tmp.path(), &["compile"]);
	assert_eq!(code, Some(1), "{text}");
	assert!(text.contains("param-value-nondeterministic"), "{text}");
	assert!(text.contains("[typegen] acknowledge"), "{text}");

	write(
		&tmp.path().join("surrealkit.toml"),
		"[typegen]\nschema_version = 2\njson = \"out/schema.json\"\nacknowledge = { \"param-value-nondeterministic\" = \"\" }\n",
	);
	let (code, text) = surrealkit(tmp.path(), &["compile"]);
	assert_eq!(code, Some(1), "{text}");
	assert!(text.contains("has no reason"), "{text}");

	write(
		&tmp.path().join("surrealkit.toml"),
		"[typegen]\nschema_version = 2\njson = \"out/schema.json\"\nacknowledge = { \"param-value-nondeterministic\" = \"the start time is informational\" }\n",
	);
	let (code, text) = surrealkit(tmp.path(), &["compile"]);
	assert_eq!(code, Some(0), "{text}");
	// The acknowledged diagnostic stays in the document.
	let json = fs::read_to_string(tmp.path().join("out/schema.json")).expect("schema.json");
	assert!(json.contains("param-value-nondeterministic"), "{json}");
}
