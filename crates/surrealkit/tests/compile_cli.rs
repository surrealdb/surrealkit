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

/// A 4.0 schema compiles under strict (the default): sized numbers carry their width, the other
/// 4.0 kinds have nodes of their own, and `types` is absent without a `DEFINE TYPE`. The
/// TypeScript interfaces (v1) read the kinds with no v1 form as `unknown`.
#[test]
fn compile_reads_the_four_oh_kinds() {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	write(
		&tmp.path().join("database/schema/001.surql"),
		"DEFINE TABLE t SCHEMAFULL;\nDEFINE FIELD n ON t TYPE i32;\nDEFINE FIELD d ON t TYPE \
		 option<date>;\nDEFINE FIELD s ON t TYPE enum<'b', 'a c'>;\n",
	);
	write(
		&tmp.path().join("surrealkit.toml"),
		"[typegen]\nschema_version = 2\njson = \"out/schema.json\"\ntypescript = \"out/types\"\n",
	);
	let (code, text) = surrealkit(tmp.path(), &["compile"]);
	assert_eq!(code, Some(0), "{text}");
	let json = fs::read_to_string(tmp.path().join("out/schema.json")).expect("schema.json");
	let doc: serde_json::Value = serde_json::from_str(&json).expect("json");
	let fields = doc["tables"][0]["fields"].as_array().expect("fields");
	let field = |name: &str| fields.iter().find(|f| f["path"][0] == name).expect(name).clone();
	assert_eq!(field("n")["kind"], serde_json::json!({"kind": "int", "width": 32}));
	assert_eq!(field("d")["kind"], serde_json::json!({"kind": "date"}));
	assert_eq!(field("d")["optional"], true);
	assert_eq!(field("s")["kind"], serde_json::json!({"kind": "enum", "labels": ["b", "a c"]}));
	assert_eq!(doc["diagnostics"], serde_json::json!([]));
	assert!(doc.get("types").is_none());
	let ts = fs::read_to_string(tmp.path().join("out/types/index.ts")).expect("index.ts");
	assert!(ts.contains("export interface T"), "{ts}");
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
	let digest = "0".repeat(64);
	write(
		&tmp.path().join("database/schema/001.surql"),
		"DEFINE BUCKET avatars BACKEND \"memory\";\nDEFINE TABLE a SCHEMAFULL;\nDEFINE FIELD f ON a TYPE option<file<avatars>>;\n",
	);
	// 4.0 gates no bucket: it compiles with no capability, and `files` is accepted and changes
	// nothing.
	let (code, text) = surrealkit(tmp.path(), &["compile", "--stdout"]);
	assert_eq!(code, Some(0), "DEFINE BUCKET needs no capability: {text}");
	assert!(text.contains("\"avatars\""), "{text}");
	let (code, text) =
		surrealkit(tmp.path(), &["compile", "--stdout", "--allow-experimental", "files"]);
	assert_eq!(code, Some(0), "{text}");

	write(
		&tmp.path().join("database/schema/002.surql"),
		&format!(
			"DEFINE MODULE mod::demo FROM f\"avatars:/demo.wasm\" UNSIGNED DIGEST 'sha256:{digest}';\n"
		),
	);
	let (code, text) = surrealkit(tmp.path(), &["compile", "--stdout"]);
	assert_eq!(code, Some(1), "DEFINE MODULE needs the surrealism capability: {text}");
	let (code, text) =
		surrealkit(tmp.path(), &["compile", "--stdout", "--allow-experimental", "surrealism"]);
	assert_eq!(code, Some(0), "{text}");
	assert!(text.contains("\"mod::demo\""), "{text}");
	write(&tmp.path().join("surrealkit.toml"), "[typegen]\nexperimental = [\"surrealism\"]\n");
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

#[test]
fn generate_is_an_alias_of_compile() {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	write_project(tmp.path(), true);
	let (code, text) = surrealkit(tmp.path(), &["generate"]);
	assert_eq!(code, Some(0), "{text}");
	let json = fs::read_to_string(tmp.path().join("out/schema.json")).expect("schema.json");
	let doc: serde_json::Value = serde_json::from_str(&json).expect("json");
	assert_eq!(doc["provenance"]["source"]["kind"], "compile");
}

/// A project that records what each emitter reads on stdin: `plain` has not opted in to
/// `queries`, `typed` has.
fn write_queries_project(root: &Path, with_queries: bool) {
	// The emitters record their input beside their `out`, which SurrealKit creates only when
	// it publishes.
	fs::create_dir_all(root.join("out")).expect("out dir");
	write(
		&root.join("database/schema/001_person.surql"),
		"DEFINE TABLE person SCHEMAFULL;\nDEFINE FIELD name ON person TYPE string;\n",
	);
	let script = root.join("record.sh");
	write(
		&script,
		"#!/bin/sh\ncat > \"$SURREALKIT_EMIT_OUT.stdin\"\nprintf '{\"protocol\":1,\"files\":[]}'\n",
	);
	let mut perms = fs::metadata(&script).expect("script metadata").permissions();
	std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o755);
	fs::set_permissions(&script, perms).expect("script permissions");
	let queries = if with_queries {
		"\n[typegen.queries]\nfiles = [\"database/queries\"]\nsources = [\"src\"]\ntypescript = \
		 \"src/queries.ts\"\n"
	} else {
		""
	};
	write(
		&root.join("surrealkit.toml"),
		&format!(
			"[typegen]\nschema_version = 2\njson = \"out/schema.json\"\n{queries}\n[typegen.emit.plain]\ncommand = \
			 [\"sh\", {script:?}]\nout = \"out/plain\"\n\n[typegen.emit.typed]\ncommand = [\"sh\", \
			 {script:?}]\nout = \"out/typed\"\nqueries = true\n",
			script = script.to_string_lossy()
		),
	);
	write(
		&root.join("database/queries/people.surql"),
		"-- name: ListPeople\nSELECT name FROM person;\n",
	);
	write(&root.join("src/app.ts"), "db.query(\"SELECT * FROM person\");\n");
}

/// Whether the embedded engine, the `surrealdb` this build links, types queries: the same
/// question `compile` asks it, through the same seam.
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

#[test]
fn a_query_the_engine_rejects_fails_the_compile_and_leaves_the_others_typed() {
	let can_type = the_embedded_engine_types_queries();
	let dir = tempfile::TempDir::new().expect("tempdir");
	write_queries_project(dir.path(), true);
	// A host file with one query the engine cannot parse between two it can.
	write(
		&dir.path().join("src/routes.ts"),
		"db.query(\"SELECT name FROM person\");\n\
		 db.query(`SELECT * FROM person\nWHERE;`);\n\
		 db.query(\"SELECT count() FROM person GROUP ALL\");\n",
	);
	let (code, text) = surrealkit(dir.path(), &["compile"]);
	if !can_type {
		// Nothing is typed, so nothing fails: the section is absent, with its warning.
		assert_eq!(code, Some(0), "{text}");
		assert!(text.contains("queries-unsupported-engine"), "{text}");
		return;
	}
	// The failure is an error at its statement's start, the literal's first character on line
	// 2 (S0001's span starts where the failing statement does), and a strict compile refuses it.
	assert_ne!(code, Some(0), "{text}");
	assert!(text.contains("query-invalid"), "{text}");
	assert!(text.contains("src/routes.ts:2:11:"), "{text}");

	let (code, text) = surrealkit(dir.path(), &["compile", "--lenient"]);
	assert_eq!(code, Some(0), "{text}");
	let json = fs::read_to_string(dir.path().join("out/schema.json")).expect("schema.json");
	let doc: serde_json::Value = serde_json::from_str(&json).expect("json");
	let queries = doc["queries"].as_array().expect("queries");
	let failed: Vec<&serde_json::Value> =
		queries.iter().filter(|q| q.get("error").is_some()).collect();
	assert_eq!(failed.len(), 1, "{queries:?}");
	assert_eq!(failed[0]["error"]["code"], "S0001");
	assert!(failed[0]["statements"].as_array().is_some_and(Vec::is_empty));
	let typed: Vec<&str> = queries
		.iter()
		.filter(|q| q.get("error").is_none())
		.map(|q| q["key"].as_str().unwrap())
		.collect();
	assert!(typed.contains(&"SELECT name FROM person"), "{typed:?}");
	assert!(typed.contains(&"SELECT count() FROM person GROUP ALL"), "{typed:?}");
	// The registry has the good queries and not the failed one.
	let module = fs::read_to_string(dir.path().join("src/queries.ts")).expect("queries.ts");
	assert!(module.contains("\"SELECT name FROM person\""), "{module}");
	assert!(!module.contains("WHERE;"), "{module}");
}

#[test]
fn queries_on_an_engine_that_cannot_type_them_warn_and_change_nothing_else() {
	// On an engine that can type queries, the same project is typed and not warned about.
	let can_type = the_embedded_engine_types_queries();
	let with = tempfile::TempDir::new().expect("tempdir");
	write_queries_project(with.path(), true);
	let (code, text) = surrealkit(with.path(), &["compile"]);
	assert_eq!(code, Some(0), "{text}");
	let json = fs::read_to_string(with.path().join("out/schema.json")).expect("schema.json");
	let doc: serde_json::Value = serde_json::from_str(&json).expect("json");
	let codes: Vec<&str> = doc["diagnostics"]
		.as_array()
		.unwrap()
		.iter()
		.map(|d| d["code"].as_str().unwrap())
		.collect();
	if can_type {
		let keys: Vec<&str> = doc["queries"]
			.as_array()
			.expect("the engine typed the queries")
			.iter()
			.map(|q| q["key"].as_str().unwrap())
			.collect();
		assert!(keys.contains(&"ListPeople"), "{keys:?}");
		assert!(!codes.contains(&"queries-unsupported-engine"), "{codes:?}");
	} else {
		assert!(doc.get("queries").is_none(), "the engine gave no queries");
		assert!(codes.contains(&"queries-unsupported-engine"), "{codes:?}");
	}
	// The per-query module is written, with an empty registry when nothing was typed.
	assert!(fs::read_to_string(with.path().join("src/queries.ts")).unwrap().contains("Queries"));
	let (code, text) = surrealkit(with.path(), &["compile", "--check"]);
	assert_eq!(code, Some(0), "{text}");

	// The emitter that did not opt in reads exactly what the project reads without the section.
	let without = tempfile::TempDir::new().expect("tempdir");
	write_queries_project(without.path(), false);
	let (code, text) = surrealkit(without.path(), &["compile"]);
	assert_eq!(code, Some(0), "{text}");
	let stdin = |dir: &Path, name: &str| fs::read(dir.join(format!("out/{name}.stdin"))).unwrap();
	assert_eq!(stdin(with.path(), "plain"), stdin(without.path(), "plain"));
	assert_eq!(stdin(with.path(), "typed"), json.as_bytes());
	assert_eq!(
		String::from_utf8(stdin(with.path(), "typed"))
			.unwrap()
			.contains("queries-unsupported-engine"),
		!can_type
	);
	assert!(!String::from_utf8(stdin(with.path(), "plain")).unwrap().contains("queries-"));
}

#[test]
fn a_bad_query_file_fails_a_strict_compile_and_is_logged_by_a_lenient_one() {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	write_queries_project(tmp.path(), true);
	write(&tmp.path().join("database/queries/bad.surql"), "-- name: not-a-name\nRETURN 1;\n");
	let (code, text) = surrealkit(tmp.path(), &["compile"]);
	assert_eq!(code, Some(1), "{text}");
	assert!(text.contains("query-name-invalid"), "{text}");
	let (code, text) = surrealkit(tmp.path(), &["compile", "--lenient"]);
	assert_eq!(code, Some(0), "{text}");

	// A configured directory that is missing fails the command.
	write(
		&tmp.path().join("surrealkit.toml"),
		"[typegen]\nschema_version = 2\njson = \"out/schema.json\"\n[typegen.queries]\nfiles = [\"nowhere\"]\n",
	);
	let (code, text) = surrealkit(tmp.path(), &["compile", "--lenient"]);
	assert_eq!(code, Some(1), "{text}");
	assert!(text.contains("nowhere"), "{text}");
}

/// The stdout of `surrealkit compile --watch-paths` in `dir`: one JSON document and nothing else.
fn watch_paths(dir: &Path, extra: &[&str]) -> serde_json::Value {
	let output = Command::new(env!("CARGO_BIN_EXE_surrealkit"))
		.current_dir(dir)
		.env_clear()
		.env("RUST_MIN_STACK", "8388608")
		.args(["compile", "--watch-paths"])
		.args(extra)
		.output()
		.expect("run surrealkit");
	assert_eq!(output.status.code(), Some(0), "{}", String::from_utf8_lossy(&output.stderr));
	serde_json::from_slice(&output.stdout).expect("stdout is one JSON document")
}

/// The contract the Vite plugin reads: the files a compile reads and writes, from the
/// configuration alone, with no compile.
#[test]
fn watch_paths_prints_the_files_a_compile_reads_and_writes() {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	// Nothing here exists but the configuration: no schema file, no source directory.
	write(
		&tmp.path().join("surrealkit.toml"),
		"[schema.core]\n[schema.auth]\ndepends_on = [\"core\"]\n\n[typegen]\nschema_version = 2\n\
		 json = \"generated/schema.json\"\ntypescript = \"src/generated/types.ts\"\n\n\
		 [typegen.queries]\nfiles = [\"database/queries\"]\nsources = [\"src\", \"app/page.ts\"]\n\
		 typescript = \"src/generated/queries.ts\"\n\n[typegen.emit.surreal]\ncommand = \"true\"\n\
		 out = \"src/generated/surreal\"\n",
	);
	let paths = watch_paths(tmp.path(), &[]);
	assert_eq!(
		paths,
		serde_json::json!({
			"version": 1,
			"schema": ["database/modules/core/schema", "database/modules/auth/schema"],
			"config": ["surrealkit.toml", ".env", ".env.local"],
			"queryFiles": ["database/queries"],
			"querySources": ["src", "app/page.ts"],
			"queryExtensions":
				["ts", "mts", "cts", "tsx", "js", "mjs", "cjs", "jsx", "svelte", "vue", "astro"],
			"queryIgnoredDirectories": [
				"node_modules", "dist", "build", "target", ".svelte-kit", ".next", ".nuxt",
				".astro", ".output"
			],
			"pythonFiles": [],
			"pythonIgnoredDirectories":
				[".venv", "venv", "__pycache__", "site-packages", "node_modules"],
			"outputs": [
				"generated/schema.json",
				"src/generated/types.ts",
				"src/generated/queries.ts",
				"src/generated/surreal"
			],
		})
	);
	// Nothing was compiled or written.
	assert!(!tmp.path().join("generated").exists());

	// `--schema` selects, and `--out` is where the JSON goes.
	let one = watch_paths(tmp.path(), &["--schema", "core", "--out", "elsewhere/doc.json"]);
	assert_eq!(one["schema"], serde_json::json!(["database/modules/core/schema"]));
	assert_eq!(one["outputs"][0], "elsewhere/doc.json");
}

#[test]
fn watch_paths_names_the_python_globs_and_module() {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	write(
		&tmp.path().join("surrealkit.toml"),
		"[typegen]\nschema_version = 2\n\n[typegen.python]\nout = \"app/surreal_queries.py\"\n\
		 files = [\"app/**/*.py\", \"tools/run.py\"]\n",
	);
	let paths = watch_paths(tmp.path(), &[]);
	assert_eq!(paths["pythonFiles"], serde_json::json!(["app/**/*.py", "tools/run.py"]));
	assert!(paths["outputs"].as_array().unwrap().contains(&"app/surreal_queries.py".into()));
	// Python alone sets no TypeScript source.
	assert_eq!(paths["querySources"], serde_json::json!([]));
	assert_eq!(paths["queryExtensions"].as_array().unwrap().iter().any(|e| e == "py"), false);
}

#[test]
fn watch_paths_has_every_array_for_a_project_with_no_configuration() {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	let paths = watch_paths(tmp.path(), &["--folder", "db"]);
	assert_eq!(paths["schema"], serde_json::json!(["db/schema"]));
	assert_eq!(paths["config"], serde_json::json!(["surrealkit.toml", ".env", ".env.local"]));
	assert_eq!(paths["queryFiles"], serde_json::json!([]));
	assert_eq!(paths["querySources"], serde_json::json!([]));
	assert_eq!(paths["outputs"], serde_json::json!(["db/types/schema.json"]));
}

#[test]
fn watch_paths_needs_a_configuration_compile_would_accept() {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	write(&tmp.path().join("surrealkit.toml"), "[typegen]\njson = \"out/schema.json\"\n");
	let (code, text) = surrealkit(tmp.path(), &["compile", "--watch-paths"]);
	assert_eq!(code, Some(1), "{text}");
	assert!(text.contains("schema_version = 2"), "{text}");
}
