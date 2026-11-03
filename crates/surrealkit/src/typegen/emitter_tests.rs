//! External emitters end to end: `typegen`, `typegen --from`, `compile` and `sync` run them
//! over schema document v2 with the same atomic, check and strict semantics as SurrealKit's
//! own outputs. The emitters here are POSIX shell scripts, so these tests are Unix-only, like
//! the formatter tests.
#![cfg(unix)]

use std::path::{Path, PathBuf};

use super::{Emitter, SchemaVersion, TypegenOpts, run_typegen};

/// Write an executable `sh` script.
fn script(dir: &Path, name: &str, body: &str) -> String {
	let path = dir.join(name);
	std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
	let mut perms = std::fs::metadata(&path).unwrap().permissions();
	std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o755);
	std::fs::set_permissions(&path, perms).unwrap();
	path.to_string_lossy().into_owned()
}

/// An emitter that records its input beside its `out`, counts its runs, and returns a file
/// holding the document's digest plus one nested file.
fn digest_emitter(dir: &Path) -> String {
	script(
		dir,
		"digest.sh",
		r#"cat > "$SURREALKIT_EMIT_OUT.input.json"
echo run >> "$SURREALKIT_EMIT_OUT.runs"
digest=$(grep -o '"semanticDigest": "[^"]*"' "$SURREALKIT_EMIT_OUT.input.json" | head -1 | cut -d'"' -f4)
printf '{"protocol":%s,"files":[{"path":"digest.txt","contents":"%s\\n"},{"path":"nested/name.txt","contents":"%s"}]}' "$SURREALKIT_EMIT_PROTOCOL" "$digest" "$SURREALKIT_EMIT_NAME""#,
	)
}

/// An emitter running the script `command` through `sh`: executing a file this process just
/// wrote can fail with ETXTBSY while another test thread forks.
fn emitter(name: &str, command: &str, out: PathBuf) -> Emitter {
	Emitter {
		name: name.to_string(),
		command: vec!["sh".to_string(), command.to_string()],
		out,
	}
}

async fn db() -> surrealdb::Surreal<surrealdb::engine::any::Any> {
	let db = crate::test_db::fresh("typegen_emitters").await;
	db.query("DEFINE TABLE person SCHEMAFULL; DEFINE FIELD name ON person TYPE string;")
		.await
		.and_then(|r| r.check())
		.expect("schema");
	db
}

fn opts(tmp: &Path, emitters: Vec<Emitter>, strict: bool, check: bool) -> TypegenOpts {
	TypegenOpts {
		out: Some(tmp.join("schema.json")),
		pretty: true,
		strict,
		check,
		omit_generated_at: true,
		schema_version: SchemaVersion::V2,
		emitters,
		..TypegenOpts::default()
	}
}

fn read(path: &Path) -> String {
	std::fs::read_to_string(path).unwrap_or_else(|err| panic!("{}: {err}", path.display()))
}

#[tokio::test]
async fn typegen_runs_an_emitter_publishes_atomically_and_checks() {
	let db = db().await;
	let tmp = tempfile::TempDir::new().unwrap();
	let out = tmp.path().join("gen");
	let e = emitter("effect", &digest_emitter(tmp.path()), out.clone());

	run_typegen(&db, "./database", opts(tmp.path(), vec![e.clone()], true, false))
		.await
		.expect("first run");
	// The emitter got exactly the document SurrealKit wrote.
	assert_eq!(read(&tmp.path().join("gen.input.json")), read(&tmp.path().join("schema.json")));
	let doc: serde_json::Value =
		serde_json::from_str(&read(&tmp.path().join("schema.json"))).unwrap();
	assert_eq!(read(&out.join("digest.txt")).trim(), doc["semanticDigest"].as_str().unwrap());
	assert_eq!(read(&out.join("nested/name.txt")), "effect");
	let manifest: serde_json::Value =
		serde_json::from_str(&read(&out.join(super::EMITTER_MANIFEST))).unwrap();
	assert_eq!(manifest["files"], serde_json::json!(["digest.txt", "nested/name.txt"]));
	assert_eq!(manifest["emitter"], "effect");

	// Unchanged output is not rewritten, and --check passes.
	let modified = std::fs::metadata(out.join("digest.txt")).unwrap().modified().unwrap();
	run_typegen(&db, "./database", opts(tmp.path(), vec![e.clone()], true, false))
		.await
		.expect("again");
	assert_eq!(std::fs::metadata(out.join("digest.txt")).unwrap().modified().unwrap(), modified);
	run_typegen(&db, "./database", opts(tmp.path(), vec![e.clone()], true, true))
		.await
		.expect("up to date");

	// --check: stale and extra files are reported, and nothing is written.
	std::fs::write(out.join("digest.txt"), "edited\n").unwrap();
	std::fs::write(out.join("handwritten.ts"), "x\n").unwrap();
	let err = run_typegen(&db, "./database", opts(tmp.path(), vec![e.clone()], true, true))
		.await
		.unwrap_err()
		.to_string();
	assert!(err.contains("stale:") && err.contains("digest.txt"), "{err}");
	assert!(err.contains("extra:") && err.contains("handwritten.ts"), "{err}");
	assert_eq!(read(&out.join("digest.txt")), "edited\n");

	// Publishing next to a file SurrealKit did not write fails in strict mode, before
	// writing anything; leniently it is a warning and the file is kept.
	let err = run_typegen(&db, "./database", opts(tmp.path(), vec![e.clone()], true, false))
		.await
		.unwrap_err()
		.to_string();
	assert!(err.contains("handwritten.ts"), "{err}");
	assert_eq!(read(&out.join("digest.txt")), "edited\n", "a strict failure writes nothing");
	run_typegen(&db, "./database", opts(tmp.path(), vec![e.clone()], false, false))
		.await
		.expect("lenient");
	assert!(out.join("handwritten.ts").exists());
	assert_ne!(read(&out.join("digest.txt")), "edited\n");
}

#[tokio::test]
async fn files_an_emitter_stops_producing_are_removed() {
	let db = db().await;
	let tmp = tempfile::TempDir::new().unwrap();
	let out = tmp.path().join("gen");
	let two = script(
		tmp.path(),
		"two.sh",
		r#"cat > /dev/null; printf '{"protocol":1,"files":[{"path":"a.ts","contents":"a"},{"path":"deep/b.ts","contents":"b"}]}'"#,
	);
	let one = script(
		tmp.path(),
		"one.sh",
		r#"cat > /dev/null; printf '{"protocol":1,"files":[{"path":"a.ts","contents":"a"}]}'"#,
	);
	run_typegen(
		&db,
		"./database",
		opts(tmp.path(), vec![emitter("e", &two, out.clone())], true, false),
	)
	.await
	.unwrap();
	assert!(out.join("deep/b.ts").exists());
	run_typegen(
		&db,
		"./database",
		opts(tmp.path(), vec![emitter("e", &one, out.clone())], true, false),
	)
	.await
	.unwrap();
	assert!(out.join("a.ts").exists());
	assert!(
		!out.join("deep/b.ts").exists() && !out.join("deep").exists(),
		"stale file and its empty directory removed"
	);
}

#[tokio::test]
async fn a_failing_emitter_fails_strict_runs_and_is_skipped_otherwise() {
	let db = db().await;
	let tmp = tempfile::TempDir::new().unwrap();
	let out = tmp.path().join("gen");
	let cases = [
		("exit", "cat > /dev/null; echo boom >&2; exit 3", "exited with"),
		("garbage", "cat > /dev/null; echo not json", "not an emitter reply"),
		("protocol", r#"cat > /dev/null; printf '{"protocol":2,"files":[]}'"#, "protocol 2"),
		(
			"escape",
			r#"cat > /dev/null; printf '{"protocol":1,"files":[{"path":"../x","contents":""}]}'"#,
			"../x",
		),
		(
			"twice",
			r#"cat > /dev/null; printf '{"protocol":1,"files":[{"path":"a","contents":""},{"path":"a","contents":""}]}'"#,
			"twice",
		),
		(
			"unknown",
			r#"cat > /dev/null; printf '{"protocol":1,"files":[],"extra":1}'"#,
			"not an emitter reply",
		),
		(
			"diagnostic",
			r#"cat > /dev/null; printf '{"protocol":1,"files":[],"diagnostics":[{"severity":"error","code":"value-unresolved","source":"x","reason":"no"}]}'"#,
			"error diagnostic",
		),
	];
	for (name, body, expect) in cases {
		let command = script(tmp.path(), &format!("{name}.sh"), body);
		let e = emitter(name, &command, out.clone());
		let json = tmp.path().join("schema.json");
		let _ = std::fs::remove_file(&json);
		let _ = std::fs::remove_dir_all(&out);
		let err = run_typegen(&db, "./database", opts(tmp.path(), vec![e.clone()], true, false))
			.await
			.unwrap_err();
		let text = format!("{err:#}");
		assert!(text.contains(expect), "{name}: {text}");
		assert!(text.contains(&format!("[typegen.emit.{name}]")), "{name}: {text}");
		assert!(!json.exists(), "{name}: a strict failure writes nothing");
		// Lenient: logged, the emitter's directory untouched, everything else written.
		run_typegen(&db, "./database", opts(tmp.path(), vec![e], false, false))
			.await
			.unwrap_or_else(|err| panic!("{name} lenient: {err:#}"));
		assert!(json.exists(), "{name}");
		if name == "diagnostic" {
			// Leniently an error diagnostic is logged and the (empty) output published.
			assert!(out.join(super::EMITTER_MANIFEST).exists(), "{name}");
		} else {
			assert!(!out.exists(), "{name}: nothing published for a failed emitter");
		}
	}
}

#[tokio::test]
async fn an_emitter_never_overwrites_a_file_it_did_not_write() {
	let db = db().await;
	let tmp = tempfile::TempDir::new().unwrap();
	let out = tmp.path().join("gen");
	std::fs::create_dir_all(&out).unwrap();
	std::fs::write(out.join("digest.txt"), "mine\n").unwrap();
	let e = emitter("effect", &digest_emitter(tmp.path()), out.clone());
	for strict in [true, false] {
		let err = run_typegen(&db, "./database", opts(tmp.path(), vec![e.clone()], strict, false))
			.await
			.unwrap_err()
			.to_string();
		assert!(err.contains("would overwrite") && err.contains("digest.txt"), "{err}");
		assert_eq!(read(&out.join("digest.txt")), "mine\n");
	}
}

#[tokio::test]
async fn emitters_get_v2_even_when_the_json_is_v1() {
	let db = db().await;
	let tmp = tempfile::TempDir::new().unwrap();
	let out = tmp.path().join("gen");
	let e = emitter("effect", &digest_emitter(tmp.path()), out.clone());
	let mut o = opts(tmp.path(), vec![e], true, false);
	o.schema_version = SchemaVersion::V1;
	run_typegen(&db, "./database", o).await.unwrap();
	assert!(read(&tmp.path().join("schema.json")).contains("\"version\": 1"));
	assert!(read(&out.join("digest.txt")).starts_with("sha256:"));
	assert!(read(&tmp.path().join("gen.input.json")).contains("\"version\": 2"));
}

#[tokio::test]
async fn typegen_from_runs_emitters_over_a_v2_document_only() {
	let db = db().await;
	let tmp = tempfile::TempDir::new().unwrap();
	run_typegen(&db, "./database", opts(tmp.path(), vec![], true, false)).await.unwrap();
	let out = tmp.path().join("gen");
	let e = emitter("effect", &digest_emitter(tmp.path()), out.clone());
	let mut o = opts(tmp.path(), vec![e.clone()], true, false);
	o.out = Some(tmp.path().join("copy.json"));
	super::run_typegen_from(&tmp.path().join("schema.json"), "./database", o.clone())
		.expect("from v2");
	assert!(read(&out.join("digest.txt")).starts_with("sha256:"));
	super::run_typegen_from(
		&tmp.path().join("schema.json"),
		"./database",
		TypegenOpts {
			check: true,
			..o
		},
	)
	.expect("from v2, checked");

	let mut v1 = opts(tmp.path(), vec![], true, false);
	v1.schema_version = SchemaVersion::V1;
	v1.out = Some(tmp.path().join("v1.json"));
	run_typegen(&db, "./database", v1).await.unwrap();
	let mut o = opts(tmp.path(), vec![e], true, false);
	o.out = Some(tmp.path().join("copy1.json"));
	let err = super::run_typegen_from(&tmp.path().join("v1.json"), "./database", o).unwrap_err();
	assert!(err.to_string().contains("schema document v2"), "{err}");
}

#[cfg(feature = "kv-mem")]
#[tokio::test]
async fn compile_runs_emitters() {
	use crate::compile::{CompileModule, CompileOpts, compile};
	let tmp = tempfile::TempDir::new().unwrap();
	let out = tmp.path().join("gen");
	let module = CompileModule {
		name: "default".into(),
		files: vec![crate::schema_state::SchemaFile {
			path: "schema/a.surql".into(),
			sql: "DEFINE TABLE a SCHEMAFULL;".into(),
			hash: String::new(),
		}],
	};
	let compiled = compile(&[module], &CompileOpts::default()).await.unwrap();
	let e = emitter("effect", &digest_emitter(tmp.path()), out.clone());
	super::produce(
		&compiled.v1,
		Some(&compiled.document),
		"./database",
		&opts(tmp.path(), vec![e], true, false),
	)
	.unwrap();
	assert_eq!(read(&out.join("digest.txt")).trim(), compiled.document.semantic_digest);
}

/// `sync` (and so every `sync --watch` iteration) runs the emitters after a change, and on
/// the first sync before they have published; an idle sync does not run them.
#[tokio::test]
async fn sync_runs_emitters_after_changes_only() {
	let tmp = tempfile::TempDir::new().unwrap();
	let folder = tmp.path().join("database");
	std::fs::create_dir_all(folder.join("schema")).unwrap();
	std::fs::write(folder.join("schema/a.surql"), "DEFINE TABLE a SCHEMAFULL;\n").unwrap();
	let out = tmp.path().join("gen");
	let db = crate::test_db::fresh("sync_emitters").await;
	let sync_opts = crate::sync::SyncOpts {
		fail_fast: true,
		prune: true,
		folder: folder.to_string_lossy().into_owned(),
		module: crate::module::Module::default_module(),
		typegen_strict: true,
		typegen_emitters: vec![emitter("effect", &digest_emitter(tmp.path()), out.clone())],
		..crate::sync::SyncOpts::default()
	};
	let runs = || read(&tmp.path().join("gen.runs")).lines().count();
	crate::sync::run_sync(&db, sync_opts.clone()).await.expect("first sync");
	assert!(read(&out.join("digest.txt")).starts_with("sha256:"));
	assert_eq!(runs(), 1);
	crate::sync::run_sync(&db, sync_opts.clone()).await.expect("idle sync");
	assert_eq!(runs(), 1, "no change, no emitter run");
	let before = read(&out.join("digest.txt"));
	std::fs::write(folder.join("schema/b.surql"), "DEFINE TABLE b SCHEMAFULL;\n").unwrap();
	crate::sync::run_sync(&db, sync_opts).await.expect("changed sync");
	assert_eq!(runs(), 2);
	assert_ne!(read(&out.join("digest.txt")), before, "the new table changed the digest");
}

/// `--check` cannot vouch for files an emitter did not produce: a failing emitter fails the
/// check whether or not the run is strict, and nothing is written.
#[tokio::test]
async fn check_fails_when_an_emitter_cannot_run_even_without_strict() {
	let db = db().await;
	let tmp = tempfile::TempDir::new().unwrap();
	let out = tmp.path().join("gen");
	run_typegen(&db, "./database", opts(tmp.path(), vec![], true, false)).await.expect("json");
	let broken =
		emitter("broken", &script(tmp.path(), "broken.sh", "cat > /dev/null; exit 1"), out.clone());
	for strict in [false, true] {
		let err =
			run_typegen(&db, "./database", opts(tmp.path(), vec![broken.clone()], strict, true))
				.await
				.unwrap_err();
		let text = format!("{err:#}");
		assert!(text.contains("[typegen.emit.broken]"), "strict={strict}: {text}");
		assert!(!out.exists(), "strict={strict}: --check writes nothing");
	}
	let err = run_typegen(&db, "./database", opts(tmp.path(), vec![broken], false, true))
		.await
		.unwrap_err()
		.to_string();
	assert!(err.contains("failed:  [typegen.emit.broken]"), "{err}");
}

/// `compile` tolerates diagnostics with `--lenient` but never a broken emitter.
#[tokio::test]
async fn fatal_emitter_failures_fail_a_lenient_run() {
	let db = db().await;
	let tmp = tempfile::TempDir::new().unwrap();
	let broken = emitter(
		"broken",
		&script(tmp.path(), "broken.sh", "cat > /dev/null; exit 1"),
		tmp.path().join("gen"),
	);
	let mut o = opts(tmp.path(), vec![broken], false, false);
	o.emitter_failures_fatal = true;
	let err = run_typegen(&db, "./database", o.clone()).await.unwrap_err();
	assert!(format!("{err:#}").contains("[typegen.emit.broken]"), "{err:#}");
	assert!(!tmp.path().join("schema.json").exists(), "nothing written");

	// An emitter's error diagnostic is a diagnostic: tolerated leniently.
	let diagnostic = emitter(
		"diagnostic",
		&script(
			tmp.path(),
			"diagnostic.sh",
			r#"cat > /dev/null; printf '{"protocol":1,"files":[],"diagnostics":[{"severity":"error","code":"value-unresolved","source":"x","reason":"no"}]}'"#,
		),
		tmp.path().join("gen"),
	);
	o.emitters = vec![diagnostic];
	run_typegen(&db, "./database", o).await.expect("lenient about diagnostics");
}

/// `[typegen] acknowledge` lets a strict run pass an acknowledged error code, from the
/// document or from an emitter, and emitters receive the list.
#[tokio::test]
async fn acknowledged_codes_pass_a_strict_run_and_reach_the_emitters() {
	let db = db().await;
	let tmp = tempfile::TempDir::new().unwrap();
	let out = tmp.path().join("gen");
	let body = r#"cat > /dev/null; printf '%s' "$SURREALKIT_EMIT_ACKNOWLEDGE" > "$SURREALKIT_EMIT_OUT.ack"
printf '{"protocol":1,"files":[],"diagnostics":[{"severity":"error","code":"value-unresolved","source":"x","reason":"no"}]}'"#;
	let e = emitter("ack", &script(tmp.path(), "ack.sh", body), out.clone());
	let err = run_typegen(&db, "./database", opts(tmp.path(), vec![e.clone()], true, false))
		.await
		.unwrap_err();
	assert!(format!("{err:#}").contains("error diagnostic"), "{err:#}");
	assert_eq!(read(&tmp.path().join("gen.ack")), "{}");

	let mut o = opts(tmp.path(), vec![e], true, false);
	o.acknowledge.insert("value-unresolved".into(), "reviewed by hand".into());
	run_typegen(&db, "./database", o).await.expect("acknowledged");
	assert_eq!(read(&tmp.path().join("gen.ack")), r#"{"value-unresolved":"reviewed by hand"}"#);
	assert!(out.join(super::EMITTER_MANIFEST).exists());
}
