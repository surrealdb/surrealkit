use surrealdb::Surreal;
use surrealdb::engine::any::{Any, connect};
use surrealdb::opt::Config;
use surrealdb::opt::capabilities::Capabilities;
use surrealkit::typegen::{
	FieldType, PrimitiveType, TypegenOpts, format_file, generate, render_typescript, run_typegen,
};

async fn mem_db() -> Surreal<Any> {
	let cfg = Config::new().capabilities(Capabilities::all());
	let db = connect(("mem://", cfg)).await.expect("connect mem://");
	db.use_ns("surrealkit_test").use_db("typegen_test").await.expect("use_ns/use_db");
	db
}

/// Define a representative schema covering tables, fields (string/option/array/
/// record/object + dotted sub-field), a function, a param, an analyzer, an
/// index, and an event.
async fn define_schema(db: &Surreal<Any>) {
	let sql = r#"
		DEFINE TABLE user SCHEMAFULL;
		DEFINE FIELD name ON user TYPE string;
		DEFINE FIELD nickname ON user TYPE option<string>;
		DEFINE FIELD tags ON user TYPE array<string>;
		DEFINE FIELD best_friend ON user TYPE record<user>;
		DEFINE FIELD address ON user TYPE object;
		DEFINE FIELD address.city ON user TYPE string;
		DEFINE INDEX user_name ON TABLE user FIELDS name UNIQUE;
		DEFINE EVENT user_created ON TABLE user WHEN $event = "CREATE" THEN {};
		DEFINE FUNCTION fn::greet($name: string, $loud: option<bool>) -> string { RETURN $name; };
		DEFINE PARAM $app_name VALUE "surrealkit";
		DEFINE ANALYZER simple TOKENIZERS blank;
	"#;
	db.query(sql).await.expect("define schema").check().expect("schema ok");
}

#[tokio::test]
async fn generate_captures_all_elements() {
	let db = mem_db().await;
	define_schema(&db).await;

	let doc = generate(&db).await.expect("generate");

	// Table present and SCHEMAFULL.
	let user = doc.tables.iter().find(|t| t.name == "user").expect("user table");
	assert_eq!(user.schemafull, Some(true));

	// Fields are sorted by name; look them up explicitly.
	let field =
		|n: &str| user.fields.iter().find(|f| f.name == n).unwrap_or_else(|| panic!("field {n}"));

	assert_eq!(
		field("name").r#type,
		FieldType::Primitive {
			name: PrimitiveType::String
		}
	);
	assert!(!field("name").optional);

	// option<string> is unwrapped into optional + inner string.
	let nickname = field("nickname");
	assert!(nickname.optional);
	assert_eq!(
		nickname.r#type,
		FieldType::Primitive {
			name: PrimitiveType::String
		}
	);

	// array<string>
	assert_eq!(
		field("tags").r#type,
		FieldType::Array {
			inner: Box::new(FieldType::Primitive {
				name: PrimitiveType::String
			}),
			max: None,
		}
	);

	// record<user>
	assert_eq!(
		field("best_friend").r#type,
		FieldType::Record {
			tables: vec!["user".to_string()]
		}
	);

	// Dotted sub-field path is preserved verbatim.
	assert_eq!(
		field("address.city").r#type,
		FieldType::Primitive {
			name: PrimitiveType::String
		}
	);

	// Index and event present.
	assert!(user.indexes.iter().any(|i| i.name == "user_name"));
	assert!(user.events.iter().any(|e| e.name == "user_created"));

	// Function with args + RETURNS.
	let greet = doc.functions.iter().find(|f| f.name.contains("greet")).expect("greet fn");
	assert_eq!(greet.args.len(), 2);
	assert_eq!(greet.args[0].name, "name");
	assert!(!greet.args[0].optional);
	assert_eq!(greet.args[1].name, "loud");
	assert!(greet.args[1].optional);
	assert_eq!(
		greet.returns,
		Some(FieldType::Primitive {
			name: PrimitiveType::String
		})
	);

	// Param and analyzer present.
	assert!(doc.params.iter().any(|p| p.name == "app_name"));
	assert!(doc.analyzers.iter().any(|a| a.name == "simple"));
}

#[tokio::test]
async fn generate_reports_the_session_namespace_and_database() {
	let db = mem_db().await;
	define_schema(&db).await;

	let doc = generate(&db).await.expect("generate");
	assert_eq!(doc.namespace.as_deref(), Some("surrealkit_test"));
	assert_eq!(doc.database.as_deref(), Some("typegen_test"));
}

#[tokio::test]
async fn run_typegen_writes_valid_json_file() {
	let db = mem_db().await;
	define_schema(&db).await;

	let tmp = tempfile::TempDir::new().expect("temp dir");
	let out = tmp.path().join("schema.json");

	run_typegen(
		&db,
		"./database",
		TypegenOpts {
			out: Some(out.clone()),
			stdout: false,
			pretty: true,
			ts_out: None,
			ts_format: None,
			..TypegenOpts::default()
		},
	)
	.await
	.expect("run_typegen");

	let contents = std::fs::read_to_string(&out).expect("read output");
	let parsed: serde_json::Value = serde_json::from_str(&contents).expect("valid json");
	assert_eq!(parsed["version"], 1);
	assert_eq!(parsed["namespace"], "surrealkit_test");
	assert_eq!(parsed["database"], "typegen_test");
	assert!(parsed["tables"].as_array().expect("tables array").iter().any(|t| t["name"] == "user"));
	assert!(!parsed["generatedAt"].as_str().expect("generatedAt").is_empty());
}

#[tokio::test]
async fn render_typescript_emits_sdk_interfaces() {
	let db = mem_db().await;
	define_schema(&db).await;

	let doc = generate(&db).await.expect("generate");
	let ts = render_typescript(&doc).expect("render ts");

	// PascalCase interface + synthesized typed id.
	assert!(ts.contains("export interface User {"), "got:\n{ts}");
	assert!(ts.contains("id: RecordId<'user'>;"), "got:\n{ts}");
	// Field mappings from the introspected schema.
	assert!(ts.contains("name: string;"), "got:\n{ts}");
	assert!(ts.contains("nickname?: string;"), "got:\n{ts}");
	assert!(ts.contains("tags: string[];"), "got:\n{ts}");
	assert!(ts.contains("best_friend: RecordId<'user'>;"), "got:\n{ts}");
	// Dotted sub-field becomes a nested object property.
	assert!(ts.contains("address: {"), "got:\n{ts}");
	assert!(ts.contains("city: string;"), "got:\n{ts}");
	// SDK import line.
	assert!(ts.contains("import type { RecordId } from 'surrealdb';"), "got:\n{ts}");
}

#[tokio::test]
async fn run_typegen_writes_typescript_file_when_configured() {
	let db = mem_db().await;
	define_schema(&db).await;

	let tmp = tempfile::TempDir::new().expect("temp dir");
	let json_out = tmp.path().join("schema.json");
	let ts_dir = tmp.path().join("types");

	run_typegen(
		&db,
		"./database",
		TypegenOpts {
			out: Some(json_out.clone()),
			stdout: false,
			pretty: true,
			ts_out: Some(ts_dir.clone()),
			ts_format: None,
			..TypegenOpts::default()
		},
	)
	.await
	.expect("run_typegen");

	assert!(json_out.exists(), "json should still be written");
	let ts = std::fs::read_to_string(ts_dir.join("index.ts")).expect("read index.ts");
	assert!(ts.contains("export interface User {"), "got:\n{ts}");
	assert!(ts.contains("id: RecordId<'user'>;"), "got:\n{ts}");
}

#[tokio::test]
async fn run_typegen_writes_to_a_named_typescript_file() {
	// #75: a project that keeps `index.ts` as its own barrel names the generated
	// file instead.
	let db = mem_db().await;
	define_schema(&db).await;
	let tmp = tempfile::TempDir::new().expect("temp dir");
	let ts_file = tmp.path().join("libs/database/types/database.ts");
	run_typegen(
		&db,
		"./database",
		TypegenOpts {
			out: Some(tmp.path().join("schema.json")),
			stdout: false,
			pretty: true,
			ts_out: Some(ts_file.clone()),
			ts_format: None,
			..TypegenOpts::default()
		},
	)
	.await
	.expect("run_typegen");
	let ts = std::fs::read_to_string(&ts_file).expect("read the named file");
	assert!(ts.contains("export interface User {"), "got:\n{ts}");
	assert!(
		!tmp.path().join("libs/database/types/index.ts").exists(),
		"index.ts must be left to the project"
	);
}

#[tokio::test]
async fn run_typegen_to_stdout_still_writes_configured_typescript() {
	// `--stdout` replaces the JSON file only; it used to return before the
	// TypeScript step, leaving configured types stale without a word.
	let db = mem_db().await;
	define_schema(&db).await;
	let tmp = tempfile::TempDir::new().expect("temp dir");
	let folder = tmp.path().join("database");
	let ts_file = tmp.path().join("types/database.ts");
	run_typegen(
		&db,
		folder.to_str().unwrap(),
		TypegenOpts {
			out: None,
			stdout: true,
			pretty: true,
			ts_out: Some(ts_file.clone()),
			ts_format: None,
			..TypegenOpts::default()
		},
	)
	.await
	.expect("run_typegen");
	let ts = std::fs::read_to_string(&ts_file).expect("TypeScript written");
	assert!(ts.contains("export interface User {"), "got:\n{ts}");
	assert!(!folder.join("types/schema.json").exists(), "stdout replaces the JSON file");
}

fn outputs_in(dir: &std::path::Path) -> TypegenOpts {
	TypegenOpts {
		out: Some(dir.join("schema.json")),
		pretty: true,
		ts_out: Some(dir.join("types")),
		..TypegenOpts::default()
	}
}

#[tokio::test]
async fn omitting_generated_at_makes_typegen_reproducible() {
	let db = mem_db().await;
	define_schema(&db).await;
	let tmp = tempfile::TempDir::new().expect("temp dir");
	let opts = TypegenOpts {
		omit_generated_at: true,
		..outputs_in(tmp.path())
	};
	run_typegen(&db, "./database", opts.clone()).await.expect("first run");
	let first = std::fs::read(tmp.path().join("schema.json")).unwrap();
	assert!(!String::from_utf8_lossy(&first).contains("generatedAt"));
	run_typegen(&db, "./database", opts).await.expect("second run");
	assert_eq!(std::fs::read(tmp.path().join("schema.json")).unwrap(), first);
}

#[tokio::test]
async fn check_reports_drift_without_writing() {
	let db = mem_db().await;
	define_schema(&db).await;
	let tmp = tempfile::TempDir::new().expect("temp dir");
	let check = TypegenOpts {
		check: true,
		..outputs_in(tmp.path())
	};

	// Nothing generated yet: both files are missing, and still are afterwards.
	let err = run_typegen(&db, "./database", check.clone()).await.unwrap_err().to_string();
	assert!(err.contains("2 of 2 generated file(s) out of date"), "{err}");
	assert!(!tmp.path().join("schema.json").exists());
	assert!(!tmp.path().join("types").exists());

	// Generated (with the default timestamp), a check passes: it compares content,
	// reusing the timestamp on disk rather than failing on the clock.
	run_typegen(&db, "./database", outputs_in(tmp.path())).await.expect("generate");
	run_typegen(&db, "./database", check.clone()).await.expect("up to date");

	// A schema change makes both stale; a hand edit makes one stale. Neither is
	// rewritten by the check.
	db.query("DEFINE FIELD age ON user TYPE int;").await.unwrap().check().unwrap();
	let err = run_typegen(&db, "./database", check.clone()).await.unwrap_err().to_string();
	assert!(err.contains("stale:"), "{err}");
	assert!(!std::fs::read_to_string(tmp.path().join("types/index.ts")).unwrap().contains("age"));

	run_typegen(&db, "./database", outputs_in(tmp.path())).await.expect("regenerate");
	std::fs::write(tmp.path().join("types/index.ts"), "// hand edited\n").unwrap();
	let err = run_typegen(&db, "./database", check).await.unwrap_err().to_string();
	assert!(err.contains("1 of 2"), "{err}");
	assert!(err.contains("index.ts"), "{err}");
	assert_eq!(
		std::fs::read_to_string(tmp.path().join("types/index.ts")).unwrap(),
		"// hand edited\n"
	);
}

#[tokio::test]
async fn strict_typegen_fails_on_a_broken_formatter_and_writes_nothing() {
	let db = mem_db().await;
	define_schema(&db).await;
	let tmp = tempfile::TempDir::new().expect("temp dir");
	let broken = TypegenOpts {
		ts_format: Some("surrealkit-no-such-formatter-xyz --write".into()),
		..outputs_in(tmp.path())
	};

	// Lenient (the default): a warning, and the unformatted types are written.
	run_typegen(&db, "./database", broken.clone()).await.expect("lenient");
	let ts = std::fs::read_to_string(tmp.path().join("types/index.ts")).unwrap();
	assert!(ts.contains("export interface User {"), "got:\n{ts}");

	// Strict: an error, and neither output is touched.
	std::fs::remove_dir_all(tmp.path().join("types")).unwrap();
	std::fs::remove_file(tmp.path().join("schema.json")).unwrap();
	let err = run_typegen(
		&db,
		"./database",
		TypegenOpts {
			strict: true,
			..broken
		},
	)
	.await
	.unwrap_err();
	assert!(format!("{err:#}").contains("surrealkit-no-such-formatter-xyz"), "{err:#}");
	assert!(!tmp.path().join("schema.json").exists());
	assert!(!tmp.path().join("types/index.ts").exists());
}

#[test]
fn the_typescript_file_follows_the_configured_path() {
	use std::path::Path;
	use surrealkit::typegen::typescript_file;
	assert_eq!(typescript_file(Path::new("types")), Path::new("types/index.ts"));
	assert_eq!(typescript_file(Path::new("types/db.ts")), Path::new("types/db.ts"));
	assert_eq!(typescript_file(Path::new("types/db.mts")), Path::new("types/db.mts"));
	assert_eq!(typescript_file(Path::new("types/v1.2")), Path::new("types/v1.2/index.ts"));
}

#[test]
fn format_file_runs_command_with_path_appended() {
	// Bogus command must not panic and must leave the file untouched.
	let tmp = tempfile::TempDir::new().expect("temp dir");
	let target = tmp.path().join("index.ts");
	std::fs::write(&target, "original\n").expect("write");
	format_file("surrealkit-no-such-formatter-xyz --write", &target);
	assert_eq!(std::fs::read_to_string(&target).unwrap(), "original\n");

	// Empty command is a no-op.
	format_file("   ", &target);
	assert_eq!(std::fs::read_to_string(&target).unwrap(), "original\n");
}

#[cfg(unix)]
#[test]
fn format_file_invokes_program_on_the_written_file() {
	// A stand-in "formatter" that appends a marker to the file it is given.
	// Proves the configured command runs with the generated path as final arg.
	let tmp = tempfile::TempDir::new().expect("temp dir");
	let script = tmp.path().join("fmt.sh");
	std::fs::write(&script, "#!/bin/sh\nprintf '// formatted\\n' >> \"$1\"\n")
		.expect("write script");
	let mut perms = std::fs::metadata(&script).unwrap().permissions();
	std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o755);
	std::fs::set_permissions(&script, perms).expect("chmod");

	let target = tmp.path().join("index.ts");
	std::fs::write(&target, "export interface User {}\n").expect("write");
	format_file(script.to_str().unwrap(), &target);

	let contents = std::fs::read_to_string(&target).expect("read");
	assert!(contents.contains("// formatted"), "formatter did not run, got:\n{contents}");
}
