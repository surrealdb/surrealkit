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
async fn run_typegen_writes_valid_json_file() {
	let db = mem_db().await;
	define_schema(&db).await;

	let tmp = tempfile::TempDir::new().expect("temp dir");
	let out = tmp.path().join("schema.json");

	run_typegen(
		&db,
		"./database",
		"surrealkit_test",
		"typegen_test",
		TypegenOpts {
			out: Some(out.clone()),
			stdout: false,
			pretty: true,
			ts_out: None,
			ts_format: None,
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
		"surrealkit_test",
		"typegen_test",
		TypegenOpts {
			out: Some(json_out.clone()),
			stdout: false,
			pretty: true,
			ts_out: Some(ts_dir.clone()),
			ts_format: None,
		},
	)
	.await
	.expect("run_typegen");

	assert!(json_out.exists(), "json should still be written");
	let ts = std::fs::read_to_string(ts_dir.join("index.ts")).expect("read index.ts");
	assert!(ts.contains("export interface User {"), "got:\n{ts}");
	assert!(ts.contains("id: RecordId<'user'>;"), "got:\n{ts}");
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
