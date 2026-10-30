//! v1 output stays byte-for-byte what it was.
//!
//! Effect-native typegen rewrites the `TYPE` parser and the `DEFINE` clause
//! scanner. Existing projects must see no change in `schema.json` or the
//! generated TypeScript unless their schema used a construct the old parser got
//! wrong. These snapshots were recorded with the parser as it was before that
//! rewrite (the old `type_parser`), over every schema the repository already
//! shipped typegen coverage or examples for, and must not be edited to make a
//! parser change pass.

use std::path::Path;

use surrealdb::Surreal;
use surrealdb::engine::any::{Any, connect};
use surrealdb::opt::Config;
use surrealdb::opt::capabilities::Capabilities;
use surrealkit::typegen::{generate_unstamped, render_typescript, write_json};

async fn mem_db() -> Surreal<Any> {
	let cfg = Config::new().capabilities(Capabilities::all());
	let db = connect(("mem://", cfg)).await.expect("connect mem://");
	db.use_ns("golden").use_db("golden").await.expect("use_ns/use_db");
	db
}

/// The JSON document and TypeScript that typegen produces for `sql`, joined
/// into one snapshot.
async fn v1_outputs(sql: &str) -> String {
	let db = mem_db().await;
	db.query(sql).await.expect("apply schema").check().expect("schema ok");
	let doc = generate_unstamped(&db).await.expect("generate");
	let tmp = tempfile::TempDir::new().expect("temp dir");
	let json_path = tmp.path().join("schema.json");
	write_json(&doc, &json_path, true).expect("write json");
	let json = std::fs::read_to_string(&json_path).expect("read json");
	let ts = render_typescript(&doc).expect("render ts");
	format!("--- schema.json\n{json}--- index.ts\n{ts}")
}

/// Every `.surql` file in `dir`, concatenated in path order.
fn schema_dir(dir: &Path) -> String {
	let mut files: Vec<_> = std::fs::read_dir(dir)
		.unwrap_or_else(|err| panic!("{}: {err}", dir.display()))
		.map(|e| e.expect("entry").path())
		.filter(|p| p.extension().is_some_and(|e| e == "surql"))
		.collect();
	files.sort();
	files
		.iter()
		.map(|p| std::fs::read_to_string(p).expect("read schema"))
		.collect::<Vec<_>>()
		.join("\n")
}

fn examples() -> std::path::PathBuf {
	Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples")
}

/// The schema `tests/typegen_api.rs` has always exercised.
const API_SCHEMA: &str = r#"
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

/// Everything the old parser understood: each primitive, optionality, arrays and
/// sets with and without bounds, records, geometry, literal string, integer and
/// boolean unions, nullable unions, nested objects, element paths, defaults,
/// readonly and permissions, a field without `TYPE` — and a function with a
/// declared return.
const SUPPORTED_SCHEMA: &str = r#"
	DEFINE TABLE thing SCHEMAFULL PERMISSIONS FOR select FULL, FOR create, update, delete NONE;
	DEFINE FIELD s ON thing TYPE string;
	DEFINE FIELD i ON thing TYPE int DEFAULT 0;
	DEFINE FIELD f ON thing TYPE float;
	DEFINE FIELD n ON thing TYPE number;
	DEFINE FIELD d ON thing TYPE decimal;
	DEFINE FIELD b ON thing TYPE bool;
	DEFINE FIELD dt ON thing TYPE datetime DEFAULT time::now() READONLY;
	DEFINE FIELD du ON thing TYPE duration;
	DEFINE FIELD u ON thing TYPE uuid;
	DEFINE FIELD bs ON thing TYPE bytes;
	DEFINE FIELD a ON thing TYPE any;
	DEFINE FIELD o ON thing TYPE object;
	DEFINE FIELD opt ON thing TYPE option<string>;
	DEFINE FIELD nul ON thing TYPE string | null;
	DEFINE FIELD arr ON thing TYPE array<int>;
	DEFINE FIELD arr_n ON thing TYPE array<string, 3>;
	DEFINE FIELD st ON thing TYPE set<string>;
	DEFINE FIELD rec ON thing TYPE record<user>;
	DEFINE FIELD recs ON thing TYPE option<array<record<user | thing>>>;
	DEFINE FIELD geo ON thing TYPE geometry<point | polygon>;
	DEFINE FIELD status ON thing TYPE 'open' | 'closed' DEFAULT 'open';
	DEFINE FIELD code ON thing TYPE 200 | 404;
	DEFINE FIELD flag ON thing TYPE true | false;
	DEFINE FIELD nested ON thing TYPE object;
	DEFINE FIELD nested.inner ON thing TYPE object;
	DEFINE FIELD nested.inner.leaf ON thing TYPE option<int>;
	DEFINE FIELD items ON thing TYPE array<object> DEFAULT [];
	DEFINE FIELD items.*.label ON thing TYPE string;
	DEFINE FIELD guarded ON thing TYPE string ASSERT $value != 'DEFAULT';
	DEFINE FIELD untyped ON thing;
	DEFINE TABLE user SCHEMAFULL;
	DEFINE FIELD name ON user TYPE string;
	DEFINE FUNCTION fn::add($a: int, $b: option<int>) -> int { RETURN $a + ($b ?? 0); };
"#;

#[tokio::test]
async fn typegen_api_schema_is_unchanged() {
	insta::assert_snapshot!("typegen_api_schema", v1_outputs(API_SCHEMA).await);
}

#[tokio::test]
async fn supported_constructs_are_unchanged() {
	insta::assert_snapshot!("supported_constructs", v1_outputs(SUPPORTED_SCHEMA).await);
}

#[tokio::test]
async fn rollout_testing_example_is_unchanged() {
	let sql = schema_dir(&examples().join("rollout-testing/database/schema"));
	insta::assert_snapshot!("rollout_testing_example", v1_outputs(&sql).await);
}

#[tokio::test]
async fn rollout_repro_example_is_unchanged() {
	let sql = schema_dir(&examples().join("rollout-repro/project/database/schema"));
	insta::assert_snapshot!("rollout_repro_example", v1_outputs(&sql).await);
}
