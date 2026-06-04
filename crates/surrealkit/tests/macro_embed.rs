//! Compile + run check for the `embed_schema!` macro and its generated
//! `embedded_schema::sync` (which delegates to the `Sync` builder). This guards
//! against the macro emitting a call that no longer exists in the library.

use surrealdb::Surreal;
use surrealdb::engine::any::{Any, connect};
use surrealdb::opt::Config;
use surrealdb::opt::capabilities::Capabilities;

// Path is relative to this crate's Cargo.toml.
surrealkit::embed_schema!("tests/fixtures/embed_schema");

async fn mem_db() -> Surreal<Any> {
	let cfg = Config::new().capabilities(Capabilities::all());
	let db = connect(("mem://", cfg)).await.expect("connect mem://");
	db.use_ns("surrealkit_test").use_db("macro_embed").await.expect("use_ns/use_db");
	db
}

#[test]
fn embed_schema_generates_schema_slice() {
	// The generated SCHEMA static contains the embedded file.
	assert!(
		embedded_schema::SCHEMA.iter().any(|f| f.sql.contains("DEFINE TABLE widget")),
		"embedded SCHEMA should include the fixture file"
	);
}

#[tokio::test]
async fn embed_schema_sync_applies_schema() {
	let db = mem_db().await;
	embedded_schema::sync(&db).await.expect("generated sync must apply the embedded schema");

	let mut resp = db.query("INFO FOR DB;").await.expect("INFO FOR DB");
	let info: Option<serde_json::Value> = resp.take(0).expect("take");
	let has_widget = info
		.as_ref()
		.and_then(|v| v.get("tables"))
		.and_then(|v| v.as_object())
		.map(|m| m.contains_key("widget"))
		.unwrap_or(false);
	assert!(has_widget, "embedded sync should have created the 'widget' table");
}
