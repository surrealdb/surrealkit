//! Compile + run check for the `embed_schema!` macro and its generated
//! `embedded_schema::sync` (which delegates to the `Sync` builder). This guards
//! against the macro emitting a call that no longer exists in the library.

use surrealdb::Surreal;
use surrealdb::engine::any::{Any, connect};
use surrealdb::opt::Config;
use surrealdb::opt::capabilities::Capabilities;

// Path is relative to this crate's Cargo.toml.
surrealkit::embed_schema!("tests/fixtures/embed_schema");
surrealkit::embed_seed!("tests/fixtures/embed_seed");

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

#[test]
fn embed_seed_generates_seed_slice() {
	assert!(
		embedded_seed::SEEDS.iter().any(|f| f.sql.contains("CREATE widget:gadget")),
		"embedded SEEDS should include the fixture file"
	);
}

#[tokio::test]
async fn embed_seed_runs_once_and_tracks() {
	let db = mem_db().await;
	embedded_seed::seed(&db).await.expect("generated seed must apply");
	// A second run is a no-op thanks to `__seed` tracking. Without it the
	// `CREATE widget:gadget` would fail with "already exists".
	embedded_seed::seed(&db).await.expect("second seed run must be a tracked no-op");

	let mut resp = db.query("SELECT count() FROM widget GROUP ALL").await.expect("count widget");
	let count: Option<serde_json::Value> = resp.take(0).expect("take");
	let n = count.and_then(|v| v["count"].as_u64()).unwrap_or(0);
	assert_eq!(n, 1, "seed should have run exactly once");
}
