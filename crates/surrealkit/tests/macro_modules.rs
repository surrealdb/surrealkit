//! The named-module form of `embed_schema!`: several independently-tracked
//! modules declared in one invocation.
//!
//! This lives in its own test file because the macro still emits a single
//! `pub mod embedded_schema`; declaring several modules is what the named form is
//! for, not calling the macro twice in one scope.

use surrealdb::Surreal;
use surrealdb::engine::any::{Any, connect};
use surrealdb::opt::Config;
use surrealdb::opt::capabilities::Capabilities;

async fn mem_db() -> Surreal<Any> {
	let cfg = Config::new().capabilities(Capabilities::all());
	let db = connect(("mem://", cfg)).await.expect("connect mem://");
	db.use_ns("surrealkit_test").use_db("macro_modules").await.expect("use_ns/use_db");
	db
}

// Two named modules in ONE scope. Before v1 this was a duplicate-module compile
// error, because both invocations emitted `pub mod embedded_schema`.
surrealkit::embed_schema!(
	core = "tests/fixtures/embed_modules/core",
	billing = "tests/fixtures/embed_modules/billing",
);

#[test]
fn named_modules_expose_their_own_slices() {
	assert_eq!(embedded_schema::core::NAME, "core");
	assert_eq!(embedded_schema::billing::NAME, "billing");
	assert_eq!(embedded_schema::core::SCHEMA.len(), 1);
	assert_eq!(embedded_schema::billing::SCHEMA.len(), 1);
	assert!(embedded_schema::core::SCHEMA[0].sql.contains("mod_core_thing"));
	assert!(embedded_schema::billing::SCHEMA[0].sql.contains("mod_billing_thing"));
}

#[test]
fn modules_listing_preserves_declaration_order() {
	// Declaration order is the apply order, so it must not be sorted or reordered.
	let names: Vec<&str> = embedded_schema::MODULES.iter().map(|(n, _)| *n).collect();
	assert_eq!(names, vec!["core", "billing"]);
}

#[tokio::test]
async fn generated_sync_applies_every_module_scoped() {
	let db = mem_db().await;
	embedded_schema::sync(&db).await.expect("sync all modules");

	let mut resp = db.query("INFO FOR DB;").await.expect("info");
	let info: Option<serde_json::Value> = resp.take(0).expect("take");
	let tables = info.as_ref().and_then(|v| v.get("tables")).expect("tables");
	assert!(tables.get("mod_core_thing").is_some(), "core applied");
	assert!(tables.get("mod_billing_thing").is_some(), "billing applied");

	// Each module tracked its files in its own partition.
	for ns in ["sync@core", "sync@billing"] {
		let mut resp = db
			.query("SELECT key FROM __entity WHERE ns = $ns;")
			.bind(("ns", ns))
			.await
			.expect("query");
		let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
		assert_eq!(rows.len(), 1, "{ns} should track exactly its own file");
	}
}

#[tokio::test]
async fn a_single_generated_module_sync_does_not_disturb_the_others() {
	let db = mem_db().await;
	embedded_schema::sync(&db).await.expect("sync all");
	embedded_schema::billing::sync(&db).await.expect("sync billing only");

	let mut resp = db.query("INFO FOR DB;").await.expect("info");
	let info: Option<serde_json::Value> = resp.take(0).expect("take");
	let tables = info.as_ref().and_then(|v| v.get("tables")).expect("tables");
	assert!(tables.get("mod_core_thing").is_some(), "core must survive a billing-only sync");
}
