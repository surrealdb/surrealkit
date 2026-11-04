//! `MIGRATE EXISTING ROWS` through sync and rollouts, on the embedded SurrealDB
//! 4.0 engine, where a new database is at compatibility level 4.
//!
//! The rollout tests change the working directory, so they serialise on
//! `FS_LOCK` and hold it across `.await`.
#![expect(
	clippy::await_holding_lock,
	reason = "FS_LOCK serialises cwd-mutating tests; the guard must span the whole test body"
)]

use std::path::Path;
use std::sync::Mutex;

use serde_json::{Value, json};
use surrealdb::Surreal;
use surrealdb::engine::any::{Any, connect};
use surrealdb::opt::Config;
use surrealdb::opt::capabilities::Capabilities;
use surrealdb_types::SurrealValue;
use surrealkit::constants::DEFAULT_ROOT_DIR;
use surrealkit::module::Module;
use surrealkit::rollout::{
	RolloutExecutionOpts, RolloutPlanOpts, run_baseline, run_complete, run_lint, run_plan,
	run_rollback, run_start,
};
use surrealkit::{EmbeddedSchemaFile, Sync, TemplateVars};

async fn mem_db() -> Surreal<Any> {
	let cfg = Config::new().capabilities(Capabilities::all());
	let db = connect(("mem://", cfg)).await.expect("connect mem://");
	db.use_ns("surrealkit_test").use_db("migrate_rows").await.expect("use_ns/use_db");
	db
}

async fn sync(db: &Surreal<Any>, files: &[EmbeddedSchemaFile]) -> anyhow::Result<()> {
	Sync::embedded(files).run(db).await
}

fn schema(sql: &'static str) -> [EmbeddedSchemaFile; 1] {
	[EmbeddedSchemaFile {
		path: "schema/product.surql",
		sql,
	}]
}

async fn query(db: &Surreal<Any>, sql: &str) -> Value {
	let mut response = db.query(sql).await.expect(sql).check().expect(sql);
	let raw: surrealdb_types::Value = response.take(0).expect(sql);
	Value::from_value(raw).unwrap_or(Value::Null)
}

async fn field_definition(db: &Surreal<Any>, table: &str, field: &str) -> String {
	let info = query(db, &format!("INFO FOR TABLE {table};")).await;
	info["fields"][field].as_str().unwrap_or_default().to_string()
}

const STOCK_STRING: &str = "DEFINE TABLE product SCHEMAFULL;
DEFINE FIELD stock ON product TYPE string;";

const STOCK_INT: &str = "DEFINE TABLE product SCHEMAFULL;
DEFINE FIELD stock ON product TYPE int;";

const STOCK_TO_INT: &str = "DEFINE TABLE product SCHEMAFULL;
DEFINE FIELD stock ON product TYPE int
    MIGRATE EXISTING ROWS AS stock_to_int USING <int> $value FALLBACK 0;";

const STOCK_TO_INT_COMMENTED: &str = "DEFINE TABLE product SCHEMAFULL;
DEFINE FIELD stock ON product TYPE int COMMENT 'units on hand'
    MIGRATE EXISTING ROWS AS stock_to_int USING <int> $value FALLBACK 0;";

const STOCK_TO_INT_OTHER_FALLBACK: &str = "DEFINE TABLE product SCHEMAFULL;
DEFINE FIELD stock ON product TYPE int COMMENT 'units on hand'
    MIGRATE EXISTING ROWS AS stock_to_int USING <int> $value FALLBACK -1;";

async fn seed_stock(db: &Surreal<Any>) {
	query(db, "CREATE product:1 SET stock = '7'; CREATE product:2 SET stock = 'unknown';").await;
}

#[tokio::test]
async fn sync_asks_for_the_clause_and_runs_it_once() {
	let db = mem_db().await;
	sync(&db, &schema(STOCK_STRING)).await.expect("first sync");
	seed_stock(&db).await;

	// Level 4 refuses the narrowing change, and SurrealKit says what to add.
	let err = sync(&db, &schema(STOCK_INT)).await.expect_err("a narrowing change without a clause");
	let message = format!("{err:#}");
	assert!(message.contains("MIGRATE EXISTING ROWS AS <name>"), "{message}");
	assert!(message.contains("`product.stock`"), "{message}");

	sync(&db, &schema(STOCK_TO_INT)).await.expect("sync with the clause");
	assert_eq!(query(&db, "SELECT VALUE stock FROM product ORDER BY id;").await, json!([7, 0]));

	// The clause stays in the file. A later change to the field still lands,
	// although SurrealDB would skip the whole statement if it were sent with
	// a migration name the table has run.
	sync(&db, &schema(STOCK_TO_INT_COMMENTED)).await.expect("sync a comment change");
	assert!(
		field_definition(&db, "product", "stock").await.contains("units on hand"),
		"the comment was not applied: {}",
		field_definition(&db, "product", "stock").await
	);
	assert_eq!(query(&db, "SELECT VALUE stock FROM product ORDER BY id;").await, json!([7, 0]));
}

#[tokio::test]
async fn sync_applies_a_migration_to_a_table_the_file_creates() {
	// A new environment applies the file with its clause before the table exists.
	let db = mem_db().await;
	sync(&db, &schema(STOCK_TO_INT)).await.expect("sync on an empty database");
	sync(&db, &schema(STOCK_TO_INT_COMMENTED)).await.expect("sync again");
	assert!(field_definition(&db, "product", "stock").await.contains("units on hand"));
}

#[tokio::test]
async fn sync_refuses_a_name_reused_for_another_transform() {
	let db = mem_db().await;
	sync(&db, &schema(STOCK_STRING)).await.expect("first sync");
	seed_stock(&db).await;
	sync(&db, &schema(STOCK_TO_INT_COMMENTED)).await.expect("migrate");

	let err = sync(&db, &schema(STOCK_TO_INT_OTHER_FALLBACK))
		.await
		.expect_err("the same name for another transform");
	assert!(format!("{err:#}").contains("already ran with a different transform"), "{err:#}");
}

#[tokio::test]
async fn sync_removes_and_redefines_the_indexes_a_migrated_field_reads() {
	let db = mem_db().await;
	let before = [
		EmbeddedSchemaFile {
			path: "schema/item.surql",
			sql: "DEFINE TABLE item SCHEMAFULL;
DEFINE FIELD sku ON item TYPE string;
DEFINE FIELD price ON item TYPE string;
DEFINE INDEX item_price ON item FIELDS price;",
		},
		EmbeddedSchemaFile {
			path: "schema/item_indexes.surql",
			sql: "DEFINE INDEX item_sku_price ON item FIELDS sku, price UNIQUE;",
		},
	];
	sync(&db, &before).await.expect("first sync");
	query(
		&db,
		"CREATE item:1 SET sku = 'a', price = '5'; CREATE item:2 SET sku = 'b', price = '9';",
	)
	.await;

	let after = [
		EmbeddedSchemaFile {
			path: "schema/item.surql",
			sql: "DEFINE TABLE item SCHEMAFULL;
DEFINE FIELD sku ON item TYPE string;
DEFINE FIELD price ON item TYPE int
    MIGRATE EXISTING ROWS AS price_to_int USING <int> $value FALLBACK 0;
DEFINE INDEX item_price ON item FIELDS price;",
		},
		before[1],
	];
	sync(&db, &after).await.expect("migrate an indexed field");

	assert_eq!(query(&db, "SELECT VALUE price FROM item ORDER BY id;").await, json!([5, 9]));
	let info = query(&db, "INFO FOR TABLE item;").await;
	let indexes = info["indexes"].as_object().expect("indexes");
	assert!(indexes.contains_key("item_price"), "{info}");
	assert!(indexes.contains_key("item_sku_price"), "{info}");
	// The unique index enforces again.
	let duplicate =
		db.query("CREATE item:3 SET sku = 'a', price = 5;").await.expect("query").check();
	assert!(duplicate.is_err(), "the UNIQUE index no longer holds");

	// The next sync is a no-op for the migration, and the index stays.
	sync(&db, &after).await.expect("second sync");
	let info = query(&db, "INFO FOR TABLE item;").await;
	assert!(info["indexes"].as_object().expect("indexes").contains_key("item_price"), "{info}");
}

#[tokio::test]
async fn sync_prune_at_level_4_clears_the_values() {
	let db = mem_db().await;
	sync(
		&db,
		&schema(
			"DEFINE TABLE product SCHEMAFULL;
DEFINE FIELD stock ON product TYPE int;
DEFINE FIELD note ON product TYPE option<string>;",
		),
	)
	.await
	.expect("first sync");
	query(&db, "CREATE product:1 SET stock = 1, note = 'keep?';").await;

	sync(&db, &schema(STOCK_INT)).await.expect("prune the note field");
	assert_eq!(query(&db, "SELECT VALUE note FROM product;").await, json!([null]));
}

#[tokio::test]
async fn sync_runs_remove_undeclared_fields_from_a_schema_file() {
	let db = mem_db().await;
	sync(
		&db,
		&schema(
			"DEFINE TABLE product SCHEMALESS;
DEFINE FIELD stock ON product TYPE int;",
		),
	)
	.await
	.expect("first sync");
	query(&db, "CREATE product:1 SET stock = 1, leftover = 'x';").await;

	let clean = schema(
		"DEFINE TABLE product SCHEMAFULL;
DEFINE FIELD stock ON product TYPE int;
ALTER TABLE product MIGRATE EXISTING ROWS AS drop_leftovers REMOVE UNDECLARED FIELDS;",
	);
	sync(&db, &clean).await.expect("clean the table");
	sync(&db, &clean).await.expect("a second run does no harm");
	assert_eq!(
		query(&db, "SELECT * FROM product;").await,
		json!([{ "id": "product:1", "stock": 1 }])
	);
}

// Rollouts, which plan from a project folder in the working directory.

static FS_LOCK: Mutex<()> = Mutex::new(());

struct RestoreCwd(std::path::PathBuf);

impl Drop for RestoreCwd {
	fn drop(&mut self) {
		let _ = std::env::set_current_dir(&self.0);
	}
}

fn enter_tempdir() -> (tempfile::TempDir, RestoreCwd) {
	let original = std::env::current_dir().expect("get cwd");
	let tmp = tempfile::TempDir::new().expect("create temp dir");
	std::env::set_current_dir(tmp.path()).expect("set cwd");
	(tmp, RestoreCwd(original))
}

fn write_schema_file(dir: &Path, name: &str, sql: &str) {
	let schema_dir = dir.join("database/schema");
	std::fs::create_dir_all(&schema_dir).expect("create schema dir");
	std::fs::write(schema_dir.join(name), sql).expect("write schema file");
}

fn latest_manifest(base: &Path) -> (String, std::path::PathBuf) {
	let dir = base.join("database/rollouts");
	let mut entries: Vec<_> = std::fs::read_dir(&dir)
		.expect("rollouts dir")
		.filter_map(|e| e.ok())
		.map(|e| e.path())
		.filter(|p| p.extension().and_then(|s| s.to_str()) == Some("toml"))
		.collect();
	entries.sort();
	let path = entries.pop().expect("a manifest");
	let id = path.file_stem().and_then(|stem| stem.to_str()).expect("manifest id").to_string();
	(id, path)
}

/// A project whose `product.stock` is a string with records, baselined, and a
/// rollout planned that migrates it to an int.
async fn planned_stock_rollout(base: &Path) -> (Surreal<Any>, String, std::path::PathBuf) {
	let db = mem_db().await;
	write_schema_file(base, "product.surql", STOCK_STRING);
	query(&db, STOCK_STRING).await;
	seed_stock(&db).await;
	run_baseline(&db, DEFAULT_ROOT_DIR, &Module::default_module()).await.expect("baseline");

	write_schema_file(base, "product.surql", STOCK_TO_INT);
	run_plan(
		DEFAULT_ROOT_DIR,
		RolloutPlanOpts {
			name: Some("stock_to_int".to_string()),
			dry_run: false,
			allow_modified: false,
		},
	)
	.await
	.expect("a migration is planned without --allow-modified");
	let (id, path) = latest_manifest(base);
	(db, id, path)
}

fn opts(id: &str) -> RolloutExecutionOpts {
	RolloutExecutionOpts::new(Some(id.to_string()))
}

#[tokio::test]
async fn a_planned_migration_rolls_back_through_its_revert() {
	let _guard = FS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
	let (tmp, _restore) = enter_tempdir();
	let (db, id, manifest) = planned_stock_rollout(tmp.path()).await;

	let text = std::fs::read_to_string(&manifest).expect("read manifest");
	assert!(text.contains("kind = \"revert_migration\""), "{text}");
	assert!(text.contains("kind = \"await_migrations\""), "{text}");
	assert!(!text.contains("restore_definitions"), "{text}");

	// Not until the author says what the values become again.
	let err = run_start(&db, DEFAULT_ROOT_DIR, opts(&id), &TemplateVars::default())
		.await
		.expect_err("an empty revert");
	assert!(format!("{err:#}").contains("has an empty `using`"), "{err:#}");
	assert!(run_lint(DEFAULT_ROOT_DIR, opts(&id)).await.is_err());

	let filled = text
		.replace("using = \"\"", "using = \"<string> $value\"")
		.replace("fallback = \"\"", "fallback = \"''\"");
	std::fs::write(&manifest, filled).expect("write manifest");
	run_lint(DEFAULT_ROOT_DIR, opts(&id)).await.expect("lint the filled-in revert");
	run_start(&db, DEFAULT_ROOT_DIR, opts(&id), &TemplateVars::default()).await.expect("start");
	assert_eq!(query(&db, "SELECT VALUE stock FROM product ORDER BY id;").await, json!([7, 0]));

	run_rollback(&db, DEFAULT_ROOT_DIR, opts(&id), &TemplateVars::default())
		.await
		.expect("rollback");
	assert_eq!(query(&db, "SELECT VALUE stock FROM product ORDER BY id;").await, json!(["7", "0"]));
	assert!(field_definition(&db, "product", "stock").await.contains("TYPE string"));
}

#[tokio::test]
async fn a_migration_without_a_revert_cannot_be_rolled_back() {
	let _guard = FS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
	let (tmp, _restore) = enter_tempdir();
	let (db, id, manifest) = planned_stock_rollout(tmp.path()).await;

	let text = std::fs::read_to_string(&manifest).expect("read manifest");
	let kept: Vec<&str> =
		text.split("[[steps]]").filter(|block| !block.contains("revert_migration")).collect();
	std::fs::write(&manifest, kept.join("[[steps]]")).expect("write manifest");

	run_start(&db, DEFAULT_ROOT_DIR, opts(&id), &TemplateVars::default()).await.expect("start");
	let err = run_rollback(&db, DEFAULT_ROOT_DIR, opts(&id), &TemplateVars::default())
		.await
		.expect_err("no revert");
	assert!(format!("{err:#}").contains("cannot be rolled back"), "{err:#}");

	// Forward is still open: complete waits for the rewrite and finishes.
	run_complete(&db, DEFAULT_ROOT_DIR, opts(&id), &TemplateVars::default())
		.await
		.expect("complete");
	let status =
		query(&db, &format!("SELECT VALUE status FROM type::record('__rollout', '{id}');")).await;
	assert_eq!(status, json!(["completed"]));
}

#[tokio::test]
async fn compat_raise_reports_fields_whose_records_do_not_fit() {
	let _guard = FS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
	let (tmp, _restore) = enter_tempdir();
	let db = mem_db().await;
	// A database as a 3.x release left it: a field retyped without its records.
	query(&db, "ALTER DATABASE COMPATIBILITY 3;").await;
	query(&db, STOCK_STRING).await;
	seed_stock(&db).await;
	query(&db, "DEFINE FIELD OVERWRITE stock ON product TYPE int;").await;
	write_schema_file(tmp.path(), "product.surql", STOCK_INT);

	let raise = |yes| surrealkit::compat::CompatRaiseOpts {
		wait: true,
		timeout: std::time::Duration::from_secs(30),
		yes,
	};
	let err = surrealkit::compat::run_compat_raise(&db, DEFAULT_ROOT_DIR, raise(false))
		.await
		.expect_err("needs --yes");
	assert!(format!("{err:#}").contains("--yes"), "{err:#}");
	assert_eq!(query(&db, "(INFO FOR DB).compatibility;").await, json!(3));

	let err = surrealkit::compat::run_compat_raise(&db, DEFAULT_ROOT_DIR, raise(true))
		.await
		.expect_err("records that do not fit");
	assert!(format!("{err:#}").contains("do not fit"), "{err:#}");
	assert_eq!(query(&db, "(INFO FOR DB).compatibility;").await, json!(4));

	// The fix it points at works: say what the records become.
	write_schema_file(tmp.path(), "product.surql", STOCK_TO_INT);
	query(
		&db,
		"DEFINE FIELD OVERWRITE stock ON product TYPE int \
		 MIGRATE EXISTING ROWS AS stock_fit USING <int> $value FALLBACK 0;",
	)
	.await;
	assert_eq!(query(&db, "SELECT VALUE stock FROM product ORDER BY id;").await, json!([7, 0]));
	surrealkit::compat::run_compat_status(&db).await.expect("status");
}
