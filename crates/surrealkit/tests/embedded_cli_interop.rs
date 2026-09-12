//! The CLI filesystem path and the embedded macro path against one database.
//!
//! These are the two ways the same project reaches the same database: a
//! developer running `surrealkit sync` / `surrealkit seed`, and the application
//! booting with `embed_schema!` / `embed_seed!`. They track files independently,
//! so if their key conventions disagree each one treats the other's rows as
//! foreign, and whichever ran last rewrites them. For seeds that means the seed
//! executes again on every alternation, which is the data-corruption case seed
//! tracking exists to prevent.
//!
//! Nothing covered the combination before 1.0.0-beta.2, which is how a divergence
//! shipped: the CLI moved to folder-relative keys and the macro kept emitting
//! folder-prefixed ones.

use surrealdb::Surreal;
use surrealdb::engine::any::{Any, connect};
use surrealdb::opt::Config;
use surrealdb::opt::capabilities::Capabilities;
use surrealkit::core::sha256_hex;
use surrealkit::seed::{EmbeddedSeedFile, Seed};
use surrealkit::sync::{EmbeddedSchemaFile, Sync, SyncOpts, run_sync};
use tempfile::TempDir;

const SEED_SQL: &str = "CREATE visit SET at = time::now();";
const SCHEMA_SQL: &str = "DEFINE TABLE visit SCHEMALESS;";

/// What `embed_seed!()` emits for `<folder>/seed/000_init.surql`.
const EMBEDDED_SEEDS: &[EmbeddedSeedFile] = &[EmbeddedSeedFile {
	path: "seed/000_init.surql",
	sql: SEED_SQL,
}];

/// What `embed_schema!()` emits for `<folder>/schema/001_visit.surql`.
const EMBEDDED_SCHEMA: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
	path: "schema/001_visit.surql",
	sql: SCHEMA_SQL,
}];

async fn memory_db(ns: &str) -> Surreal<Any> {
	let cfg = Config::new().capabilities(Capabilities::all());
	let db = connect(("mem://", cfg)).await.expect("connect mem");
	db.use_ns(ns).use_db(ns).await.expect("select namespace");
	db
}

async fn visit_count(db: &Surreal<Any>) -> i64 {
	let mut response =
		db.query("SELECT count() FROM visit GROUP ALL;").await.expect("count visits");
	let count: Option<i64> = response.take((0, "count")).unwrap_or_default();
	count.unwrap_or(0)
}

async fn tracked_seed_keys(db: &Surreal<Any>) -> Vec<String> {
	let mut response =
		db.query("SELECT VALUE key FROM __seed ORDER BY key;").await.expect("read __seed");
	response.take::<Vec<String>>(0).unwrap_or_default()
}

async fn tracked_sync_keys(db: &Surreal<Any>) -> Vec<String> {
	let mut response = db
		.query("SELECT VALUE key FROM __entity WHERE ns = 'sync' ORDER BY key;")
		.await
		.expect("read __entity");
	response.take::<Vec<String>>(0).unwrap_or_default()
}

/// Write the filesystem half of the project and return its folder.
fn project(temp: &TempDir) -> String {
	let folder = temp.path().join("database");
	std::fs::create_dir_all(folder.join("seed")).expect("seed dir");
	std::fs::create_dir_all(folder.join("schema")).expect("schema dir");
	std::fs::write(folder.join("seed/000_init.surql"), SEED_SQL).expect("seed file");
	std::fs::write(folder.join("schema/001_visit.surql"), SCHEMA_SQL).expect("schema file");
	folder.to_string_lossy().to_string()
}

/// Alternating between the two must run the seed exactly once.
///
/// Before the fix this produced a permanent ping-pong: the CLI migrated
/// `database/seed/000_init.surql` to `seed/000_init.surql` and deleted the
/// original, the app did not recognise the new key, re-ran the seed and wrote the
/// old one back, and the next CLI run undid that again.
#[tokio::test]
async fn seeding_alternates_between_cli_and_embedded_without_rerunning() {
	let db = memory_db("interop_seed").await;
	let temp = TempDir::new().expect("tempdir");
	let folder = project(&temp);

	Seed::embedded(EMBEDDED_SEEDS).run(&db).await.expect("embedded seed");
	assert_eq!(visit_count(&db).await, 1, "the first seed should run");

	Seed::from_dir(&folder).run(&db).await.expect("cli seed");
	assert_eq!(visit_count(&db).await, 1, "the CLI must not re-run an embedded seed");

	Seed::embedded(EMBEDDED_SEEDS).run(&db).await.expect("embedded seed again");
	assert_eq!(visit_count(&db).await, 1, "an app reboot must not re-run a CLI seed");

	Seed::from_dir(&folder).run(&db).await.expect("cli seed again");
	assert_eq!(visit_count(&db).await, 1, "and it must still not re-run after that");

	assert_eq!(
		tracked_seed_keys(&db).await,
		vec!["seed/000_init.surql".to_string()],
		"both paths must converge on one key, or each keeps rewriting the other's"
	);
}

/// The same for schema. Re-applying is idempotent, but `ensure_overwrite` turns
/// it into `DEFINE INDEX ... OVERWRITE`, so a divergence rebuilds every index on
/// every boot.
#[tokio::test]
async fn syncing_alternates_between_cli_and_embedded_without_reapplying() {
	let db = memory_db("interop_sync").await;
	let temp = TempDir::new().expect("tempdir");
	let folder = project(&temp);

	Sync::embedded(EMBEDDED_SCHEMA).run(&db).await.expect("embedded sync");
	run_sync(
		&db,
		SyncOpts {
			folder: folder.clone(),
			..Default::default()
		},
	)
	.await
	.expect("cli sync");
	Sync::embedded(EMBEDDED_SCHEMA).run(&db).await.expect("embedded sync again");

	assert_eq!(
		tracked_sync_keys(&db).await,
		vec!["schema/001_visit.surql".to_string()],
		"both paths must converge on one key"
	);
}

/// A database seeded by a pre-1.0.0-beta.2 application still carries the
/// folder-prefixed key. It must migrate once and then settle, not re-run.
#[tokio::test]
async fn a_legacy_embedded_key_migrates_once_and_settles() {
	let db = memory_db("interop_legacy").await;
	let temp = TempDir::new().expect("tempdir");
	let folder = project(&temp);

	// What the old macro wrote, plus the row the seed would have created.
	db.query("CREATE __seed CONTENT { key: 'database/seed/000_init.surql', hash: $hash };")
		.bind(("hash", sha256_hex(SEED_SQL.as_bytes())))
		.await
		.expect("seed legacy tracking row");
	db.query(SEED_SQL).await.expect("legacy seed effect");
	assert_eq!(visit_count(&db).await, 1);

	Seed::from_dir(&folder).run(&db).await.expect("cli seed");
	assert_eq!(visit_count(&db).await, 1, "the legacy key must be recognised, not re-run");

	Seed::embedded(EMBEDDED_SEEDS).run(&db).await.expect("embedded seed");
	assert_eq!(visit_count(&db).await, 1, "and the app must agree after the migration");

	assert_eq!(tracked_seed_keys(&db).await, vec!["seed/000_init.surql".to_string()]);
}

/// An application that seeds only through `embed_seed!()` and never runs the CLI.
///
/// Upgrading changes the key the macro emits, so without a migration on the
/// embedded path the app finds nothing under the new name and re-executes every
/// seed file. For a `CREATE`-style seed that is duplicated rows, which is the
/// case seed tracking exists to prevent. The stale row is never cleaned up
/// either, so it stays wrong.
#[tokio::test]
async fn an_embedded_only_app_does_not_reseed_after_upgrading() {
	let db = memory_db("interop_embedded_only").await;

	// What a pre-1.0.0-beta.2 build of the same app left behind.
	db.query("CREATE __seed CONTENT { key: 'database/seed/000_init.surql', hash: $hash };")
		.bind(("hash", sha256_hex(SEED_SQL.as_bytes())))
		.await
		.expect("seed legacy tracking row");
	db.query(SEED_SQL).await.expect("legacy seed effect");
	assert_eq!(visit_count(&db).await, 1);

	// The upgraded app boots. No CLI has ever run against this database.
	Seed::embedded(EMBEDDED_SEEDS).run(&db).await.expect("embedded seed");
	assert_eq!(
		visit_count(&db).await,
		1,
		"the embedded path must recognise its own pre-upgrade key, not re-run the seed"
	);

	assert_eq!(tracked_seed_keys(&db).await, vec!["seed/000_init.surql".to_string()]);
}
