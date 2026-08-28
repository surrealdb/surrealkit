// These tests mutate process-global state (the current working directory), so they
// serialise on `FS_LOCK`. The guard is deliberately held across `.await` points:
// releasing it early would let a concurrent test chdir out from under this one.
#![expect(
	clippy::await_holding_lock,
	reason = "FS_LOCK serialises cwd-mutating tests; the guard must span the whole test body"
)]

use std::path::Path;
use std::sync::Mutex;

use surrealdb::Surreal;
use surrealdb::engine::any::{Any, connect};
use surrealdb::opt::Config;
use surrealdb::opt::capabilities::Capabilities;
use surrealkit::constants::DEFAULT_ROOT_DIR;
use surrealkit::module::Module;
// CLI-backing filesystem functions live behind their modules (doc-hidden); the
// library happy-path uses the `Sync` builder and `Rollout` facade exported at the
// crate root.
use surrealkit::rollout::{
	RolloutExecutionOpts, RolloutPlanOpts, run_baseline, run_complete, run_plan, run_rollback,
	run_start, run_status,
};
use surrealkit::seed::seed_from_dir;
use surrealkit::setup::run_setup;
use surrealkit::{
	EmbeddedSchemaFile, EntityKey, EntityKind, Rollout, RolloutPhase, RolloutSpec, RolloutStep,
	Sync, TemplateVars,
};

async fn mem_db() -> Surreal<Any> {
	let cfg = Config::new().capabilities(Capabilities::all());
	let db = connect(("mem://", cfg)).await.expect("connect mem://");
	db.use_ns("surrealkit_test").use_db("library_api_test").await.expect("use_ns/use_db");
	db
}

// Tests that change cwd must hold this lock to avoid races.
//
// Always taken with `unwrap_or_else(|e| e.into_inner())`: a panicking test poisons
// the mutex, and a bare `unwrap()` would turn that single failure into a cascade of
// `PoisonError`s across every other cwd test, hiding which one actually broke.
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

#[tokio::test]
async fn setup_initialises_metadata_tables() {
	let _lock = FS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
	let (_tmp, _cwd) = enter_tempdir();
	let db = mem_db().await;
	run_setup(&db, DEFAULT_ROOT_DIR).await.expect("run_setup");

	db.query("SELECT * FROM __entity LIMIT 1;")
		.await
		.expect("query __entity")
		.check()
		.expect("__entity must exist");

	db.query("SELECT * FROM __rollout LIMIT 1;")
		.await
		.expect("query __rollout")
		.check()
		.expect("__rollout must exist");
}

#[tokio::test]
async fn sync_embedded_applies_schema_and_tracks_file() {
	let db = mem_db().await;

	static FILES: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/person.surql",
		sql: "DEFINE TABLE person SCHEMALESS;",
	}];

	Sync::embedded(FILES).run(&db).await.expect("sync embedded");

	let mut resp = db.query("SELECT key FROM __entity WHERE ns = 'sync';").await.expect("query");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get("key").and_then(|v| v.as_str()), Some("database/schema/person.surql"));
}

#[tokio::test]
async fn sync_embedded_is_idempotent() {
	let db = mem_db().await;

	static FILES: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/idempotent.surql",
		sql: "DEFINE TABLE idempotent_test SCHEMALESS;",
	}];

	Sync::embedded(FILES).run(&db).await.expect("first sync");
	Sync::embedded(FILES).run(&db).await.expect("second sync");

	let mut resp = db.query("SELECT key FROM __entity WHERE ns = 'sync';").await.expect("query");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert_eq!(rows.len(), 1, "no duplicate tracking records");
}

#[tokio::test]
async fn sync_embedded_prunes_removed_files() {
	let db = mem_db().await;

	static TWO_FILES: &[EmbeddedSchemaFile] = &[
		EmbeddedSchemaFile {
			path: "database/schema/alpha.surql",
			sql: "DEFINE TABLE alpha SCHEMALESS;",
		},
		EmbeddedSchemaFile {
			path: "database/schema/beta.surql",
			sql: "DEFINE TABLE beta SCHEMALESS;",
		},
	];
	Sync::embedded(TWO_FILES).run(&db).await.expect("initial sync");

	let mut resp =
		db.query("SELECT key FROM __entity WHERE ns = 'sync' ORDER BY key;").await.expect("query");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert_eq!(rows.len(), 2);

	static ONE_FILE: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/alpha.surql",
		sql: "DEFINE TABLE alpha SCHEMALESS;",
	}];
	Sync::embedded(ONE_FILE).prune(true).run(&db).await.expect("pruning sync");

	let mut resp = db.query("SELECT key FROM __entity WHERE ns = 'sync';").await.expect("query");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert_eq!(rows.len(), 1, "beta must be pruned");
	assert_eq!(rows[0].get("key").and_then(|v| v.as_str()), Some("database/schema/alpha.surql"));
}

#[tokio::test]
async fn sync_embedded_refuses_to_prune_everything_when_no_files_found() {
	let db = mem_db().await;

	static FILES: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/alpha.surql",
		sql: "DEFINE TABLE alpha SCHEMALESS;",
	}];
	Sync::embedded(FILES).run(&db).await.expect("initial sync");

	// An empty file set makes every managed entity look stale. That almost always
	// means the schema folder is misconfigured, so it must be refused rather than
	// silently dropping the whole schema.
	let err = Sync::embedded(&[]).run(&db).await.expect_err("empty sync must be refused");
	let msg = format!("{err:#}");
	assert!(msg.contains("refusing to prune"), "unexpected error: {msg}");
	assert!(msg.contains("--allow-empty-prune"), "error must name the override: {msg}");

	// Nothing was removed.
	let mut resp = db.query("SELECT key FROM __entity WHERE ns = 'sync';").await.expect("query");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert_eq!(rows.len(), 1, "guard must not have pruned anything");

	let mut resp = db.query("INFO FOR DB;").await.expect("info");
	let info: Option<serde_json::Value> = resp.take(0).expect("take");
	let tables = info.as_ref().and_then(|v| v.get("tables")).expect("tables");
	assert!(tables.get("alpha").is_some(), "alpha table must still exist");
}

#[tokio::test]
async fn sync_embedded_empty_prune_is_allowed_with_opt_in() {
	let db = mem_db().await;

	static FILES: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/alpha.surql",
		sql: "DEFINE TABLE alpha SCHEMALESS;",
	}];
	Sync::embedded(FILES).run(&db).await.expect("initial sync");

	Sync::embedded(&[])
		.allow_empty_prune(true)
		.run(&db)
		.await
		.expect("opt-in empty prune should succeed");

	let mut resp = db.query("SELECT key FROM __entity WHERE ns = 'sync';").await.expect("query");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert!(rows.is_empty(), "opt-in prune should have removed tracking");
}

#[tokio::test]
async fn sync_embedded_empty_is_fine_when_nothing_is_managed() {
	// The guard must only fire when there is something to lose.
	let db = mem_db().await;
	Sync::embedded(&[]).run(&db).await.expect("empty sync on empty db is a no-op");
}

/// Names of the tables currently defined in the database.
async fn table_names(db: &Surreal<Any>) -> Vec<String> {
	let mut resp = db.query("INFO FOR DB;").await.expect("info for db");
	let info: Option<serde_json::Value> = resp.take(0).expect("take");
	let mut names: Vec<String> = info
		.as_ref()
		.and_then(|v| v.get("tables"))
		.and_then(|v| v.as_object())
		.map(|m| m.keys().cloned().collect())
		.unwrap_or_default();
	names.sort();
	names
}

#[tokio::test]
async fn modules_do_not_prune_each_other() {
	// The core multi-schema guarantee. Before v1 every sync treated *all* tracked
	// entities as its own, so syncing module B against a database that module A had
	// synced deleted A's tables outright.
	let db = mem_db().await;

	static CORE: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/core/account.surql",
		sql: "DEFINE TABLE core_account SCHEMALESS;",
	}];
	static BILLING: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/billing/invoice.surql",
		sql: "DEFINE TABLE billing_invoice SCHEMALESS;",
	}];

	Sync::embedded(CORE).module("core").expect("core").run(&db).await.expect("sync core");
	Sync::embedded(BILLING)
		.module("billing")
		.expect("billing")
		.run(&db)
		.await
		.expect("sync billing");

	let tables = table_names(&db).await;
	assert!(tables.contains(&"core_account".to_string()), "core survived billing: {tables:?}");
	assert!(tables.contains(&"billing_invoice".to_string()), "billing applied: {tables:?}");

	// Re-syncing core must not touch billing either.
	Sync::embedded(CORE).module("core").expect("core").run(&db).await.expect("re-sync core");
	let tables = table_names(&db).await;
	assert!(tables.contains(&"billing_invoice".to_string()), "billing survived core: {tables:?}");
}

#[tokio::test]
async fn a_module_prunes_only_its_own_entities() {
	let db = mem_db().await;

	static CORE: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/core/account.surql",
		sql: "DEFINE TABLE core_account SCHEMALESS;",
	}];
	static BILLING_TWO: &[EmbeddedSchemaFile] = &[
		EmbeddedSchemaFile {
			path: "database/schema/billing/invoice.surql",
			sql: "DEFINE TABLE billing_invoice SCHEMALESS;",
		},
		EmbeddedSchemaFile {
			path: "database/schema/billing/plan.surql",
			sql: "DEFINE TABLE billing_plan SCHEMALESS;",
		},
	];
	static BILLING_ONE: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/billing/invoice.surql",
		sql: "DEFINE TABLE billing_invoice SCHEMALESS;",
	}];

	Sync::embedded(CORE).module("core").expect("core").run(&db).await.expect("sync core");
	Sync::embedded(BILLING_TWO)
		.module("billing")
		.expect("billing")
		.run(&db)
		.await
		.expect("sync billing");

	// Drop billing_plan from the billing module only.
	Sync::embedded(BILLING_ONE)
		.module("billing")
		.expect("billing")
		.run(&db)
		.await
		.expect("prune billing");

	let tables = table_names(&db).await;
	assert!(!tables.contains(&"billing_plan".to_string()), "billing_plan pruned: {tables:?}");
	assert!(tables.contains(&"billing_invoice".to_string()), "billing_invoice kept: {tables:?}");
	assert!(tables.contains(&"core_account".to_string()), "core untouched: {tables:?}");
}

#[tokio::test]
async fn module_metadata_lands_in_its_own_partition() {
	let db = mem_db().await;

	static FILES: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/billing/invoice.surql",
		sql: "DEFINE TABLE billing_invoice SCHEMALESS;",
	}];
	Sync::embedded(FILES).module("billing").expect("billing").run(&db).await.expect("sync");

	let mut resp = db
		.query("SELECT ns FROM __entity WHERE ns = 'sync@billing';")
		.await
		.expect("query billing partition");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert_eq!(rows.len(), 1, "billing file hash must live in sync@billing");

	// And nothing leaked into the default module's partition -- this is what makes
	// a pre-v1 binary blind to named modules.
	let mut resp =
		db.query("SELECT ns FROM __entity WHERE ns = 'sync';").await.expect("query default");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert!(rows.is_empty(), "named module must not write to the default partition");
}

#[tokio::test]
async fn default_module_adopts_pre_v1_rows_without_pruning() {
	// Upgrade path: a database written by 0.7 has its rows at ns='sync'/'schema'.
	// The default module reads exactly those partitions, so an upgraded binary must
	// see the schema as already in sync and issue no REMOVE.
	let db = mem_db().await;

	static FILES: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/legacy.surql",
		sql: "DEFINE TABLE legacy_thing SCHEMALESS;",
	}];
	// Sync with no module: writes the bare partitions, exactly as 0.7 did.
	Sync::embedded(FILES).run(&db).await.expect("initial sync");

	let mut resp = db.query("SELECT ns FROM __entity WHERE ns = 'sync';").await.expect("query");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert_eq!(rows.len(), 1, "pre-v1 rows live in the bare partition");

	// Re-running as the explicit default module is a no-op, not a re-apply+prune.
	Sync::embedded(FILES)
		.module("default")
		.expect("default")
		.run(&db)
		.await
		.expect("explicit default module");

	let tables = table_names(&db).await;
	assert!(tables.contains(&"legacy_thing".to_string()), "table must survive: {tables:?}");
}

#[tokio::test]
async fn sync_embedded_self_heals_catalog_drift() {
	// Catalog drift: an entity tracked in __entity is already missing from the
	// live DB (e.g., a `run_sql REMOVE …` rollout step dropped it but didn't
	// update the catalog). On the next sync, the pruner emits REMOVE for that
	// entity. Without `IF EXISTS` the REMOVE fails with "X does not exist" and
	// halts the whole prune batch. With `IF EXISTS` the prune succeeds and the
	// catalog row is reaped, so drift self-heals on the next sync.

	let db = mem_db().await;

	static WITH_FIELD: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/drift.surql",
		sql: "DEFINE TABLE drift_target SCHEMAFULL;\n\
		      DEFINE FIELD nickname ON drift_target TYPE string;",
	}];
	Sync::embedded(WITH_FIELD).run(&db).await.expect("initial sync");

	// Simulate the rollout step that dropped the field by raw `run_sql`:
	// the live DB no longer has it, but __entity still tracks it.
	db.query("REMOVE FIELD nickname ON drift_target;")
		.await
		.expect("manual remove query")
		.check()
		.expect("manual remove succeeded");

	// Drop the file entirely so the pruner is asked to remove BOTH the table
	// (still present) AND the field (already gone). Pre-fix, the field prune
	// errors and the table is never reached.
	// An empty file set is just a convenient way to make every entity stale here.
	// That trips the empty-prune guard, so opt out of it explicitly: this test is
	// about prune mechanics, not about the guard.
	static EMPTY: &[EmbeddedSchemaFile] = &[];
	Sync::embedded(EMPTY)
		.prune(true)
		.allow_empty_prune(true)
		.run(&db)
		.await
		.expect("pruning sync should self-heal drift, not error on missing field");

	let mut resp = db.query("SELECT * FROM __entity WHERE ns = 'sync';").await.expect("query");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert!(rows.is_empty(), "catalog must be fully reaped: {rows:?}");
}

#[tokio::test]
async fn sync_embedded_dry_run_makes_no_changes() {
	let db = mem_db().await;

	static FILES: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/dry.surql",
		sql: "DEFINE TABLE dry_run_test SCHEMALESS;",
	}];

	Sync::embedded(FILES).dry_run(true).run(&db).await.expect("dry run");

	let mut resp = db.query("SELECT * FROM __entity WHERE ns = 'sync';").await.expect("query");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert!(rows.is_empty(), "dry run must not write tracking records");
}

#[tokio::test]
async fn sync_embedded_does_not_scaffold_files_on_disk() {
	// Regression: embedded/library sync must initialize the metadata tables in the
	// database only — it must NOT write a `database/setup.surql` (or create a
	// `database/` directory) in the process's working directory. That filesystem
	// scaffolding is a CLI concern; under `cargo test` the CWD is the crate root,
	// so the old behavior leaked e.g. `crates/<crate>/database/setup.surql`.
	let _lock = FS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
	let (tmp, _restore) = enter_tempdir();

	let db = mem_db().await;

	static FILES: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/thing.surql",
		sql: "DEFINE TABLE thing SCHEMALESS;",
	}];

	Sync::embedded(FILES).run(&db).await.expect("embedded sync");

	// The `path` above is only a tracking key — nothing should hit the filesystem.
	let leaked = walk_paths(tmp.path());
	assert!(
		!tmp.path().join("database").exists(),
		"embedded sync must not create a `database/` directory on disk; found: {leaked:?}"
	);

	// And the metadata tables must still have been set up in the DB.
	db.query("SELECT * FROM __entity LIMIT 1;")
		.await
		.expect("query __entity")
		.check()
		.expect("__entity must exist after embedded sync");
}

#[tokio::test]
async fn rollout_facade_does_not_scaffold_files_on_disk() {
	// Regression: Rollout::{start,complete,rollback} hardcoded ./database and
	// routed through run_setup, so a code-driven rollout created
	// `./database/setup.surql` in the caller's working directory -- the same leak
	// run_setup_embedded was introduced to avoid for Sync.
	let _lock = FS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
	let (tmp, _restore) = enter_tempdir();

	let db = mem_db().await;
	let spec = add_table_spec("no_scaffold_rollout", "scaffold_probe");

	let rollout = Rollout::new(spec, &[]);
	rollout.start(&db).await.expect("start");
	rollout.complete(&db).await.expect("complete");

	let leaked: Vec<String> = walk_paths(tmp.path());
	assert!(
		!tmp.path().join("database").exists(),
		"code-driven rollout must not create a `database/` directory on disk; found: {leaked:?}"
	);

	// The metadata tables were still initialised in the database.
	db.query("SELECT * FROM __rollout LIMIT 1;")
		.await
		.expect("query __rollout")
		.check()
		.expect("__rollout must exist after a code-driven rollout");
}

#[tokio::test]
async fn rollout_facade_folder_opts_into_scaffolding() {
	// The CLI workflow still wants setup.surql on disk; `.folder()` opts in.
	let _lock = FS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
	let (tmp, _restore) = enter_tempdir();

	let db = mem_db().await;
	let spec = add_table_spec("scaffold_rollout", "scaffold_probe_2");

	Rollout::new(spec, &[]).folder("database").start(&db).await.expect("start with folder");

	assert!(
		tmp.path().join("database").join("setup.surql").exists(),
		"`.folder()` must scaffold setup.surql"
	);
}

#[tokio::test]
async fn rollout_status_is_empty_when_no_rollouts_exist() {
	// `run_status` is a CLI entry point: it calls run_setup, which scaffolds
	// `<folder>/setup.surql` relative to the process cwd. Without the lock and a
	// tempdir this test drops that file into whichever tempdir a concurrent cwd
	// test is using, making *that* test fail instead of this one.
	let _lock = FS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
	let (_tmp, _restore) = enter_tempdir();
	let db = mem_db().await;
	run_status(&db, DEFAULT_ROOT_DIR, None).await.expect("run_status on empty DB");
}

// Regression: run_status crashed (SIGABRT via panic=abort) when called after a rollout
// had been completed, because resp.take::<Vec<serde_json::Value>> panics over HTTP/CBOR
// when rows include SurrealDB datetime fields (started_at, completed_at).
#[tokio::test]
async fn rollout_status_does_not_crash_after_completed_rollout() {
	let _guard = FS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
	let (tmp, _restore) = enter_tempdir();

	let folder = DEFAULT_ROOT_DIR;

	let db = mem_db().await;

	write_schema_file(tmp.path(), "person.surql", "DEFINE TABLE person SCHEMALESS;");
	run_baseline(&db, folder, &Module::default_module()).await.expect("baseline");

	write_schema_file(tmp.path(), "account.surql", "DEFINE TABLE account SCHEMALESS;");
	run_plan(
		folder,
		RolloutPlanOpts {
			name: Some("add_account_status_test".to_string()),
			dry_run: false,
		},
	)
	.await
	.expect("plan");

	let rollout_id = find_latest_rollout_id(tmp.path()).expect("rollout TOML not found");

	run_start(
		&db,
		folder,
		RolloutExecutionOpts {
			selector: Some(rollout_id.clone()),
		},
		&TemplateVars::default(),
	)
	.await
	.expect("start");

	run_complete(
		&db,
		folder,
		RolloutExecutionOpts {
			selector: Some(rollout_id.clone()),
		},
		&TemplateVars::default(),
	)
	.await
	.expect("complete");

	// Targeted lookup — this is what the customer's CI script calls.
	run_status(&db, folder, Some(rollout_id.clone()))
		.await
		.expect("run_status with selector must not crash on completed rollout");

	// List-all form.
	run_status(&db, folder, None)
		.await
		.expect("run_status without selector must not crash on completed rollout");
}

fn write_schema_file(dir: &Path, name: &str, sql: &str) {
	let schema_dir = dir.join("database/schema");
	std::fs::create_dir_all(&schema_dir).expect("create schema dir");
	std::fs::write(schema_dir.join(name), sql).expect("write schema file");
}

#[tokio::test]
async fn rollout_full_lifecycle_via_cli_functions() {
	let _guard = FS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
	let (tmp, _restore) = enter_tempdir();

	let folder = DEFAULT_ROOT_DIR;

	let db = mem_db().await;

	write_schema_file(tmp.path(), "person.surql", "DEFINE TABLE person SCHEMALESS;");
	run_baseline(&db, folder, &Module::default_module()).await.expect("baseline");

	// ns = 'schema' is the internal key used by the managed-entity tracker.
	let mut resp = db.query("SELECT * FROM __entity WHERE ns = 'schema';").await.expect("query");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert!(!rows.is_empty(), "baseline must track managed entities");

	write_schema_file(tmp.path(), "account.surql", "DEFINE TABLE account SCHEMALESS;");
	run_plan(
		folder,
		RolloutPlanOpts {
			name: Some("add_account".to_string()),
			dry_run: false,
		},
	)
	.await
	.expect("plan");

	let rollout_id = find_latest_rollout_id(tmp.path()).expect("rollout TOML not found");

	run_start(
		&db,
		folder,
		RolloutExecutionOpts {
			selector: Some(rollout_id.clone()),
		},
		&TemplateVars::default(),
	)
	.await
	.expect("start");

	assert_eq!(query_rollout_status(&db, &rollout_id).await.as_deref(), Some("ready_to_complete"));

	run_complete(
		&db,
		folder,
		RolloutExecutionOpts {
			selector: Some(rollout_id.clone()),
		},
		&TemplateVars::default(),
	)
	.await
	.expect("complete");

	assert_eq!(query_rollout_status(&db, &rollout_id).await.as_deref(), Some("completed"));
}

#[tokio::test]
async fn rollout_rollback_after_start_via_cli_functions() {
	let _guard = FS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
	let (tmp, _restore) = enter_tempdir();

	let folder = DEFAULT_ROOT_DIR;

	let db = mem_db().await;

	write_schema_file(tmp.path(), "order.surql", "DEFINE TABLE order SCHEMALESS;");
	run_baseline(&db, folder, &Module::default_module()).await.expect("baseline");

	write_schema_file(tmp.path(), "invoice.surql", "DEFINE TABLE invoice SCHEMALESS;");
	run_plan(
		folder,
		RolloutPlanOpts {
			name: Some("add_invoice".to_string()),
			dry_run: false,
		},
	)
	.await
	.expect("plan");

	let rollout_id = find_latest_rollout_id(tmp.path()).expect("rollout TOML not found");

	run_start(
		&db,
		folder,
		RolloutExecutionOpts {
			selector: Some(rollout_id.clone()),
		},
		&TemplateVars::default(),
	)
	.await
	.expect("start");

	run_rollback(
		&db,
		folder,
		RolloutExecutionOpts {
			selector: Some(rollout_id.clone()),
		},
		&TemplateVars::default(),
	)
	.await
	.expect("rollback");

	assert_eq!(query_rollout_status(&db, &rollout_id).await.as_deref(), Some("rolled_back"));
}

#[tokio::test]
async fn seed_from_dir_is_accessible_via_library() {
	let _guard = FS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
	let (tmp, _restore) = enter_tempdir();

	let db = mem_db().await;

	let seed_dir = tmp.path().join("custom_seed");
	std::fs::create_dir_all(&seed_dir).expect("create seed dir");
	std::fs::write(seed_dir.join("01_data.surql"), "CREATE person:alice SET name = 'Alice';")
		.expect("write seed file");

	seed_from_dir(&db, &seed_dir, &TemplateVars::default()).await.expect("seed_from_dir");

	let mut resp = db.query("SELECT name FROM person WHERE id = person:alice;").await.expect("q");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get("name").and_then(|v| v.as_str()), Some("Alice"));
}

fn find_latest_rollout_id(base: &Path) -> Option<String> {
	let rollouts_dir = base.join("database/rollouts");
	let mut entries: Vec<_> = std::fs::read_dir(&rollouts_dir)
		.ok()?
		.filter_map(|e| e.ok())
		.filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("toml"))
		.collect();
	entries.sort_by_key(|e| e.file_name());
	let stem = entries.last()?.path().file_stem()?.to_str()?.to_string();
	Some(stem)
}

async fn query_rollout_status(db: &Surreal<Any>, rollout_id: &str) -> Option<String> {
	let mut resp = db
		.query("SELECT status FROM __rollout WHERE record::id(id) = $id LIMIT 1;")
		.bind(("id", rollout_id.to_string()))
		.await
		.ok()?;
	let rows: Vec<serde_json::Value> = resp.take(0).ok()?;
	rows.into_iter().next()?.get("status")?.as_str().map(str::to_string)
}

// Builds a minimal code-driven RolloutSpec that adds one table via inline SQL,
// with a rollback step that removes it. Uses the builder + step constructors —
// the type system guarantees each step's shape.
fn add_table_spec(id: &str, table: &str) -> RolloutSpec {
	RolloutSpec::builder(id)
		.step(RolloutStep::apply_schema(
			"apply",
			RolloutPhase::Start,
			format!("DEFINE TABLE {table} SCHEMALESS;"),
		))
		.step(RolloutStep::remove_entities(
			"rollback",
			RolloutPhase::Rollback,
			vec![EntityKey {
				kind: EntityKind::Table,
				scope: None,
				name: table.to_string(),
			}],
		))
		.build()
}

#[tokio::test]
async fn rollout_facade_full_lifecycle() {
	let db = mem_db().await;

	static SOURCE: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/order.surql",
		sql: "DEFINE TABLE order SCHEMALESS;",
	}];
	static TARGET: &[EmbeddedSchemaFile] = &[
		EmbeddedSchemaFile {
			path: "database/schema/order.surql",
			sql: "DEFINE TABLE order SCHEMALESS;",
		},
		EmbeddedSchemaFile {
			path: "database/schema/invoice.surql",
			sql: "DEFINE TABLE invoice SCHEMALESS;",
		},
	];

	// Establish baseline using the source schema.
	Sync::embedded(SOURCE).run(&db).await.expect("baseline sync");

	let rollout = Rollout::new(add_table_spec("add_invoice", "invoice"), TARGET);

	rollout.start(&db).await.expect("start");
	assert_eq!(
		query_rollout_status(&db, "add_invoice").await.as_deref(),
		Some("ready_to_complete")
	);

	// The structured status report is available via the facade.
	let report = rollout.status(&db).await.expect("status").expect("record exists");
	assert_eq!(report.status, Some(surrealkit::RolloutStatus::ReadyToComplete));

	rollout.complete(&db).await.expect("complete");
	assert_eq!(query_rollout_status(&db, "add_invoice").await.as_deref(), Some("completed"));
}

#[tokio::test]
async fn rollout_facade_rollback() {
	let db = mem_db().await;

	static SOURCE: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/product.surql",
		sql: "DEFINE TABLE product SCHEMALESS;",
	}];
	static TARGET: &[EmbeddedSchemaFile] = &[
		EmbeddedSchemaFile {
			path: "database/schema/product.surql",
			sql: "DEFINE TABLE product SCHEMALESS;",
		},
		EmbeddedSchemaFile {
			path: "database/schema/variant.surql",
			sql: "DEFINE TABLE variant SCHEMALESS;",
		},
	];

	Sync::embedded(SOURCE).run(&db).await.expect("baseline sync");

	let rollout = Rollout::new(add_table_spec("add_variant", "variant"), TARGET);

	rollout.start(&db).await.expect("start");
	rollout.rollback(&db).await.expect("rollback");
	assert_eq!(query_rollout_status(&db, "add_variant").await.as_deref(), Some("rolled_back"));
}

#[tokio::test]
async fn rollout_facade_blocks_concurrent_rollout() {
	let db = mem_db().await;

	static SOURCE: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/user.surql",
		sql: "DEFINE TABLE user SCHEMALESS;",
	}];
	static TARGET_A: &[EmbeddedSchemaFile] = &[
		EmbeddedSchemaFile {
			path: "database/schema/user.surql",
			sql: "DEFINE TABLE user SCHEMALESS;",
		},
		EmbeddedSchemaFile {
			path: "database/schema/session.surql",
			sql: "DEFINE TABLE session SCHEMALESS;",
		},
	];
	static TARGET_B: &[EmbeddedSchemaFile] = &[
		EmbeddedSchemaFile {
			path: "database/schema/user.surql",
			sql: "DEFINE TABLE user SCHEMALESS;",
		},
		EmbeddedSchemaFile {
			path: "database/schema/token.surql",
			sql: "DEFINE TABLE token SCHEMALESS;",
		},
	];

	Sync::embedded(SOURCE).run(&db).await.expect("baseline sync");

	let rollout_a = Rollout::new(add_table_spec("add_session_x", "session"), TARGET_A);
	rollout_a.start(&db).await.expect("first rollout starts");

	// A second, different rollout must be rejected while the first is active.
	let rollout_b = Rollout::new(add_table_spec("add_token_x", "token"), TARGET_B);
	let err = rollout_b.start(&db).await.expect_err("concurrent rollout must be rejected");
	assert!(err.to_string().contains("active"), "error should mention active rollout: {err}");
}

#[tokio::test]
async fn rollout_abandon_unsticks_active_rollout() {
	let db = mem_db().await;

	static SOURCE: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/widget.surql",
		sql: "DEFINE TABLE widget SCHEMALESS;",
	}];
	static TARGET_A: &[EmbeddedSchemaFile] = &[
		EmbeddedSchemaFile {
			path: "database/schema/widget.surql",
			sql: "DEFINE TABLE widget SCHEMALESS;",
		},
		EmbeddedSchemaFile {
			path: "database/schema/gadget.surql",
			sql: "DEFINE TABLE gadget SCHEMALESS;",
		},
	];

	Sync::embedded(SOURCE).run(&db).await.expect("baseline sync");

	let stuck = Rollout::new(add_table_spec("stuck_rollout", "gadget"), TARGET_A);
	stuck.start(&db).await.expect("start");

	// A second rollout is blocked while the first is active.
	let next = Rollout::new(add_table_spec("next_rollout", "gizmo"), &[]);
	next.start(&db).await.expect_err("blocked by active rollout");

	// Abandoning the stuck rollout frees the lane.
	Rollout::<'_>::abandon(&db, "stuck_rollout").await.expect("abandon");
	assert_eq!(query_rollout_status(&db, "stuck_rollout").await.as_deref(), Some("rolled_back"));

	next.start(&db).await.expect("new rollout can start after abandon");
	assert_eq!(
		query_rollout_status(&db, "next_rollout").await.as_deref(),
		Some("ready_to_complete")
	);
}

// Template variable tests

#[tokio::test]
async fn sync_embedded_with_vars_substitutes_table_name() {
	let db = mem_db().await;

	let mut vars = std::collections::HashMap::new();
	vars.insert("PREFIX".to_string(), "acme".to_string());
	let template_vars = TemplateVars {
		vars,
	};

	static FILES: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/prefixed.surql",
		sql: "DEFINE TABLE ${prefix}_users SCHEMALESS;",
	}];

	Sync::embedded(FILES).vars(template_vars).run(&db).await.expect("sync with vars");

	let mut resp = db.query("INFO FOR DB;").await.expect("INFO FOR DB");
	let info: Option<serde_json::Value> = resp.take(0).expect("take");
	let tables = info
		.as_ref()
		.and_then(|v| v.get("tables"))
		.and_then(|v| v.as_object())
		.map(|m| m.keys().cloned().collect::<Vec<_>>())
		.unwrap_or_default();
	assert!(
		tables.iter().any(|t| t == "acme_users"),
		"expected table 'acme_users' but got: {tables:?}"
	);
}

#[tokio::test]
async fn sync_embedded_with_undefined_var_returns_error() {
	let db = mem_db().await;

	static FILES: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/bad.surql",
		sql: "DEFINE TABLE ${undefined_var} SCHEMALESS;",
	}];

	let err = Sync::embedded(FILES).run(&db).await.expect_err("undefined var must error");

	// Variable name lives in the cause chain (wrapped by file-path context); {:#} prints full
	// chain.
	let chain = format!("{err:#}");
	assert!(
		chain.contains("UNDEFINED_VAR"),
		"error chain should name the missing variable: {chain}"
	);
}

#[tokio::test]
async fn seed_with_vars_substitutes_in_seed_file() {
	let _lock = FS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
	let (tmp, _cwd) = enter_tempdir();

	let db = mem_db().await;

	let seed_dir = tmp.path().join("custom_seed");
	std::fs::create_dir_all(&seed_dir).expect("create seed dir");
	std::fs::write(seed_dir.join("01_data.surql"), "CREATE person:1 SET role = '${role}';")
		.expect("write seed file");

	let mut vars = std::collections::HashMap::new();
	vars.insert("ROLE".to_string(), "admin".to_string());
	let template_vars = TemplateVars {
		vars,
	};

	seed_from_dir(&db, &seed_dir, &template_vars).await.expect("seed_from_dir with vars");

	let mut resp = db.query("SELECT role FROM person WHERE id = person:1;").await.expect("q");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get("role").and_then(|v| v.as_str()), Some("admin"));
}

#[tokio::test]
async fn seed_with_undefined_var_returns_error() {
	let _lock = FS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
	let (tmp, _cwd) = enter_tempdir();

	let db = mem_db().await;

	let seed_dir = tmp.path().join("seed");
	std::fs::create_dir_all(&seed_dir).expect("create seed dir");
	std::fs::write(seed_dir.join("01_bad.surql"), "CREATE x:1 SET y = '${NO_SUCH_VAR}';")
		.expect("write seed file");

	let err = seed_from_dir(&db, &seed_dir, &TemplateVars::default())
		.await
		.expect_err("undefined var must error");
	assert!(
		format!("{err:#}").contains("NO_SUCH_VAR"),
		"error should name the missing variable: {err:#}"
	);
}

#[tokio::test]
async fn rollout_apply_files_step_substitutes_vars() {
	// ApplyFiles reads from disk; rollout_run_sql_step_with_vars covers inline SQL.
	let _lock = FS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
	let (tmp, _cwd) = enter_tempdir();

	let db = mem_db().await;

	let schema_path = tmp.path().join("schema_for_apply.surql");
	std::fs::write(&schema_path, "DEFINE TABLE ${tbl_name} SCHEMALESS;")
		.expect("write schema file");

	let spec = RolloutSpec::builder("apply_files_with_vars")
		.step(RolloutStep::apply_files(
			"apply",
			RolloutPhase::Start,
			vec![schema_path.to_string_lossy().into_owned()],
		))
		.build();

	let mut vars = std::collections::HashMap::new();
	vars.insert("TBL_NAME".to_string(), "applied_table".to_string());
	let template_vars = TemplateVars {
		vars,
	};

	Rollout::new(spec, &[]).vars(template_vars).start(&db).await.expect("apply_files with vars");

	let mut resp = db.query("INFO FOR DB;").await.expect("INFO FOR DB");
	let info: Option<serde_json::Value> = resp.take(0).expect("take");
	let tables = info
		.as_ref()
		.and_then(|v| v.get("tables"))
		.and_then(|v| v.as_object())
		.map(|m| m.keys().cloned().collect::<Vec<_>>())
		.unwrap_or_default();
	assert!(
		tables.iter().any(|t| t == "applied_table"),
		"expected substituted table name in DB: {tables:?}"
	);
}

#[tokio::test]
async fn sync_error_context_includes_offending_file_path() {
	// Error chain must include the file path so operators can locate the bad file.
	let db = mem_db().await;

	static FILES: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/needs_var.surql",
		sql: "DEFINE TABLE ${ABSENT_VAR}_table SCHEMALESS;",
	}];

	let err = Sync::embedded(FILES).run(&db).await.expect_err("undefined var must error");

	let chain = format!("{err:#}");
	assert!(chain.contains("ABSENT_VAR"), "error chain must name the variable: {chain}");
	assert!(
		chain.contains("database/schema/needs_var.surql"),
		"error chain must include the file path: {chain}"
	);
}

#[tokio::test]
async fn rollout_run_sql_step_with_vars() {
	let db = mem_db().await;

	// A run_sql step that references a template variable.
	// SurrealDB creates schemaless tables on demand, so no pre-setup needed.
	let spec = RolloutSpec::builder("rollout_with_var")
		.step(RolloutStep::run_sql(
			"insert_record",
			RolloutPhase::Start,
			"CREATE vartest:1 SET marker = '${marker_value}';",
		))
		.build();

	let mut vars = std::collections::HashMap::new();
	vars.insert("MARKER_VALUE".to_string(), "hello_from_var".to_string());
	let template_vars = TemplateVars {
		vars,
	};

	Rollout::new(spec, &[]).vars(template_vars).start(&db).await.expect("start with var");

	let mut resp = db.query("SELECT marker FROM vartest WHERE id = vartest:1;").await.expect("q");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert_eq!(rows.len(), 1);
	assert_eq!(
		rows[0].get("marker").and_then(|v| v.as_str()),
		Some("hello_from_var"),
		"template variable should have been substituted in run_sql step"
	);
}

/// List every path under `root`, relative, for diagnosing filesystem leaks.
fn walk_paths(root: &Path) -> Vec<String> {
	fn rec(dir: &Path, root: &Path, out: &mut Vec<String>) {
		if let Ok(entries) = std::fs::read_dir(dir) {
			for e in entries.flatten() {
				let p = e.path();
				out.push(p.strip_prefix(root).unwrap_or(&p).display().to_string());
				if p.is_dir() {
					rec(&p, root, out);
				}
			}
		}
	}
	let mut out = Vec::new();
	rec(root, root, &mut out);
	out
}
