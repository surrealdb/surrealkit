use std::path::Path;
use std::sync::Mutex;

use surrealdb::Surreal;
use surrealdb::engine::any::{Any, connect};
use surrealdb::opt::Config;
use surrealdb::opt::capabilities::Capabilities;
use surrealkit::schema_state::EntityKey;
use surrealkit::{
	EmbeddedSchemaFile, RolloutExecutionOpts, RolloutPhase, RolloutPlanOpts, RolloutSpec,
	RolloutStep, RolloutStepKind, SyncOpts, run_baseline, run_complete, run_complete_with_spec,
	run_plan, run_rollback, run_rollback_with_spec, run_setup, run_start, run_start_with_spec,
	run_status, run_sync_embedded, run_sync_embedded_with_opts, seed_from_dir,
};

async fn mem_db() -> Surreal<Any> {
	let cfg = Config::new().capabilities(Capabilities::all());
	let db = connect(("mem://", cfg)).await.expect("connect mem://");
	db.use_ns("surrealkit_test").use_db("library_api_test").await.expect("use_ns/use_db");
	db
}

// Tests that change cwd must hold this lock to avoid races.
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
	let _lock = FS_LOCK.lock().unwrap();
	let (_tmp, _cwd) = enter_tempdir();
	let db = mem_db().await;
	run_setup(&db).await.expect("run_setup");

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

	run_sync_embedded(&db, FILES).await.expect("run_sync_embedded");

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

	run_sync_embedded(&db, FILES).await.expect("first sync");
	run_sync_embedded(&db, FILES).await.expect("second sync");

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
	run_sync_embedded(&db, TWO_FILES).await.expect("initial sync");

	let mut resp =
		db.query("SELECT key FROM __entity WHERE ns = 'sync' ORDER BY key;").await.expect("query");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert_eq!(rows.len(), 2);

	static ONE_FILE: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/alpha.surql",
		sql: "DEFINE TABLE alpha SCHEMALESS;",
	}];
	run_sync_embedded_with_opts(
		&db,
		ONE_FILE,
		&SyncOpts {
			watch: false,
			debounce_ms: 0,
			dry_run: false,
			fail_fast: true,
			prune: true,
			allow_shared_prune: false,
		},
	)
	.await
	.expect("pruning sync");

	let mut resp = db.query("SELECT key FROM __entity WHERE ns = 'sync';").await.expect("query");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert_eq!(rows.len(), 1, "beta must be pruned");
	assert_eq!(rows[0].get("key").and_then(|v| v.as_str()), Some("database/schema/alpha.surql"));
}

#[tokio::test]
async fn sync_embedded_dry_run_makes_no_changes() {
	let db = mem_db().await;

	static FILES: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/dry.surql",
		sql: "DEFINE TABLE dry_run_test SCHEMALESS;",
	}];

	run_sync_embedded_with_opts(
		&db,
		FILES,
		&SyncOpts {
			watch: false,
			debounce_ms: 0,
			dry_run: true,
			fail_fast: true,
			prune: true,
			allow_shared_prune: false,
		},
	)
	.await
	.expect("dry run");

	let mut resp = db.query("SELECT * FROM __entity WHERE ns = 'sync';").await.expect("query");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert!(rows.is_empty(), "dry run must not write tracking records");
}

#[tokio::test]
async fn rollout_status_is_empty_when_no_rollouts_exist() {
	let db = mem_db().await;
	run_status(&db, None).await.expect("run_status on empty DB");
}

fn write_schema_file(dir: &Path, name: &str, sql: &str) {
	let schema_dir = dir.join("database/schema");
	std::fs::create_dir_all(&schema_dir).expect("create schema dir");
	std::fs::write(schema_dir.join(name), sql).expect("write schema file");
}

#[tokio::test]
async fn rollout_full_lifecycle_via_library() {
	let _guard = FS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
	let (tmp, _restore) = enter_tempdir();

	let db = mem_db().await;

	write_schema_file(tmp.path(), "person.surql", "DEFINE TABLE person SCHEMALESS;");
	run_baseline(&db).await.expect("baseline");

	// ns = 'schema' is the internal key used by the managed-entity tracker.
	let mut resp = db.query("SELECT * FROM __entity WHERE ns = 'schema';").await.expect("query");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert!(!rows.is_empty(), "baseline must track managed entities");

	write_schema_file(tmp.path(), "account.surql", "DEFINE TABLE account SCHEMALESS;");
	run_plan(RolloutPlanOpts {
		name: Some("add_account".to_string()),
		dry_run: false,
	})
	.await
	.expect("plan");

	let rollout_id = find_latest_rollout_id(tmp.path()).expect("rollout TOML not found");

	run_start(
		&db,
		RolloutExecutionOpts {
			selector: Some(rollout_id.clone()),
		},
	)
	.await
	.expect("start");

	assert_eq!(query_rollout_status(&db, &rollout_id).await.as_deref(), Some("ready_to_complete"));

	run_complete(
		&db,
		RolloutExecutionOpts {
			selector: Some(rollout_id.clone()),
		},
	)
	.await
	.expect("complete");

	assert_eq!(query_rollout_status(&db, &rollout_id).await.as_deref(), Some("completed"));
}

#[tokio::test]
async fn rollout_rollback_after_start_via_library() {
	let _guard = FS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
	let (tmp, _restore) = enter_tempdir();

	let db = mem_db().await;

	write_schema_file(tmp.path(), "order.surql", "DEFINE TABLE order SCHEMALESS;");
	run_baseline(&db).await.expect("baseline");

	write_schema_file(tmp.path(), "invoice.surql", "DEFINE TABLE invoice SCHEMALESS;");
	run_plan(RolloutPlanOpts {
		name: Some("add_invoice".to_string()),
		dry_run: false,
	})
	.await
	.expect("plan");

	let rollout_id = find_latest_rollout_id(tmp.path()).expect("rollout TOML not found");

	run_start(
		&db,
		RolloutExecutionOpts {
			selector: Some(rollout_id.clone()),
		},
	)
	.await
	.expect("start");

	run_rollback(
		&db,
		RolloutExecutionOpts {
			selector: Some(rollout_id.clone()),
		},
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

	seed_from_dir(&db, &seed_dir).await.expect("seed_from_dir");

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

// Builds a minimal RolloutSpec that adds one table via inline SQL (no filesystem).
fn add_table_spec(id: &str, table: &str) -> RolloutSpec {
	RolloutSpec {
		id: id.to_string(),
		name: id.to_string(),
		source_schema_hash: String::new(),
		target_schema_hash: String::new(),
		compatibility: "phased".to_string(),
		renames: vec![],
		steps: vec![
			RolloutStep {
				id: "apply".to_string(),
				phase: RolloutPhase::Start,
				kind: RolloutStepKind::ApplySchema,
				files: vec![],
				sql: Some(format!("DEFINE TABLE {table} SCHEMALESS;")),
				expect: None,
				entities: vec![],
				idempotent: None,
			},
			RolloutStep {
				id: "rollback".to_string(),
				phase: RolloutPhase::Rollback,
				kind: RolloutStepKind::RemoveEntities,
				files: vec![],
				sql: None,
				expect: None,
				entities: vec![EntityKey {
					kind: "table".to_string(),
					scope: None,
					name: table.to_string(),
				}],
				idempotent: None,
			},
		],
	}
}

#[tokio::test]
async fn rollout_with_spec_full_lifecycle() {
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
	run_sync_embedded(&db, SOURCE).await.expect("baseline sync");

	let spec = add_table_spec("add_invoice", "invoice");

	run_start_with_spec(&db, &spec, TARGET).await.expect("start_with_spec");
	assert_eq!(query_rollout_status(&db, &spec.id).await.as_deref(), Some("ready_to_complete"));

	run_complete_with_spec(&db, &spec).await.expect("complete_with_spec");
	assert_eq!(query_rollout_status(&db, &spec.id).await.as_deref(), Some("completed"));
}

#[tokio::test]
async fn rollout_with_spec_rollback() {
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

	run_sync_embedded(&db, SOURCE).await.expect("baseline sync");

	let spec = add_table_spec("add_variant", "variant");

	run_start_with_spec(&db, &spec, TARGET).await.expect("start_with_spec");
	run_rollback_with_spec(&db, &spec).await.expect("rollback_with_spec");
	assert_eq!(query_rollout_status(&db, &spec.id).await.as_deref(), Some("rolled_back"));
}

#[tokio::test]
async fn rollout_with_spec_blocks_concurrent_rollout() {
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

	run_sync_embedded(&db, SOURCE).await.expect("baseline sync");

	let spec_a = add_table_spec("add_session_x", "session");
	run_start_with_spec(&db, &spec_a, TARGET_A).await.expect("first rollout starts");

	// A second, different rollout must be rejected while the first is active.
	let spec_b = add_table_spec("add_token_x", "token");
	let err = run_start_with_spec(&db, &spec_b, TARGET_B)
		.await
		.expect_err("concurrent rollout must be rejected");
	assert!(err.to_string().contains("active"), "error should mention active rollout: {err}");
}
