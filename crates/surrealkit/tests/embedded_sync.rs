use surrealdb::engine::any::connect;
use surrealdb::opt::Config;
use surrealdb::opt::capabilities::Capabilities;

use surrealkit::core::sha256_hex;
use surrealkit::schema_state::SchemaFile;
use surrealkit::sync::{
	SyncSchemaOpts, list_applied_data_migrations, migrate, reset_data_migrations,
	revert_data_migrations_to, revert_last_data_migration, run_data_migrations,
	run_data_migrations_to, run_next_data_migration, sync_schemas,
};

async fn mem_db() -> surrealdb::Surreal<surrealdb::engine::any::Any> {
    let config = Config::new()
        .capabilities(Capabilities::all().with_all_experimental_features_allowed());
    let db = connect(("mem://", config)).await.expect("mem:// connect");
    db.use_ns("test").use_db("test").await.expect("use ns/db");
    db
}

fn schema_file(path: &str, sql: &str) -> SchemaFile {
    SchemaFile {
        path: path.to_string(),
        sql: sql.to_string(),
        hash: sha256_hex(sql.as_bytes()),
    }
}

async fn setup_schema(db: &surrealdb::Surreal<surrealdb::engine::any::Any>) {
    let schemas = surrealkit::embed_migrations!("tests/fixtures/schema");
    sync_schemas(db, &schemas, &SyncSchemaOpts::default()).await.unwrap();
}

#[tokio::test]
async fn embed_explicit_files() {
    let schemas = surrealkit::embed_migrations!(
        "tests/fixtures/schema/tables.surql",
        "tests/fixtures/schema/fields.surql",
        "tests/fixtures/schema/indexes.surql",
    );

    assert_eq!(schemas.len(), 3);
    assert_eq!(schemas[0].path, "tests/fixtures/schema/tables.surql");
    assert!(schemas[0].sql.contains("DEFINE TABLE person"));
    assert_eq!(schemas[0].hash, sha256_hex(schemas[0].sql.as_bytes()));
    assert!(schemas[1].sql.contains("DEFINE FIELD name ON person"));
    assert!(schemas[2].sql.contains("DEFINE INDEX by_email ON person"));
}

#[tokio::test]
async fn embed_directory() {
    let schemas = surrealkit::embed_migrations!("tests/fixtures/schema");

    assert_eq!(schemas.len(), 3);
    assert!(schemas[0].path.contains("fields"));
    assert!(schemas[1].path.contains("indexes"));
    assert!(schemas[2].path.contains("tables"));

    for file in &schemas {
        assert_eq!(file.hash, sha256_hex(file.sql.as_bytes()));
    }
}

#[tokio::test]
async fn embed_directory_is_sorted() {
    let schemas = surrealkit::embed_migrations!("tests/fixtures/schema");
    let paths: Vec<&str> = schemas.iter().map(|s| s.path.as_str()).collect();
    let mut sorted = paths.clone();
    sorted.sort();
    assert_eq!(paths, sorted);
}

#[tokio::test]
async fn embed_data_migrations_accepts_non_define() {
    let data = surrealkit::embed_data_migrations!("tests/fixtures/seed");
    assert_eq!(data.len(), 2);
    assert!(data[0].sql.contains("UPSERT person:admin"));
    assert!(data[1].sql.contains("UPSERT post:welcome"));
}

#[tokio::test]
async fn sync_applies_all_on_first_run() {
    let db = mem_db().await;
    let schemas = vec![
        schema_file("schema/tables.surql", "DEFINE TABLE person SCHEMAFULL;\nDEFINE TABLE post SCHEMAFULL;"),
        schema_file("schema/fields.surql", "DEFINE FIELD name ON person TYPE string;"),
    ];

    let report = sync_schemas(&db, &schemas, &SyncSchemaOpts::default()).await.unwrap();
    assert_eq!(report.applied.len(), 2);
    assert!(report.unchanged.is_empty());
    assert!(!report.already_in_sync);
}

#[tokio::test]
async fn sync_is_idempotent() {
    let db = mem_db().await;
    let schemas = vec![schema_file("schema/tables.surql", "DEFINE TABLE person SCHEMAFULL;")];

    sync_schemas(&db, &schemas, &SyncSchemaOpts::default()).await.unwrap();
    let report = sync_schemas(&db, &schemas, &SyncSchemaOpts::default()).await.unwrap();

    assert!(report.applied.is_empty());
    assert_eq!(report.unchanged.len(), 1);
    assert!(report.already_in_sync);
}

#[tokio::test]
async fn sync_detects_modified_files() {
    let db = mem_db().await;

    let v1 = vec![schema_file("schema/tables.surql", "DEFINE TABLE person SCHEMAFULL;")];
    sync_schemas(&db, &v1, &SyncSchemaOpts::default()).await.unwrap();

    let v2 = vec![schema_file("schema/tables.surql", "DEFINE TABLE person SCHEMAFULL;\nDEFINE TABLE post SCHEMAFULL;")];
    let report = sync_schemas(&db, &v2, &SyncSchemaOpts::default()).await.unwrap();

    assert_eq!(report.applied, vec!["schema/tables.surql"]);
}

#[tokio::test]
async fn sync_dry_run_skips_apply() {
    let db = mem_db().await;
    let schemas = vec![schema_file("schema/tables.surql", "DEFINE TABLE person SCHEMAFULL;")];

    let dry = SyncSchemaOpts { dry_run: true, ..Default::default() };
    let report = sync_schemas(&db, &schemas, &dry).await.unwrap();
    assert!(report.applied.is_empty());

    let report = sync_schemas(&db, &schemas, &SyncSchemaOpts::default()).await.unwrap();
    assert_eq!(report.applied.len(), 1);
}

#[tokio::test]
async fn data_migrations_apply_and_track() {
    let db = mem_db().await;
    setup_schema(&db).await;

    let data = surrealkit::embed_data_migrations!("tests/fixtures/seed");
    let report = run_data_migrations(&db, &data).await.unwrap();

    assert_eq!(report.applied.len(), 2);
    assert!(report.skipped.is_empty());

    let mut resp = db.query("SELECT * FROM person:admin;").await.unwrap();
    let rows: Vec<serde_json::Value> = resp.take(0).unwrap();
    assert_eq!(rows[0].get("name").unwrap().as_str().unwrap(), "Admin");
}

#[tokio::test]
async fn data_migrations_are_idempotent() {
    let db = mem_db().await;
    setup_schema(&db).await;

    let data = surrealkit::embed_data_migrations!("tests/fixtures/seed");
    run_data_migrations(&db, &data).await.unwrap();

    let report = run_data_migrations(&db, &data).await.unwrap();
    assert!(report.applied.is_empty());
    assert_eq!(report.skipped.len(), 2);
}

#[tokio::test]
async fn data_migrations_error_on_modified_file() {
    let db = mem_db().await;
    surrealkit::ensure_metadata_tables(&db).await.unwrap();

    let v1 = vec![schema_file("migrations/001_seed.surql", "UPSERT person:admin SET name = 'Admin';")];
    run_data_migrations(&db, &v1).await.unwrap();

    let v2 = vec![schema_file("migrations/001_seed.surql", "UPSERT person:admin SET name = 'Changed';")];
    let err = run_data_migrations(&db, &v2).await.unwrap_err();
    assert!(err.to_string().contains("modified after being applied"));
}

#[tokio::test]
async fn run_next_applies_exactly_one() {
    let db = mem_db().await;
    setup_schema(&db).await;

    let data = surrealkit::embed_data_migrations!("tests/fixtures/seed");

    let report = run_next_data_migration(&db, &data).await.unwrap();
    assert_eq!(report.applied.len(), 1);
    assert!(report.applied[0].contains("001_seed_users"));

    let report = run_next_data_migration(&db, &data).await.unwrap();
    assert_eq!(report.applied.len(), 1);
    assert!(report.applied[0].contains("002_seed_posts"));

    let report = run_next_data_migration(&db, &data).await.unwrap();
    assert!(report.applied.is_empty());
    assert_eq!(report.skipped.len(), 2);
}

#[tokio::test]
async fn run_to_stops_at_target() {
    let db = mem_db().await;
    setup_schema(&db).await;

    let data = surrealkit::embed_data_migrations!("tests/fixtures/seed");
    let target = &data[0].path;

    let report = run_data_migrations_to(&db, &data, target).await.unwrap();
    assert_eq!(report.applied.len(), 1);
    assert!(report.applied[0].contains("001_seed_users"));

    let mut resp = db.query("SELECT * FROM person:admin;").await.unwrap();
    let rows: Vec<serde_json::Value> = resp.take(0).unwrap();
    assert_eq!(rows.len(), 1);

    let mut resp = db.query("SELECT * FROM post:welcome;").await.unwrap();
    let rows: Vec<serde_json::Value> = resp.take(0).unwrap();
    assert_eq!(rows.len(), 0);
}

#[tokio::test]
async fn run_to_errors_on_unknown_target() {
    let db = mem_db().await;
    surrealkit::ensure_metadata_tables(&db).await.unwrap();

    let data = surrealkit::embed_data_migrations!("tests/fixtures/seed");
    let err = run_data_migrations_to(&db, &data, "nonexistent.surql").await.unwrap_err();
    assert!(err.to_string().contains("target migration not found"));
}

#[tokio::test]
async fn revert_last_removes_most_recent() {
    let db = mem_db().await;
    setup_schema(&db).await;

    let data = surrealkit::embed_data_migrations!("tests/fixtures/seed");
    run_data_migrations(&db, &data).await.unwrap();

    let report = revert_last_data_migration(&db).await.unwrap();
    assert_eq!(report.reverted.len(), 1);
    assert!(report.reverted[0].contains("002_seed_posts"));

    let applied = list_applied_data_migrations(&db).await.unwrap();
    assert_eq!(applied.len(), 1);
    assert!(applied[0].path.contains("001_seed_users"));
}

#[tokio::test]
async fn revert_last_on_empty_is_noop() {
    let db = mem_db().await;
    surrealkit::ensure_metadata_tables(&db).await.unwrap();

    let report = revert_last_data_migration(&db).await.unwrap();
    assert!(report.reverted.is_empty());
}

#[tokio::test]
async fn revert_to_removes_after_target() {
    let db = mem_db().await;
    setup_schema(&db).await;

    let data = surrealkit::embed_data_migrations!("tests/fixtures/seed");
    run_data_migrations(&db, &data).await.unwrap();

    let target = &data[0].path;
    let report = revert_data_migrations_to(&db, target).await.unwrap();
    assert_eq!(report.reverted.len(), 1);
    assert!(report.reverted[0].contains("002_seed_posts"));

    let applied = list_applied_data_migrations(&db).await.unwrap();
    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].path, *target);
}

#[tokio::test]
async fn revert_to_errors_on_unapplied_target() {
    let db = mem_db().await;
    surrealkit::ensure_metadata_tables(&db).await.unwrap();

    let err = revert_data_migrations_to(&db, "nonexistent.surql").await.unwrap_err();
    assert!(err.to_string().contains("target migration not found"));
}

#[tokio::test]
async fn reset_removes_all_tracking() {
    let db = mem_db().await;
    setup_schema(&db).await;

    let data = surrealkit::embed_data_migrations!("tests/fixtures/seed");
    run_data_migrations(&db, &data).await.unwrap();

    let report = reset_data_migrations(&db).await.unwrap();
    assert_eq!(report.reverted.len(), 2);

    let applied = list_applied_data_migrations(&db).await.unwrap();
    assert!(applied.is_empty());
}

#[tokio::test]
async fn reset_on_empty_is_noop() {
    let db = mem_db().await;
    surrealkit::ensure_metadata_tables(&db).await.unwrap();

    let report = reset_data_migrations(&db).await.unwrap();
    assert!(report.reverted.is_empty());
}

#[tokio::test]
async fn revert_then_reapply() {
    let db = mem_db().await;
    setup_schema(&db).await;

    let data = surrealkit::embed_data_migrations!("tests/fixtures/seed");
    run_data_migrations(&db, &data).await.unwrap();

    reset_data_migrations(&db).await.unwrap();

    let report = run_data_migrations(&db, &data).await.unwrap();
    assert_eq!(report.applied.len(), 2);
    assert!(report.skipped.is_empty());
}

#[tokio::test]
async fn list_applied_returns_in_order() {
    let db = mem_db().await;
    setup_schema(&db).await;

    let data = surrealkit::embed_data_migrations!("tests/fixtures/seed");
    run_data_migrations(&db, &data).await.unwrap();

    let applied = list_applied_data_migrations(&db).await.unwrap();
    assert_eq!(applied.len(), 2);
    assert!(applied[0].path < applied[1].path);
    assert!(!applied[0].hash.is_empty());
    assert!(!applied[0].applied_at.is_empty());
}

#[tokio::test]
async fn list_applied_empty_on_fresh_db() {
    let db = mem_db().await;
    let applied = list_applied_data_migrations(&db).await.unwrap();
    assert!(applied.is_empty());
}

#[tokio::test]
async fn migrate_runs_schema_then_data() {
    let db = mem_db().await;
    let schemas = surrealkit::embed_migrations!("tests/fixtures/schema");
    let data = surrealkit::embed_data_migrations!("tests/fixtures/seed");

    let report = migrate(&db, &schemas, &data, &SyncSchemaOpts::default()).await.unwrap();
    assert_eq!(report.schema.applied.len(), 3);
    assert_eq!(report.data.applied.len(), 2);

    let mut resp = db.query("SELECT * FROM person;").await.unwrap();
    let rows: Vec<serde_json::Value> = resp.take(0).unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn migrate_is_idempotent() {
    let db = mem_db().await;
    let schemas = surrealkit::embed_migrations!("tests/fixtures/schema");
    let data = surrealkit::embed_data_migrations!("tests/fixtures/seed");

    migrate(&db, &schemas, &data, &SyncSchemaOpts::default()).await.unwrap();
    let report = migrate(&db, &schemas, &data, &SyncSchemaOpts::default()).await.unwrap();

    assert!(report.schema.already_in_sync);
    assert!(report.data.applied.is_empty());
    assert_eq!(report.data.skipped.len(), 2);
}

#[tokio::test]
async fn migrate_with_empty_data() {
    let db = mem_db().await;
    let schemas = surrealkit::embed_migrations!("tests/fixtures/schema");

    let report = migrate(&db, &schemas, &[], &SyncSchemaOpts::default()).await.unwrap();
    assert_eq!(report.schema.applied.len(), 3);
    assert!(report.data.applied.is_empty());
}

#[tokio::test]
async fn ensure_metadata_tables_idempotent() {
    let db = mem_db().await;
    surrealkit::ensure_metadata_tables(&db).await.unwrap();
    surrealkit::ensure_metadata_tables(&db).await.unwrap();

    let mut resp = db.query("INFO FOR TABLE __entity;").await.unwrap();
    let info: Option<serde_json::Value> = resp.take(0).unwrap();
    assert!(info.is_some());
}
