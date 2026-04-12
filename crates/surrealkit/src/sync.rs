use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::io::Write;
use std::time::Duration;

use anyhow::{Result, bail};
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::core::exec_surql;
use crate::rollout::{
	acquire_lock, delete_managed_entities, delete_sync_hashes, load_active_rollout_id,
	load_managed_entities, release_lock, upsert_managed_entities,
};
use crate::schema_state::{
	CatalogEntity, EntityKey, SchemaFile, build_catalog_snapshot, collect_schema_files,
	ensure_local_state_dirs, ensure_overwrite, render_remove_sql,
};
use crate::setup::{ensure_metadata_tables, run_setup};

#[derive(Debug, Clone)]
pub struct SyncOpts {
	pub watch: bool,
	pub debounce_ms: u64,
	pub dry_run: bool,
	pub fail_fast: bool,
	pub prune: bool,
	pub allow_shared_prune: bool,
}

pub async fn run_sync(db: &Surreal<Any>, opts: SyncOpts) -> Result<()> {
	run_setup(db).await?;
	ensure_local_state_dirs()?;

	if opts.watch {
		run_sync_once(db, &opts, true).await?;
		println!(
			"Watch mode active ({}ms interval). Waiting for schema changes... (Ctrl+C to stop)",
			opts.debounce_ms.max(250)
		);
		let _ = std::io::stdout().flush();
		loop {
			tokio::select! {
				_ = tokio::signal::ctrl_c() => {
					println!("\nStopping schema watch.");
					break;
				}
				_ = tokio::time::sleep(Duration::from_millis(opts.debounce_ms.max(250))) => {
					if let Err(err) = run_sync_once(db, &opts, true).await {
						if opts.fail_fast {
							return Err(err);
						}
						eprintln!("sync iteration error: {err:#}");
					}
				}
			}
		}
		Ok(())
	} else {
		run_sync_once(db, &opts, false).await
	}
}

async fn run_sync_once(db: &Surreal<Any>, opts: &SyncOpts, watch_mode: bool) -> Result<()> {
	let files = collect_schema_files()?;

	if files.is_empty() && !watch_mode {
		println!("No schema files found in database/schema");
	}

	let inner_opts = SyncInnerOpts {
		dry_run: opts.dry_run,
		fail_fast: opts.fail_fast,
		prune: opts.prune,
		allow_shared_prune: opts.allow_shared_prune,
	};

	let report = sync_files_to_db(db, &files, &inner_opts).await?;

	if watch_mode {
		let has_changes =
			!report.applied.is_empty() || !report.pruned.is_empty() || report.stale_tracking_removed > 0;
		if has_changes {
			if opts.dry_run {
				println!(
					"Change detected (dry-run): {} schema file(s), {} stale entity(ies), {} stale tracking file(s) would be reconciled.",
					report.applied.len() + report.skipped_dry_run.len(),
					report.stale_entity_count,
					report.stale_tracking_removed
				);
			} else {
				println!(
					"Change detected and pushed: {} schema file(s) synced, {} stale entity(ies) pruned, {} stale tracking file(s) removed.",
					report.applied.len(),
					report.pruned.len(),
					report.stale_tracking_removed
				);
			}
			let _ = std::io::stdout().flush();
		}
	} else if report.already_in_sync {
		println!("schema already in sync");
	}

	if !report.failed.is_empty() {
		eprintln!("sync completed with {} apply error(s)", report.failed.len());
	}
	if report.stale_entity_count > 0 && !opts.prune {
		println!(
			"detected {} stale managed entities; rerun without --no-prune to remove",
			report.stale_entity_count
		);
	}

	Ok(())
}

#[derive(Debug, Clone, Default)]
pub struct SyncSchemaOpts {
	pub dry_run: bool,
	pub fail_fast: bool,
	pub prune: bool,
}

#[derive(Debug, Clone, Default)]
pub struct SyncReport {
	pub applied: Vec<String>,
	pub unchanged: Vec<String>,
	pub failed: Vec<String>,
	pub pruned: Vec<String>,
	pub already_in_sync: bool,
}

#[derive(Debug, Clone, Default)]
pub struct DataMigrationReport {
	pub applied: Vec<String>,
	pub skipped: Vec<String>,
	pub reverted: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct MigrateReport {
	pub schema: SyncReport,
	pub data: DataMigrationReport,
}

#[derive(Debug, Clone)]
pub struct AppliedDataMigration {
	pub path: String,
	pub hash: String,
	pub applied_at: String,
}

pub async fn sync_schemas(
	db: &Surreal<Any>,
	files: &[SchemaFile],
	opts: &SyncSchemaOpts,
) -> Result<SyncReport> {
	ensure_metadata_tables(db).await?;

	let inner_opts = SyncInnerOpts {
		dry_run: opts.dry_run,
		fail_fast: opts.fail_fast,
		prune: opts.prune,
		allow_shared_prune: false,
	};

	let inner = sync_files_to_db(db, files, &inner_opts).await?;

	Ok(SyncReport {
		applied: inner.applied,
		unchanged: inner.unchanged,
		failed: inner.failed,
		pruned: inner.pruned,
		already_in_sync: inner.already_in_sync,
	})
}

enum ApplyLimit<'a> {
	All,
	Next,
	UpTo(&'a str),
}

pub async fn run_data_migrations(
	db: &Surreal<Any>,
	files: &[SchemaFile],
) -> Result<DataMigrationReport> {
	apply_data_migrations(db, files, ApplyLimit::All).await
}

pub async fn run_next_data_migration(
	db: &Surreal<Any>,
	files: &[SchemaFile],
) -> Result<DataMigrationReport> {
	apply_data_migrations(db, files, ApplyLimit::Next).await
}

pub async fn run_data_migrations_to(
	db: &Surreal<Any>,
	files: &[SchemaFile],
	target: &str,
) -> Result<DataMigrationReport> {
	if !files.iter().any(|f| f.path == target) {
		bail!("target migration not found: {target}");
	}
	apply_data_migrations(db, files, ApplyLimit::UpTo(target)).await
}

pub async fn revert_last_data_migration(db: &Surreal<Any>) -> Result<DataMigrationReport> {
	ensure_metadata_tables(db).await?;
	let applied = list_applied_data_migrations(db).await?;
	let Some(last) = applied.last() else {
		return Ok(DataMigrationReport::default());
	};
	delete_migration_record(db, &last.path).await?;
	Ok(DataMigrationReport {
		reverted: vec![last.path.clone()],
		..Default::default()
	})
}

pub async fn revert_data_migrations_to(
	db: &Surreal<Any>,
	target: &str,
) -> Result<DataMigrationReport> {
	ensure_metadata_tables(db).await?;
	let applied = list_applied_data_migrations(db).await?;
	if !applied.iter().any(|m| m.path == target) {
		bail!("target migration not found in applied list: {target}");
	}

	let mut reverted = Vec::new();
	for migration in applied.iter().rev() {
		if migration.path == target {
			break;
		}
		delete_migration_record(db, &migration.path).await?;
		reverted.push(migration.path.clone());
	}

	Ok(DataMigrationReport {
		reverted,
		..Default::default()
	})
}

pub async fn reset_data_migrations(db: &Surreal<Any>) -> Result<DataMigrationReport> {
	ensure_metadata_tables(db).await?;
	let applied = list_applied_data_migrations(db).await?;
	let reverted: Vec<String> = applied.iter().rev().map(|m| m.path.clone()).collect();

	if !reverted.is_empty() {
		db.query("DELETE __entity WHERE ns = 'migration';")
			.await?
			.check()?;
	}

	Ok(DataMigrationReport {
		reverted,
		..Default::default()
	})
}

pub async fn list_applied_data_migrations(
	db: &Surreal<Any>,
) -> Result<Vec<AppliedDataMigration>> {
	ensure_metadata_tables(db).await?;
	let mut resp = db
		.query("SELECT key, val FROM __entity WHERE ns = 'migration' ORDER BY key ASC;")
		.await?;
	let rows: Vec<serde_json::Value> = resp.take(0)?;

	let mut out = Vec::with_capacity(rows.len());
	for row in rows {
		let path = row.get("key").and_then(|v| v.as_str()).unwrap_or_default();
		let val = row.get("val");
		let hash = val
			.and_then(|v| v.get("hash"))
			.and_then(|v| v.as_str())
			.unwrap_or_default();
		let applied_at = val
			.and_then(|v| v.get("applied_at"))
			.and_then(|v| v.as_str())
			.unwrap_or_default();
		out.push(AppliedDataMigration {
			path: path.to_string(),
			hash: hash.to_string(),
			applied_at: applied_at.to_string(),
		});
	}
	Ok(out)
}

async fn apply_data_migrations(
	db: &Surreal<Any>,
	files: &[SchemaFile],
	limit: ApplyLimit<'_>,
) -> Result<DataMigrationReport> {
	ensure_metadata_tables(db).await?;
	let tracked = load_migration_hashes(db).await?;

	let mut report = DataMigrationReport::default();

	for file in files {
		if let Some(stored_hash) = tracked.get(&file.path) {
			if *stored_hash != file.hash {
				bail!(
					"migration '{}' was modified after being applied (stored hash: {}, current: {})",
					file.path,
					&stored_hash[..12],
					&file.hash[..12]
				);
			}
			report.skipped.push(file.path.clone());
			continue;
		}

		exec_surql(db, &file.sql).await?;
		store_migration_record(db, &file.path, &file.hash).await?;
		report.applied.push(file.path.clone());

		match limit {
			ApplyLimit::Next => break,
			ApplyLimit::UpTo(target) if file.path == target => break,
			_ => {}
		}
	}

	Ok(report)
}

pub async fn migrate(
	db: &Surreal<Any>,
	schemas: &[SchemaFile],
	data: &[SchemaFile],
	opts: &SyncSchemaOpts,
) -> Result<MigrateReport> {
	let schema = sync_schemas(db, schemas, opts).await?;
	let data = run_data_migrations(db, data).await?;
	Ok(MigrateReport { schema, data })
}

async fn load_migration_hashes(db: &Surreal<Any>) -> Result<BTreeMap<String, String>> {
	let mut resp = db
		.query("SELECT key, val FROM __entity WHERE ns = 'migration';")
		.await?;
	let rows: Vec<serde_json::Value> = resp.take(0)?;

	let mut out = BTreeMap::new();
	for row in rows {
		let path = row.get("key").and_then(|v| v.as_str()).map(str::to_string);
		let hash = row
			.get("val")
			.and_then(|v| v.get("hash"))
			.and_then(|v| v.as_str())
			.map(str::to_string);
		if let (Some(path), Some(hash)) = (path, hash) {
			out.insert(path, hash);
		}
	}
	Ok(out)
}

async fn store_migration_record(db: &Surreal<Any>, path: &str, hash: &str) -> Result<()> {
	db.query(
		"DELETE __entity WHERE ns = 'migration' AND key = $path; \
		 CREATE __entity CONTENT { ns: 'migration', key: $path, val: { hash: $hash, applied_at: time::now() }, updated_at: time::now() };",
	)
	.bind(("path", path.to_string()))
	.bind(("hash", hash.to_string()))
	.await?
	.check()?;
	Ok(())
}

async fn delete_migration_record(db: &Surreal<Any>, path: &str) -> Result<()> {
	db.query("DELETE __entity WHERE ns = 'migration' AND key = $path;")
		.bind(("path", path.to_string()))
		.await?
		.check()?;
	Ok(())
}

#[derive(Debug, Clone)]
struct SyncInnerOpts {
	dry_run: bool,
	fail_fast: bool,
	prune: bool,
	allow_shared_prune: bool,
}

#[derive(Debug, Clone, Default)]
struct SyncInnerReport {
	applied: Vec<String>,
	unchanged: Vec<String>,
	skipped_dry_run: Vec<String>,
	failed: Vec<String>,
	pruned: Vec<String>,
	stale_entity_count: usize,
	stale_tracking_removed: usize,
	already_in_sync: bool,
}

async fn sync_files_to_db(
	db: &Surreal<Any>,
	files: &[SchemaFile],
	opts: &SyncInnerOpts,
) -> Result<SyncInnerReport> {
	let desired_catalog = build_catalog_snapshot(files)?;
	let tracked = load_sync_hashes(db).await?;
	let managed = load_managed_entities(db).await?;

	let file_paths: BTreeSet<String> = files.iter().map(|file| file.path.clone()).collect();
	let removed_paths: Vec<String> =
		tracked.keys().filter(|path| !file_paths.contains(*path)).cloned().collect();

	let mut report = SyncInnerReport::default();
	let mut failed_paths = BTreeSet::new();

	for file in files {
		let tracked_hash = tracked.get(&file.path);
		if tracked_hash == Some(&file.hash) {
			report.unchanged.push(file.path.clone());
			continue;
		}

		if opts.dry_run {
			report.skipped_dry_run.push(file.path.clone());
			continue;
		}

		let sql = ensure_overwrite(&file.sql);
		match exec_surql(db, &sql).await {
			Ok(_) => {
				store_sync_hash(db, &file.path, &file.hash).await?;
				report.applied.push(file.path.clone());
			}
			Err(err) => {
				failed_paths.insert(file.path.clone());
				report.failed.push(file.path.clone());
				if opts.fail_fast {
					return Err(err);
				}
			}
		}
	}

	let effective_entities: Vec<CatalogEntity> = desired_catalog
		.entities
		.iter()
		.filter(|entity| !failed_paths.contains(&entity.source_path))
		.cloned()
		.collect();
	let effective_keys: BTreeSet<EntityKey> =
		effective_entities.iter().map(CatalogEntity::key).collect();

	let stale_records: Vec<_> = managed
		.iter()
		.filter(|record| {
			!effective_keys.contains(&record.entity.key())
				&& !failed_paths.contains(&record.entity.source_path)
		})
		.cloned()
		.collect();
	let stale_entities: Vec<EntityKey> =
		stale_records.iter().map(|record| record.entity.key()).collect();
	let stale_count = stale_entities.len();
	report.stale_entity_count = stale_count;
	let destructive_change = stale_count > 0;

	let shared = if destructive_change {
		detect_shared_db(db).await?
	} else {
		false
	};
	if destructive_change {
		if load_active_rollout_id(db).await?.is_some() {
			bail!("refusing destructive sync while a rollout is active");
		}
		if shared && !opts.allow_shared_prune {
			bail!("database is marked shared; refusing stale prune without --allow-shared-prune");
		}
	}

	if !opts.dry_run {
		upsert_managed_entities(db, &effective_entities, None, "active").await?;
		if !removed_paths.is_empty() {
			delete_sync_hashes(db, &removed_paths).await?;
			report.stale_tracking_removed = removed_paths.len();
		}
	}

	if opts.prune && stale_count > 0 && !opts.dry_run {
		if shared {
			acquire_lock(db, "global").await?;
			let result = prune_managed_entities(db, &stale_entities).await;
			let release = release_lock(db, "global").await;
			result?;
			release?;
		} else {
			prune_managed_entities(db, &stale_entities).await?;
		}
		report.pruned = format_entity_keys(&stale_entities);
	}

	if !opts.dry_run {
		write_meta_from_env(db).await?;
		store_last_sync_meta(db).await?;
	}

	report.already_in_sync = report.applied.is_empty()
		&& report.failed.is_empty()
		&& report.stale_entity_count == 0
		&& removed_paths.is_empty()
		&& report.skipped_dry_run.is_empty();

	Ok(report)
}

fn format_entity_keys(entities: &[EntityKey]) -> Vec<String> {
	entities
		.iter()
		.map(|e| {
			format!(
				"{}:{}:{}",
				e.kind,
				e.scope.as_deref().unwrap_or(""),
				e.name
			)
		})
		.collect()
}

async fn prune_managed_entities(db: &Surreal<Any>, stale_entities: &[EntityKey]) -> Result<()> {
	let sql = render_remove_sql(stale_entities, true)?.join("\n");
	if !sql.trim().is_empty() {
		exec_surql(db, &sql).await?;
	}
	delete_managed_entities(db, stale_entities).await
}

async fn load_sync_hashes(db: &Surreal<Any>) -> Result<BTreeMap<String, String>> {
	let mut resp = db.query("SELECT key, val FROM __entity WHERE ns = 'sync';").await?;
	let rows: Vec<serde_json::Value> = resp.take(0)?;

	let mut out = BTreeMap::new();
	for row in rows {
		let path = row.get("key").and_then(|v| v.as_str()).map(str::to_string);
		let hash =
			row.get("val").and_then(|v| v.get("hash")).and_then(|v| v.as_str()).map(str::to_string);
		if let (Some(path), Some(hash)) = (path, hash) {
			out.insert(path, hash);
		}
	}
	Ok(out)
}

async fn store_sync_hash(db: &Surreal<Any>, path: &str, hash: &str) -> Result<()> {
	db.query(
		"DELETE __entity WHERE ns = 'sync' AND key = $path; \
		 CREATE __entity CONTENT { ns: 'sync', key: $path, val: { hash: $hash }, updated_at: time::now() };",
	)
	.bind(("path", path.to_string()))
	.bind(("hash", hash.to_string()))
	.await?
	.check()?;
	Ok(())
}

async fn detect_shared_db(db: &Surreal<Any>) -> Result<bool> {
	if let Ok(value) = env::var("SURREALKIT_SHARED_DB")
		&& let Some(parsed) = parse_bool(&value)
	{
		return Ok(parsed);
	}

	let mut resp =
		db.query("SELECT val FROM __entity WHERE ns = 'meta' AND key = 'shared' LIMIT 1;").await?;
	let row: Option<serde_json::Value> = resp.take(0)?;
	let shared = row.as_ref().and_then(|v| v.get("val")).and_then(|v| v.as_bool()).unwrap_or(false);
	Ok(shared)
}

async fn write_meta_from_env(db: &Surreal<Any>) -> Result<()> {
	if let Ok(raw_shared) = env::var("SURREALKIT_SHARED_DB")
		&& let Some(shared) = parse_bool(&raw_shared)
	{
		upsert_meta(db, "shared", serde_json::json!(shared)).await?;
	}
	if let Ok(owner) = env::var("SURREALKIT_OWNER")
		&& !owner.trim().is_empty()
	{
		upsert_meta(db, "owner", serde_json::json!(owner)).await?;
	}
	Ok(())
}

async fn store_last_sync_meta(db: &Surreal<Any>) -> Result<()> {
	let ts = OffsetDateTime::now_utc().format(&Rfc3339)?;
	upsert_meta(db, "last_sync", serde_json::json!(ts)).await
}

async fn upsert_meta(db: &Surreal<Any>, key: &str, value: serde_json::Value) -> Result<()> {
	db.query(
		"DELETE __entity WHERE ns = 'meta' AND key = $key; \
		 CREATE __entity CONTENT { ns: 'meta', key: $key, val: $value, updated_at: time::now() };",
	)
	.bind(("key", key.to_string()))
	.bind(("value", value))
	.await?
	.check()?;
	Ok(())
}

fn parse_bool(value: &str) -> Option<bool> {
	match value.trim().to_ascii_lowercase().as_str() {
		"1" | "true" | "yes" | "y" => Some(true),
		"0" | "false" | "no" | "n" => Some(false),
		_ => None,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse_bool_handles_common_spellings() {
		assert_eq!(parse_bool("true"), Some(true));
		assert_eq!(parse_bool("Yes"), Some(true));
		assert_eq!(parse_bool("0"), Some(false));
		assert_eq!(parse_bool("unknown"), None);
	}
}
