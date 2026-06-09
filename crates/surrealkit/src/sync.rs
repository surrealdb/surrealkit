use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::io::Write;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::core::{exec_surql, sha256_hex};
use crate::rollout::{
	acquire_lock, delete_managed_entities, delete_sync_hashes, load_active_rollout_id,
	load_managed_entities, release_lock, upsert_managed_entities,
};
use crate::schema_state::{
	CatalogEntity, EntityKey, SchemaFile, build_catalog_snapshot, collect_schema_files,
	ensure_local_state_dirs, ensure_overwrite, render_remove_sql,
};
use crate::setup::{run_setup, run_setup_embedded};
use crate::variables::TemplateVars;

#[derive(Debug, Clone, Default)]
#[doc(hidden)]
pub struct SyncOpts {
	pub watch: bool,
	pub debounce_ms: u64,
	pub dry_run: bool,
	pub fail_fast: bool,
	pub prune: bool,
	pub allow_shared_prune: bool,
	/// Allow non-DEFINE statements (e.g. INSERT, UPDATE) in schema files.
	/// When set, schema files are not parsed for catalog entity tracking;
	/// they are applied as-is and only file-level hashes are tracked.
	pub allow_all_statements: bool,
	/// Template variables substituted into `.surql` content before execution.
	pub vars: TemplateVars,
	/// Root folder for the database directory (default: `./database`).
	pub folder: String,
	/// When set (via `[typegen] typescript` in `surrealkit.toml`), regenerate
	/// TypeScript types into this directory after applying schema changes.
	pub typegen_ts_out: Option<std::path::PathBuf>,
	/// Optional formatter command (`[typegen] format`) run on the regenerated
	/// `index.ts`.
	pub typegen_ts_format: Option<String>,
}

/// A schema file embedded into the binary at compile time (via [`embed_schema!`])
/// or constructed by hand for runtime sync.
///
/// `path` is a **stable tracking key**, not a path that must exist on disk:
/// SurrealKit uses it to identify the file in its metadata tables so it can detect
/// when the content changes and prune files that disappear. Keep it stable across
/// releases — renaming it makes SurrealKit treat the old key as deleted and the new
/// one as added. `sql` is the actual SurrealQL content; changing it (with `path`
/// held constant) is what triggers a re-apply on the next sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmbeddedSchemaFile {
	/// Stable tracking key (typically the source file's relative path).
	pub path: &'static str,
	/// The SurrealQL content applied to the database.
	pub sql: &'static str,
}

#[doc(hidden)]
pub async fn run_sync(db: &Surreal<Any>, opts: SyncOpts) -> Result<()> {
	run_setup(db, &opts.folder).await?;
	ensure_local_state_dirs(&opts.folder)?;

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

/// A fluent builder for applying an embedded schema to a database.
///
/// This is the library entry point for schema sync: it reconciles the database
/// against the supplied [`EmbeddedSchemaFile`] slice, applying changed files and
/// (by default) pruning database objects that are no longer present. It reads
/// nothing from the filesystem and writes no scaffolding files.
///
/// Defaults: `prune = true`, `fail_fast = true`, no template variables.
///
/// ```no_run
/// # use surrealkit::{Sync, EmbeddedSchemaFile, Surreal, engine::any::Any};
/// # async fn run(db: &Surreal<Any>) -> anyhow::Result<()> {
/// static SCHEMA: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
///     path: "database/schema/person.surql",
///     sql: "DEFINE TABLE person SCHEMALESS;",
/// }];
/// Sync::embedded(SCHEMA).run(db).await?;
/// // or, customized:
/// Sync::embedded(SCHEMA).prune(false).run(db).await?;
/// # Ok(()) }
/// ```
#[derive(Debug, Clone)]
pub struct Sync<'a> {
	files: &'a [EmbeddedSchemaFile],
	prune: bool,
	fail_fast: bool,
	allow_shared_prune: bool,
	allow_all_statements: bool,
	dry_run: bool,
	vars: TemplateVars,
}

impl<'a> Sync<'a> {
	/// Build a sync for the given compile-time-embedded schema slice (from
	/// [`embed_schema!`](crate::embed_schema) or constructed by hand).
	pub fn embedded(files: &'a [EmbeddedSchemaFile]) -> Self {
		Self {
			files,
			prune: true,
			fail_fast: true,
			allow_shared_prune: false,
			allow_all_statements: false,
			dry_run: false,
			vars: TemplateVars::default(),
		}
	}

	/// Remove database objects no longer present in the schema slice (default: `true`).
	pub fn prune(mut self, prune: bool) -> Self {
		self.prune = prune;
		self
	}

	/// Stop at the first apply error instead of continuing (default: `true`).
	pub fn fail_fast(mut self, fail_fast: bool) -> Self {
		self.fail_fast = fail_fast;
		self
	}

	/// Permit pruning even when the database appears to be shared (default: `false`).
	pub fn allow_shared_prune(mut self, allow: bool) -> Self {
		self.allow_shared_prune = allow;
		self
	}

	/// Allow non-`DEFINE` statements (e.g. `INSERT`/`UPDATE`) in schema content
	/// (default: `false`). When set, files are applied as-is and only file-level
	/// hashes are tracked.
	pub fn allow_all_statements(mut self, allow: bool) -> Self {
		self.allow_all_statements = allow;
		self
	}

	/// Report what would change without applying anything (default: `false`).
	pub fn dry_run(mut self, dry_run: bool) -> Self {
		self.dry_run = dry_run;
		self
	}

	/// Template variables substituted into `${VAR}` placeholders before execution.
	pub fn vars(mut self, vars: TemplateVars) -> Self {
		self.vars = vars;
		self
	}

	/// Apply the schema to `db`. Idempotent: re-running with unchanged content is a
	/// no-op.
	pub async fn run(self, db: &Surreal<Any>) -> Result<()> {
		let opts = SyncOpts {
			watch: false,
			debounce_ms: 0,
			dry_run: self.dry_run,
			fail_fast: self.fail_fast,
			prune: self.prune,
			allow_shared_prune: self.allow_shared_prune,
			allow_all_statements: self.allow_all_statements,
			vars: self.vars,
			folder: String::new(),
			typegen_ts_out: None,
			typegen_ts_format: None,
		};
		sync_embedded(db, self.files, &opts).await
	}
}

/// Apply embedded schema files using the given options, without touching the
/// filesystem. The `watch` and `folder` fields of `opts` are ignored.
async fn sync_embedded(
	db: &Surreal<Any>,
	files: &[EmbeddedSchemaFile],
	opts: &SyncOpts,
) -> Result<()> {
	run_setup_embedded(db).await?;
	let schema_files: Vec<SchemaFile> = files
		.iter()
		.map(|f| {
			let sql = opts
				.vars
				.apply(f.sql)
				.with_context(|| format!("applying template variables in {}", f.path))?;
			Ok(SchemaFile {
				path: f.path.to_string(),
				// Hash is computed from the raw template so that variable value changes
				// don't invalidate the sync hash (variables are env-specific config).
				hash: sha256_hex(f.sql.as_bytes()),
				sql,
			})
		})
		.collect::<anyhow::Result<Vec<_>>>()?;
	run_sync_with_files(db, opts, &schema_files, false).await
}

async fn run_sync_once(db: &Surreal<Any>, opts: &SyncOpts, watch_mode: bool) -> Result<()> {
	let files = collect_schema_files(&opts.folder)?;
	run_sync_with_files(db, opts, &files, watch_mode).await
}

async fn run_sync_with_files(
	db: &Surreal<Any>,
	opts: &SyncOpts,
	files: &[SchemaFile],
	watch_mode: bool,
) -> Result<()> {
	let desired_catalog = build_catalog_snapshot(files, opts.allow_all_statements)?;
	let tracked = load_sync_hashes(db).await?;
	let managed = load_managed_entities(db).await?;

	if files.is_empty() && !watch_mode {
		println!("No schema files found in {}/schema", opts.folder);
	}

	let file_paths: BTreeSet<String> = files.iter().map(|file| file.path.clone()).collect();
	let removed_paths: Vec<String> =
		tracked.keys().filter(|path| !file_paths.contains(*path)).cloned().collect();

	let mut changed_count = 0usize;
	let mut apply_errors = 0usize;
	let mut synced_paths = BTreeSet::new();
	let mut failed_paths = BTreeSet::new();
	for file in files {
		let tracked_hash = tracked.get(&file.path);
		if tracked_hash == Some(&file.hash) {
			continue;
		}

		changed_count += 1;
		if opts.dry_run {
			if !watch_mode {
				println!("DRY RUN: would apply {}", file.path);
			}
			synced_paths.insert(file.path.clone());
			continue;
		}

		let substituted = opts
			.vars
			.apply(&file.sql)
			.with_context(|| format!("applying template variables in {}", file.path))?;
		let sql = ensure_overwrite(&substituted);
		match exec_surql(db, &sql).await {
			Ok(_) => {
				if !watch_mode {
					println!("applied {}", file.path);
				}
				store_sync_hash(db, &file.path, &file.hash).await?;
				synced_paths.insert(file.path.clone());
			}
			Err(err) => {
				apply_errors += 1;
				failed_paths.insert(file.path.clone());
				eprintln!("error applying {}: {err:#}", file.path);
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
		}
	}

	let mut pruned_count = 0usize;
	if opts.prune && stale_count > 0 {
		let remove_sql = render_remove_sql(&stale_entities, true)?;
		if opts.dry_run {
			if !watch_mode {
				println!("DRY RUN: would prune {} stale managed entities", remove_sql.len());
				for stmt in &remove_sql {
					println!("  {}", stmt);
				}
			}
		} else if shared {
			acquire_lock(db, "global").await?;
			let result = prune_managed_entities(db, &stale_entities).await;
			let release = release_lock(db, "global").await;
			match (result, release) {
				(Err(err), _) => return Err(err),
				(Ok(_), Err(err)) => return Err(err),
				(Ok(()), Ok(())) => {}
			}
			pruned_count = stale_count;
		} else {
			prune_managed_entities(db, &stale_entities).await?;
			pruned_count = stale_count;
		}
	}

	// Run operations (non-DEFINE statements) after all entities have been applied.
	let pending_operations: Vec<_> = desired_catalog
		.operations
		.iter()
		.filter(|op| !failed_paths.contains(&op.source_path))
		.collect();
	if !pending_operations.is_empty() {
		if opts.dry_run {
			if !watch_mode {
				println!("DRY RUN: would run {} operation(s)", pending_operations.len());
			}
		} else {
			for op in &pending_operations {
				match exec_surql(db, &op.sql).await {
					Ok(_) => {
						if !watch_mode {
							println!("ran operation from {}", op.source_path);
						}
					}
					Err(err) => {
						apply_errors += 1;
						eprintln!("error running operation from {}: {err:#}", op.source_path);
						if opts.fail_fast {
							return Err(err);
						}
					}
				}
			}
		}
	}

	if !opts.dry_run {
		write_meta_from_env(db).await?;
		store_last_sync_meta(db).await?;
	}

	let has_changes = changed_count > 0 || stale_count > 0 || !removed_paths.is_empty();

	// Regenerate TypeScript types when configured. Gate on actual changes (or a
	// missing output file) so idle watch ticks don't re-introspect every cycle.
	if let Some(ts_dir) = &opts.typegen_ts_out
		&& !opts.dry_run
	{
		let ts_path = ts_dir.join("index.ts");
		if has_changes || !ts_path.exists() {
			match crate::typegen::generate(db).await {
				Ok(doc) => match crate::typegen::write_typescript_formatted(
					&doc,
					ts_dir,
					opts.typegen_ts_format.as_deref(),
				) {
					Ok(path) => println!("typegen: wrote {}", path.display()),
					Err(err) => eprintln!("typegen: failed to write types: {err:#}"),
				},
				Err(err) => eprintln!("typegen: failed to introspect schema: {err:#}"),
			}
		}
	}

	if watch_mode {
		if has_changes {
			if opts.dry_run {
				println!(
					"Change detected (dry-run): {} schema file(s), {} stale entity(ies), {} stale tracking file(s) would be reconciled.",
					changed_count,
					stale_count,
					removed_paths.len()
				);
			} else {
				println!(
					"Change detected and pushed: {} schema file(s) synced, {} stale entity(ies) pruned, {} stale tracking file(s) removed.",
					changed_count,
					pruned_count,
					removed_paths.len()
				);
			}
			let _ = std::io::stdout().flush();
		}
	} else if changed_count == 0 && removed_paths.is_empty() && stale_count == 0 {
		println!("schema already in sync");
	}

	if apply_errors > 0 {
		eprintln!("sync completed with {} apply error(s)", apply_errors);
	}
	if stale_count > 0 && !opts.prune {
		println!(
			"detected {} stale managed entities; rerun without --no-prune to remove",
			stale_count
		);
	}

	Ok(())
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
