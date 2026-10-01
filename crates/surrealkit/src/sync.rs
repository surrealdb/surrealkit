use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::io::Write;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::constants::Layout;
use crate::core::{exec_surql, sha256_hex};
use crate::module::{Module, Partition};
use crate::rollout::{
	ManagedEntityRecord, PartitionWrite, acquire_lock, delete_managed_entities, delete_sync_hashes,
	load_active_rollout_id, load_managed_entities, release_lock, upsert_managed_entities,
	write_partition,
};
use crate::schema_state::{
	CatalogEntity, EntityKey, EntityKind, SchemaFile, build_catalog_snapshot, canonicalise_keys,
	collect_schema_files_at, ensure_local_state_dirs_for, overwrite_sequences, prepare_schema_sql,
	render_remove_sql,
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
	/// Permit a prune that would remove *every* entity this sync manages because
	/// no schema files were found. Off by default: an empty file set almost always
	/// means a misconfigured folder, not an intentional teardown.
	pub allow_empty_prune: bool,
	/// Allow non-DEFINE statements (e.g. INSERT, UPDATE) in schema files.
	/// When set, schema files are not parsed for catalog entity tracking;
	/// they are applied as-is and only file-level hashes are tracked.
	pub allow_all_statements: bool,
	/// Template variables substituted into `.surql` content before execution.
	pub vars: TemplateVars,
	/// Root folder for the database directory (default: `./database`).
	pub folder: String,
	/// The schema module this sync owns. Pruning is scoped to it, so a module
	/// never removes another module's database objects.
	pub module: Module,
	/// When set (via `[typegen] typescript` in `surrealkit.toml`), regenerate
	/// TypeScript types after applying schema changes: into this file when it
	/// ends in `.ts`, otherwise into `index.ts` in this directory.
	pub typegen_ts_out: Option<std::path::PathBuf>,
	/// Optional formatter command (`[typegen] format`) run on the regenerated
	/// TypeScript file.
	pub typegen_ts_format: Option<String>,
}

/// A schema file embedded into the binary at compile time (via [`embed_schema!`](crate::embed_schema))
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
	let layout = Layout::new(opts.folder.clone(), opts.module.clone());
	let files = collect_filesystem_schema_files(
		layout.folder(),
		&layout.schema_dir(),
		layout.module(),
		opts.allow_empty_prune,
	)?;
	run_sync_with_filesystem_sources(db, opts, &layout, &files).await
}

/// Resolve and validate filesystem schema sources before a CLI caller connects.
#[doc(hidden)]
pub fn collect_filesystem_schema_files(
	root: &str,
	schema_dir: &std::path::Path,
	module: &Module,
	allow_empty_prune: bool,
) -> Result<Vec<SchemaFile>> {
	let files = collect_schema_files_at(root, schema_dir)?;
	if files.is_empty() && !allow_empty_prune {
		bail!(
			"refusing filesystem sync: schema_module={} resolved_schema_dir={} source_count=0; \
			 check --folder / SURREALDB_FOLDER and module selection, or pass \
			 --allow-empty-prune if the empty source set is intentional",
			module.name(),
			schema_dir.display()
		);
	}
	Ok(files)
}

/// Run a filesystem sync whose source set was validated before connection.
#[doc(hidden)]
pub async fn run_sync_with_filesystem_sources(
	db: &Surreal<Any>,
	opts: SyncOpts,
	layout: &Layout,
	files: &[SchemaFile],
) -> Result<()> {
	run_setup(db, layout.folder()).await?;
	ensure_local_state_dirs_for(layout)?;

	if opts.watch {
		run_sync_with_files(db, &opts, layout, files, true, Substituted::No).await?;
		log::info!(
			"Watch mode active ({}ms interval). Waiting for schema changes... (Ctrl+C to stop)",
			opts.debounce_ms.max(250)
		);
		let _ = std::io::stdout().flush();
		loop {
			tokio::select! {
				_ = tokio::signal::ctrl_c() => {
					log::info!("\nStopping schema watch.");
					break;
				}
				_ = tokio::time::sleep(Duration::from_millis(opts.debounce_ms.max(250))) => {
					if let Err(err) = run_sync_once(db, &opts, layout, true).await {
						if opts.fail_fast {
							return Err(err);
						}
						log::error!("sync iteration error: {err:#}");
					}
				}
			}
		}
		Ok(())
	} else {
		run_sync_with_files(db, &opts, layout, files, false, Substituted::No).await
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
	allow_empty_prune: bool,
	module: Module,
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
			allow_empty_prune: false,
			module: Module::default_module(),
			allow_all_statements: false,
			dry_run: false,
			vars: TemplateVars::default(),
		}
	}

	/// Apply this slice as a named schema module (default: the unnamed default
	/// module).
	///
	/// Pruning is scoped to the module, so two modules can share one database
	/// without removing each other's objects.
	///
	/// ```no_run
	/// # use surrealkit::{Sync, EmbeddedSchemaFile, Surreal, engine::any::Any};
	/// # async fn run(db: &Surreal<Any>, billing: &'static [EmbeddedSchemaFile]) -> anyhow::Result<()> {
	/// Sync::embedded(billing).module("billing")?.run(db).await?;
	/// # Ok(()) }
	/// ```
	pub fn module(mut self, name: impl Into<String>) -> anyhow::Result<Self> {
		self.module = Module::new(name)?;
		Ok(self)
	}

	/// Remove database objects no longer present in the schema slice (default: `true`).
	pub fn prune(mut self, prune: bool) -> Self {
		self.prune = prune;
		self
	}

	/// Allow a prune that would remove every managed entity because no schema files
	/// were found (default: `false`). See [`SyncOpts::allow_empty_prune`].
	pub fn allow_empty_prune(mut self, allow: bool) -> Self {
		self.allow_empty_prune = allow;
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
			allow_empty_prune: self.allow_empty_prune,
			module: self.module,
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
	let layout = Layout::new(opts.folder.clone(), opts.module.clone());
	// The files above are already substituted. Their catalog, and so the entity
	// keys already in `__entity`, come from the substituted text, so that stays;
	// what must not happen is a second substitution at apply time, which turned a
	// `$${X}` escape into `${X}` and then substituted or rejected it.
	run_sync_with_files(db, opts, &layout, &schema_files, false, Substituted::Yes).await
}

/// Whether a file's SQL has had template variables applied yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Substituted {
	Yes,
	No,
}

async fn run_sync_once(
	db: &Surreal<Any>,
	opts: &SyncOpts,
	layout: &Layout,
	watch_mode: bool,
) -> Result<()> {
	let files = collect_schema_files_at(layout.folder(), &layout.schema_dir())?;
	run_sync_with_files(db, opts, layout, &files, watch_mode, Substituted::No).await
}

async fn run_sync_with_files(
	db: &Surreal<Any>,
	opts: &SyncOpts,
	layout: &Layout,
	files: &[SchemaFile],
	watch_mode: bool,
	substituted: Substituted,
) -> Result<()> {
	let substitute = |sql: &str, source: &str| -> Result<String> {
		match substituted {
			Substituted::Yes => Ok(sql.to_string()),
			Substituted::No => opts
				.vars
				.apply(sql)
				.with_context(|| format!("applying template variables in {source}")),
		}
	};
	let desired_catalog = build_catalog_snapshot(files, opts.allow_all_statements)?;
	let tracked = migrate_legacy_sync_keys(db, layout, files, opts.dry_run).await?;
	let managed = load_managed_entities(db, layout.module(), Some(layout.folder())).await?;
	for sequence in
		changed_sequences(&managed, &desired_catalog.entities, &overwrite_sequences(files))
	{
		log::warn!(
			"{}: the definition of sequence `{}` changed, but SurrealKit applies sequences with IF \
			 NOT EXISTS so an existing one keeps its old settings and position. Redefining it with \
			 OVERWRITE resets the counter to START; do that deliberately, in a rollout run_sql step \
			 or by hand.",
			sequence.source_path,
			sequence.name
		);
	}

	if files.is_empty() && !watch_mode {
		log::info!("No schema files found in {}", layout.schema_dir().display());
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
				log::info!("DRY RUN: would apply {}", file.path);
			}
			synced_paths.insert(file.path.clone());
			continue;
		}

		let sql = substitute(&file.sql, &file.path)?;
		let applied = match prepare_schema_sql(&sql) {
			Ok(prepared) => {
				for warning in &prepared.warnings {
					log::warn!("{}: {warning}", file.path);
				}
				exec_surql(db, &prepared.sql).await
			}
			Err(err) => Err(err.context(format!("preparing {} for apply", file.path))),
		};
		match applied {
			Ok(_) => {
				if !watch_mode {
					log::info!("applied {}", file.path);
				}
				store_sync_hash(db, layout.module(), &file.path, &file.hash).await?;
				synced_paths.insert(file.path.clone());
			}
			Err(err) => {
				apply_errors += 1;
				failed_paths.insert(file.path.clone());
				log::error!("error applying {}: {err:#}", file.path);
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
		// An empty file set makes every managed entity look stale, so an unguarded
		// prune here drops the whole schema. In practice this means the folder is
		// wrong (a mistyped --folder, or running from the wrong directory), not that
		// the user meant to tear the database down.
		// The key migration above rewrites legacy spellings onto the canonical
		// ones. If it matched nothing while files exist and the store was not
		// empty, the match is broken and every tracked key looks removed — which
		// would prune the whole schema. Refuse rather than act on that.
		if opts.prune
			&& !files.is_empty()
			&& !tracked.is_empty()
			&& tracked.keys().all(|key| !files.iter().any(|f| &f.path == key))
			&& !opts.allow_empty_prune
		{
			bail!(
				"refusing to prune: none of the {} tracked file key(s) match the {} schema \
				 file(s) found in {}.\n\
				 That usually means the project folder moved or the tracking keys were \
				 written by a different layout, not that every file was deleted.\n\
				 Check --folder / SURREALDB_FOLDER, or pass --allow-empty-prune to proceed \
				 anyway.",
				tracked.len(),
				files.len(),
				layout.schema_dir().display()
			);
		}
		if opts.prune && files.is_empty() && !opts.allow_empty_prune {
			bail!(
				"refusing to prune all {stale_count} managed entities: no schema files were found{}.\n\
				 This usually means the schema folder is wrong rather than that the schema was deleted.\n\
				 Check --folder / SURREALDB_FOLDER, or pass --allow-empty-prune if you really do want \
				 to remove everything.",
				if layout.folder().is_empty() {
					String::new()
				} else {
					format!(" in {}", layout.schema_dir().display())
				}
			);
		}
	}

	if !opts.dry_run {
		upsert_managed_entities(db, layout.module(), &effective_entities, None, "active").await?;
		if !removed_paths.is_empty() {
			delete_sync_hashes(db, layout.module(), &removed_paths).await?;
		}
	}

	let mut pruned_count = 0usize;
	if opts.prune && stale_count > 0 {
		let remove_sql = render_remove_sql(&stale_entities, true)?;
		if opts.dry_run {
			if !watch_mode {
				log::info!("DRY RUN: would prune {} stale managed entities", remove_sql.len());
				for stmt in &remove_sql {
					log::info!("  {}", stmt);
				}
			}
		} else if shared {
			let lock = acquire_lock(db, layout.module(), "global").await?;
			let result = prune_managed_entities(db, layout.module(), &stale_entities).await;
			let release = release_lock(db, &lock).await;
			match (result, release) {
				(Err(err), _) => return Err(err),
				(Ok(_), Err(err)) => return Err(err),
				(Ok(()), Ok(())) => {}
			}
			pruned_count = stale_count;
		} else {
			prune_managed_entities(db, layout.module(), &stale_entities).await?;
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
				log::info!("DRY RUN: would run {} operation(s)", pending_operations.len());
			}
		} else {
			for op in &pending_operations {
				// Operations come from the unsubstituted file on the filesystem path,
				// so they need the same substitution the file apply got.
				let sql = substitute(&op.sql, &op.source_path)?;
				match exec_surql(db, &sql).await {
					Ok(_) => {
						if !watch_mode {
							log::info!("ran operation from {}", op.source_path);
						}
					}
					Err(err) => {
						apply_errors += 1;
						log::error!("error running operation from {}: {err:#}", op.source_path);
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
	if let Some(ts_out) = &opts.typegen_ts_out
		&& !opts.dry_run
	{
		let ts_path = crate::typegen::typescript_file(ts_out);
		if has_changes || !ts_path.exists() {
			match crate::typegen::generate(db).await {
				Ok(doc) => match crate::typegen::write_typescript_formatted(
					&doc,
					&ts_path,
					opts.typegen_ts_format.as_deref(),
				) {
					Ok(path) => log::info!("typegen: wrote {}", path.display()),
					Err(err) => log::error!("typegen: failed to write types: {err:#}"),
				},
				Err(err) => log::error!("typegen: failed to introspect schema: {err:#}"),
			}
		}
	}

	if watch_mode {
		if has_changes {
			if opts.dry_run {
				log::info!(
					"Change detected (dry-run): {} schema file(s), {} stale entity(ies), {} stale tracking file(s) would be reconciled.",
					changed_count,
					stale_count,
					removed_paths.len()
				);
			} else {
				log::info!(
					"Change detected and pushed: {} schema file(s) synced, {} stale entity(ies) pruned, {} stale tracking file(s) removed.",
					changed_count,
					pruned_count,
					removed_paths.len()
				);
			}
			let _ = std::io::stdout().flush();
		}
	} else if changed_count == 0 && removed_paths.is_empty() && stale_count == 0 {
		log::info!("schema already in sync");
	}

	if apply_errors > 0 {
		log::error!("sync completed with {} apply error(s)", apply_errors);
	}
	if stale_count > 0 && !opts.prune {
		log::info!(
			"detected {} stale managed entities; rerun without --no-prune to remove",
			stale_count
		);
	}

	Ok(())
}

async fn prune_managed_entities(
	db: &Surreal<Any>,
	module: &Module,
	stale_entities: &[EntityKey],
) -> Result<()> {
	let sql = render_remove_sql(stale_entities, true)?.join("\n");
	if !sql.trim().is_empty() {
		exec_surql(db, &sql).await?;
	}
	delete_managed_entities(db, module, stale_entities).await
}

/// Load tracked file hashes, rewriting any pre-1.0.0-beta.2 keys to the
/// folder-relative form first.
///
/// Keys used to be working-directory-relative, so the same project tracked under
/// `database/schema/a.surql` locally and `/database/schema/a.surql` in a
/// container. Left alone, every file would look new *and* every old key would
/// look removed, which is a schema-wide prune. Match legacy keys by path suffix,
/// migrate them in place, and say how many moved.
async fn migrate_legacy_sync_keys(
	db: &Surreal<Any>,
	layout: &Layout,
	files: &[SchemaFile],
	dry_run: bool,
) -> Result<BTreeMap<String, String>> {
	let stored = load_sync_hashes(db, layout.module()).await?;
	let canonical: Vec<String> = files.iter().map(|f| f.path.clone()).collect();
	let (migrated, re_keyed) = canonicalise_keys(&stored, &canonical);

	if re_keyed.is_empty() {
		return Ok(migrated);
	}

	// A matcher that finds nothing while the store is non-empty means the matcher
	// is broken, not that every file was deleted. Guarded at the prune site.
	log::info!(
		"re-keyed {} tracked file(s) to folder-relative paths (e.g. {} -> {})",
		re_keyed.len(),
		re_keyed[0].0,
		re_keyed[0].1
	);
	if !dry_run {
		for (legacy, target) in &re_keyed {
			let hash = migrated.get(target).cloned().unwrap_or_default();
			delete_sync_hashes(db, layout.module(), std::slice::from_ref(legacy)).await?;
			store_sync_hash(db, layout.module(), target, &hash).await?;
		}
	}
	Ok(migrated)
}

pub(crate) async fn load_sync_hashes(
	db: &Surreal<Any>,
	module: &Module,
) -> Result<BTreeMap<String, String>> {
	let mut resp = db
		.query("SELECT key, val FROM __entity WHERE ns = $ns;")
		.bind(("ns", module.partition(Partition::Sync)))
		.await?;
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

async fn store_sync_hash(db: &Surreal<Any>, module: &Module, path: &str, hash: &str) -> Result<()> {
	db.query(
		"DELETE __entity WHERE ns = $ns AND key = $path; \
		 CREATE __entity CONTENT { ns: $ns, key: $path, val: { hash: $hash }, updated_at: time::now() };",
	)
	.bind(("ns", module.partition(Partition::Sync)))
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
	// In place, not delete-then-create: those were two transactions, so a failed
	// create lost the row.
	let rows = BTreeMap::from([(key.to_string(), value)]);
	write_partition(db, Partition::Meta.as_str(), &rows, PartitionWrite::Merge).await
}

/// Managed sequences whose definition changed, apart from those the files now
/// define with an explicit `OVERWRITE` (applying those does reset them, and
/// `prepare_schema_sql` warns about that instead).
fn changed_sequences<'a>(
	managed: &[ManagedEntityRecord],
	desired: &'a [CatalogEntity],
	overwritten: &BTreeSet<String>,
) -> Vec<&'a CatalogEntity> {
	let previous: BTreeMap<EntityKey, &str> = managed
		.iter()
		.filter(|record| record.entity.kind == EntityKind::Sequence)
		.map(|record| (record.entity.key(), record.entity.statement_hash.as_str()))
		.collect();
	desired
		.iter()
		.filter(|entity| entity.kind == EntityKind::Sequence && !overwritten.contains(&entity.name))
		.filter(|entity| {
			previous.get(&entity.key()).is_some_and(|hash| *hash != entity.statement_hash)
		})
		.collect()
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
	use std::fs;

	use surrealdb::engine::any::connect;
	use surrealdb::opt::Config;
	use surrealdb::opt::capabilities::Capabilities;

	use super::*;

	#[test]
	fn parse_bool_handles_common_spellings() {
		assert_eq!(parse_bool("true"), Some(true));
		assert_eq!(parse_bool("Yes"), Some(true));
		assert_eq!(parse_bool("0"), Some(false));
		assert_eq!(parse_bool("unknown"), None);
	}

	#[tokio::test]
	async fn watch_refresh_keeps_using_the_resolved_custom_layout() {
		let temp = tempfile::TempDir::new().expect("tempdir");
		let folder = temp.path().join("database");
		let schema_dir = folder.join("custom/core");
		fs::create_dir_all(&schema_dir).expect("custom schema dir");
		let source = schema_dir.join("001_core.surql");
		fs::write(&source, "DEFINE TABLE before_refresh SCHEMALESS;\n").expect("initial schema");

		let module = Module::new("core").expect("module");
		let folder = folder.to_string_lossy().into_owned();
		let layout = Layout::with_schema_dir(&folder, module.clone(), &schema_dir);
		let opts = SyncOpts {
			folder,
			module,
			..SyncOpts::default()
		};

		let cfg = Config::new().capabilities(Capabilities::all());
		let db = connect(("mem://", cfg)).await.expect("connect mem");
		db.use_ns("watch_layout").use_db("watch_layout").await.expect("select namespace");
		let files =
			collect_filesystem_schema_files(layout.folder(), &schema_dir, layout.module(), false)
				.expect("preflight custom path");
		run_sync_with_filesystem_sources(&db, opts.clone(), &layout, &files)
			.await
			.expect("initial custom-path sync");

		fs::write(
			&source,
			"DEFINE TABLE before_refresh SCHEMALESS;\n\
			 DEFINE TABLE after_refresh SCHEMALESS;\n",
		)
		.expect("updated schema");
		run_sync_once(&db, &opts, &layout, true).await.expect("watch refresh");

		let mut response = db.query("INFO FOR DB;").await.expect("database info");
		let info: Option<serde_json::Value> = response.take(0).expect("take database info");
		let tables = info.as_ref().and_then(|value| value.get("tables")).expect("tables");
		assert!(tables.get("after_refresh").is_some(), "refresh read the wrong layout: {tables}");
	}

	async fn mem() -> Surreal<Any> {
		let cfg = Config::new().capabilities(Capabilities::all());
		let db = connect(("mem://", cfg)).await.expect("connect mem");
		db.use_ns("sync_test").use_db("sync_test").await.expect("select namespace");
		db
	}

	/// A filesystem project with one schema file, and a sync over it.
	struct Project {
		_temp: tempfile::TempDir,
		layout: Layout,
		file: std::path::PathBuf,
		opts: SyncOpts,
	}

	impl Project {
		fn new(sql: &str) -> Self {
			let temp = tempfile::TempDir::new().expect("tempdir");
			let folder = temp.path().join("database").to_string_lossy().into_owned();
			let layout = Layout::new(folder.clone(), Module::default_module());
			fs::create_dir_all(layout.schema_dir()).expect("schema dir");
			let file = layout.schema_dir().join("main.surql");
			fs::write(&file, sql).expect("schema");
			let opts = SyncOpts {
				folder,
				module: Module::default_module(),
				prune: true,
				fail_fast: true,
				..SyncOpts::default()
			};
			Self {
				_temp: temp,
				layout,
				file,
				opts,
			}
		}

		fn write(&self, sql: &str) {
			fs::write(&self.file, sql).expect("schema");
		}

		async fn sync(&self, db: &Surreal<Any>) -> Result<()> {
			let files = collect_filesystem_schema_files(
				self.layout.folder(),
				&self.layout.schema_dir(),
				self.layout.module(),
				true,
			)?;
			run_sync_with_filesystem_sources(db, self.opts.clone(), &self.layout, &files).await
		}
	}

	async fn next_id(db: &Surreal<Any>) -> i64 {
		let mut res =
			db.query("RETURN sequence::nextval('order_no');").await.unwrap().check().unwrap();
		let value: Option<i64> = res.take(0).unwrap();
		value.expect("nextval")
	}

	async fn tables(db: &Surreal<Any>) -> Vec<String> {
		let mut response = db.query("INFO FOR DB;").await.expect("database info");
		let info: Option<serde_json::Value> = response.take(0).expect("take database info");
		let mut names: Vec<String> = info
			.and_then(|v| {
				v.get("tables").and_then(|t| t.as_object()).map(|t| t.keys().cloned().collect())
			})
			.unwrap_or_default();
		names.retain(|name: &String| !name.starts_with("__"));
		names.sort();
		names
	}

	#[tokio::test]
	async fn a_resync_does_not_rewind_a_sequence() {
		// #93. Before, the re-sync sent `DEFINE SEQUENCE OVERWRITE order_no ...`,
		// which SurrealDB 3.3 answers by putting the counter back to START.
		let db = mem().await;
		let project = Project::new("DEFINE SEQUENCE order_no BATCH 1 START 1;\n");
		project.sync(&db).await.expect("first sync");
		assert_eq!([next_id(&db).await, next_id(&db).await, next_id(&db).await], [1, 2, 3]);

		project
			.write("DEFINE SEQUENCE order_no BATCH 1 START 1;\nDEFINE TABLE invoice SCHEMALESS;\n");
		project.sync(&db).await.expect("re-sync");
		assert_eq!(next_id(&db).await, 4, "the re-sync rewound the sequence");

		// An explicit IF NOT EXISTS is left alone too.
		project.write("DEFINE SEQUENCE IF NOT EXISTS order_no BATCH 1 START 1;\n");
		project.sync(&db).await.expect("re-sync");
		assert_eq!(next_id(&db).await, 5);
	}

	#[tokio::test]
	async fn an_explicit_sequence_overwrite_still_repositions_it() {
		// The control for the test above: this engine really does rewind on
		// OVERWRITE, so a passing re-sync test is not an accident of the engine.
		let db = mem().await;
		let project = Project::new("DEFINE SEQUENCE order_no BATCH 1 START 1;\n");
		project.sync(&db).await.expect("first sync");
		assert_eq!(next_id(&db).await, 1);

		project.write("DEFINE SEQUENCE OVERWRITE order_no BATCH 1 START 500;\n");
		project.sync(&db).await.expect("re-sync");
		assert_eq!(next_id(&db).await, 500);
	}

	#[tokio::test]
	async fn a_file_that_does_not_scan_is_neither_applied_nor_pruned() {
		// The old splitter glued `DEFINE TABLE b` into the unterminated string, lost
		// it from the catalog, applied the file, and pruned table b.
		let db = mem().await;
		let project = Project::new("DEFINE TABLE a SCHEMALESS;\nDEFINE TABLE b SCHEMALESS;\n");
		project.sync(&db).await.expect("first sync");
		db.query("CREATE b:keep SET n = 1;").await.unwrap().check().unwrap();

		project.write("DEFINE TABLE a SCHEMALESS;\nDEFINE PARAM $x VALUE 'oops;\nDEFINE TABLE b SCHEMALESS;\nDEFINE TABLE c;\n");
		let err = project.sync(&db).await.expect_err("an unterminated string must stop the sync");
		assert!(format!("{err:#}").contains("line 2, column 23"), "{err:#}");

		assert_eq!(tables(&db).await, vec!["a", "b"]);
		let mut res = db.query("SELECT VALUE n FROM b:keep;").await.unwrap();
		let kept: Vec<i64> = res.take(0).unwrap();
		assert_eq!(kept, vec![1], "the data in b must survive");
	}

	#[tokio::test]
	async fn the_92_regex_param_syncs_and_resyncs_without_pruning() {
		let db = mem().await;
		let sql = "DEFINE PARAM OVERWRITE $PRE_FILTER VALUE /[ \\-_().,\\\\\\/$&+,:;=?@#|'<>^*%!]/;\n\
			DEFINE FUNCTION OVERWRITE fn::preFilter($str: option<string>) {\n\
			\tRETURN IF $str { string::replace($str, $PRE_FILTER, '') };\n\
			};\n\
			DEFINE TABLE after SCHEMALESS;\n";
		let project = Project::new(sql);
		project.sync(&db).await.expect("the #92 file must sync");
		let mut res =
			db.query("RETURN fn::preFilter('a-b c#d;e');").await.unwrap().check().unwrap();
		let cleaned: Option<String> = res.take(0).unwrap();
		assert_eq!(cleaned.as_deref(), Some("abcde"));

		project.write(&format!("{sql}-- touched\n"));
		project.sync(&db).await.expect("re-sync");
		assert_eq!(tables(&db).await, vec!["after"]);
	}

	#[tokio::test]
	async fn operations_get_template_variables_on_the_filesystem_path() {
		// Non-DEFINE statements run as operations, from the unsubstituted file, so
		// `${X}` reached the server verbatim.
		let db = mem().await;
		let mut project = Project::new(
			"DEFINE TABLE item SCHEMALESS;\nUPSERT item:one SET label = '${LABEL}';\n",
		);
		project.opts.allow_all_statements = true;
		project.opts.vars = TemplateVars {
			vars: [("LABEL".to_string(), "from-vars".to_string())].into(),
		};
		project.sync(&db).await.expect("sync");
		let mut res = db.query("SELECT VALUE label FROM item:one;").await.unwrap();
		let labels: Vec<String> = res.take(0).unwrap();
		assert_eq!(labels, vec!["from-vars"]);
	}

	#[tokio::test]
	async fn embedded_sync_substitutes_once() {
		// The escape `$${KEEP}` must reach the database as the literal `${KEEP}`.
		// Substituting twice turned it into `${KEEP}` and then failed on it.
		static FILES: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
			path: "schema/escape.surql",
			sql: "DEFINE PARAM $literal VALUE '$${KEEP}';\nDEFINE PARAM $real VALUE '${REAL}';\n",
		}];
		let db = mem().await;
		Sync::embedded(FILES)
			.vars(TemplateVars {
				vars: [("REAL".to_string(), "yes".to_string())].into(),
			})
			.run(&db)
			.await
			.expect("embedded sync");
		let mut res = db.query("RETURN [$literal, $real];").await.unwrap().check().unwrap();
		let values: Vec<String> = res.take(0).unwrap();
		assert_eq!(values, vec!["${KEEP}".to_string(), "yes".to_string()]);
	}

	#[test]
	fn changed_sequences_skips_unchanged_new_and_explicitly_overwritten_ones() {
		let entity = |name: &str, hash: &str| CatalogEntity {
			kind: EntityKind::Sequence,
			scope: None,
			name: name.to_string(),
			source_path: "schema/s.surql".to_string(),
			statement_hash: hash.to_string(),
			file_hash: "f".to_string(),
		};
		let managed: Vec<ManagedEntityRecord> =
			[entity("same", "1"), entity("changed", "1"), entity("forced", "1")]
				.into_iter()
				.map(|entity| ManagedEntityRecord {
					entity,
					active_rollout_id: None,
					state: "active".to_string(),
				})
				.collect();
		let desired = vec![
			entity("same", "1"),
			entity("changed", "2"),
			entity("forced", "2"),
			entity("new", "9"),
		];
		let overwritten = BTreeSet::from(["forced".to_string()]);
		let changed: Vec<&str> = changed_sequences(&managed, &desired, &overwritten)
			.iter()
			.map(|e| e.name.as_str())
			.collect();
		assert_eq!(changed, vec!["changed"]);
	}

	#[tokio::test]
	async fn sync_regenerates_typescript_at_the_configured_file() {
		let db = mem().await;
		let mut project =
			Project::new("DEFINE TABLE item SCHEMAFULL;\nDEFINE FIELD name ON item TYPE string;\n");
		let ts_file = project.layout.schema_dir().join("../types/database.ts");
		project.opts.typegen_ts_out = Some(ts_file.clone());
		project.sync(&db).await.expect("sync");
		let ts = fs::read_to_string(&ts_file).expect("typegen wrote the named file");
		assert!(ts.contains("export interface Item {"), "{ts}");
		assert!(!ts_file.with_file_name("index.ts").exists());

		// Deleted, it comes back on the next sync even with nothing to apply.
		fs::remove_file(&ts_file).unwrap();
		project.sync(&db).await.expect("idle sync");
		assert!(ts_file.exists());
	}
}
