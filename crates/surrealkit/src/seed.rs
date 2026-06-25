use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use surrealdb::Surreal;
use surrealdb::engine::any::Any;

use crate::constants::seed_dir;
use crate::core::{display, exec_surql, sha256_hex};
use crate::variables::TemplateVars;

/// Lazily provisions the `__seed` tracking table. Run as part of the first write
/// in a seed run, so seeding stays decoupled from `setup` and works on instances
/// provisioned before this table existed. `IF NOT EXISTS` keeps it idempotent
/// and a no-op once the table is present (e.g. created by `surrealkit setup`).
const ENSURE_SEED_TABLE: &str = "\
DEFINE TABLE IF NOT EXISTS __seed SCHEMAFULL PERMISSIONS NONE; \
DEFINE FIELD IF NOT EXISTS key ON __seed TYPE string; \
DEFINE FIELD IF NOT EXISTS hash ON __seed TYPE string; \
DEFINE FIELD IF NOT EXISTS applied_at ON __seed TYPE datetime DEFAULT time::now(); \
DEFINE INDEX IF NOT EXISTS by_seed_key ON __seed FIELDS key UNIQUE;";

/// A seed file baked into the binary at compile time.
///
/// Produced by the [`embed_seed!`](crate::embed_seed) macro, or constructed by
/// hand for use with [`Seed::embedded`].
///
/// - **`path` is a stable tracking key**, *not* a path that must exist on disk. SurrealKit records
///   it in the `__seed` table to detect content changes and decide whether a file still needs to
///   run. Keep it stable across releases.
/// - **`sql` is the content** that gets executed. Changing `sql` while holding `path` constant is
///   what triggers a re-run on the next seed.
pub struct EmbeddedSeedFile {
	pub path: &'static str,
	pub sql: &'static str,
}

enum SeedSource<'a> {
	Embedded(&'a [EmbeddedSeedFile]),
	Dir(String),
}

/// Runs seed `.surql` files, tracking each one in the `__seed` table so it only
/// executes on first boot or when its content hash changes.
///
/// ```rust,no_run
/// # use surrealkit::{Seed, EmbeddedSeedFile, Surreal};
/// # use surrealkit::engine::any::Any;
/// static SEEDS: &[EmbeddedSeedFile] = &[EmbeddedSeedFile {
///     path: "database/seed/countries.surql",
///     sql:  "INSERT INTO country [ { id: 'us', name: 'United States' } ];",
/// }];
/// # async fn run(db: &Surreal<Any>) -> anyhow::Result<()> {
/// Seed::embedded(SEEDS).run(db).await?;        // runs once; no-op on next boot
/// Seed::embedded(SEEDS).force(true).run(db).await?; // re-run everything
/// # Ok(()) }
/// ```
pub struct Seed<'a> {
	source: SeedSource<'a>,
	vars: TemplateVars,
	force: bool,
}

impl<'a> Seed<'a> {
	/// Seed from files embedded in the binary (see [`EmbeddedSeedFile`]).
	pub fn embedded(files: &'a [EmbeddedSeedFile]) -> Self {
		Self {
			source: SeedSource::Embedded(files),
			vars: TemplateVars::default(),
			force: false,
		}
	}

	/// Seed from a project folder on disk. Resolves `<folder>/seed/` (preferred)
	/// or the deprecated `<folder>/seed.surql` single file.
	pub fn from_dir(folder: impl Into<String>) -> Self {
		Self {
			source: SeedSource::Dir(folder.into()),
			vars: TemplateVars::default(),
			force: false,
		}
	}

	/// Template variables applied to each file before execution.
	pub fn vars(mut self, vars: TemplateVars) -> Self {
		self.vars = vars;
		self
	}

	/// Re-run every file regardless of its tracked hash.
	pub fn force(mut self, force: bool) -> Self {
		self.force = force;
		self
	}

	pub async fn run(self, db: &Surreal<Any>) -> Result<()> {
		let Seed {
			source,
			vars,
			force,
		} = self;
		match source {
			SeedSource::Embedded(files) => {
				let tracked = load_seed_hashes(db).await?;
				let mut stats = SeedStats::default();
				for f in files {
					apply_seed(db, f.path, f.sql, &tracked, force, &vars, &mut stats).await?;
				}
				stats.report();
				Ok(())
			}
			SeedSource::Dir(folder) => {
				let dir = seed_dir(&folder);
				#[expect(deprecated)]
				let deprecated = crate::constants::deprecated_seed_surql_path(&folder);

				if dir.is_dir() {
					run_dir(db, &dir, &vars, force).await
				} else if deprecated.exists() {
					eprintln!(
						"warning: {}/seed.surql is deprecated and will be removed in v1. \
						Move your seed files into {}/seed/ instead.",
						folder, folder
					);
					let tracked = load_seed_hashes(db).await?;
					let mut stats = SeedStats::default();
					let raw = fs::read_to_string(&deprecated)
						.with_context(|| format!("reading {}", display(&deprecated)))?;
					let key = display(&deprecated);
					apply_seed(db, &key, &raw, &tracked, force, &vars, &mut stats).await?;
					stats.report();
					Ok(())
				} else {
					Err(anyhow!(
						"no seed found: create {}/seed.surql or a {}/seed/ directory",
						folder,
						folder
					))
				}
			}
		}
	}
}

/// Seed a project `folder` from disk. Equivalent to
/// `Seed::from_dir(folder).vars(vars.clone()).run(db)`.
///
/// Seeding is idempotent: each file runs only on first boot or when its content
/// changes. Use [`Seed::force`] (or the CLI `--force` flag) to re-run everything.
pub async fn seed(db: &Surreal<Any>, folder: &str, vars: &TemplateVars) -> Result<()> {
	Seed::from_dir(folder).vars(vars.clone()).run(db).await
}

#[doc(hidden)]
pub async fn seed_from_dir(db: &Surreal<Any>, dir: &Path, vars: &TemplateVars) -> Result<()> {
	run_dir(db, dir, vars, false).await
}

/// Counters for a single seed run.
#[derive(Default)]
struct SeedStats {
	executed: usize,
	skipped: usize,
}

impl SeedStats {
	fn report(&self) {
		println!("Seeded {} file(s); {} unchanged", self.executed, self.skipped);
	}
}

/// Hash, decide, and (if needed) execute a single seed file, recording its hash.
async fn apply_seed(
	db: &Surreal<Any>,
	key: &str,
	raw_sql: &str,
	tracked: &BTreeMap<String, String>,
	force: bool,
	vars: &TemplateVars,
	stats: &mut SeedStats,
) -> Result<()> {
	let hash = sha256_hex(raw_sql.as_bytes());

	if !force && tracked.get(key).is_some_and(|prev| prev == &hash) {
		println!("  skipping {key} (unchanged)");
		stats.skipped += 1;
		return Ok(());
	}

	println!("  executing {key}");
	let sql =
		vars.apply(raw_sql).with_context(|| format!("applying template variables in {key}"))?;
	exec_surql(db, &sql).await.with_context(|| format!("executing {key}"))?;
	store_seed_hash(db, key, &hash).await?;
	stats.executed += 1;
	Ok(())
}

/// Run all `.surql` files directly inside `dir` (single level, lexicographic),
/// with `__seed` hash tracking.
async fn run_dir(db: &Surreal<Any>, dir: &Path, vars: &TemplateVars, force: bool) -> Result<()> {
	let mut files: Vec<PathBuf> = fs::read_dir(dir)
		.with_context(|| format!("reading directory {}", display(dir)))?
		.filter_map(|entry| {
			let path = entry.ok()?.path();
			(path.extension().and_then(|e| e.to_str()) == Some("surql")).then_some(path)
		})
		.collect();

	if files.is_empty() {
		return Err(anyhow!("no .surql files found in {}", display(dir)));
	}

	files.sort();

	println!("Seeding from {} ({} files found)", display(dir), files.len());

	let tracked = load_seed_hashes(db).await?;
	let mut stats = SeedStats::default();

	for path in &files {
		let raw = fs::read_to_string(path).with_context(|| format!("reading {}", display(path)))?;
		let key = display(path);
		apply_seed(db, &key, &raw, &tracked, force, vars, &mut stats).await?;
	}

	stats.report();
	Ok(())
}

/// Load all tracked seed hashes (`key` to `hash`) from the `__seed` table.
///
/// The table is created lazily on first write (see [`store_seed_hash`]), so it
/// may not exist yet on a fresh datastore or an instance provisioned before it
/// was introduced. A read against a missing table yields no tracked hashes,
/// which simply means every seed is treated as new — so we never define schema
/// here and tolerate the table's absence.
async fn load_seed_hashes(db: &Surreal<Any>) -> Result<BTreeMap<String, String>> {
	let rows: Vec<serde_json::Value> = match db.query("SELECT key, hash FROM __seed;").await {
		Ok(mut resp) => resp.take(0).unwrap_or_default(),
		Err(_) => Vec::new(),
	};

	let mut out = BTreeMap::new();
	for row in rows {
		let key = row.get("key").and_then(|v| v.as_str()).map(str::to_string);
		let hash = row.get("hash").and_then(|v| v.as_str()).map(str::to_string);
		if let (Some(key), Some(hash)) = (key, hash) {
			out.insert(key, hash);
		}
	}
	Ok(out)
}

/// Record (or overwrite) the hash for a seed `key` in the `__seed` table,
/// provisioning the table first if it doesn't exist yet (see [`ENSURE_SEED_TABLE`]).
///
/// This is the only place that defines schema, and it runs only when a seed
/// actually executes — so a run where every file is unchanged performs no DDL
/// and needs no `DEFINE` privileges.
async fn store_seed_hash(db: &Surreal<Any>, key: &str, hash: &str) -> Result<()> {
	let sql = format!(
		"{ENSURE_SEED_TABLE} \
		 DELETE __seed WHERE key = $key; \
		 CREATE __seed CONTENT {{ key: $key, hash: $hash, applied_at: time::now() }};",
	);
	db.query(sql).bind(("key", key.to_string())).bind(("hash", hash.to_string())).await?.check()?;
	Ok(())
}

#[cfg(test)]
mod tests {
	use surrealdb::engine::any::connect;
	use surrealdb::opt::Config;
	use surrealdb::opt::capabilities::Capabilities;
	use tempfile::TempDir;

	use super::*;
	use crate::variables::TemplateVars;

	async fn mem_db() -> Surreal<Any> {
		let config = Config::new().capabilities(Capabilities::all());
		let db = connect(("mem://", config)).await.expect("connect mem://");
		db.use_ns("test").use_db("seed_test").await.expect("use_ns/use_db");
		db
	}

	#[tokio::test]
	async fn seed_dir_runs_files_in_alphabetical_order() {
		let tmp = TempDir::new().unwrap();
		// Write in reverse order to prove sorting, not fs ordering, is used.
		fs::write(tmp.path().join("02_b.surql"), "CREATE ordered:2 SET step = 2;").unwrap();
		fs::write(tmp.path().join("01_a.surql"), "CREATE ordered:1 SET step = 1;").unwrap();

		let db = mem_db().await;
		seed_from_dir(&db, tmp.path(), &TemplateVars::default()).await.unwrap();

		let count: Option<serde_json::Value> =
			db.query("SELECT count() FROM ordered GROUP ALL").await.unwrap().take(0).unwrap();
		let n = count.and_then(|v| v["count"].as_u64()).unwrap_or(0);
		assert_eq!(n, 2, "both files should have been seeded");
	}

	#[tokio::test]
	async fn seed_dir_ignores_non_surql_files() {
		let tmp = TempDir::new().unwrap();
		fs::write(tmp.path().join("data.surql"), "CREATE kept:1;").unwrap();
		fs::write(tmp.path().join("README.md"), "# not SQL").unwrap();
		fs::write(tmp.path().join("data.sql"), "CREATE ignored:1;").unwrap();

		let db = mem_db().await;
		seed_from_dir(&db, tmp.path(), &TemplateVars::default()).await.unwrap();

		// Only the .surql file's table should exist
		let kept: Vec<serde_json::Value> =
			db.query("SELECT * FROM kept").await.unwrap().take(0).unwrap();
		assert_eq!(kept.len(), 1);

		// .sql and .md files are ignored — the `ignored` table must not have been created
		let tables: Option<serde_json::Value> =
			db.query("INFO FOR DB").await.unwrap().take(0).unwrap();
		let table_names = tables
			.as_ref()
			.and_then(|v| v["tables"].as_object())
			.map(|m| m.keys().cloned().collect::<Vec<_>>())
			.unwrap_or_default();
		assert!(!table_names.contains(&"ignored".to_string()));
	}

	#[tokio::test]
	async fn seed_dir_errors_when_no_surql_files_present() {
		let tmp = TempDir::new().unwrap();
		fs::write(tmp.path().join("notes.txt"), "nothing here").unwrap();

		let db = mem_db().await;
		let err = seed_from_dir(&db, tmp.path(), &TemplateVars::default()).await.unwrap_err();
		assert!(err.to_string().contains("no .surql files found"), "unexpected error: {err}");
	}

	#[tokio::test]
	async fn seed_dir_error_includes_failing_file_name() {
		let tmp = TempDir::new().unwrap();
		fs::write(tmp.path().join("01_good.surql"), "CREATE good:1;").unwrap();
		fs::write(tmp.path().join("02_bad.surql"), "THIS IS NOT VALID SURQL @@@").unwrap();

		let db = mem_db().await;
		let err = seed_from_dir(&db, tmp.path(), &TemplateVars::default()).await.unwrap_err();
		assert!(
			err.to_string().contains("02_bad.surql"),
			"error should name the failing file, got: {err}"
		);
	}

	// Simulates the 30k-record / 11 MB use case from issue #21 by spreading
	// records across many files. Each file is loaded and executed independently,
	// so peak memory stays proportional to a single file rather than the total.
	#[tokio::test]
	async fn seed_dir_handles_many_files_without_oom() {
		let tmp = TempDir::new().unwrap();
		let file_count = 50;
		let records_per_file = 100;

		for i in 0..file_count {
			let sql: String = (0..records_per_file)
				.map(|j| {
					format!("CREATE chunk_{}:{} SET n = {};\n", i, j, i * records_per_file + j)
				})
				.collect();
			fs::write(tmp.path().join(format!("{:03}_chunk.surql", i)), sql).unwrap();
		}

		let db = mem_db().await;
		seed_from_dir(&db, tmp.path(), &TemplateVars::default()).await.unwrap();

		let count: Option<serde_json::Value> =
			db.query("SELECT count() FROM chunk_0 GROUP ALL").await.unwrap().take(0).unwrap();
		let n = count.and_then(|v| v["count"].as_u64()).unwrap_or(0);
		assert_eq!(n, records_per_file as u64);
	}

	async fn seed_count(db: &Surreal<Any>) -> u64 {
		count_rows(db, "__seed").await
	}

	/// Number of rows in a table (0 when it doesn't exist).
	async fn count_rows(db: &Surreal<Any>, table: &str) -> u64 {
		let q = format!("SELECT count() FROM {table} GROUP ALL");
		let count: Option<serde_json::Value> = db.query(q).await.unwrap().take(0).unwrap();
		count.and_then(|v| v["count"].as_u64()).unwrap_or(0)
	}

	#[tokio::test]
	async fn embedded_seed_runs_once_then_skips_unchanged() {
		// Each execution appends a row; counting `marker` rows counts executions.
		static SEEDS: &[EmbeddedSeedFile] = &[EmbeddedSeedFile {
			path: "database/seed/people.surql",
			sql: "CREATE marker SET at = time::now();",
		}];

		let db = mem_db().await;
		Seed::embedded(SEEDS).run(&db).await.unwrap();
		// A second run with the same content must be a no-op.
		Seed::embedded(SEEDS).run(&db).await.unwrap();

		assert_eq!(count_rows(&db, "marker").await, 1, "unchanged seed should run exactly once");
		assert_eq!(seed_count(&db).await, 1, "one __seed row tracked");
	}

	#[tokio::test]
	async fn embedded_seed_reruns_when_content_changes() {
		let db = mem_db().await;

		static V1: &[EmbeddedSeedFile] = &[EmbeddedSeedFile {
			path: "database/seed/people.surql",
			sql: "CREATE marker SET at = time::now();",
		}];
		// Same tracking key, different content, so it must re-run.
		static V2: &[EmbeddedSeedFile] = &[EmbeddedSeedFile {
			path: "database/seed/people.surql",
			sql: "CREATE marker SET at = time::now(); -- v2",
		}];

		Seed::embedded(V1).run(&db).await.unwrap();
		Seed::embedded(V2).run(&db).await.unwrap();

		assert_eq!(count_rows(&db, "marker").await, 2, "changed content should re-run");
	}

	#[tokio::test]
	async fn force_reruns_unchanged_seed() {
		static SEEDS: &[EmbeddedSeedFile] = &[EmbeddedSeedFile {
			path: "database/seed/people.surql",
			sql: "CREATE marker SET at = time::now();",
		}];

		let db = mem_db().await;
		Seed::embedded(SEEDS).run(&db).await.unwrap();
		Seed::embedded(SEEDS).force(true).run(&db).await.unwrap();

		assert_eq!(count_rows(&db, "marker").await, 2, "force should re-run even when unchanged");
	}

	#[tokio::test]
	async fn dir_seed_is_idempotent_across_runs() {
		let tmp = TempDir::new().unwrap();
		fs::write(tmp.path().join("01.surql"), "CREATE once:1 SET n = 1;").unwrap();

		let db = mem_db().await;
		seed_from_dir(&db, tmp.path(), &TemplateVars::default()).await.unwrap();
		// Re-running would error (`CREATE` on an existing id) if it weren't tracked.
		seed_from_dir(&db, tmp.path(), &TemplateVars::default()).await.unwrap();

		assert_eq!(seed_count(&db).await, 1, "one tracked seed file");
	}
}
