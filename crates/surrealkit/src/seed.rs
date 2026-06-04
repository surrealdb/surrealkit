use std::fs;
use std::path::Path;

use anyhow::{Context, Result, anyhow};
use surrealdb::Surreal;
use surrealdb::engine::any::Any;

use crate::constants::seed_dir;
use crate::core::{display, exec_surql};
use crate::variables::TemplateVars;

pub async fn seed(db: &Surreal<Any>, folder: &str, vars: &TemplateVars) -> Result<()> {
	let seed_dir = seed_dir(folder);
	let seed_file = crate::constants::deprecated_seed_surql_path(folder);

	if seed_dir.is_dir() {
		seed_from_dir(db, &seed_dir, vars).await
	} else if seed_file.exists() {
		eprintln!(
			"warning: {}/seed.surql is deprecated and will be removed in v1. \
			Move your seed files into {}/seed/ instead.",
			folder, folder
		);
		let sql = fs::read_to_string(&seed_file)
			.with_context(|| format!("reading {}", display(&seed_file)))?;
		let sql = vars.apply(&sql)?;
		exec_surql(db, &sql).await
	} else {
		Err(anyhow!("no seed found: create {}/seed.surql or a {}/seed/ directory", folder, folder))
	}
}

#[doc(hidden)]
pub async fn seed_from_dir(db: &Surreal<Any>, dir: &Path, vars: &TemplateVars) -> Result<()> {
	let mut files: Vec<_> = fs::read_dir(dir)
		.with_context(|| format!("reading directory {}", display(dir)))?
		.filter_map(|entry| {
			let entry = entry.ok()?;
			let path = entry.path();
			if path.extension().and_then(|e| e.to_str()) == Some("surql") {
				Some(path)
			} else {
				None
			}
		})
		.collect();

	if files.is_empty() {
		return Err(anyhow!("no .surql files found in {}", display(dir)));
	}

	files.sort();

	println!("Seeding from {} ({} files found)", display(dir), files.len());

	for path in &files {
		println!("  executing {}", display(path));
		let sql = fs::read_to_string(path).with_context(|| format!("reading {}", display(path)))?;
		let sql = vars
			.apply(&sql)
			.with_context(|| format!("applying template variables in {}", display(path)))?;
		exec_surql(db, &sql).await.with_context(|| format!("executing {}", display(path)))?;
	}

	println!("Seeded {} files", files.len());

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
}
