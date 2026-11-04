//! A database's compatibility level, which SurrealDB 4.0 introduced.
//!
//! A database is at level 3 or 4. At level 4 a `SCHEMAFULL` table that holds
//! records refuses a field change its records might not fit unless the
//! statement says what they become (`MIGRATE EXISTING ROWS`), and `REMOVE FIELD`
//! clears the values its records held. A database a 4.0 datastore creates
//! starts at level 4; one created by 3.x stays at level 3 after the upgrade,
//! until someone raises it.
//!
//! `compat status` shows the level and what SurrealDB is doing to each table.
//! `compat raise` raises a database to level 4, waits for SurrealDB to check
//! every `SCHEMAFULL` table against its definitions, and points at the schema
//! file to fix for each field whose records do not fit.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use serde_json::Value;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;

use crate::rollout::migrations::progress;
use crate::schema_state::collect_schema_files;

/// How often `compat raise --wait` reads the tables' progress.
const POLL: Duration = Duration::from_secs(2);

/// Options for [`run_compat_raise`].
#[derive(Debug, Clone)]
#[doc(hidden)]
pub struct CompatRaiseOpts {
	/// Wait for SurrealDB to check every table, and report what does not fit.
	pub wait: bool,
	/// How long to wait.
	pub timeout: Duration,
	/// The operator has confirmed the change.
	pub yes: bool,
}

async fn db_info(db: &Surreal<Any>) -> Result<Value> {
	crate::rollout::retry_conflicts("reading the database", || async {
		crate::rollout::info_json(db, "INFO FOR DB;", None).await
	})
	.await
}

async fn table_info(db: &Surreal<Any>, table: &str) -> Result<Value> {
	crate::rollout::retry_conflicts(&format!("reading table `{table}`"), || async {
		crate::rollout::info_json(db, "INFO FOR TABLE $table;", Some(table)).await
	})
	.await
}

/// The user tables of a database, with whether each is `SCHEMAFULL`.
fn tables(info: &Value) -> BTreeMap<String, bool> {
	info.get("tables")
		.and_then(Value::as_object)
		.map(|tables| {
			tables
				.iter()
				.filter(|(name, _)| !name.starts_with("__"))
				.map(|(name, definition)| {
					let schemafull = definition.as_str().is_some_and(|text| {
						text.split_whitespace().any(|word| word.eq_ignore_ascii_case("SCHEMAFULL"))
					});
					(crate::migrate::unquote(name), schemafull)
				})
				.collect()
		})
		.unwrap_or_default()
}

/// Log the server's version and the database's compatibility level.
pub(crate) async fn log_server(db: &Surreal<Any>) {
	let version = db.version().await.map(|v| v.to_string()).unwrap_or_else(|_| "?".into());
	match db_info(db).await {
		Ok(info) => log::info!(
			"SurrealDB {version}; this database is at compatibility level {}.",
			crate::migrate::compatibility(&info)
		),
		Err(err) => log::warn!("could not read this database's compatibility level: {err:#}"),
	}
}

/// Show the compatibility level, and for each table the migrations it has run
/// and what SurrealDB is still doing to it.
#[doc(hidden)]
pub async fn run_compat_status(db: &Surreal<Any>) -> Result<()> {
	log_server(db).await;
	let info = db_info(db).await?;
	for (table, schemafull) in tables(&info) {
		let table_info = table_info(db, &table).await?;
		let Some(section) = table_info.get("migrations") else {
			continue;
		};
		let names: Vec<&str> = section
			.get("migrations")
			.and_then(Value::as_array)
			.into_iter()
			.flatten()
			.filter_map(|run| run.get("name").and_then(Value::as_str))
			.collect();
		let guaranteed: Vec<&str> = section
			.get("guaranteed")
			.and_then(Value::as_array)
			.into_iter()
			.flatten()
			.filter_map(Value::as_str)
			.collect();
		log::info!("");
		log::info!(
			"table `{table}`{}",
			if schemafull {
				" (SCHEMAFULL)"
			} else {
				""
			}
		);
		if !names.is_empty() {
			log::info!("  migrations run: {}", names.join(", "));
		}
		if !guaranteed.is_empty() {
			log::info!("  every record fits: {}", guaranteed.join(", "));
		}
		let now = progress(&table, &table_info);
		for line in now.running {
			log::info!("  running: {line}");
		}
		for line in now.failed {
			log::warn!("  {line}");
		}
	}
	Ok(())
}

/// Raise this database to compatibility level 4.
#[doc(hidden)]
pub async fn run_compat_raise(
	db: &Surreal<Any>,
	folder: &str,
	opts: CompatRaiseOpts,
) -> Result<()> {
	let info = db_info(db).await?;
	let level = crate::migrate::compatibility(&info);
	if level >= 4 {
		log::info!("This database is already at compatibility level {level}.");
		return Ok(());
	}
	if !opts.yes {
		bail!(
			"raising this database to compatibility level 4 changes what SurrealDB does to records \
			 that already exist: a SCHEMAFULL table then refuses a field change its records might \
			 not fit, unless the DEFINE FIELD has MIGRATE EXISTING ROWS, and REMOVE FIELD clears \
			 the values the field held. Raise it only once every node runs SurrealDB 4.0. Re-run \
			 with --yes to go ahead"
		);
	}
	crate::rollout::retry_conflicts("raising the compatibility level", || {
		crate::core::exec_surql(db, "ALTER DATABASE COMPATIBILITY 4;")
	})
	.await?;
	log::info!("Raised this database to compatibility level 4.");
	if !opts.wait {
		log::info!(
			"SurrealDB now checks every SCHEMAFULL table in the background. Run `surrealkit \
			 compat status` to see what it finds."
		);
		return Ok(());
	}

	let schemafull: Vec<String> =
		tables(&info).into_iter().filter(|(_, full)| *full).map(|(name, _)| name).collect();
	let deadline = Instant::now() + opts.timeout;
	let mut failures: BTreeMap<String, Vec<String>> = BTreeMap::new();
	loop {
		let mut running = Vec::new();
		failures.clear();
		for table in &schemafull {
			let table_info = table_info(db, table).await?;
			let now = progress(table, &table_info);
			running.extend(now.running);
			if !now.failed.is_empty() {
				failures.insert(table.clone(), failing_fields(&table_info));
			}
		}
		if running.is_empty() {
			break;
		}
		if Instant::now() >= deadline {
			bail!(
				"SurrealDB is still checking after {}s:\n  - {}\nRun `surrealkit compat status` \
				 later to see what it finds",
				opts.timeout.as_secs(),
				running.join("\n  - ")
			);
		}
		tokio::time::sleep(POLL).await;
	}
	if failures.is_empty() {
		log::info!("Every record of every SCHEMAFULL table fits its definition.");
		return Ok(());
	}
	report_fixes(folder, &failures);
	bail!(
		"{} table(s) hold records that do not fit their definitions at level 4; fix the fields \
		 listed above",
		failures.len()
	)
}

/// The fields of a table whose verification finished with failures, and
/// `undeclared` when records hold fields the table does not declare.
fn failing_fields(info: &Value) -> Vec<String> {
	let Some(section) = info.get("migrations") else {
		return Vec::new();
	};
	let mut out: Vec<String> = section
		.get("verification")
		.and_then(Value::as_array)
		.into_iter()
		.flatten()
		.filter(|entry| entry.get("failures").and_then(Value::as_i64).unwrap_or(0) > 0)
		.filter_map(|entry| entry.get("field").and_then(Value::as_str).map(str::to_string))
		.collect();
	if section
		.get("undeclared")
		.and_then(|u| u.get("failures"))
		.and_then(Value::as_i64)
		.is_some_and(|n| n > 0)
	{
		out.push(UNDECLARED.to_string());
	}
	out
}

const UNDECLARED: &str = "\0undeclared";

/// Say, for each failing field, which schema file to change and how.
fn report_fixes(folder: &str, failures: &BTreeMap<String, Vec<String>>) {
	let files = collect_schema_files(folder).unwrap_or_default();
	let mut defined: BTreeMap<(String, String), String> = BTreeMap::new();
	let mut tables: BTreeMap<String, String> = BTreeMap::new();
	for file in &files {
		let Ok(statements) = crate::surql_scan::scan(&file.sql) else {
			continue;
		};
		for stmt in &statements {
			let at = format!("{}:{}", file.path, stmt.line);
			if let Some((table, field)) = crate::migrate::defines_field(stmt, &file.sql) {
				defined.insert((table, field), at);
			} else if let Some(table) = crate::migrate::defines_table(stmt, &file.sql) {
				tables.insert(table, at);
			}
		}
	}
	for (table, fields) in failures {
		for field in fields {
			if field == UNDECLARED {
				let at = tables.get(table).map_or_else(String::new, |at| format!("{at}: "));
				log::warn!(
					"{at}records of `{table}` hold fields it does not declare. Add after its \
					 definition: ALTER TABLE {table} MIGRATE EXISTING ROWS AS {table}_undeclared \
					 REMOVE UNDECLARED FIELDS;"
				);
				continue;
			}
			let at = defined
				.get(&(table.clone(), field.clone()))
				.map_or_else(String::new, |at| format!("{at}: "));
			log::warn!(
				"{at}records of `{table}` hold values of `{field}` that its definition does not \
				 accept. Add to its DEFINE FIELD: MIGRATE EXISTING ROWS AS {field}_fit USING \
				 <expression over $value> FALLBACK <value>"
			);
		}
	}
}
