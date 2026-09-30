use std::collections::BTreeMap;
use std::fs;

use anyhow::{Context, Result};
use surrealdb::Surreal;
use surrealdb::engine::any::Any;

use crate::constants::setup_surql_path;
use crate::core::sha256_hex;
use crate::module::Partition;
use crate::rollout::{PartitionWrite, write_partition};
use crate::scaffold::DEFAULT_SETUP;

/// The `ns = 'meta'` key holding the hash of the setup DDL last applied here.
const SETUP_HASH_KEY: &str = "setup";

/// Ensure the metadata tables exist, running the setup DDL only when it changed.
///
/// Every command calls this, and on SurrealDB 3.2 `DEFINE INDEX OVERWRITE` does
/// not compare definitions: it drops the index's data and rebuilds it under a new
/// id, every time. Running the DDL unconditionally therefore rebuilt `by_ns_key`
/// -- the index the entity catalog and the lock's mutual exclusion depend on --
/// on every sync and rollout command, twice when the project's `setup.surql`
/// differs from the built-in one. Now it runs only when its hash differs from the
/// one recorded, or when a metadata index has gone missing.
#[doc(hidden)]
pub async fn run_setup(db: &Surreal<Any>, folder: &str) -> Result<()> {
	setup_from_folder(db, folder, false).await
}

/// Run the setup DDL even when it is unchanged. Backs `surrealkit setup`, the
/// explicit way to redefine the metadata tables.
#[doc(hidden)]
pub async fn force_setup(db: &Surreal<Any>, folder: &str) -> Result<()> {
	setup_from_folder(db, folder, true).await
}

async fn setup_from_folder(db: &Surreal<Any>, folder: &str, force: bool) -> Result<()> {
	let setup_file = setup_surql_path(folder);

	// Default setup file. A read-only or differently-owned project folder -- the
	// normal case for a distroless image running as uid 65532 against a bind
	// mount -- must not make every rollout command impossible. The metadata DDL
	// is compiled in, so fall back to it and carry on.
	if !setup_file.exists()
		&& let Err(err) = scaffold_setup_file(&setup_file)
	{
		log::warn!(
			"could not scaffold {}: {err:#}. Using the built-in metadata schema instead.",
			setup_file.display()
		);
		return apply_setup(db, &[DEFAULT_SETUP], force).await;
	}

	let sql =
		fs::read_to_string(&setup_file).with_context(|| format!("reading {:?}", setup_file))?;

	// The scaffolded file *is* DEFAULT_SETUP, so running both meant executing the
	// whole metadata DDL twice on every command. Only run it too when the user has
	// edited the file, where it still guarantees the metadata tables exist.
	if sql.trim() == DEFAULT_SETUP.trim() {
		apply_setup(db, &[&sql], force).await
	} else {
		apply_setup(db, &[&sql, DEFAULT_SETUP], force).await
	}
}

fn scaffold_setup_file(setup_file: &std::path::Path) -> Result<()> {
	if let Some(parent) = setup_file.parent() {
		fs::create_dir_all(parent).context("creating setup file directory")?;
	}
	fs::write(setup_file, DEFAULT_SETUP).with_context(|| format!("writing {:?}", setup_file))
}

/// Initialize the metadata tables without touching the filesystem.
///
/// Used by the embedded/library sync path, which has no project folder to
/// scaffold — so it must not write a `setup.surql` into the caller's working
/// directory the way [`run_setup`] does.
pub(crate) async fn run_setup_embedded(db: &Surreal<Any>) -> Result<()> {
	apply_setup(db, &[DEFAULT_SETUP], false).await
}

/// Run `parts` in order, unless the hash of exactly these parts is the one
/// recorded here and every metadata index is still defined.
async fn apply_setup(db: &Surreal<Any>, parts: &[&str], force: bool) -> Result<()> {
	let hash = sha256_hex(parts.join("\n").as_bytes());
	if !force && setup_is_current(db, &hash).await {
		log::debug!("metadata schema is unchanged; skipping the setup DDL");
		return Ok(());
	}
	for sql in parts {
		db.query(*sql).await?.check()?;
	}
	// Only an optimisation: if it is not recorded, the next command runs the DDL
	// again, which is what every command did before.
	let rows = BTreeMap::from([(SETUP_HASH_KEY.to_string(), serde_json::json!(hash))]);
	if let Err(err) =
		write_partition(db, Partition::Meta.as_str(), &rows, PartitionWrite::Merge).await
	{
		log::warn!("could not record the setup hash: {err:#}");
	}
	Ok(())
}

/// Whether `hash` is recorded as the setup last applied, and the unique indexes
/// the built-in setup defines all still exist.
///
/// The index check is what lets a hand-dropped index come back on the next
/// command, as it did when every command ran the DDL. Any error -- on a fresh
/// database the tables do not exist yet -- reads as "not current".
async fn setup_is_current(db: &Surreal<Any>, hash: &str) -> bool {
	let result = db
		.query(
			"RETURN { \
			 	hash: (SELECT VALUE val FROM __entity WHERE ns = $ns AND key = $key)[0], \
			 	indexes: [ \
			 		(INFO FOR TABLE __entity).indexes.by_ns_key, \
			 		(INFO FOR TABLE __rollout).indexes.by_rollout_id, \
			 		(INFO FOR TABLE __seed).indexes.by_seed_key \
			 	] \
			 };",
		)
		.bind(("ns", Partition::Meta.as_str().to_string()))
		.bind(("key", SETUP_HASH_KEY.to_string()))
		.await
		.and_then(|resp| resp.check());
	let Ok(mut resp) = result else {
		return false;
	};
	let Ok(Some(state)) = resp.take::<Option<serde_json::Value>>(0) else {
		return false;
	};
	let recorded = state.get("hash").and_then(|v| v.as_str());
	let indexes_present = state
		.get("indexes")
		.and_then(|v| v.as_array())
		.is_some_and(|indexes| indexes.iter().all(|ix| !ix.is_null()));
	recorded == Some(hash) && indexes_present
}

#[cfg(test)]
mod tests {
	use surrealdb::engine::any::connect;
	use surrealdb::opt::Config;
	use surrealdb::opt::capabilities::Capabilities;

	use super::*;

	async fn connect_mem_db() -> Surreal<Any> {
		let config = Config::new().capabilities(Capabilities::all());
		let db = connect(("mem://", config)).await.expect("connect mem://");
		db.use_ns("surrealkit_test").use_db("setup_test").await.expect("use_ns/use_db");
		db
	}

	/// Redefine a field the setup DDL owns, so a later run shows whether it ran:
	/// running it puts the `datetime` type back.
	async fn tamper(db: &Surreal<Any>) {
		db.query("DEFINE FIELD OVERWRITE updated_at ON __entity TYPE any;")
			.await
			.expect("tamper")
			.check()
			.expect("tamper");
	}

	async fn ddl_ran(db: &Surreal<Any>) -> bool {
		let mut resp = db
			.query("RETURN (INFO FOR TABLE __entity).fields.updated_at;")
			.await
			.expect("info for table");
		let definition: Option<String> = resp.take(0).expect("take definition");
		definition.unwrap_or_default().contains("datetime")
	}

	// On SurrealDB 3.2 every `DEFINE INDEX OVERWRITE` rebuilds the index, so the
	// setup DDL must not run on every command, only when there is something to do.
	#[tokio::test]
	async fn setup_runs_only_when_there_is_something_to_do() {
		let db = connect_mem_db().await;
		let dir = tempfile::tempdir().expect("tempdir");
		let folder = dir.path().to_str().expect("utf-8 tempdir");

		run_setup(&db, folder).await.expect("first run");
		assert!(ddl_ran(&db).await, "a fresh database runs the setup DDL");

		tamper(&db).await;
		run_setup(&db, folder).await.expect("unchanged run");
		assert!(!ddl_ran(&db).await, "an unchanged setup ran its DDL again");

		force_setup(&db, folder).await.expect("forced run");
		assert!(ddl_ran(&db).await, "`surrealkit setup` always runs the DDL");

		tamper(&db).await;
		let setup_file = setup_surql_path(folder);
		let edited = format!("{}\n-- edited\n", fs::read_to_string(&setup_file).expect("read"));
		fs::write(&setup_file, edited).expect("edit setup.surql");
		run_setup(&db, folder).await.expect("edited run");
		assert!(ddl_ran(&db).await, "an edited setup.surql runs again");

		tamper(&db).await;
		db.query("REMOVE INDEX by_seed_key ON __seed;")
			.await
			.expect("drop index")
			.check()
			.expect("drop index");
		run_setup(&db, folder).await.expect("run after a dropped index");
		assert!(ddl_ran(&db).await, "a dropped metadata index brings the DDL back");
		let mut resp = db
			.query("RETURN (INFO FOR TABLE __seed).indexes.by_seed_key;")
			.await
			.expect("info for __seed");
		let index: Option<String> = resp.take(0).expect("take index");
		assert!(index.is_some(), "by_seed_key was not redefined");
	}

	#[tokio::test]
	async fn embedded_setup_runs_only_when_there_is_something_to_do() {
		let db = connect_mem_db().await;
		run_setup_embedded(&db).await.expect("first run");
		assert!(ddl_ran(&db).await);
		tamper(&db).await;
		run_setup_embedded(&db).await.expect("unchanged run");
		assert!(!ddl_ran(&db).await, "an unchanged embedded setup ran its DDL again");
	}
}
