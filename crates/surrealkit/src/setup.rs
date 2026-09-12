use std::fs;

use anyhow::{Context, Result};
use surrealdb::Surreal;
use surrealdb::engine::any::Any;

use crate::constants::setup_surql_path;
use crate::scaffold::DEFAULT_SETUP;

#[doc(hidden)]
pub async fn run_setup(db: &Surreal<Any>, folder: &str) -> Result<()> {
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
		db.query(DEFAULT_SETUP).await?.check()?;
		return Ok(());
	}

	let sql =
		fs::read_to_string(&setup_file).with_context(|| format!("reading {:?}", setup_file))?;

	db.query(&sql).await?.check()?;

	// The scaffolded file *is* DEFAULT_SETUP, so running both meant executing the
	// whole metadata DDL twice on every command. Only re-run it when the user has
	// edited the file, where it still guarantees the metadata tables exist.
	if sql.trim() != DEFAULT_SETUP.trim() {
		db.query(DEFAULT_SETUP).await?.check()?;
	}
	Ok(())
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
	db.query(DEFAULT_SETUP).await?.check()?;
	Ok(())
}
