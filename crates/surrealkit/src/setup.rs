use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use surrealdb::Surreal;
use surrealdb::engine::any::Any;

use crate::scaffold::{DEFAULT_SETUP, SETUP_FILE_HEADER};

/// Applies the SurrealKit metadata schema to the database. No filesystem access.
/// Safe to call on every startup; all statements use DEFINE ... OVERWRITE.
pub async fn run_setup(db: &Surreal<Any>) -> Result<()> {
	db.query(DEFAULT_SETUP).await?.check()?;
	Ok(())
}

/// CLI variant: writes `database/setup.surql` if absent, then executes it.
/// Also re-applies `DEFAULT_SETUP` directly so the schema stays current if
/// the file predates a SurrealKit upgrade.
pub async fn run_setup_from_file(db: &Surreal<Any>) -> Result<()> {
	let setup_file = Path::new("database/setup.surql");

	if !setup_file.exists() {
		if let Some(parent) = setup_file.parent() {
			fs::create_dir_all(parent).context("creating setup file directory")?;
		}
		let content = format!("{}{}", SETUP_FILE_HEADER, DEFAULT_SETUP);
		fs::write(setup_file, content).with_context(|| format!("writing {:?}", setup_file))?;
	}

	let sql =
		fs::read_to_string(setup_file).with_context(|| format!("reading {:?}", setup_file))?;
	db.query(&sql).await?.check()?;

	db.query(DEFAULT_SETUP).await?.check()?;
	Ok(())
}
