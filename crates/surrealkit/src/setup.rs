use std::fs;

use anyhow::{Context, Result};
use surrealdb::Surreal;
use surrealdb::engine::any::Any;

use crate::constants::setup_surql_path;
use crate::scaffold::DEFAULT_SETUP;

pub async fn run_setup(db: &Surreal<Any>, folder: &str) -> Result<()> {
	let setup_file = setup_surql_path(folder);

	// Default setup file
	if !setup_file.exists() {
		if let Some(parent) = setup_file.parent() {
			fs::create_dir_all(parent).context("creating setup file directory")?;
		}

		fs::write(&setup_file, DEFAULT_SETUP)
			.with_context(|| format!("writing {:?}", setup_file))?;
	}

	let sql =
		fs::read_to_string(&setup_file).with_context(|| format!("reading {:?}", setup_file))?;

	db.query(&sql).await?.check()?;

	db.query(DEFAULT_SETUP).await?.check()?;
	Ok(())
}
