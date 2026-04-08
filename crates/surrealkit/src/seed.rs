use std::fs;
use std::path::Path;

use anyhow::{Result, anyhow};
use surrealdb::Surreal;
use surrealdb::engine::any::Any;

use crate::core::{display, exec_surql};

pub async fn seed(db: &Surreal<Any>) -> Result<()> {
	let path = Path::new("database/seed.surql");

	if !path.exists() {
		return Err(anyhow!("seed file not found: {}", display(path)));
	}

	let sql = fs::read_to_string(path)?;
	exec_surql(db, &sql).await
}
