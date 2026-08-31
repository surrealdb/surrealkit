use std::path::Path;

use sha2::{Digest, Sha256};
use surrealdb::Surreal;
use surrealdb::engine::any::{Any, connect};
use surrealdb::opt::Config;
use surrealdb::opt::capabilities::Capabilities;

pub async fn create_surreal_client(address: &String) -> Result<Surreal<Any>, surrealdb::Error> {
	let config =
		Config::new().capabilities(Capabilities::all().with_all_experimental_features_allowed());

	connect((address, config)).await
}

pub async fn exec_surql(db: &Surreal<Any>, sql: &str) -> anyhow::Result<()> {
	db.query(sql).await?.check()?;
	Ok(())
}

/// Render a path for user-facing messages, lossily for non-UTF-8 paths.
pub fn display(p: &Path) -> String {
	p.to_string_lossy().into_owned()
}

/// Lowercase hex SHA-256 of `bytes`.
///
/// Used for the content hashes SurrealKit tracks per schema and seed file, so
/// unchanged files are skipped on re-runs.
pub fn sha256_hex(bytes: &[u8]) -> String {
	let mut hasher = Sha256::new();
	hasher.update(bytes);
	hex::encode(hasher.finalize())
}
