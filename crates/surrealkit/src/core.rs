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

/// The oldest major version of SurrealDB this release works with.
pub const MIN_SERVER_MAJOR: u64 = 4;

/// Refuse a server older than SurrealDB 4.0.
///
/// The 4.0 SDK still connects to 3.x servers, but SurrealKit now writes
/// SurrealQL that only 4.0 reads, and relies on how 4.0 treats existing records.
/// A server that does not answer the `version` method is let through with a
/// warning: its operator chose not to say.
pub async fn ensure_supported_server(db: &Surreal<Any>) -> anyhow::Result<()> {
	match db.version().await {
		Ok(version) => check_server_major(version.major, &version.to_string()),
		Err(err) => {
			log::warn!(
				"could not read the SurrealDB server's version, so cannot check that it is 4.0 or \
				 later: {err}"
			);
			Ok(())
		}
	}
}

fn check_server_major(major: u64, version: &str) -> anyhow::Result<()> {
	if major < MIN_SERVER_MAJOR {
		anyhow::bail!(
			"SurrealKit {} needs SurrealDB {MIN_SERVER_MAJOR}.0 or later, and this server is \
			 {version}. Use SurrealKit 1.0.0-beta.8 with SurrealDB 3.x, or upgrade the server: \
			 https://surrealdb.com/docs/build/migrating/from-old-surrealdb-versions/3x-to-4x",
			env!("CARGO_PKG_VERSION")
		);
	}
	Ok(())
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

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn refuses_a_3x_server() {
		let err = check_server_major(3, "3.3.5").unwrap_err().to_string();
		assert!(err.contains("this server is 3.3.5"), "{err}");
		assert!(err.contains("1.0.0-beta.8"), "{err}");
	}

	#[test]
	fn accepts_4x_and_later() {
		check_server_major(4, "4.0.0").unwrap();
		check_server_major(5, "5.1.0").unwrap();
	}
}
