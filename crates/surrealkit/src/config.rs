use anyhow::{Context, Result};
use rust_dotenv::dotenv::DotEnv;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb::opt::auth::Root;

use crate::core::create_surreal_client;

#[derive(Debug, Clone)]
pub struct DbCfg {
	host: String,
	ns: String,
	db: String,
	user: String,
	pass: String,
}

impl DbCfg {
	pub fn from_env(_env: &DotEnv) -> Result<Self> {
		let dotenv = DotEnv::new("");

		let host = dotenv
			.get_var("SURREALDB_HOST".to_string())
			.or_else(|| dotenv.get_var("DATABASE_HOST".to_string()))
			.unwrap_or(String::from("http://localhost:8000"));

		let db = dotenv
			.get_var("SURREALDB_NAME".to_string())
			.or_else(|| dotenv.get_var("DATABASE_NAME".to_string()))
			.unwrap_or(String::from("test"));

		let ns = dotenv
			.get_var("SURREALDB_NAMESPACE".to_string())
			.or_else(|| dotenv.get_var("DATABASE_NAMESPACE".to_string()))
			.unwrap_or(String::from("db"));

		let user = dotenv
			.get_var("SURREALDB_USER".to_string())
			.or_else(|| dotenv.get_var("DATABASE_USER".to_string()))
			.unwrap_or(String::from("root"));

		let pass = dotenv
			.get_var("SURREALDB_PASSWORD".to_string())
			.or_else(|| dotenv.get_var("DATABASE_PASSWORD".to_string()))
			.unwrap_or(String::from("root"));

		Ok(Self {
			host,
			ns,
			db,
			user,
			pass,
		})
	}

	pub fn host(&self) -> &str {
		&self.host
	}

	pub fn ns(&self) -> &str {
		&self.ns
	}

	pub fn db(&self) -> &str {
		&self.db
	}

	pub fn user(&self) -> &str {
		&self.user
	}

	pub fn pass(&self) -> &str {
		&self.pass
	}
}

pub async fn connect(cfg: &DbCfg) -> Result<Surreal<Any>> {
	let db = create_surreal_client(&cfg.host)
		.await
		.with_context(|| format!("Failed connecting to {}", cfg.host))?;

	db.signin(Root {
		username: cfg.user.clone(),
		password: cfg.pass.clone(),
	})
	.await
	.context("signin failed")?;
	db.use_ns(&cfg.ns)
		.use_db(&cfg.db)
		.await
		.with_context(|| format!("use_ns/use_db failed for ns={} db= {}", cfg.ns, cfg.db))?;

	Ok(db)
}
