//! A fresh database for a test: in memory by default, or a new namespace on the
//! server at `SURREALKIT_TEST_URL`, so the same tests can run against released
//! servers as well as the embedded engine. CI runs them on SurrealDB 3.2.4 and
//! 3.3.0, whose SurrealQL differs in places the embedded 3.3 engine cannot show.

use surrealdb::Surreal;
use surrealdb::engine::any::{Any, connect};
use surrealdb::opt::Config;
use surrealdb::opt::auth::Root;
use surrealdb::opt::capabilities::Capabilities;

pub(crate) async fn fresh(label: &str) -> Surreal<Any> {
	let db = match std::env::var("SURREALKIT_TEST_URL").ok().filter(|url| !url.is_empty()) {
		Some(url) => {
			let db = connect(url).await.expect("connect to SURREALKIT_TEST_URL");
			db.signin(Root {
				username: std::env::var("SURREALKIT_TEST_USER").unwrap_or_else(|_| "root".into()),
				password: std::env::var("SURREALKIT_TEST_PASS").unwrap_or_else(|_| "secret".into()),
			})
			.await
			.expect("signin to SURREALKIT_TEST_URL");
			db
		}
		None => connect(("mem://", Config::new().capabilities(Capabilities::all())))
			.await
			.expect("connect mem://"),
	};
	// Unique per call: on a shared server, parallel tests must not meet.
	let ns = format!("{label}_{}", surrealdb_types::uuid::Uuid::new_v4().simple());
	db.use_ns(ns).use_db(label).await.expect("use_ns/use_db");
	db
}
