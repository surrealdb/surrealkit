use std::fs;
use std::path::Path;

use anyhow::{Context, Result};

use crate::setup::DEFAULT_SETUP;

pub fn scaffold() -> Result<()> {
	let database_dir = Path::new("database");
	let schema_dir = database_dir.join("schema");
	let rollouts_dir = database_dir.join("rollouts");
	let state_dir = database_dir.join("snapshots");
	let tests_dir = database_dir.join("tests");
	let test_suites_dir = tests_dir.join("suites");
	let test_fixtures_dir = tests_dir.join("fixtures");

	fs::create_dir_all(&schema_dir).context("creating database/schema")?;
	fs::create_dir_all(&rollouts_dir).context("creating database/rollouts")?;
	fs::create_dir_all(&state_dir).context("creating database/snapshots")?;
	fs::create_dir_all(&tests_dir).context("creating database/tests")?;
	fs::create_dir_all(&test_suites_dir).context("creating database/tests/suites")?;
	fs::create_dir_all(&test_fixtures_dir).context("creating database/tests/fixtures")?;

	// seed.surql (idempotent-ish example)
	let seed_path = database_dir.join("seed.surql");
	if !seed_path.exists() {
		fs::write(&seed_path, "--- SEED\n").context("Writing seed.surql")?;
	}

	// setup.surql defines SurrealKit metadata tables.
	let setup_path = database_dir.join("setup.surql");
	if !setup_path.exists() {
		fs::write(&setup_path, DEFAULT_SETUP).context("Writing setup.surql")?;
	}

	let test_config_path = tests_dir.join("config.toml");
	if !test_config_path.exists() {
		fs::write(&test_config_path, DEFAULT_TEST_CONFIG)
			.context("Writing database/tests/config.toml")?;
	}

	let test_suite_path = test_suites_dir.join("smoke.toml");
	if !test_suite_path.exists() {
		fs::write(&test_suite_path, DEFAULT_TEST_SUITE)
			.context("Writing database/tests/suites/smoke.toml")?;
	}

	println!("Scaffolded project in ./database\n");
	println!("  database/");
	println!("  ├── schema/");
	println!("  ├── rollouts/");
	println!("  ├── snapshots/");
	println!("  ├── tests/");
	println!("  │   ├── suites/");
	println!("  │   └── fixtures/");
	println!("  ├── seed.surql");
	println!("  └── setup.surql");
	Ok(())
}

pub const DEFAULT_TEST_CONFIG: &str = r#"[defaults]
timeout_ms = 10000

[actors.root]
kind = "root"
"#;

pub const DEFAULT_TEST_SUITE: &str = r#"name = "smoke"
tags = ["smoke"]

[[cases]]
name = "rollout_table_visible"
kind = "schema_metadata"
sql = "INFO FOR TABLE __rollout;"
contains = ["__rollout"]
"#;
