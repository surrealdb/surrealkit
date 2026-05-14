use std::fs;
use std::path::Path;

use anyhow::{Context, Result};

use crate::constants::{
	fixtures_dir, rollouts_dir, schema_dir, seed_dir, seed_surql_path, setup_surql_path, state_dir,
	suites_dir, tests_dir,
};

pub fn scaffold(folder: &str) -> Result<()> {
	let schema_dir = schema_dir(folder);
	let rollouts_dir = rollouts_dir(folder);
	let state_dir = state_dir(folder);
	let tests_dir = tests_dir(folder);
	let test_suites_dir = suites_dir(folder);
	let test_fixtures_dir = fixtures_dir(folder);

	fs::create_dir_all(&schema_dir).with_context(|| format!("creating {}/schema", folder))?;
	fs::create_dir_all(&rollouts_dir).with_context(|| format!("creating {}/rollouts", folder))?;
	fs::create_dir_all(&state_dir).with_context(|| format!("creating {}/snapshots", folder))?;
	fs::create_dir_all(&tests_dir).with_context(|| format!("creating {}/tests", folder))?;
	fs::create_dir_all(&test_suites_dir)
		.with_context(|| format!("creating {}/tests/suites", folder))?;
	fs::create_dir_all(&test_fixtures_dir)
		.with_context(|| format!("creating {}/tests/fixtures", folder))?;

	let seed_dir = seed_dir(folder);
	fs::create_dir_all(&seed_dir).with_context(|| format!("creating {}/seed", folder))?;
	let seed_path = seed_surql_path(folder);
	if !seed_path.exists() {
		fs::write(&seed_path, "--- SEED\n")
			.with_context(|| format!("Writing {}/seed/seed.surql", folder))?;
	}

	// setup.surql defines SurrealKit metadata tables.
	let setup_path = setup_surql_path(folder);
	if !setup_path.exists() {
		fs::write(&setup_path, DEFAULT_SETUP)
			.with_context(|| format!("Writing {}/setup.surql", folder))?;
	}

	let test_config_path = tests_dir.join("config.toml");
	if !test_config_path.exists() {
		fs::write(&test_config_path, DEFAULT_TEST_CONFIG)
			.with_context(|| format!("Writing {}/tests/config.toml", folder))?;
	}

	let test_suite_path = test_suites_dir.join("smoke.toml");
	if !test_suite_path.exists() {
		fs::write(&test_suite_path, DEFAULT_TEST_SUITE)
			.with_context(|| format!("Writing {}/tests/suites/smoke.toml", folder))?;
	}

	let project_config_path = Path::new("surrealkit.toml");
	if !project_config_path.exists() {
		fs::write(project_config_path, DEFAULT_PROJECT_CONFIG)
			.context("Writing surrealkit.toml")?;
	}

	println!("Scaffolded project in {}\n", folder);
	println!("  surrealkit.toml");
	println!("  {}/", folder);
	println!("  ├── schema/");
	println!("  ├── rollouts/");
	println!("  ├── snapshots/");
	println!("  ├── tests/");
	println!("  │   ├── suites/");
	println!("  │   └── fixtures/");
	println!("  ├── seed/");
	println!("  │   └── seed.surql");
	println!("  └── setup.surql");
	Ok(())
}

pub const DEFAULT_SETUP: &str = r#"DEFINE TABLE OVERWRITE __entity SCHEMAFULL
	PERMISSIONS NONE;

DEFINE FIELD OVERWRITE ns ON __entity
	TYPE string;

DEFINE FIELD OVERWRITE key ON __entity
	TYPE string;

DEFINE FIELD OVERWRITE val ON __entity
	TYPE any;

DEFINE FIELD OVERWRITE updated_at ON __entity
	TYPE datetime
	DEFAULT time::now();

DEFINE INDEX OVERWRITE by_ns_key ON __entity
	FIELDS ns, key
	UNIQUE;

DEFINE TABLE OVERWRITE __rollout SCHEMAFULL
	PERMISSIONS NONE;

DEFINE FIELD OVERWRITE id ON __rollout
	TYPE string;

DEFINE FIELD OVERWRITE name ON __rollout
	TYPE string;

DEFINE FIELD OVERWRITE manifest_path ON __rollout
	TYPE string;

DEFINE FIELD OVERWRITE manifest_checksum ON __rollout
	TYPE string;

DEFINE FIELD OVERWRITE source_schema_hash ON __rollout
	TYPE string;

DEFINE FIELD OVERWRITE target_schema_hash ON __rollout
	TYPE string;

DEFINE FIELD OVERWRITE status ON __rollout
	TYPE string;

DEFINE FIELD OVERWRITE source_entities ON __rollout
	TYPE any;

DEFINE FIELD OVERWRITE target_entities ON __rollout
	TYPE any;

DEFINE FIELD OVERWRITE steps ON __rollout
	TYPE any
	DEFAULT [];

DEFINE FIELD OVERWRITE started_at ON __rollout
	TYPE datetime
	DEFAULT time::now();

DEFINE FIELD OVERWRITE completed_at ON __rollout
	TYPE option<datetime>;

DEFINE FIELD OVERWRITE updated_at ON __rollout
	TYPE datetime
	DEFAULT time::now();

DEFINE FIELD OVERWRITE last_error ON __rollout
	TYPE option<string>;

DEFINE INDEX OVERWRITE by_rollout_id ON __rollout
	FIELDS id
	UNIQUE;
"#;

pub const DEFAULT_TEST_CONFIG: &str = r#"[defaults]
timeout_ms = 10000

[actors.root]
kind = "root"
"#;

pub const DEFAULT_PROJECT_CONFIG: &str = r#"# Template variables for use in .surql schema and seed files.
# Values here have the lowest priority:
#   --var KEY=VALUE  >  SURREALKIT_VAR_KEY env vars  >  this file
#
# [variables]
# schema_prefix = "myapp"
# environment   = "development"
"#;

pub const DEFAULT_TEST_SUITE: &str = r#"name = "smoke"
tags = ["smoke"]

[[cases]]
name = "rollout_table_visible"
kind = "schema_metadata"
sql = "INFO FOR TABLE __rollout;"
contains = ["__rollout"]
"#;
