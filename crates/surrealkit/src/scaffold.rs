use std::fs;
use std::path::Path;

use anyhow::{Context, Result};

pub fn scaffold() -> Result<()> {
	let database_dir = Path::new("database");
	let schema_dir = database_dir.join("schema");
	let rollouts_dir = database_dir.join("rollouts");
	let state_dir = database_dir.join(".surrealkit");
	let tests_dir = database_dir.join("tests");
	let test_suites_dir = tests_dir.join("suites");
	let test_fixtures_dir = tests_dir.join("fixtures");

	fs::create_dir_all(&schema_dir).context("creating database/schema")?;
	fs::create_dir_all(&rollouts_dir).context("creating database/rollouts")?;
	fs::create_dir_all(&state_dir).context("creating database/.surrealkit")?;
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

	println!(
		"Scaffolded ./database, ./database/schema, ./database/rollouts, ./database/.surrealkit, ./database/tests, ./database/tests/suites, ./database/tests/fixtures, seed.surql, setup.surql"
	);
	Ok(())
}

pub const DEFAULT_SETUP: &str = r#"DEFINE TABLE OVERWRITE _surrealkit_sync SCHEMAFULL
	PERMISSIONS NONE;

DEFINE FIELD OVERWRITE path ON _surrealkit_sync
	TYPE string;

DEFINE FIELD OVERWRITE hash ON _surrealkit_sync
	TYPE string;

DEFINE FIELD OVERWRITE synced_at ON _surrealkit_sync
	TYPE datetime
	DEFAULT time::now();

DEFINE INDEX OVERWRITE by_path ON _surrealkit_sync
	FIELDS path
	UNIQUE;

DEFINE TABLE OVERWRITE _surrealkit_sync_meta SCHEMAFULL
	PERMISSIONS NONE;

DEFINE FIELD OVERWRITE key ON _surrealkit_sync_meta
	TYPE string;

DEFINE FIELD OVERWRITE value ON _surrealkit_sync_meta
	TYPE any;

DEFINE FIELD OVERWRITE updated_at ON _surrealkit_sync_meta
	TYPE datetime
	DEFAULT time::now();

DEFINE INDEX OVERWRITE by_key ON _surrealkit_sync_meta
	FIELDS key
	UNIQUE;

DEFINE TABLE OVERWRITE _surrealkit_managed_entity SCHEMAFULL
	PERMISSIONS NONE;

DEFINE FIELD OVERWRITE kind ON _surrealkit_managed_entity
	TYPE string;

DEFINE FIELD OVERWRITE scope ON _surrealkit_managed_entity
	TYPE option<string>;

DEFINE FIELD OVERWRITE name ON _surrealkit_managed_entity
	TYPE string;

DEFINE FIELD OVERWRITE source_path ON _surrealkit_managed_entity
	TYPE string;

DEFINE FIELD OVERWRITE statement_hash ON _surrealkit_managed_entity
	TYPE string;

DEFINE FIELD OVERWRITE file_hash ON _surrealkit_managed_entity
	TYPE string;

DEFINE FIELD OVERWRITE active_rollout_id ON _surrealkit_managed_entity
	TYPE option<string>;

DEFINE FIELD OVERWRITE state ON _surrealkit_managed_entity
	TYPE string
	DEFAULT "active";

DEFINE FIELD OVERWRITE updated_at ON _surrealkit_managed_entity
	TYPE datetime
	DEFAULT time::now();

DEFINE INDEX OVERWRITE by_entity_key ON _surrealkit_managed_entity
	FIELDS kind, scope, name
	UNIQUE;

DEFINE TABLE OVERWRITE _surrealkit_rollout SCHEMAFULL
	PERMISSIONS NONE;

DEFINE FIELD OVERWRITE id ON _surrealkit_rollout
	TYPE string;

DEFINE FIELD OVERWRITE name ON _surrealkit_rollout
	TYPE string;

DEFINE FIELD OVERWRITE manifest_path ON _surrealkit_rollout
	TYPE string;

DEFINE FIELD OVERWRITE manifest_checksum ON _surrealkit_rollout
	TYPE string;

DEFINE FIELD OVERWRITE source_schema_hash ON _surrealkit_rollout
	TYPE string;

DEFINE FIELD OVERWRITE target_schema_hash ON _surrealkit_rollout
	TYPE string;

DEFINE FIELD OVERWRITE status ON _surrealkit_rollout
	TYPE string;

DEFINE FIELD OVERWRITE source_entities ON _surrealkit_rollout
	TYPE any;

DEFINE FIELD OVERWRITE target_entities ON _surrealkit_rollout
	TYPE any;

DEFINE FIELD OVERWRITE started_at ON _surrealkit_rollout
	TYPE datetime
	DEFAULT time::now();

DEFINE FIELD OVERWRITE completed_at ON _surrealkit_rollout
	TYPE option<datetime>;

DEFINE FIELD OVERWRITE updated_at ON _surrealkit_rollout
	TYPE datetime
	DEFAULT time::now();

DEFINE FIELD OVERWRITE last_error ON _surrealkit_rollout
	TYPE option<string>;

DEFINE INDEX OVERWRITE by_rollout_id ON _surrealkit_rollout
	FIELDS id
	UNIQUE;

DEFINE TABLE OVERWRITE _surrealkit_rollout_step SCHEMAFULL
	PERMISSIONS NONE;

DEFINE FIELD OVERWRITE rollout_id ON _surrealkit_rollout_step
	TYPE string;

DEFINE FIELD OVERWRITE step_id ON _surrealkit_rollout_step
	TYPE string;

DEFINE FIELD OVERWRITE phase ON _surrealkit_rollout_step
	TYPE string;

DEFINE FIELD OVERWRITE kind ON _surrealkit_rollout_step
	TYPE string;

DEFINE FIELD OVERWRITE checksum ON _surrealkit_rollout_step
	TYPE string;

DEFINE FIELD OVERWRITE status ON _surrealkit_rollout_step
	TYPE string;

DEFINE FIELD OVERWRITE started_at ON _surrealkit_rollout_step
	TYPE datetime
	DEFAULT time::now();

DEFINE FIELD OVERWRITE finished_at ON _surrealkit_rollout_step
	TYPE option<datetime>;

DEFINE FIELD OVERWRITE error ON _surrealkit_rollout_step
	TYPE option<string>;

DEFINE INDEX OVERWRITE by_rollout_step ON _surrealkit_rollout_step
	FIELDS rollout_id, step_id
	UNIQUE;

DEFINE TABLE OVERWRITE _surrealkit_lock SCHEMAFULL
	PERMISSIONS NONE;

DEFINE FIELD OVERWRITE key ON _surrealkit_lock
	TYPE string;

DEFINE FIELD OVERWRITE owner ON _surrealkit_lock
	TYPE string;

DEFINE FIELD OVERWRITE created_at ON _surrealkit_lock
	TYPE datetime
	DEFAULT time::now();

DEFINE INDEX OVERWRITE by_lock_key ON _surrealkit_lock
	FIELDS key
	UNIQUE;
"#;

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
sql = "INFO FOR TABLE _surrealkit_rollout;"
contains = ["_surrealkit_rollout"]
"#;
