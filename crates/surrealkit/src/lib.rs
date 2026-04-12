pub mod config;
pub mod core;
pub mod rollout;
pub mod scaffold;
pub mod schema_state;
pub mod seed;
pub mod setup;
pub mod sync;
pub mod tester;

pub use config::{DbCfg, connect};
pub use core::{create_surreal_client, exec_surql, sha256_hex};
pub use schema_state::SchemaFile;
pub use setup::ensure_metadata_tables;
pub use sync::{
	AppliedDataMigration, DataMigrationReport, MigrateReport, SyncReport, SyncSchemaOpts,
	list_applied_data_migrations, migrate, reset_data_migrations, revert_data_migrations_to,
	revert_last_data_migration, run_data_migrations, run_data_migrations_to,
	run_next_data_migration, sync_schemas,
};

pub use surrealkit_macros::{embed_data_migrations, embed_migrations};
