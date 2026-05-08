pub mod config;
pub mod core;
pub mod rollout;
pub mod scaffold;
pub mod schema_state;
pub mod seed;
pub mod setup;
pub mod sync;
pub mod tester;

pub use anyhow;
pub use config::{AuthLevel, DbCfg, DbOverrides, connect};
pub use rollout::{
	RolloutAction, RolloutCompatibility, RolloutExecutionOpts, RolloutPhase, RolloutPlanOpts,
	RolloutSpec, RolloutStep, run_abandon_rollout, run_baseline, run_complete,
	run_complete_with_spec, run_lint, run_plan, run_rollback, run_rollback_with_spec, run_start,
	run_start_with_spec, run_status,
};
pub use seed::{seed, seed_from_dir};
pub use setup::run_setup;
pub use surrealdb::{self, Surreal, engine};
pub use surrealkit_macros::embed_schema;
pub use sync::{
	EmbeddedSchemaFile, SyncOpts, run_sync, run_sync_embedded, run_sync_embedded_with_opts,
};
