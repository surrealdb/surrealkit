pub mod config;
pub mod constants;
pub mod core;
pub mod rollout;
pub mod scaffold;
pub mod schema_state;
pub mod seed;
pub mod setup;
pub mod sync;
pub mod tester;
pub mod typegen;
pub mod variables;

// Re-exported dependencies used in the public API surface.
pub use anyhow;
pub use surrealdb::{self, Surreal, engine};
pub use surrealkit_macros::embed_schema;

// Connecting.
pub use config::{AuthLevel, DbCfg, DbOverrides, connect};

// Schema sync (the simple, desired-state path).
pub use sync::{EmbeddedSchemaFile, Sync};

// Rollouts (the staged, reversible path).
pub use rollout::{
	Rollout, RolloutAction, RolloutCompatibility, RolloutPhase, RolloutSpec, RolloutSpecBuilder,
	RolloutStatus, RolloutStatusReport, RolloutStep, RolloutStepStatus,
};
pub use schema_state::{EntityKey, EntityKind};

// Seeding.
pub use seed::seed;

// Type generation (programmatic).
pub use typegen::{SchemaTypes, TypegenOpts, generate};

// Template variables.
pub use variables::TemplateVars;
