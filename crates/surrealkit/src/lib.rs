#![doc = include_str!("../README.md")]

pub mod analyze;
pub mod config;
pub mod constants;
pub mod core;
pub mod module;
pub mod project;
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
pub use analyze::AnalyzeConfig;
pub use anyhow;
// The analyzer SurrealKit's `check`/`generate`/`watch` drive, re-exported so a
// consumer can name its types without pinning the git revision themselves.
#[cfg(feature = "analyze")]
pub use surrealql_analyzer as analyzer;
// Connecting.
pub use config::{AuthLevel, DbCfg, DbOverrides, connect};
// Rollouts (the staged, reversible path).
pub use module::{Module, Partition};
pub use project::{ProjectConfig, Target};
pub use rollout::{
	Rollout, RolloutAction, RolloutCompatibility, RolloutPhase, RolloutSpec, RolloutSpecBuilder,
	RolloutStatus, RolloutStatusReport, RolloutStep, RolloutStepStatus,
};
pub use schema_state::{EntityKey, EntityKind};
// Seeding.
pub use seed::{EmbeddedSeedFile, Seed, seed};
pub use surrealdb::{self, Surreal, engine};
pub use surrealkit_macros::{embed_schema, embed_seed};
// Schema sync (the simple, desired-state path).
pub use sync::{EmbeddedSchemaFile, Sync};
// Type generation (programmatic).
pub use typegen::{SchemaTypes, TypegenOpts, generate};
// Template variables.
pub use variables::TemplateVars;
