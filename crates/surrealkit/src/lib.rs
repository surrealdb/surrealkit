#![doc = include_str!("../README.md")]

// `generate`: TypeScript from the engine's `TypesDocument`, plus per-query types
// for the queries host code sends.
pub mod codegen;
pub mod config;
pub mod constants;
pub mod core;
// The MCP server. stdio transport; see the module docs for the statelessness
// contract and why stdout is off-limits.
#[cfg(feature = "mcp")]
pub mod mcp;
pub mod module;
pub mod paths;
pub mod progress;
pub mod project;
pub mod rollout;
pub mod scaffold;
pub mod schema_state;
pub mod seed;
pub mod selection;
pub mod setup;
mod surql_scan;
pub mod sync;
// `init`'s template engine. Lives in the library (not the binary) so the MCP
// server can scaffold projects too; `cli` and `mcp` both enable it.
#[cfg(feature = "templates")]
pub mod templates;
#[cfg(test)]
mod test_db;
pub mod tester;
pub mod typegen;
pub mod variables;

// Re-exported dependencies used in the public API surface.
pub use anyhow;
// Connecting.
pub use config::{AuthLevel, DbCfg, DbOverrides, connect};
// Rollouts (the staged, reversible path).
pub use module::{Module, Partition};
pub use project::{ProjectConfig, Target};
pub use rollout::{
	FileRef, FrozenFile, Rollout, RolloutAction, RolloutChainReport, RolloutCompatibility,
	RolloutPhase, RolloutSpec, RolloutSpecBuilder, RolloutStatus, RolloutStatusReport, RolloutStep,
	RolloutStepStatus, Rollouts, UpReport,
};
pub use schema_state::{EntityKey, EntityKind};
// The (schema module x database target) matrix, shared by every front end.
pub use selection::Selection;
// Seeding.
pub use seed::{EmbeddedSeedFile, Seed, seed};
pub use surrealdb::{self, Surreal, engine};
pub use surrealkit_macros::{embed_schema, embed_seed};
// Schema sync (the simple, desired-state path).
pub use sync::{EmbeddedSchemaFile, Sync};
// Testing (structured results for programmatic callers).
pub use tester::{AssertionReport, CaseReport, RunReport, SuiteReport, TestOpts};
// Type generation (programmatic).
pub use typegen::{SchemaTypes, TypegenOpts, generate};
// Template variables.
pub use variables::TemplateVars;
