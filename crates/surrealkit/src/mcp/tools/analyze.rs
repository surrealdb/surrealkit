//! Static analysis of the project's SurrealQL.
//!
//! These drive the same analyzer as `surrealkit check` and `surrealkit generate`
//! and touch no database, which makes `check` the cheapest useful thing an agent
//! can call: it catches unknown tables and fields, kind mismatches, bad graph
//! traversals and comparisons that can never be true, all before anything
//! reaches an instance.
//!
//! `watch` has no tool. It loops until interrupted, so it cannot return a
//! result; the CLI is the place for it.

use anyhow::{Context as _, bail};
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::{schemars, tool, tool_router};
use serde::{Deserialize, Serialize};

use super::WorkRequest;
use crate::mcp::SurrealKitMcp;
use crate::mcp::result::run_captured;
use crate::paths::safe_join;

/// Build the analysis tool router.
pub(super) fn router() -> ToolRouter<SurrealKitMcp> {
	SurrealKitMcp::analyze_router()
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CheckInput {
	#[serde(flatten)]
	pub work: WorkRequest,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GenerateInput {
	#[serde(flatten)]
	pub work: WorkRequest,
	/// Where to write the typed client, relative to the project root. Defaults
	/// to `[analyze] out` in surrealkit.toml.
	#[serde(default)]
	pub out: Option<String>,
}

/// The analyzer's report, plus whether the run is clean.
#[derive(Debug, Clone, Serialize)]
pub struct CheckReport {
	pub passed: bool,
	/// The analyzer's own stable document: `{ summary, diagnostics[] }`.
	pub report: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct GenerateReport {
	pub path: String,
	pub queries: usize,
}

#[tool_router(router = analyze_router)]
impl SurrealKitMcp {
	#[tool(
		name = "check",
		description = "Statically analyse the project's SurrealQL against its own schema: \
		               unknown tables and fields, kind mismatches, bad graph traversals, \
		               comparisons that can never be true, clauses the engine accepts and \
		               then ignores. Reads every module's schema/ directory as the schema, \
		               every other .surql file as queries, and the SurrealQL embedded in \
		               host code. Contacts no database, so it is safe and cheap to run \
		               before anything else. Failing diagnostics come back in the result \
		               rather than as an error: read `passed` and `report.diagnostics`.",
		annotations(
			title = "Check SurrealQL",
			read_only_hint = true,
			destructive_hint = false,
			idempotent_hint = true,
			open_world_hint = false
		)
	)]
	pub async fn check(&self, Parameters(input): Parameters<CheckInput>) -> CallToolResult {
		let server = self.clone();
		run_captured(
			|report: &CheckReport| {
				let summary = report.report.get("summary");
				let errors = summary
					.and_then(|s| s.get("errors"))
					.and_then(serde_json::Value::as_u64)
					.unwrap_or(0);
				let sources = summary
					.and_then(|s| s.get("sources_checked"))
					.and_then(serde_json::Value::as_u64)
					.unwrap_or(0);
				if report.passed {
					format!("check passed: {sources} source(s), no errors")
				} else {
					format!("check failed: {errors} error(s) across {sources} source(s)")
				}
			},
			|| async move {
				let ctx = server.context(&input.work)?;
				let analyzer_project = crate::analyze::analyzer_project(
					&ctx.project,
					&ctx.folder,
					&input.work.schema,
					input.work.no_deps,
				)?;
				let report = surrealql_analyzer::check(&analyzer_project)?;
				Ok(CheckReport {
					passed: report.passed(),
					report: serde_json::from_str(&report.to_json()?)
						.context("parsing the analyzer's report")?,
				})
			},
		)
		.await
	}

	#[tool(
		name = "generate",
		description = "Generate a typed client for the project's embedded queries, from the \
		               same analysis `check` runs. Writes to `out`, or to `[analyze] out` in \
		               surrealkit.toml when that is set. Contacts no database. Refuses when \
		               the analysis is blocked, since a client generated from a schema that \
		               does not typecheck would be wrong in ways the compiler cannot see.",
		annotations(
			title = "Generate typed client",
			read_only_hint = false,
			destructive_hint = false,
			idempotent_hint = true,
			open_world_hint = false
		)
	)]
	pub async fn generate(&self, Parameters(input): Parameters<GenerateInput>) -> CallToolResult {
		let server = self.clone();
		run_captured(
			|report: &GenerateReport| format!("wrote {} ({} queries)", report.path, report.queries),
			|| async move {
				let ctx = server.context(&input.work)?;
				// A caller-supplied path is contained; the configured one is the
				// project's own and is resolved by the analyzer as the CLI does.
				let out = match &input.out {
					Some(out) => Some(safe_join(&ctx.root, out)?),
					None => crate::analyze::configured_out(&ctx.project)?,
				};
				let analyzer_project = crate::analyze::analyzer_project(
					&ctx.project,
					&ctx.folder,
					&input.work.schema,
					input.work.no_deps,
				)?;
				let _guard = server.lock_folder(&ctx.folder).await;
				match surrealql_analyzer::generate(&analyzer_project, out.as_deref()) {
					Ok(report) => Ok(GenerateReport {
						path: analyzer_project.display_relative(&report.path),
						queries: report.queries,
					}),
					Err(surrealql_analyzer::GenerateError::Blocked(blocked)) => {
						bail!("{blocked}")
					}
					Err(surrealql_analyzer::GenerateError::Io(error)) => Err(error.into()),
				}
			},
		)
		.await
	}
}
