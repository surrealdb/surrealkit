//! The check family: `surrealkit check` for the whole project and for one query.
//!
//! Both read files and nothing else, but for `check` with a `target`, which connects to that
//! target to read its schema. Neither calls `std::env::current_dir`: the root, the folder and
//! the `surrealkit.toml` all come from the call's [`CallContext`], so concurrent calls cannot
//! interfere.

use std::collections::HashMap;

use anyhow::Result;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::{schemars, tool, tool_router};
use serde::Deserialize;

use super::WorkRequest;
use crate::check::Prepared;
use crate::check::query::{QueryCheck, QueryCheckRequest, check_query};
use crate::check::render::{self, JsonReport};
use crate::mcp::SurrealKitMcp;
use crate::mcp::context::CallContext;
use crate::mcp::result::run_captured;
use crate::project::CONFIG_FILE_NAME;
use crate::selection::Selection;
use crate::variables::{TemplateVars, build_project_vars};

/// Build the check tool router.
pub(super) fn router() -> ToolRouter<SurrealKitMcp> {
	SurrealKitMcp::check_router()
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CheckQueryInput {
	/// The SurrealQL to check. It is only read: nothing in it is executed.
	pub query: String,
	/// The name its findings are reported under, in place of a file path. Default:
	/// `<query>`.
	#[serde(default)]
	pub name: Option<String>,
	/// A `[target.<name>]` from surrealkit.toml. The schema the query is checked against is
	/// then the modules that target accepts. Nothing connects to it. Omit to use every
	/// selected module.
	#[serde(default)]
	pub target: Option<String>,
	/// Schema module names from `[schema.<name>]`. Omit for every declared module, or the
	/// default module when none are declared.
	#[serde(default)]
	pub schema: Vec<String>,
	/// Project folder, relative to the server's project root.
	/// Defaults to `SURREALDB_FOLDER`, else `./database`.
	#[serde(default)]
	pub folder: Option<String>,
	/// Template variables substituted into `${VAR}` placeholders.
	#[serde(default)]
	pub vars: HashMap<String, String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CheckInput {
	/// A `[target.<name>]` from surrealkit.toml. When given, the files are also compared with
	/// that database, which this connects to and reads (drift: the K91xx findings). Omit to
	/// read only files.
	#[serde(default)]
	pub target: Option<String>,
	/// Schema module names from `[schema.<name>]`. Omit for every declared module, or the
	/// default module when none are declared.
	#[serde(default)]
	pub schema: Vec<String>,
	/// Project folder, relative to the server's project root.
	/// Defaults to `SURREALDB_FOLDER`, else `./database`.
	#[serde(default)]
	pub folder: Option<String>,
	/// Template variables substituted into `${VAR}` placeholders.
	#[serde(default)]
	pub vars: HashMap<String, String>,
}

/// The shared part of a call's input as the request every tool resolves its context from.
fn work(schema: &[String], folder: &Option<String>, vars: &HashMap<String, String>) -> WorkRequest {
	WorkRequest {
		schema: schema.to_vec(),
		folder: folder.clone(),
		vars: vars.clone(),
		..WorkRequest::default()
	}
}

/// The template variables a check reads: `surrealkit.toml [variables]`, the server's `--var`
/// flags and the call's `vars`, and never `SURREALKIT_VAR_*`, which `check` does not read. The
/// config is the one the call resolved: with none found, a path that does not exist, never the
/// process working directory's.
fn check_vars(
	server: &SurrealKitMcp,
	ctx: &CallContext,
	vars: &HashMap<String, String>,
) -> Result<TemplateVars> {
	let mut merged = server.config().vars.clone();
	merged.extend(vars.iter().map(|(key, value)| (key.clone(), value.clone())));
	let config = ctx.config_path.clone().unwrap_or_else(|| ctx.root.join(CONFIG_FILE_NAME));
	Ok(TemplateVars {
		vars: build_project_vars(&merged, Some(&config))?,
	})
}

/// "N errors, M warnings", with `1 error` and `1 warning` in the singular.
fn counts(errors: usize, warnings: usize) -> String {
	let n = |n: usize, one: &str, many: &str| {
		format!(
			"{n} {}",
			if n == 1 {
				one
			} else {
				many
			}
		)
	};
	format!("{}, {}", n(errors, "error", "errors"), n(warnings, "warning", "warnings"))
}

#[tool_router(router = check_router)]
impl SurrealKitMcp {
	/// Check one query against the project's schema.
	#[tool(
		name = "check_query",
		description = "Check one SurrealQL query against the project's schema files, the way \
		               `surrealkit check` checks a file: under the [analyze] policy in \
		               surrealkit.toml and the `-- surreal: allow(...)` suppressions in the \
		               query's own comments. It never executes the query and never connects \
		               to a database. A `target` only selects the modules that [target.<name>] \
		               accepts as the schema; it is not connected to. The result has the \
		               findings in the shape of `check --json`'s diagnostics, with spans and \
		               lines within the query. `analyzed: false` means the query itself was \
		               not analyzed, so a query with mistakes still passes: that needs \
		               SurrealDB 4.1. `schema_errors` above zero means the schema did not \
		               apply cleanly; run `check` to see why.",
		annotations(
			title = "Check a query",
			read_only_hint = true,
			destructive_hint = false,
			idempotent_hint = true,
			open_world_hint = false
		)
	)]
	pub async fn check_query(
		&self,
		Parameters(input): Parameters<CheckQueryInput>,
	) -> CallToolResult {
		let server = self.clone();
		run_captured(
			|found: &QueryCheck| {
				let mut summary = counts(found.errors, found.warnings);
				if !found.analyzed {
					summary.push_str(" (not analyzed: needs SurrealDB 4.1)");
				}
				summary
			},
			|| async move {
				let ctx = server.context(&work(&input.schema, &input.folder, &input.vars))?;
				let vars = check_vars(&server, &ctx, &input.vars)?;
				let request = QueryCheckRequest {
					text: input.query,
					name: input.name,
					target: input.target,
					schema: input.schema,
				};
				check_query(&ctx.project, &ctx.root, &ctx.folder, &vars, &request).await
			},
		)
		.await
	}

	/// Check the whole project.
	#[tool(
		name = "check",
		description = "Check the whole project like `surrealkit check --json`: apply the \
		               schema files of the selected modules to a throwaway in-memory engine, \
		               and report what the engine rejects (K9001-K9003), the host-code lints \
		               when [typegen.queries] is set, and the findings of every other .surql \
		               file, graded by [analyze] policy and suppressions. The result is the \
		               `check --json` document: `summary` and `diagnostics`; `passed` is \
		               false when a finding is an error. Without `target` it reads files and \
		               connects to nothing. With `target` it also compares the files with \
		               that [target.<name>] database (drift, the K91xx findings): that \
		               connects to it and only reads, schema and SurrealKit's own metadata, \
		               and writes nothing.",
		annotations(
			title = "Check the project",
			read_only_hint = true,
			destructive_hint = false,
			idempotent_hint = true,
			open_world_hint = true
		)
	)]
	pub async fn check(&self, Parameters(input): Parameters<CheckInput>) -> CallToolResult {
		let server = self.clone();
		run_captured(
			|report: &JsonReport| {
				let mut summary = counts(report.summary.errors, report.summary.warnings);
				if !report.summary.queries_analyzed && report.summary.query_files > 0 {
					summary.push_str(" (query files not analyzed: needs SurrealDB 4.1)");
				}
				summary
			},
			|| async move {
				let ctx = server.context(&work(&input.schema, &input.folder, &input.vars))?;
				let vars = check_vars(&server, &ctx, &input.vars)?;
				// Only a named target resolves one: the default selection is the primary
				// target, and a check without `target` must not start connecting to it.
				let targets = match &input.target {
					Some(name) => Selection::resolve(
						&ctx.project,
						&ctx.cfg,
						&input.schema,
						std::slice::from_ref(name),
						false,
						false,
					)?
					.targets()
					.to_vec(),
					None => Vec::new(),
				};
				let prepared = Prepared::new(
					ctx.project.clone(),
					&ctx.typegen,
					&input.schema,
					false,
					false,
					vars,
				)?;
				let report = prepared.run(&ctx.root, &ctx.folder, &targets).await?;
				Ok(render::json(&report, &ctx.root))
			},
		)
		.await
	}
}
