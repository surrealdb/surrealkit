mod actors;
mod api;
mod assertions;
mod filters;
mod loader;
mod report;
mod runner;
mod types;

use std::{env, io};

use anyhow::{Result, bail};
use rust_dotenv::dotenv::DotEnv;
pub use types::{AssertionReport, CaseReport, RunReport, SuiteReport, TestOpts};

use crate::config::{AuthLevel, DbCfg, DbOverrides};
use crate::variables::TemplateVars;

/// Run the selected suites and return the structured report.
///
/// Performs no console output, and does **not** fail when test cases fail --
/// inspect [`RunReport::cases_failed`] for that. Pre-flight problems (an
/// unsupported auth level, an embedded endpoint, no suites matching the filters)
/// are still `Err`, because they are configuration errors rather than results.
///
/// `opts.json_out` is still honoured: it is a file write, not console output.
///
/// This is the entry point for callers that need the results as data. The CLI
/// uses [`run_test`], which is this plus rendering and a failure exit.
pub async fn run_test_report(
	dotenv: Option<&DotEnv>,
	opts: TestOpts,
	vars: TemplateVars,
	overrides: &DbOverrides,
) -> Result<RunReport> {
	let cfg = DbCfg::from_env(dotenv, overrides)?;
	if matches!(cfg.auth_level(), AuthLevel::Database) {
		bail!(
			"`surrealkit test` requires auth level 'root' or 'namespace'/'ns'; got 'database'. \
			 Database-scoped users cannot create the per-suite database needed for test isolation."
		);
	}
	// Embedded endpoints resolve to `AuthLevel::None` (no signin). The test
	// harness signs in per actor and creates an isolated namespace/database per
	// suite, which the no-auth path does not yet support.
	if matches!(cfg.auth_level(), AuthLevel::None)
		|| crate::config::is_embedded_endpoint(cfg.host())
	{
		bail!(
			"`surrealkit test` does not yet support embedded engines or auth level 'none'; \
			 use a server endpoint with auth level 'root' or 'namespace'/'ns'."
		);
	}
	let loaded = loader::load_specs(cfg.folder())?;
	let filter_input = types::FilterInput::from_opts(&opts);
	let suites = filters::apply_filters(loaded.suites, &filter_input);
	if suites.is_empty() {
		bail!("No suites matched the selected filters");
	}

	let base_url = resolve_base_url(&opts, &loaded.global);
	let timeout_ms = resolve_timeout_ms(&opts, &loaded.global);
	let ctx =
		runner::RunnerContext::new(cfg, opts.clone(), loaded.global, base_url, timeout_ms, vars);
	let report = ctx.run(suites).await?;
	if let Some(path) = &opts.json_out {
		report::write_json_report(path, &report)?;
	}
	Ok(report)
}

/// Render a [`RunReport`] in the CLI's human-readable form.
///
/// Split out so that callers owning a different output channel -- an MCP tool
/// result, a CI annotation -- can render on their own terms. On the stdio MCP
/// transport stdout is the JSON-RPC channel, so writing there is not an option.
pub fn render_report<W: io::Write>(out: &mut W, report: &RunReport) -> Result<()> {
	report::print_human_report(out, report)
}

/// Run the selected suites, print the human report to stdout, and fail when any
/// case failed.
///
/// The CLI's entry point. Library callers that want the results as data should
/// use [`run_test_report`] instead.
pub async fn run_test(
	dotenv: Option<&DotEnv>,
	opts: TestOpts,
	vars: TemplateVars,
	overrides: &DbOverrides,
) -> Result<()> {
	let report = run_test_report(dotenv, opts, vars, overrides).await?;
	render_report(&mut io::stdout().lock(), &report)?;
	if report.cases_failed > 0 {
		bail!("{} test cases failed", report.cases_failed);
	}
	Ok(())
}

fn resolve_base_url(opts: &TestOpts, global: &types::GlobalTestConfig) -> Option<String> {
	opts.base_url
		.clone()
		.or_else(|| global.defaults.base_url.clone())
		.or_else(|| env::var("SURREALKIT_TEST_BASE_URL").ok())
		.or_else(|| env::var("SURREALDB_HOST").ok())
		.map(normalize_base_url)
}

fn resolve_timeout_ms(opts: &TestOpts, global: &types::GlobalTestConfig) -> u64 {
	opts.timeout_ms
		.or(global.defaults.timeout_ms)
		.or_else(|| {
			env::var("SURREALKIT_TEST_TIMEOUT_MS").ok().and_then(|raw| raw.parse::<u64>().ok())
		})
		.unwrap_or(10_000)
}

fn normalize_base_url(raw: String) -> String {
	if let Some(rest) = raw.strip_prefix("ws://") {
		return format!("http://{rest}");
	}
	if let Some(rest) = raw.strip_prefix("wss://") {
		return format!("https://{rest}");
	}
	raw
}
