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
pub use types::{SchemaSource, TestOpts};

use crate::config::{AuthLevel, DbCfg, DbOverrides};
use crate::variables::TemplateVars;

/// How [`run_test_to`] writes a run's report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportFormat {
	/// The summary `surrealkit test` prints for a person.
	Human,
	/// The JSON document `--json-out` writes, pretty-printed.
	Json,
}

/// Run the test suites and print the human report on stdout.
pub async fn run_test(
	dotenv: Option<&DotEnv>,
	opts: TestOpts,
	vars: TemplateVars,
	overrides: &DbOverrides,
) -> Result<()> {
	run_test_to(dotenv, opts, vars, overrides, ReportFormat::Human, &mut io::stdout()).await
}

/// Run the test suites and write the report to `out` in `format`.
///
/// The report is written whether or not every case passed, and a run with a
/// failed case is then an error, as with [`run_test`].
pub async fn run_test_to(
	dotenv: Option<&DotEnv>,
	opts: TestOpts,
	vars: TemplateVars,
	overrides: &DbOverrides,
	format: ReportFormat,
	out: &mut (dyn io::Write + Send),
) -> Result<()> {
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

	match format {
		ReportFormat::Human => report::print_human_report(out, &report)?,
		ReportFormat::Json => {
			serde_json::to_writer_pretty(&mut *out, &report)?;
			writeln!(out)?;
		}
	}
	if let Some(path) = &opts.json_out {
		report::write_json_report(path, &report)?;
	}
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
