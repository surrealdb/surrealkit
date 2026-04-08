mod actors;
mod api;
mod assertions;
mod filters;
mod loader;
mod report;
mod runner;
mod types;

use std::env;

use anyhow::{Result, bail};
pub use types::TestOpts;

use crate::config::DbCfg;

pub async fn run_test(opts: TestOpts) -> Result<()> {
	let cfg = DbCfg::from_env(&rust_dotenv::dotenv::DotEnv::new(""))?;
	let loaded = loader::load_specs()?;
	let filter_input = types::FilterInput {
		suite_pattern: opts.suite.clone(),
		case_pattern: opts.case.clone(),
		tags: opts.tags.clone(),
	};
	let suites = filters::apply_filters(loaded.suites, &filter_input);
	if suites.is_empty() {
		bail!("No suites matched the selected filters");
	}

	let base_url = resolve_base_url(&opts, &loaded.global);
	let timeout_ms = resolve_timeout_ms(&opts, &loaded.global);
	let ctx = runner::RunnerContext::new(cfg, opts.clone(), loaded.global, base_url, timeout_ms);
	let report = ctx.run(suites).await?;

	report::print_human_report(&report);
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
		.or_else(|| env::var("PUBLIC_DATABASE_HOST").ok())
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
