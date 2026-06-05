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
use rust_dotenv::dotenv::DotEnv;
pub use types::TestOpts;

use crate::config::{AuthLevel, Cfg, ConfigOverrides};
use crate::variables::TemplateVars;

pub async fn run_test(
	dotenv: Option<&DotEnv>,
	opts: TestOpts,
	vars: TemplateVars,
	overrides: &ConfigOverrides,
) -> Result<()> {
	let cfg = Cfg::from_env(dotenv, overrides)?;
	if matches!(cfg.auth_level(), AuthLevel::Database) {
		bail!(
			"`surrealkit test` requires auth level 'root' or 'namespace'/'ns'; got 'database'. \
			 Database-scoped users cannot create the per-suite database needed for test isolation."
		);
	}
	let loaded = loader::load_specs(cfg.folder())?;
	let filter_input = types::FilterInput {
		suite_pattern: opts.suite.clone(),
		case_pattern: opts.case.clone(),
		tags: opts.tags.clone(),
	};
	let suites = filters::apply_filters(loaded.suites, &filter_input);
	if suites.is_empty() {
		bail!("No suites matched the selected filters");
	}

	let base_url = resolve_base_url(&opts, &loaded.global, cfg.host());
	let timeout_ms = resolve_timeout_ms(&opts, &loaded.global);
	let ctx =
		runner::RunnerContext::new(cfg, opts.clone(), loaded.global, base_url, timeout_ms, vars);
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

fn resolve_base_url(
	opts: &TestOpts,
	global: &types::GlobalTestConfig,
	resolved_host: &str,
) -> Option<String> {
	opts.base_url
		.clone()
		.or_else(|| global.defaults.base_url.clone())
		.or_else(|| env::var("SURREALKIT_TEST_BASE_URL").ok())
		.or_else(|| Some(resolved_host.to_string()))
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

#[cfg(test)]
mod tests {
	use std::sync::Mutex;

	use super::*;

	static ENV_LOCK: Mutex<()> = Mutex::new(());

	fn opts(base_url: Option<&str>) -> TestOpts {
		TestOpts {
			suite: None,
			case: None,
			tags: Vec::new(),
			fail_fast: false,
			parallel: 1,
			json_out: None,
			no_setup: false,
			no_sync: false,
			no_seed: false,
			base_url: base_url.map(str::to_string),
			timeout_ms: None,
			keep_db: false,
		}
	}

	#[test]
	fn base_url_falls_back_to_resolved_host() {
		let _guard = ENV_LOCK.lock().unwrap();
		unsafe { env::remove_var("SURREALKIT_TEST_BASE_URL") };

		let base_url = resolve_base_url(
			&opts(None),
			&types::GlobalTestConfig::default(),
			"ws://target-host:8000",
		);

		assert_eq!(base_url.as_deref(), Some("http://target-host:8000"));
	}

	#[test]
	fn test_specific_base_url_beats_resolved_host() {
		let _guard = ENV_LOCK.lock().unwrap();
		unsafe { env::set_var("SURREALKIT_TEST_BASE_URL", "http://env-host:8000") };

		let base_url = resolve_base_url(
			&opts(Some("http://cli-host:8000")),
			&types::GlobalTestConfig::default(),
			"http://target-host:8000",
		);

		assert_eq!(base_url.as_deref(), Some("http://cli-host:8000"));
		unsafe { env::remove_var("SURREALKIT_TEST_BASE_URL") };
	}
}
