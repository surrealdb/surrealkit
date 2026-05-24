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
use crate::schema::load_schema_catalog;
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

	let schema_catalog = load_schema_catalog(None)?;

	let targets = schema_catalog.resolve_targets(
		opts.schema.as_deref(),
		opts.skip_template_schemas,
		cfg.folder(),
		&vars,
		cfg.ns(),
		cfg.db(),
	)?;
	if targets.is_empty() {
		bail!("No resolved schemas found.");
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

	let mut total_failed = 0usize;
	let mut schema_reports = Vec::new();
	for target in targets {
		if target.schema_name().is_some() {
			println!(
				"--- Testing schema '{}' (ns={} db={})",
				target.label(),
				target.ns(),
				target.db()
			);
		}
		let ctx = runner::RunnerContext::new(
			cfg.clone(),
			opts.clone(),
			loaded.global.clone(),
			base_url.clone(),
			timeout_ms,
			vars.clone(),
			target.clone(),
		);
		let report = ctx.run(suites.clone()).await?;

		report::print_human_report(&report);
		total_failed += report.cases_failed;
		schema_reports.push(types::SchemaRunReport {
			schema: target.schema_name().map(str::to_string),
			namespace: target.ns().to_string(),
			database: target.db().to_string(),
			report,
		});
	}

	if let Some(path) = &opts.json_out {
		if schema_reports.len() == 1 {
			report::write_json_report(path, &schema_reports[0].report)?;
		} else {
			report::write_json_value(path, &aggregate_report(schema_reports.clone()))?;
		}
	}

	if total_failed > 0 {
		bail!("{} test cases failed", total_failed);
	}
	Ok(())
}

fn aggregate_report(reports: Vec<types::SchemaRunReport>) -> types::AggregateRunReport {
	let started_at =
		reports.first().map(|entry| entry.report.started_at.clone()).unwrap_or_default();
	let finished_at =
		reports.last().map(|entry| entry.report.finished_at.clone()).unwrap_or_default();
	let schemas_failed = reports.iter().filter(|entry| entry.report.cases_failed > 0).count();
	let duration_ms = reports.iter().map(|entry| entry.report.duration_ms).sum();
	let suites_total = reports.iter().map(|entry| entry.report.suites_total).sum();
	let suites_failed = reports.iter().map(|entry| entry.report.suites_failed).sum();
	let cases_total = reports.iter().map(|entry| entry.report.cases_total).sum();
	let cases_passed = reports.iter().map(|entry| entry.report.cases_passed).sum();
	let cases_failed = reports.iter().map(|entry| entry.report.cases_failed).sum();

	types::AggregateRunReport {
		started_at,
		finished_at,
		duration_ms,
		schemas_total: reports.len(),
		schemas_failed,
		suites_total,
		suites_failed,
		cases_total,
		cases_passed,
		cases_failed,
		schemas: reports,
	}
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
	use crate::tester::types::RunReport;

	static ENV_LOCK: Mutex<()> = Mutex::new(());

	fn opts(base_url: Option<&str>) -> TestOpts {
		TestOpts {
			schema: None,
			skip_template_schemas: false,
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

	#[test]
	fn aggregate_report_sums_schema_reports() {
		let report = |failed| RunReport {
			started_at: "2020-01-01T00:00:00Z".into(),
			finished_at: "2020-01-01T00:00:01Z".into(),
			duration_ms: 10,
			suites_total: 1,
			suites_failed: usize::from(failed),
			cases_total: 2,
			cases_passed: if failed {
				1
			} else {
				2
			},
			cases_failed: usize::from(failed),
			suites: Vec::new(),
		};

		let aggregate = aggregate_report(vec![
			types::SchemaRunReport {
				schema: Some("admin".into()),
				namespace: "system".into(),
				database: "main".into(),
				report: report(false),
			},
			types::SchemaRunReport {
				schema: Some("org".into()),
				namespace: "org_acme".into(),
				database: "main".into(),
				report: report(true),
			},
		]);

		assert_eq!(aggregate.schemas_total, 2);
		assert_eq!(aggregate.schemas_failed, 1);
		assert_eq!(aggregate.cases_total, 4);
		assert_eq!(aggregate.cases_failed, 1);
	}
}
