use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use serde_json::Value;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb_types::SurrealValue;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::sync::Semaphore;

use super::actors::{
	ActorSession, actor_name_or_default, build_actor_sessions, merged_actor_specs, require_actor,
};
use super::api::execute_api_case;
use super::assertions::{JsonAssertionContext, assert_json_value_with_context};
use super::types::{
	AssertionReport, CaseKind, CaseReport, FilterInput, GlobalTestConfig, JsonAssertionSpec,
	LoadedSuite, PermissionAction, RunReport, SuiteReport, TestOpts,
};
use crate::config::DbCfg;
use crate::core::create_surreal_client;
use crate::seed;
use crate::setup::run_setup;
use crate::sync::{self, SyncOpts};

pub struct RunnerContext {
	pub cfg: DbCfg,
	pub opts: TestOpts,
	pub global: GlobalTestConfig,
	pub base_url: Option<String>,
	pub timeout_ms: u64,
	run_id: String,
}

impl RunnerContext {
	pub fn new(
		cfg: DbCfg,
		opts: TestOpts,
		global: GlobalTestConfig,
		base_url: Option<String>,
		timeout_ms: u64,
	) -> Self {
		Self {
			cfg,
			opts,
			global,
			base_url,
			timeout_ms,
			run_id: unique_run_id(),
		}
	}

	pub async fn run(&self, suites: Vec<LoadedSuite>) -> Result<RunReport> {
		let started_at = OffsetDateTime::now_utc();
		let run_start = Instant::now();

		let suite_reports = if self.opts.parallel <= 1 {
			self.run_sequential(suites).await?
		} else {
			self.run_parallel(suites).await?
		};

		let suites_total = suite_reports.len();
		let suites_failed = suite_reports.iter().filter(|s| s.cases_failed > 0).count();
		let cases_total: usize = suite_reports.iter().map(|s| s.cases_total).sum();
		let cases_passed: usize = suite_reports.iter().map(|s| s.cases_passed).sum();
		let cases_failed: usize = suite_reports.iter().map(|s| s.cases_failed).sum();
		let finished_at = OffsetDateTime::now_utc();

		Ok(RunReport {
			started_at: started_at.format(&Rfc3339)?,
			finished_at: finished_at.format(&Rfc3339)?,
			duration_ms: run_start.elapsed().as_millis(),
			suites_total,
			suites_failed,
			cases_total,
			cases_passed,
			cases_failed,
			suites: suite_reports,
		})
	}

	async fn run_sequential(&self, suites: Vec<LoadedSuite>) -> Result<Vec<SuiteReport>> {
		let mut reports = Vec::new();
		for suite in suites {
			let report = self.run_suite(suite).await?;
			let failed = report.cases_failed > 0;
			reports.push(report);
			if self.opts.fail_fast && failed {
				break;
			}
		}
		Ok(reports)
	}

	async fn run_parallel(&self, suites: Vec<LoadedSuite>) -> Result<Vec<SuiteReport>> {
		let mut reports = Vec::new();
		let limit = self.opts.parallel.max(1);
		let semaphore = Arc::new(Semaphore::new(limit));
		let mut joinset = tokio::task::JoinSet::new();

		for suite in suites {
			let permit = semaphore.clone().acquire_owned().await?;
			let ctx = self.clone_for_task();
			joinset.spawn(async move {
				let _permit = permit;
				ctx.run_suite(suite).await
			});
		}

		while let Some(joined) = joinset.join_next().await {
			match joined {
				Ok(Ok(report)) => {
					let failed = report.cases_failed > 0;
					reports.push(report);
					if self.opts.fail_fast && failed {
						joinset.abort_all();
						break;
					}
				}
				Ok(Err(err)) => {
					joinset.abort_all();
					return Err(err);
				}
				Err(join_err) => {
					if !join_err.is_cancelled() {
						return Err(anyhow!("suite task failed: {}", join_err));
					}
				}
			}
		}

		reports.sort_by(|a, b| a.suite_file.cmp(&b.suite_file));
		Ok(reports)
	}

	fn clone_for_task(&self) -> Self {
		Self {
			cfg: self.cfg.clone(),
			opts: self.opts.clone(),
			global: self.global.clone(),
			base_url: self.base_url.clone(),
			timeout_ms: self.timeout_ms,
			run_id: self.run_id.clone(),
		}
	}

	async fn run_suite(&self, suite: LoadedSuite) -> Result<SuiteReport> {
		let started = Instant::now();
		let suite_name =
			suite.spec.name.clone().unwrap_or_else(|| suite.path.to_string_lossy().to_string());
		let slug = slugify(&format!("{}-{}", suite_name, suite.path.display()));
		let namespace = format!("{}_sk_test_{}_{}", self.cfg.ns(), self.run_id, slug);
		let database = format!("{}_sk_test_{}_{}", self.cfg.db(), self.run_id, slug);
		let host = self.cfg.host().to_string();

		let actors = self.prepare_suite(&suite, &host, &namespace, &database).await?;
		let mut cases = Vec::new();

		for case in &suite.spec.cases {
			let case_start = Instant::now();
			let case_result =
				run_case(case, &actors, self.base_url.as_deref(), self.timeout_ms).await;

			let report = match case_result {
				Ok(mut report) => {
					report.duration_ms = case_start.elapsed().as_millis();
					report
				}
				Err(err) => CaseReport {
					name: case.name.clone(),
					kind: case.kind.label().to_string(),
					duration_ms: case_start.elapsed().as_millis(),
					passed: false,
					message: Some(format!("{err:#}")),
					assertions: Vec::new(),
				},
			};

			let failed = !report.passed;
			cases.push(report);
			if self.opts.fail_fast && failed {
				break;
			}
		}

		let cases_total = cases.len();
		let cases_failed = cases.iter().filter(|c| !c.passed).count();
		let cases_passed = cases_total.saturating_sub(cases_failed);

		if !self.opts.keep_db
			&& let Err(err) = cleanup_suite_db(&self.cfg, &host, &namespace, &database).await {
				eprintln!(
					"warning: failed to clean up test db {}/{}: {:#}",
					namespace, database, err
				);
			}

		Ok(SuiteReport {
			suite_file: suite.path.to_string_lossy().replace('\\', "/"),
			suite_name,
			namespace,
			database,
			duration_ms: started.elapsed().as_millis(),
			cases_total,
			cases_passed,
			cases_failed,
			cases,
		})
	}

	async fn prepare_suite(
		&self,
		suite: &LoadedSuite,
		host: &str,
		namespace: &str,
		database: &str,
	) -> Result<HashMap<String, ActorSession>> {
		let merged = merged_actor_specs(&self.global.actors, &suite.spec.actors);
		let bootstrap_actors =
			build_actor_sessions(&self.cfg, host, namespace, database, &BTreeMap::new()).await?;
		let root = require_actor(&bootstrap_actors, "root")?;

		if !self.opts.no_setup {
			run_setup(&root.db).await?;
		}
		if !self.opts.no_sync {
			sync::run_sync(
				&root.db,
				SyncOpts {
					watch: false,
					debounce_ms: 250,
					dry_run: false,
					fail_fast: true,
					prune: true,
					allow_shared_prune: true,
				},
			)
			.await?;
		}
		if !self.opts.no_seed {
			seed::seed(&root.db).await?;
		}

		for fixture in self.global.fixtures.iter().filter(|f| fixture_targets_root(f)) {
			apply_fixture(fixture, &bootstrap_actors, Path::new("database/tests")).await?;
		}
		for fixture in suite.spec.fixtures.iter().filter(|f| fixture_targets_root(f)) {
			let suite_base =
				suite.path.parent().unwrap_or_else(|| Path::new("database/tests/suites"));
			apply_fixture(fixture, &bootstrap_actors, suite_base).await?;
		}

		let actors = build_actor_sessions(&self.cfg, host, namespace, database, &merged).await?;

		for fixture in self.global.fixtures.iter().filter(|f| !fixture_targets_root(f)) {
			apply_fixture(fixture, &actors, Path::new("database/tests")).await?;
		}
		for fixture in suite.spec.fixtures.iter().filter(|f| !fixture_targets_root(f)) {
			let suite_base =
				suite.path.parent().unwrap_or_else(|| Path::new("database/tests/suites"));
			apply_fixture(fixture, &actors, suite_base).await?;
		}

		Ok(actors)
	}
}

async fn run_case(
	case: &crate::tester::types::CaseSpec,
	actors: &HashMap<String, ActorSession>,
	base_url: Option<&str>,
	timeout_ms: u64,
) -> Result<CaseReport> {
	match &case.kind {
		CaseKind::SqlExpect(spec) => {
			let actor_name = actor_name_or_default(spec.actor.as_deref());
			let actor = require_actor(actors, actor_name)?;
			let result = execute_sql_value(&actor.db, &spec.sql).await;
			report_sql_expect(
				case.name.clone(),
				case.kind.label().to_string(),
				result,
				spec.allow,
				spec.error_contains.as_deref(),
				spec.error_code.as_deref(),
				&spec.assertions,
				actor,
			)
		}
		CaseKind::PermissionsMatrix(spec) => {
			if spec.rules.is_empty() {
				bail!("permissions_matrix case '{}' has no rules", case.name);
			}
			let actor_name = actor_name_or_default(spec.actor.as_deref());
			let actor = require_actor(actors, actor_name)?;
			let root = require_actor(actors, "root")?;
			let record_id = spec.record_id.clone().unwrap_or_else(|| "perm_record".to_string());

			let mut assertions = Vec::new();
			for (idx, rule) in spec.rules.iter().enumerate() {
				let seed_sql = format!(
					"UPSERT {}:{} MERGE {{ __surrealkit_perm_seed: true }};",
					spec.table, record_id
				);
				let _ = execute_sql_value(&root.db, &seed_sql).await;
				let sql = match rule.action {
					PermissionAction::Create => format!(
						"CREATE {}:{}_create_{} CONTENT {{ marker: 'perm' }};",
						spec.table, record_id, idx
					),
					PermissionAction::Select => {
						format!("SELECT * FROM {}:{};", spec.table, record_id)
					}
					PermissionAction::Update => format!(
						"UPDATE {}:{} SET marker = 'updated_{}';",
						spec.table, record_id, idx
					),
					PermissionAction::Delete => {
						format!("DELETE {}:{};", spec.table, record_id)
					}
					PermissionAction::Query => rule.sql.clone().ok_or_else(|| {
						anyhow!("permissions_matrix action=query in '{}' requires sql", case.name)
					})?,
				};

				let result = execute_sql_value(&actor.db, &sql).await;
				let mut report = evaluate_outcome(
					format!("rule_{}", idx + 1),
					result,
					rule.allow,
					rule.error_contains.as_deref(),
					None,
				)?;
				if !report.passed {
					report.message = format!("{}; sql={}", report.message, sql);
				}
				assertions.push(report);
			}

			let passed = assertions.iter().all(|x| x.passed);
			Ok(CaseReport {
				name: case.name.clone(),
				kind: case.kind.label().to_string(),
				duration_ms: 0,
				passed,
				message: if passed {
					None
				} else {
					Some("one or more permission rules failed".to_string())
				},
				assertions,
			})
		}
		CaseKind::SchemaMetadata(spec) => {
			let actor_name = actor_name_or_default(spec.actor.as_deref());
			let actor = require_actor(actors, actor_name)?;
			let sql = if let Some(sql) = &spec.sql {
				sql.clone()
			} else {
				let table = spec
					.table
					.as_deref()
					.ok_or_else(|| anyhow!("schema_metadata requires either table or sql"))?;
				format!("INFO FOR TABLE {};", table)
			};
			let value = execute_sql_value(&actor.db, &sql).await?;
			let text = value.to_string();
			let mut assertions = Vec::new();
			for (idx, needle) in spec.contains.iter().enumerate() {
				assertions.push(AssertionReport {
					name: format!("contains_{}", idx + 1),
					passed: text.contains(needle),
					message: format!("expected metadata to contain '{}'", needle),
				});
			}
			for (idx, assertion) in spec.assertions.iter().enumerate() {
				assertions.push(assert_json_value_with_context(
					&value,
					assertion,
					idx,
					&actor_assertion_context(actor),
				)?);
			}
			let passed = assertions.iter().all(|x| x.passed);
			Ok(CaseReport {
				name: case.name.clone(),
				kind: case.kind.label().to_string(),
				duration_ms: 0,
				passed,
				message: if passed {
					None
				} else {
					Some("schema metadata assertions failed".to_string())
				},
				assertions,
			})
		}
		CaseKind::SchemaBehavior(spec) => {
			let actor_name = actor_name_or_default(spec.actor.as_deref());
			let actor = require_actor(actors, actor_name)?;
			for sql in &spec.setup_sql {
				execute_sql_value(&actor.db, sql).await.with_context(|| {
					format!("schema_behavior setup failed in case '{}'", case.name)
				})?;
			}

			let action_result = execute_sql_value(&actor.db, &spec.action_sql).await;
			let mut report = report_sql_expect(
				case.name.clone(),
				case.kind.label().to_string(),
				action_result,
				spec.expect_success,
				spec.expect_error_contains.as_deref(),
				None,
				&Vec::new(),
				actor,
			)?;

			if report.passed && !spec.assertions.is_empty() {
				let verify_sql = spec.verify_sql.clone().unwrap_or_else(|| spec.action_sql.clone());
				let value = execute_sql_value(&actor.db, &verify_sql).await?;
				for (idx, assertion) in spec.assertions.iter().enumerate() {
					report.assertions.push(assert_json_value_with_context(
						&value,
						assertion,
						idx,
						&actor_assertion_context(actor),
					)?);
				}
				report.passed = report.assertions.iter().all(|x| x.passed);
				if !report.passed {
					report.message = Some("schema behavior assertions failed".to_string());
				}
			}

			Ok(report)
		}
		CaseKind::ApiRequest(spec) => {
			let actor_name = actor_name_or_default(spec.actor.as_deref());
			let actor = require_actor(actors, actor_name)?;
			let base_url = base_url.ok_or_else(|| {
				anyhow!(
					"api_request case '{}' requires base URL (--base-url, config default, or env)",
					case.name
				)
			})?;
			let api_result = execute_api_case(base_url, spec, actor, timeout_ms).await?;
			let passed = api_result.assertions.iter().all(|x| x.passed);
			Ok(CaseReport {
				name: case.name.clone(),
				kind: case.kind.label().to_string(),
				duration_ms: 0,
				passed,
				message: if passed {
					None
				} else {
					Some(format!("api assertions failed (status={})", api_result.status))
				},
				assertions: api_result.assertions,
			})
		}
	}
}

fn report_sql_expect(
	name: String,
	kind: String,
	result: Result<Value>,
	allow: bool,
	error_contains: Option<&str>,
	error_code: Option<&str>,
	json_assertions: &[JsonAssertionSpec],
	actor: &ActorSession,
) -> Result<CaseReport> {
	let mut assertions = Vec::new();
	let mut message = None;
	let passed;

	match (allow, result) {
		(true, Ok(value)) => {
			assertions.push(AssertionReport {
				name: "outcome".to_string(),
				passed: true,
				message: "query succeeded as expected".to_string(),
			});
			let ctx = actor_assertion_context(actor);
			for (idx, assertion) in json_assertions.iter().enumerate() {
				assertions.push(assert_json_value_with_context(&value, assertion, idx, &ctx)?);
			}
			passed = assertions.iter().all(|x| x.passed);
			if !passed {
				message = Some("one or more assertions failed".to_string());
			}
		}
		(true, Err(err)) => {
			let text = format!("{err:#}");
			passed = false;
			message = Some(format!("expected success, got error: {}", text));
			assertions.push(AssertionReport {
				name: "outcome".to_string(),
				passed: false,
				message: message.clone().unwrap_or_default(),
			});
		}
		(false, Ok(_)) => {
			passed = false;
			message = Some("expected failure, query succeeded".to_string());
			assertions.push(AssertionReport {
				name: "outcome".to_string(),
				passed: false,
				message: message.clone().unwrap_or_default(),
			});
		}
		(false, Err(err)) => {
			let text = format!("{err:#}");
			let contains_ok = error_contains.map(|needle| text.contains(needle)).unwrap_or(true);
			let code_ok = error_code.map(|code| text.contains(code)).unwrap_or(true);
			passed = contains_ok && code_ok;
			message = if passed {
				None
			} else {
				Some(format!("error mismatch, got '{}'", text))
			};
			assertions.push(AssertionReport {
				name: "outcome".to_string(),
				passed,
				message: if passed {
					"query failed as expected".to_string()
				} else {
					message.clone().unwrap_or_default()
				},
			});
		}
	}

	Ok(CaseReport {
		name,
		kind,
		duration_ms: 0,
		passed,
		message,
		assertions,
	})
}

fn actor_assertion_context(actor: &ActorSession) -> JsonAssertionContext {
	JsonAssertionContext {
		actor_auth: actor.auth.clone(),
	}
}

fn evaluate_outcome(
	label: String,
	result: Result<Value>,
	allow: bool,
	error_contains: Option<&str>,
	error_code: Option<&str>,
) -> Result<AssertionReport> {
	match (allow, result) {
		(true, Ok(_)) => Ok(AssertionReport {
			name: label,
			passed: true,
			message: "query succeeded as expected".to_string(),
		}),
		(true, Err(err)) => {
			let text = format!("{err:#}");
			Ok(AssertionReport {
				name: label,
				passed: false,
				message: format!("expected success, got error: {}", text),
			})
		}
		(false, Err(err)) => {
			let text = format!("{err:#}");
			let contains_ok = error_contains.map(|needle| text.contains(needle)).unwrap_or(true);
			let code_ok = error_code.map(|code| text.contains(code)).unwrap_or(true);
			Ok(AssertionReport {
				name: label,
				passed: contains_ok && code_ok,
				message: if contains_ok && code_ok {
					"query failed as expected".to_string()
				} else {
					format!("error mismatch, got '{}'", text)
				},
			})
		}
		(false, Ok(_)) => Ok(AssertionReport {
			name: label,
			passed: false,
			message: "expected failure, query succeeded".to_string(),
		}),
	}
}

async fn execute_sql_value(db: &Surreal<Any>, sql: &str) -> Result<Value> {
	let mut response = db.query(sql).await?.check()?;
	let raw: surrealdb_types::Value = response.take(0)?;
	let json = Value::from_value(raw).unwrap_or(Value::Null);
	Ok(json)
}

async fn apply_fixture(
	fixture: &crate::tester::types::FixtureSpec,
	actors: &HashMap<String, ActorSession>,
	base_dir: &Path,
) -> Result<()> {
	let actor_name = actor_name_or_default(fixture.actor.as_deref());
	let actor = require_actor(actors, actor_name)?;
	let sql = fixture_sql(fixture, base_dir)?;
	execute_sql_value(&actor.db, &sql).await.with_context(|| {
		format!("fixture '{}' failed", fixture.name.as_deref().unwrap_or("unnamed"))
	})?;
	Ok(())
}

fn fixture_sql(fixture: &crate::tester::types::FixtureSpec, base_dir: &Path) -> Result<String> {
	match (&fixture.sql, &fixture.file) {
		(Some(sql), None) => Ok(sql.clone()),
		(None, Some(file)) => {
			let path = resolve_fixture_path(base_dir, file);
			fs::read_to_string(&path)
				.with_context(|| format!("reading fixture file {}", path.display()))
		}
		(Some(_), Some(_)) => {
			bail!(
				"fixture '{}' cannot define both sql and file",
				fixture.name.as_deref().unwrap_or("unnamed")
			)
		}
		(None, None) => {
			bail!("fixture '{}' requires sql or file", fixture.name.as_deref().unwrap_or("unnamed"))
		}
	}
}

fn resolve_fixture_path(base_dir: &Path, file: &str) -> PathBuf {
	let candidate = Path::new(file);
	if candidate.is_absolute() {
		candidate.to_path_buf()
	} else {
		base_dir.join(candidate)
	}
}

fn fixture_targets_root(fixture: &crate::tester::types::FixtureSpec) -> bool {
	matches!(fixture.actor.as_deref(), None | Some("root"))
}

async fn cleanup_suite_db(cfg: &DbCfg, host: &str, namespace: &str, database: &str) -> Result<()> {
	let db = create_surreal_client(&host.to_string())
		.await
		.with_context(|| format!("connecting for cleanup {host}"))?;
	db.signin(surrealdb::opt::auth::Root {
		username: cfg.user().to_string(),
		password: cfg.pass().to_string(),
	})
	.await
	.context("cleanup root signin failed")?;
	db.use_ns(namespace).await?;
	let drop_db = format!("REMOVE DATABASE {};", database);
	let resp = db.query(drop_db).await?;
	let _ = resp.check();
	Ok(())
}

fn unique_run_id() -> String {
	let ts = OffsetDateTime::now_utc().unix_timestamp_nanos();
	format!("{}", ts)
}

fn slugify(input: &str) -> String {
	let mut out = String::new();
	let mut prev_dash = false;
	for ch in input.chars() {
		let c = ch.to_ascii_lowercase();
		if c.is_ascii_alphanumeric() {
			out.push(c);
			prev_dash = false;
		} else if !prev_dash {
			out.push('_');
			prev_dash = true;
		}
	}
	let trimmed = out.trim_matches('_');
	if trimmed.is_empty() {
		"suite".to_string()
	} else {
		trimmed.to_string()
	}
}

#[expect(dead_code)]
pub fn build_filter_input(opts: &TestOpts) -> FilterInput {
	FilterInput {
		suite_pattern: opts.suite.clone(),
		case_pattern: opts.case.clone(),
		tags: opts.tags.clone(),
	}
}

#[cfg(test)]
mod tests {
	use super::slugify;

	#[test]
	fn slugify_is_safe() {
		assert_eq!(slugify("Hello World"), "hello_world");
		assert_eq!(slugify("***"), "suite");
	}
}
