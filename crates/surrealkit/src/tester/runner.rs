use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use serde_json::Value;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb_types::{RecordId, RecordIdKey, SurrealValue, ToSql, uuid};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::sync::Semaphore;

use super::actors::{
	ActorSession, actor_name_or_default, build_actor_sessions, merged_actor_specs, require_actor,
};
use super::api::execute_api_case;
use super::assertions::{JsonAssertionContext, assert_json_value_with_context};
use super::types::{
	AssertionReport, CaseKind, CaseReport, GlobalTestConfig, JsonAssertionSpec, LoadedSuite,
	PermissionAction, RunReport, SchemaSource, SuiteReport, TestOpts,
};
use crate::config::{AuthLevel, DbCfg};
use crate::core::create_surreal_client;
use crate::seed;
use crate::setup::run_setup;
use crate::sync::{self, SyncOpts};
use crate::variables::TemplateVars;

pub struct RunnerContext {
	pub cfg: DbCfg,
	pub opts: TestOpts,
	pub global: GlobalTestConfig,
	pub base_url: Option<String>,
	pub timeout_ms: u64,
	pub vars: TemplateVars,
	run_id: String,
}

impl RunnerContext {
	pub fn new(
		cfg: DbCfg,
		opts: TestOpts,
		global: GlobalTestConfig,
		base_url: Option<String>,
		timeout_ms: u64,
		vars: TemplateVars,
	) -> Self {
		Self {
			cfg,
			opts,
			global,
			base_url,
			timeout_ms,
			vars,
			run_id: unique_run_id(),
		}
	}

	pub async fn run(&self, suites: Vec<LoadedSuite>) -> Result<RunReport> {
		let started_at = OffsetDateTime::now_utc();
		let run_start = Instant::now();

		let jobs = self.jobs(suites);
		let mut suite_reports = Vec::new();
		let replays = jobs.iter().any(|job| job.source == SchemaSource::Rollouts);
		if replays && !self.opts.no_sync && self.global.rollouts.parity.unwrap_or(true) {
			suite_reports.push(self.rollout_parity().await);
		}
		let parity_failed = suite_reports.iter().any(|report| report.cases_failed > 0);
		if !(self.opts.fail_fast && parity_failed) {
			suite_reports.extend(if self.opts.parallel <= 1 {
				self.run_sequential(jobs).await?
			} else {
				self.run_parallel(jobs).await?
			});
		}

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

	/// Each suite once per schema source it runs against.
	fn jobs(&self, suites: Vec<LoadedSuite>) -> Vec<SuiteJob> {
		let mut jobs = Vec::new();
		for suite in suites {
			let source = suite
				.spec
				.schema_from
				.or(self.opts.schema_from)
				.or(self.global.defaults.schema_from)
				.unwrap_or(SchemaSource::Sync);
			let labelled = source == SchemaSource::Both;
			for &single in source.sources() {
				jobs.push(SuiteJob {
					suite: suite.clone(),
					source: single,
					labelled,
				});
			}
		}
		jobs
	}

	async fn run_sequential(&self, jobs: Vec<SuiteJob>) -> Result<Vec<SuiteReport>> {
		let mut reports = Vec::new();
		for job in jobs {
			let report = self.run_suite(job).await?;
			let failed = report.cases_failed > 0;
			reports.push(report);
			if self.opts.fail_fast && failed {
				break;
			}
		}
		Ok(reports)
	}

	async fn run_parallel(&self, jobs: Vec<SuiteJob>) -> Result<Vec<SuiteReport>> {
		let mut reports = Vec::new();
		let limit = self.opts.parallel.max(1);
		let semaphore = Arc::new(Semaphore::new(limit));
		let mut joinset = tokio::task::JoinSet::new();

		for job in jobs {
			let permit = semaphore.clone().acquire_owned().await?;
			let ctx = self.clone_for_task();
			joinset.spawn(async move {
				let _permit = permit;
				ctx.run_suite(job).await
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

		reports.sort_by(|a, b| (&a.suite_file, &a.suite_name).cmp(&(&b.suite_file, &b.suite_name)));
		Ok(reports)
	}

	fn clone_for_task(&self) -> Self {
		Self {
			cfg: self.cfg.clone(),
			opts: self.opts.clone(),
			global: self.global.clone(),
			base_url: self.base_url.clone(),
			timeout_ms: self.timeout_ms,
			vars: self.vars.clone(),
			run_id: self.run_id.clone(),
		}
	}

	/// The isolated namespace and database for a suite, by slug.
	fn suite_names(&self, slug: &str) -> (String, String) {
		match self.cfg.auth_level() {
			AuthLevel::Root => (
				format!("{}_sk_test_{}_{}", self.cfg.ns(), self.run_id, slug),
				format!("{}_sk_test_{}_{}", self.cfg.db(), self.run_id, slug),
			),
			AuthLevel::Namespace => (
				self.cfg.ns().to_string(),
				format!("{}_sk_test_{}_{}", self.cfg.db(), self.run_id, slug),
			),
			AuthLevel::Database | AuthLevel::None => {
				unreachable!("AuthLevel::Database/None are rejected by tester::run_test")
			}
		}
	}

	async fn run_suite(&self, job: SuiteJob) -> Result<SuiteReport> {
		let SuiteJob {
			suite,
			source,
			labelled,
		} = job;
		let started = Instant::now();
		let mut suite_name =
			suite.spec.name.clone().unwrap_or_else(|| suite.path.to_string_lossy().to_string());
		let mut slug_source = format!("{}-{}", suite_name, suite.path.display());
		if labelled {
			suite_name = format!("{suite_name} [{}]", source.label());
			slug_source = format!("{slug_source}-{}", source.label());
		}
		let slug = slugify(&slug_source);
		let (namespace, database) = self.suite_names(&slug);
		let host = self.cfg.host().to_string();
		let base_url =
			self.base_url.as_ref().map(|url| format!("{}/api/{}/{}", url, namespace, database));
		let actors = self.prepare_suite(&suite, &host, &namespace, &database, source).await?;
		let mut cases = Vec::new();

		for case in &suite.spec.cases {
			let case_start = Instant::now();
			let case_result = run_case(case, &actors, base_url.as_deref(), self.timeout_ms).await;

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
			&& let Err(err) = cleanup_suite_db(&self.cfg, &host, &namespace, &database).await
		{
			log::warn!("failed to clean up test db {}/{}: {:#}", namespace, database, err);
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
		source: SchemaSource,
	) -> Result<HashMap<String, ActorSession>> {
		let merged = merged_actor_specs(&self.global.actors, &suite.spec.actors);
		let bootstrap_actors =
			build_actor_sessions(&self.cfg, host, namespace, database, &BTreeMap::new()).await?;
		let root = require_actor(&bootstrap_actors, "root")?;

		if !self.opts.no_setup {
			run_setup(&root.db, self.cfg.folder()).await?;
		}
		if !self.opts.no_sync {
			self.build_schema(&root.db, source).await?;
		}
		if !self.opts.no_seed {
			seed::seed(&root.db, self.cfg.folder(), &self.vars).await?;
		}

		let tests_dir = PathBuf::from(self.cfg.folder()).join("tests");
		let tests_suites_dir = tests_dir.join("suites");
		for fixture in self.global.fixtures.iter().filter(|f| fixture_targets_root(f)) {
			apply_fixture(fixture, &bootstrap_actors, &tests_dir, &self.vars).await?;
		}
		for fixture in suite.spec.fixtures.iter().filter(|f| fixture_targets_root(f)) {
			let suite_base = suite.path.parent().unwrap_or(&tests_suites_dir);
			apply_fixture(fixture, &bootstrap_actors, suite_base, &self.vars).await?;
		}

		let actors = build_actor_sessions(&self.cfg, host, namespace, database, &merged).await?;

		for fixture in self.global.fixtures.iter().filter(|f| !fixture_targets_root(f)) {
			apply_fixture(fixture, &actors, &tests_dir, &self.vars).await?;
		}
		for fixture in suite.spec.fixtures.iter().filter(|f| !fixture_targets_root(f)) {
			let suite_base = suite.path.parent().unwrap_or(&tests_suites_dir);
			apply_fixture(fixture, &actors, suite_base, &self.vars).await?;
		}

		Ok(actors)
	}

	/// Give a fresh suite database its schema, the way `source` says.
	async fn build_schema(&self, db: &Surreal<Any>, source: SchemaSource) -> Result<()> {
		match source {
			SchemaSource::Sync | SchemaSource::Both => {
				sync::run_sync(
					db,
					SyncOpts {
						watch: false,
						debounce_ms: 250,
						dry_run: false,
						fail_fast: true,
						prune: true,
						allow_shared_prune: true,
						allow_empty_prune: false,
						allow_all_statements: false,
						vars: self.vars.clone(),
						folder: self.cfg.folder().to_owned(),
						module: crate::module::Module::default_module(),
						typegen_ts_out: None,
						typegen_ts_format: None,
					},
				)
				.await
			}
			SchemaSource::Rollouts => {
				let report = crate::rollout::run_up(
					db,
					self.cfg.folder(),
					crate::rollout::RolloutUpOpts {
						complete_newest: true,
						..Default::default()
					},
					&self.vars,
				)
				.await
				.context(
					"replaying the rollouts into an empty suite database (schema_from = rollouts)",
				)?;
				if report.pending.is_empty() {
					bail!(
						"schema_from = rollouts, but there are no rollouts in {} to replay",
						crate::constants::rollouts_dir(self.cfg.folder()).display()
					);
				}
				Ok(())
			}
		}
	}

	/// Build one database with sync and one by replaying the rollouts, and report
	/// every definition on which they disagree. A rollout chain that has fallen
	/// behind the schema folder, or a frozen file that no longer says what the
	/// folder says, shows up here instead of in production.
	async fn rollout_parity(&self) -> SuiteReport {
		let started = Instant::now();
		let host = self.cfg.host().to_string();
		let mut built = Vec::new();
		let mut failure = None;
		for source in [SchemaSource::Sync, SchemaSource::Rollouts] {
			let (namespace, database) = self.suite_names(&format!("parity_{}", source.label()));
			let attempt = async {
				let actors =
					build_actor_sessions(&self.cfg, &host, &namespace, &database, &BTreeMap::new())
						.await?;
				let db = require_actor(&actors, "root")?.db.clone();
				run_setup(&db, self.cfg.folder()).await?;
				self.build_schema(&db, source).await?;
				describe_schema(&db).await
			}
			.await;
			match attempt {
				Ok(schema) => built.push(schema),
				Err(err) => {
					failure =
						Some(format!("building the {} database failed: {err:#}", source.label()));
				}
			}
			if !self.opts.keep_db
				&& let Err(err) = cleanup_suite_db(&self.cfg, &host, &namespace, &database).await
			{
				log::warn!("failed to clean up parity db {namespace}/{database}: {err:#}");
			}
			if failure.is_some() {
				break;
			}
		}

		let assertions = match (&failure, built.as_slice()) {
			(None, [synced, replayed]) => compare_schemas(synced, replayed),
			_ => Vec::new(),
		};
		let passed = failure.is_none() && assertions.iter().all(|a| a.passed);
		let case = CaseReport {
			name: "replaying every rollout defines what sync defines".to_string(),
			kind: "rollout_parity".to_string(),
			duration_ms: started.elapsed().as_millis(),
			passed,
			message: failure.or_else(|| {
				(!passed).then(|| {
					"the rollouts and the schema folder disagree; plan a rollout for the difference \
					 with `surrealkit rollout plan`"
						.to_string()
				})
			}),
			assertions,
		};
		SuiteReport {
			suite_file: "rollouts".to_string(),
			suite_name: "rollout parity".to_string(),
			namespace: String::new(),
			database: String::new(),
			duration_ms: started.elapsed().as_millis(),
			cases_total: 1,
			cases_passed: usize::from(passed),
			cases_failed: usize::from(!passed),
			cases: vec![case],
		}
	}
}

/// One suite run against one schema source.
#[derive(Debug, Clone)]
struct SuiteJob {
	suite: LoadedSuite,
	source: SchemaSource,
	/// Whether the suite runs once per source, so its report names the source.
	labelled: bool,
}

/// Every definition in a database, keyed by kind and name, plus the entity
/// catalog SurrealKit recorded for it. SurrealKit's own `__` tables are left out.
async fn describe_schema(db: &Surreal<Any>) -> Result<BTreeMap<String, String>> {
	let mut out = BTreeMap::new();
	let db_info = info_object(db, "INFO FOR DB;", None).await?;
	let mut tables = Vec::new();
	for (section, items) in &db_info {
		let Some(items) = items.as_object() else {
			continue;
		};
		for (name, definition) in items {
			if section == "tables" {
				if name.starts_with("__") {
					continue;
				}
				tables.push(name.clone());
			}
			out.insert(format!("{section}:{name}"), value_text(definition));
		}
	}
	for table in tables {
		let info = info_object(db, "INFO FOR TABLE $table;", Some(&table)).await?;
		for (section, items) in &info {
			if section == "lives" {
				continue;
			}
			let Some(items) = items.as_object() else {
				continue;
			};
			for (name, definition) in items {
				out.insert(format!("{section}:{table}.{name}"), value_text(definition));
			}
		}
	}
	let mut response = db
		.query("SELECT key, val.statement_hash AS hash FROM __entity WHERE ns = 'schema';")
		.await?
		.check()?;
	let rows: Vec<Value> = response.take(0)?;
	for row in rows {
		if let (Some(key), Some(hash)) = (row.get("key").and_then(Value::as_str), row.get("hash")) {
			out.insert(format!("catalog:{key}"), value_text(hash));
		}
	}
	Ok(out)
}

async fn info_object(
	db: &Surreal<Any>,
	sql: &str,
	table: Option<&str>,
) -> Result<serde_json::Map<String, Value>> {
	let mut query = db.query(sql);
	if let Some(table) = table {
		query = query.bind(("table", table.to_string()));
	}
	let mut response = query.await?.check()?;
	let raw: surrealdb_types::Value = response.take(0)?;
	Ok(Value::from_value(raw).ok().and_then(|v| v.as_object().cloned()).unwrap_or_default())
}

fn value_text(value: &Value) -> String {
	value.as_str().map(str::to_string).unwrap_or_else(|| value.to_string())
}

/// One assertion per disagreement, and one that counts the agreements.
fn compare_schemas(
	synced: &BTreeMap<String, String>,
	replayed: &BTreeMap<String, String>,
) -> Vec<AssertionReport> {
	let mut out = Vec::new();
	let mut matching = 0usize;
	let keys: std::collections::BTreeSet<&String> = synced.keys().chain(replayed.keys()).collect();
	for key in keys {
		match (synced.get(key), replayed.get(key)) {
			(Some(a), Some(b)) if a == b => matching += 1,
			(Some(a), Some(b)) => out.push(AssertionReport {
				name: key.clone(),
				passed: false,
				message: format!("differs\n      sync:     {a}\n      rollouts: {b}"),
			}),
			(Some(_), None) => out.push(AssertionReport {
				name: key.clone(),
				passed: false,
				message: "defined by sync but not by the rollouts".to_string(),
			}),
			(None, Some(_)) => out.push(AssertionReport {
				name: key.clone(),
				passed: false,
				message: "defined by the rollouts but not by sync".to_string(),
			}),
			(None, None) => {}
		}
	}
	out.insert(
		0,
		AssertionReport {
			name: "matching definitions".to_string(),
			passed: true,
			message: format!("{matching} definitions agree"),
		},
	);
	out
}

async fn get_record(
	db: &Surreal<Any>,
	record_id: RecordId,
) -> Result<Option<surrealdb_types::Object>> {
	let mut response =
		db.query("SELECT * FROM ONLY $record_id;").bind(("record_id", record_id)).await?.check()?;
	let record: Option<surrealdb_types::Value> = response.take(0)?;
	Ok(record.and_then(|x| x.as_object().cloned()))
}

async fn delete_record(db: &Surreal<Any>, record_id: RecordId) -> Result<()> {
	db.query("DELETE $record_id;").bind(("record_id", record_id)).await?.check()?;
	Ok(())
}

/// What came of copying a record to a new id.
enum Copied {
	/// The copy, at a random id in the same table.
	Made(RecordId),
	/// The copy would duplicate the original in this unique index, so there is
	/// none.
	Collides(String),
}

async fn copy_record(root_db: &Surreal<Any>, record_id: RecordId) -> Result<Copied> {
	let Some(mut content) = get_record(root_db, record_id.clone()).await? else {
		bail!("Record {:?} cannot be copied", record_id);
	};
	let tmp_record_id = RecordId::new(record_id.table.clone(), RecordIdKey::rand());
	content.remove("id");
	let created = root_db
		.query("CREATE $tmp_record_id CONTENT $content;")
		.bind(("tmp_record_id", tmp_record_id.clone()))
		.bind(("content", content))
		.await?
		.check();
	if let Err(err) = created {
		return match unique_index_conflict(&err.to_string()) {
			Some(index) => Ok(Copied::Collides(index.to_string())),
			None => Err(err.into()),
		};
	}
	match get_record(root_db, tmp_record_id.clone()).await? {
		Some(..) => Ok(Copied::Made(tmp_record_id)),
		None => bail!("New record was not copied"),
	}
}

/// The unique index a write was refused by, if `error` is SurrealDB saying the
/// write would duplicate an entry in one.
///
/// SurrealDB sends this as an `Internal` error with no structured details, so
/// the message is all there is to go on: "Database index `name` already
/// contains 'value', with record `table:id`".
fn unique_index_conflict(error: &str) -> Option<&str> {
	let (_, rest) = error.split_once("Database index `")?;
	let (index, rest) = rest.split_once('`')?;
	rest.starts_with(" already contains ").then_some(index)
}

async fn add_marker_field(db: &Surreal<Any>, table: &str) -> Result<()> {
	db.query(format!(
		"DEFINE FIELD IF NOT EXISTS _marker ON {} TYPE option<string> DEFAULT NONE;",
		table
	))
	.await?
	.check()?;
	Ok(())
}

type AssertionResult = Result<(), AssertionError>;

enum AssertionError {
	PermissionThrow(String),
	PermissionFailed(String),
	/// The action was refused for a reason other than a permission, so the rule
	/// says nothing about whether it is allowed. Fails the rule either way.
	Inconclusive(String),
	InternalError(anyhow::Error),
}

impl AssertionError {
	fn failed<E: std::fmt::Display>(error: E) -> Self {
		Self::PermissionFailed(format!("{error}"))
	}

	fn throw<E: std::fmt::Display>(error: E) -> Self {
		Self::PermissionThrow(format!("{error}"))
	}
}

impl From<anyhow::Error> for AssertionError {
	fn from(error: anyhow::Error) -> Self {
		AssertionError::InternalError(error)
	}
}

impl From<surrealdb::Error> for AssertionError {
	fn from(error: surrealdb::Error) -> Self {
		AssertionError::InternalError(anyhow::Error::from(error))
	}
}

impl std::fmt::Display for AssertionError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			AssertionError::PermissionThrow(error) => write!(f, "permission throw: {error}"),
			AssertionError::PermissionFailed(error) => write!(f, "permission failed: {error}"),
			AssertionError::Inconclusive(error) => write!(f, "inconclusive: {error}"),
			AssertionError::InternalError(error) => write!(f, "internal error: {error}"),
		}
	}
}

async fn assert_permission_action_create(
	user_db: &Surreal<Any>,
	root_db: &Surreal<Any>,
	record_id: RecordId,
) -> AssertionResult {
	let record = get_record(root_db, record_id.clone()).await?;
	let mut content = match record {
		Some(record) => record,
		None => {
			return Err(AssertionError::throw(anyhow!(
				"permissions_matrix record {:?} is required to assert permissions",
				record_id
			)));
		}
	};
	content.remove("id");
	let tmp_record_id = RecordId::new(record_id.table.clone(), RecordIdKey::rand());

	// assert create permission
	let create_result = user_db
		.query("CREATE $tmp_record_id CONTENT $content;")
		.bind(("tmp_record_id", tmp_record_id.clone()))
		.bind(("content", content))
		.await?
		.check();

	let record = get_record(root_db, tmp_record_id.clone()).await?;
	if record.is_some() {
		delete_record(root_db, tmp_record_id.clone()).await?;
	}
	if let Err(err) = create_result {
		let text = err.to_string();
		// The new record has the original's content, so a unique index refuses
		// it. That is the index speaking, not a permission, so it is no denial.
		return Err(match unique_index_conflict(&text) {
			Some(index) => AssertionError::Inconclusive(format!(
				"a record with the content of {} duplicates it in the unique index `{index}`, \
				 so the index refused the create and this rule cannot tell whether it is \
				 allowed; test create on this table with a sql_expect case that sets its own \
				 unique values ({text})",
				record_id.to_sql()
			)),
			None => AssertionError::throw(text),
		});
	}
	match record {
		None => Err(AssertionError::failed("New record was not created")),
		Some(..) => Ok(()),
	}
}

async fn assert_permission_action_select(
	user_db: &Surreal<Any>,
	record_id: RecordId,
) -> AssertionResult {
	// assert select permission
	let result = user_db
		.query("SELECT * FROM ONLY $record_id;")
		.bind(("record_id", record_id))
		.await?
		.check();

	match result {
		Err(err) => Err(AssertionError::throw(err)),
		Ok(mut records) => match records.take::<Option<surrealdb_types::Value>>(0) {
			Ok(Some(..)) => Ok(()),
			_ => Err(AssertionError::failed("Record cannot be selected")),
		},
	}
}

async fn assert_permission_action_update(
	user_db: &Surreal<Any>,
	root_db: &Surreal<Any>,
	record_id: RecordId,
) -> AssertionResult {
	add_marker_field(root_db, &record_id.table).await?;
	let tmp_record_id = match copy_record(root_db, record_id.clone()).await? {
		Copied::Made(id) => id,
		Copied::Collides(index) => {
			log::debug!(
				"a copy of {} collides in unique index `{index}`; updating it in place",
				record_id.to_sql()
			);
			return assert_permission_action_update_in_place(user_db, root_db, record_id).await;
		}
	};
	let new_marker = uuid::Uuid::new_v4().to_string();

	// assert update permission
	let update_result = user_db
		.query("UPDATE $tmp_record_id SET _marker = $new_marker;")
		.bind(("tmp_record_id", tmp_record_id.clone()))
		.bind(("new_marker", new_marker.clone()))
		.await?
		.check()
		.map_err(AssertionError::throw);

	let record = get_record(root_db, tmp_record_id.clone()).await?;
	let marker = record.as_ref().and_then(|r| r.get("_marker")).and_then(|m| m.as_string());
	if record.is_some() {
		delete_record(root_db, tmp_record_id.clone()).await?;
	}
	update_result?;
	match marker {
		Some(marker) if marker == &new_marker => Ok(()),
		_ => Err(AssertionError::failed("Record cannot be updated")),
	}
}

async fn assert_permission_action_delete(
	user_db: &Surreal<Any>,
	root_db: &Surreal<Any>,
	record_id: RecordId,
) -> AssertionResult {
	let tmp_record_id = match copy_record(root_db, record_id.clone()).await? {
		Copied::Made(id) => id,
		Copied::Collides(index) => {
			log::debug!(
				"a copy of {} collides in unique index `{index}`; deleting it in place",
				record_id.to_sql()
			);
			return assert_permission_action_delete_in_place(user_db, root_db, record_id).await;
		}
	};

	// assert delete permission
	let delete_result = user_db
		.query("DELETE $tmp_record_id;")
		.bind(("tmp_record_id", tmp_record_id.clone()))
		.await?
		.check()
		.map_err(AssertionError::throw);

	let record = get_record(root_db, tmp_record_id.clone()).await?;
	if record.is_some() {
		delete_record(root_db, tmp_record_id.clone()).await?;
	}
	delete_result?;
	match record {
		None => Ok(()),
		Some(..) => Err(AssertionError::failed("Record cannot be deleted")),
	}
}

/// The record a `record_id = "$auth"` case acts on: the one `actor` is signed in
/// as, which must be in the case's table.
fn auth_record_for(
	case: &str,
	actor_name: &str,
	actor: &ActorSession,
	table: &str,
) -> Result<RecordId> {
	let Some(record) = actor.auth_record.clone() else {
		bail!(
			"permissions_matrix case '{case}' uses record_id = \"$auth\", but actor \
			 '{actor_name}' is not signed in as a record (it needs `kind = \"record\"`)"
		);
	};
	if record.table.as_str() != table {
		bail!(
			"permissions_matrix case '{case}' is for table '{table}', but actor '{actor_name}' is \
			 signed in as {}, a record in '{}'",
			actor.auth.as_ref().map(ToString::to_string).unwrap_or_default(),
			record.table
		);
	}
	Ok(record)
}

/// Update a record as the actor, then put it back as it was.
///
/// Used for `$auth`, where only the real record can satisfy a permission keyed
/// on it, and for any record whose copy a unique index refuses. The restore runs
/// as root with the record's original content, so a field computed with `VALUE`
/// is recomputed then.
async fn assert_permission_action_update_in_place(
	user_db: &Surreal<Any>,
	root_db: &Surreal<Any>,
	record_id: RecordId,
) -> AssertionResult {
	add_marker_field(root_db, &record_id.table).await?;
	let Some(mut original) = get_record(root_db, record_id.clone()).await? else {
		return Err(AssertionError::throw(anyhow!("record {:?} does not exist", record_id)));
	};
	original.remove("id");
	let new_marker = uuid::Uuid::new_v4().to_string();
	let update_result = user_db
		.query("UPDATE $record_id SET _marker = $new_marker;")
		.bind(("record_id", record_id.clone()))
		.bind(("new_marker", new_marker.clone()))
		.await?
		.check()
		.map_err(AssertionError::throw);

	let after = get_record(root_db, record_id.clone()).await?;
	let marker = after.as_ref().and_then(|r| r.get("_marker")).and_then(|m| m.as_string()).cloned();
	root_db
		.query("UPDATE $record_id CONTENT $original;")
		.bind(("record_id", record_id))
		.bind(("original", original))
		.await?
		.check()?;
	update_result?;
	match marker {
		Some(marker) if marker == new_marker => Ok(()),
		_ => Err(AssertionError::failed("Record cannot be updated")),
	}
}

/// Delete a record as the actor, then create it again as it was.
///
/// Used for `$auth`, and for any record whose copy a unique index refuses.
/// Anything the delete cascaded to through `REFERENCE ... ON DELETE` is not put
/// back.
async fn assert_permission_action_delete_in_place(
	user_db: &Surreal<Any>,
	root_db: &Surreal<Any>,
	record_id: RecordId,
) -> AssertionResult {
	let Some(mut original) = get_record(root_db, record_id.clone()).await? else {
		return Err(AssertionError::throw(anyhow!("record {:?} does not exist", record_id)));
	};
	original.remove("id");
	let delete_result = user_db
		.query("DELETE $record_id;")
		.bind(("record_id", record_id.clone()))
		.await?
		.check()
		.map_err(AssertionError::throw);

	let still_there = get_record(root_db, record_id.clone()).await?.is_some();
	if !still_there {
		root_db
			.query("CREATE $record_id CONTENT $original;")
			.bind(("record_id", record_id))
			.bind(("original", original))
			.await?
			.check()?;
	}
	delete_result?;
	if still_there {
		Err(AssertionError::failed("Record cannot be deleted"))
	} else {
		Ok(())
	}
}

async fn assert_permission_action_query(user_db: &Surreal<Any>, sql: &str) -> AssertionResult {
	user_db.query(sql).await?.check().map(|_| ()).map_err(AssertionError::throw)
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
			// `$auth` is the record the actor is signed in as. Rules on it act on that
			// record itself: a copy has another id, so a permission written as
			// `WHERE id = $auth` could never match one.
			let in_place = spec.record_id.as_deref() == Some("$auth");
			let record_id = if in_place {
				auth_record_for(&case.name, actor_name, actor, &spec.table)?
			} else {
				RecordId::new(
					spec.table.clone(),
					RecordIdKey::String(
						spec.record_id.clone().unwrap_or_else(|| "perm_record".to_string()),
					),
				)
			};

			let mut assertions = Vec::new();
			for (idx, rule) in spec.rules.iter().enumerate() {
				let rec_id = record_id.clone();
				let result = match rule.action {
					PermissionAction::Create => {
						assert_permission_action_create(&actor.db, &root.db, rec_id).await
					}
					PermissionAction::Select => {
						assert_permission_action_select(&actor.db, rec_id).await
					}
					PermissionAction::Update if in_place => {
						assert_permission_action_update_in_place(&actor.db, &root.db, rec_id).await
					}
					PermissionAction::Update => {
						assert_permission_action_update(&actor.db, &root.db, rec_id).await
					}
					PermissionAction::Delete if in_place => {
						assert_permission_action_delete_in_place(&actor.db, &root.db, rec_id).await
					}
					PermissionAction::Delete => {
						assert_permission_action_delete(&actor.db, &root.db, rec_id).await
					}
					PermissionAction::Query => {
						let verify_sql = rule.sql.clone().ok_or_else(|| {
							anyhow!(
								"permissions_matrix action=query in '{}' requires sql",
								case.name
							)
						})?;
						assert_permission_action_query(&actor.db, &verify_sql).await
					}
				};

				if let Err(AssertionError::InternalError(error)) = result {
					return Err(error);
				}

				let label = rule
					.name
					.clone()
					.unwrap_or_else(|| format!("action:{} (#{})", rule.action.label(), idx + 1));
				let report = evaluate_outcome(
					label,
					result,
					rule.allow,
					rule.error_contains.as_deref(),
					None,
				)?;
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

// Each parameter is a distinct piece of the expectation being reported; bundling
// them into a struct would only move the argument list to the call sites.
#[expect(clippy::too_many_arguments)]
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
	result: AssertionResult,
	allow: bool,
	error_contains: Option<&str>,
	error_code: Option<&str>,
) -> Result<AssertionReport> {
	// Neither a pass nor a denial, whatever the rule expects.
	if let Err(err @ AssertionError::Inconclusive(..)) = &result {
		return Ok(AssertionReport {
			name: label,
			passed: false,
			message: format!("{err}"),
		});
	}
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
	vars: &TemplateVars,
) -> Result<()> {
	let actor_name = actor_name_or_default(fixture.actor.as_deref());
	let actor = require_actor(actors, actor_name)?;
	let raw_sql = fixture_sql(fixture, base_dir)?;
	let sql = vars.apply(&raw_sql).with_context(|| {
		format!(
			"applying template variables to fixture '{}'",
			fixture.name.as_deref().unwrap_or("unnamed")
		)
	})?;
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
	match cfg.auth_level() {
		AuthLevel::Root => {
			db.signin(surrealdb::opt::auth::Root {
				username: cfg.user().to_string(),
				password: cfg.pass().to_string(),
			})
			.await
			.context("cleanup signin failed (auth_level=root)")?;
			db.use_ns(namespace).await?;
			let drop_db = format!("REMOVE DATABASE IF EXISTS {};", database);
			let resp = db.query(drop_db).await?;
			let _ = resp.check();
			let drop_ns = format!("REMOVE NAMESPACE IF EXISTS {};", namespace);
			let resp = db.query(drop_ns).await?;
			let _ = resp.check();
		}
		AuthLevel::Namespace => {
			db.signin(surrealdb::opt::auth::Namespace {
				namespace: namespace.to_string(),
				username: cfg.user().to_string(),
				password: cfg.pass().to_string(),
			})
			.await
			.context("cleanup signin failed (auth_level=namespace)")?;
			let drop_db = format!("REMOVE DATABASE IF EXISTS {};", database);
			let resp = db.query(drop_db).await?;
			let _ = resp.check();
		}
		AuthLevel::Database | AuthLevel::None => {
			unreachable!("AuthLevel::Database/None are rejected by tester::run_test")
		}
	}
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

#[cfg(test)]
mod tests {
	use std::collections::{BTreeMap, HashMap};
	use std::path::Path;

	use super::super::actors::ActorSession;
	use super::super::types::FixtureSpec;
	use super::{apply_fixture, fixture_sql, slugify};
	use crate::variables::TemplateVars;

	#[test]
	fn slugify_is_safe() {
		assert_eq!(slugify("Hello World"), "hello_world");
		assert_eq!(slugify("***"), "suite");
	}

	#[test]
	fn fixture_sql_returns_inline_sql_verbatim() {
		// Substitution is the job of apply_fixture, not fixture_sql — sanity check.
		let f = FixtureSpec {
			name: Some("inline".into()),
			actor: None,
			sql: Some("SELECT ${KB_ROOT};".into()),
			file: None,
		};
		assert_eq!(fixture_sql(&f, Path::new(".")).unwrap(), "SELECT ${KB_ROOT};");
	}

	#[tokio::test]
	async fn apply_fixture_substitutes_template_vars_in_inline_sql() {
		// Regression test: shared/suite fixtures used to receive `${VAR}` tokens
		// verbatim because apply_fixture skipped TemplateVars::apply. Other fixture
		// flows (rollouts, seeds, schema apply) already substitute, so behavior
		// across flows was inconsistent.
		let db = crate::test_db::fresh("apply_fixture_substitution").await;

		let mut actors = HashMap::new();
		actors.insert(
			"root".to_string(),
			ActorSession {
				db: db.clone(),
				headers: BTreeMap::new(),
				auth: None,
				auth_record: None,
			},
		);

		let mut map = HashMap::new();
		map.insert("FIXTURE_PATH".to_string(), "/test/kb".to_string());
		let vars = TemplateVars {
			vars: map,
		};

		let fixture = FixtureSpec {
			name: Some("inline-substitution".into()),
			actor: None,
			sql: Some("CREATE marker:fixture_subst SET path = '${FIXTURE_PATH}';".into()),
			file: None,
		};

		apply_fixture(&fixture, &actors, Path::new("."), &vars).await.expect("apply_fixture");

		let mut resp = db
			.query("SELECT path FROM marker:fixture_subst;")
			.await
			.expect("query marker")
			.check()
			.expect("check");
		let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
		assert_eq!(rows.len(), 1, "marker row must exist");
		assert_eq!(
			rows[0].get("path").and_then(|v| v.as_str()),
			Some("/test/kb"),
			"fixture SQL must have ${{FIXTURE_PATH}} substituted before reaching the DB"
		);
	}

	#[tokio::test]
	async fn apply_fixture_propagates_undefined_var_error() {
		// When a fixture references a variable that wasn't provided, the failure
		// should surface as an error from apply_fixture (not silently send `${...}`
		// to the DB, where it would fail with a confusing syntax/parser error).
		let db = crate::test_db::fresh("apply_fixture_undefined_var").await;

		let mut actors = HashMap::new();
		actors.insert(
			"root".to_string(),
			ActorSession {
				db: db.clone(),
				headers: BTreeMap::new(),
				auth: None,
				auth_record: None,
			},
		);

		let fixture = FixtureSpec {
			name: Some("missing-var".into()),
			actor: None,
			sql: Some("SELECT '${UNDEFINED_VAR}';".into()),
			file: None,
		};

		let err = apply_fixture(&fixture, &actors, Path::new("."), &TemplateVars::default())
			.await
			.expect_err("undefined variable must error");
		let msg = err.to_string();
		assert!(msg.contains("missing-var"), "error should name the fixture: {err}");
	}

	#[test]
	fn unique_index_conflict_reads_the_index_from_surrealdb_errors() {
		use super::unique_index_conflict;
		let message = "Database index `account_email` already contains 'seeded@example.test', \
		               with record `account:seeded`";
		assert_eq!(unique_index_conflict(message), Some("account_email"));
		// However the error was wrapped on the way here.
		assert_eq!(
			unique_index_conflict(&format!("internal error: query failed: {message}")),
			Some("account_email")
		);
		// A composite index reports an array of values.
		assert_eq!(
			unique_index_conflict(
				"Database index `by_ns_key` already contains ['schema', 'table:a'], with record \
				 `__entity:x`"
			),
			Some("by_ns_key")
		);
		for other in [
			"Permission denied: You are not allowed to access this resource",
			"Database record `account:seeded` already exists",
			"Found 'x' for field `email`, with record `account:a`, but expected a string",
			"Database index `account_email` does not exist",
		] {
			assert_eq!(unique_index_conflict(other), None, "{other}");
		}
	}

	#[test]
	fn an_inconclusive_rule_fails_whatever_it_expects() {
		use super::{AssertionError, evaluate_outcome};
		for allow in [true, false] {
			let report = evaluate_outcome(
				"rule".into(),
				Err(AssertionError::Inconclusive("the index refused it".into())),
				allow,
				None,
				None,
			)
			.unwrap();
			assert!(!report.passed, "allow = {allow}");
			assert_eq!(report.message, "inconclusive: the index refused it");
		}
		// `error_contains` matching the text does not turn it into a denial.
		let report = evaluate_outcome(
			"rule".into(),
			Err(AssertionError::Inconclusive("index".into())),
			false,
			Some("index"),
			None,
		)
		.unwrap();
		assert!(!report.passed);
	}

	/// `handle` has a unique index, `plain` has none. Without signing in, the
	/// embedded engine lets a session do anything, so the root session stands in
	/// for an actor who is allowed every action.
	async fn unique_and_plain_tables(
		label: &str,
	) -> surrealdb::Surreal<surrealdb::engine::any::Any> {
		let db = crate::test_db::fresh(label).await;
		db.query(
			"DEFINE TABLE handle SCHEMAFULL;
			DEFINE FIELD name ON handle TYPE string;
			DEFINE INDEX handle_name ON handle FIELDS name UNIQUE;
			DEFINE TABLE plain SCHEMAFULL;
			DEFINE FIELD name ON plain TYPE string;
			CREATE handle:taken SET name = 'taken';
			CREATE plain:kept SET name = 'kept';",
		)
		.await
		.unwrap()
		.check()
		.unwrap();
		db
	}

	async fn rows(
		db: &surrealdb::Surreal<surrealdb::engine::any::Any>,
		table: &str,
	) -> Vec<serde_json::Value> {
		let mut response =
			db.query(format!("SELECT * FROM {table} ORDER BY id;")).await.unwrap().check().unwrap();
		response.take(0).unwrap()
	}

	fn rid(table: &str, key: &str) -> surrealdb_types::RecordId {
		surrealdb_types::RecordId::new(
			table.to_string(),
			surrealdb_types::RecordIdKey::String(key.to_string()),
		)
	}

	fn expect_ok(result: super::AssertionResult) {
		if let Err(err) = result {
			panic!("expected the action to be allowed, got {err}");
		}
	}

	#[tokio::test]
	async fn a_copy_that_collides_in_a_unique_index_is_reported_not_raised() {
		use super::{Copied, copy_record};
		let db = unique_and_plain_tables("copy_collides").await;

		match copy_record(&db, rid("handle", "taken")).await.unwrap() {
			Copied::Collides(index) => assert_eq!(index, "handle_name"),
			Copied::Made(id) => panic!("copied to {id:?} despite the unique index"),
		}
		assert_eq!(rows(&db, "handle").await.len(), 1, "a failed copy leaves nothing behind");

		match copy_record(&db, rid("plain", "kept")).await.unwrap() {
			Copied::Made(id) => assert_ne!(id, rid("plain", "kept")),
			Copied::Collides(index) => panic!("no unique index on plain, yet collided in {index}"),
		}
	}

	#[tokio::test]
	async fn update_and_delete_fall_back_to_the_record_itself_and_restore_it() {
		use super::{assert_permission_action_delete, assert_permission_action_update};
		let db = unique_and_plain_tables("unique_fallback").await;
		let before = rows(&db, "handle").await;

		expect_ok(assert_permission_action_update(&db, &db, rid("handle", "taken")).await);
		expect_ok(assert_permission_action_delete(&db, &db, rid("handle", "taken")).await);

		assert_eq!(rows(&db, "handle").await, before, "the record is back as it was");
	}

	#[tokio::test]
	async fn tables_without_a_unique_index_still_work_on_a_copy() {
		use super::{assert_permission_action_delete, assert_permission_action_update};
		let db = unique_and_plain_tables("plain_copy").await;
		// A delete event shows which record was deleted: with a copy, never the original.
		db.query(
			"DEFINE TABLE gone SCHEMALESS;
			DEFINE EVENT plain_gone ON plain WHEN $event = 'DELETE' THEN (CREATE gone SET was = $before.id);",
		)
		.await
		.unwrap()
		.check()
		.unwrap();
		let before = rows(&db, "plain").await;

		expect_ok(assert_permission_action_update(&db, &db, rid("plain", "kept")).await);
		expect_ok(assert_permission_action_delete(&db, &db, rid("plain", "kept")).await);

		assert_eq!(rows(&db, "plain").await, before);
		let mut response =
			db.query("SELECT VALUE record::id(was) FROM gone;").await.unwrap().check().unwrap();
		let deleted: Vec<String> = response.take(0).unwrap();
		assert!(!deleted.is_empty(), "the copies were deleted");
		assert!(!deleted.contains(&"kept".to_string()), "the original was deleted: {deleted:?}");
	}

	#[tokio::test]
	async fn a_create_that_collides_in_a_unique_index_is_inconclusive() {
		use super::{AssertionError, assert_permission_action_create};
		let db = unique_and_plain_tables("create_collides").await;

		match assert_permission_action_create(&db, &db, rid("handle", "taken")).await {
			Err(AssertionError::Inconclusive(message)) => {
				assert!(message.contains("unique index `handle_name`"), "{message}");
				assert!(message.contains("handle:taken"), "{message}");
			}
			Err(err) => panic!("expected an inconclusive result, got {err}"),
			Ok(()) => panic!("a duplicate of handle:taken was created"),
		}
		assert_eq!(rows(&db, "handle").await.len(), 1);

		expect_ok(assert_permission_action_create(&db, &db, rid("plain", "kept")).await);
		assert_eq!(rows(&db, "plain").await.len(), 1, "the created record is cleaned up");
	}

	#[test]
	fn compare_schemas_lists_every_disagreement() {
		let synced: BTreeMap<String, String> = [
			("tables:a".to_string(), "DEFINE TABLE a".to_string()),
			("tables:b".to_string(), "DEFINE TABLE b".to_string()),
			("fields:a.x".to_string(), "DEFINE FIELD x ON a TYPE int".to_string()),
		]
		.into();
		let replayed: BTreeMap<String, String> = [
			("tables:a".to_string(), "DEFINE TABLE a".to_string()),
			("fields:a.x".to_string(), "DEFINE FIELD x ON a TYPE string".to_string()),
			("tables:c".to_string(), "DEFINE TABLE c".to_string()),
		]
		.into();
		let out = super::compare_schemas(&synced, &replayed);
		assert!(out[0].passed && out[0].message == "1 definitions agree");
		let failing: Vec<(&str, &str)> = out
			.iter()
			.filter(|a| !a.passed)
			.map(|a| (a.name.as_str(), a.message.as_str()))
			.collect();
		assert_eq!(failing.len(), 3);
		assert!(failing.iter().any(|(n, m)| *n == "fields:a.x"
			&& m.contains("TYPE int")
			&& m.contains("TYPE string")));
		assert!(failing.contains(&("tables:b", "defined by sync but not by the rollouts")));
		assert!(failing.contains(&("tables:c", "defined by the rollouts but not by sync")));
	}
}
