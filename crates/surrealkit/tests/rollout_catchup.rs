//! Catching a database up across several rollouts (#91), end to end through the
//! same functions the CLI calls, against `mem://`.
//!
//! Every test uses its own project folder (an absolute path, so nothing changes
//! the working directory) and its own in-memory datastore.

#![expect(clippy::unwrap_used, reason = "test helpers fail the test on any unexpected state")]

use std::fs;
use std::path::{Path, PathBuf};

use surrealdb::Surreal;
use surrealdb::engine::any::{Any, connect};
use surrealdb::opt::Config;
use surrealdb::opt::capabilities::Capabilities;
use surrealkit::module::Module;
use surrealkit::rollout::{
	RolloutExecutionOpts, RolloutPlanOpts, RolloutUpOpts, run_baseline, run_complete, run_freeze,
	run_lint, run_plan, run_rollback, run_start, run_up,
};
use surrealkit::sync::{SyncOpts, run_sync};
use surrealkit::{
	EmbeddedSchemaFile, FileRef, Rollout, RolloutPhase, RolloutSpec, RolloutStep, Rollouts,
	TemplateVars,
};

async fn mem_db() -> Surreal<Any> {
	let db = connect(("mem://", Config::new().capabilities(Capabilities::all())))
		.await
		.expect("connect");
	db.use_ns("catchup").use_db("catchup").await.expect("use");
	db
}

struct Project {
	_tmp: tempfile::TempDir,
	root: PathBuf,
}

impl Project {
	fn new() -> Self {
		let tmp = tempfile::TempDir::new().expect("tempdir");
		let root = tmp.path().join("database");
		fs::create_dir_all(root.join("schema")).expect("schema dir");
		Self {
			_tmp: tmp,
			root,
		}
	}

	fn folder(&self) -> String {
		self.root.to_string_lossy().into_owned()
	}

	fn write(&self, name: &str, sql: &str) {
		let path = self.root.join("schema").join(name);
		fs::create_dir_all(path.parent().unwrap()).unwrap();
		fs::write(path, sql).expect("write schema");
	}

	fn remove(&self, name: &str) {
		fs::remove_file(self.root.join("schema").join(name)).expect("remove schema");
	}

	async fn plan(&self, name: &str) -> String {
		self.try_plan(name, false).await.expect("plan")
	}

	async fn try_plan(&self, name: &str, allow_modified: bool) -> anyhow::Result<String> {
		run_plan(
			&self.folder(),
			RolloutPlanOpts {
				name: Some(name.to_string()),
				dry_run: false,
				allow_modified,
			},
		)
		.await?;
		Ok(self.id_of(name))
	}

	fn id_of(&self, name: &str) -> String {
		let suffix = format!("__{name}.toml");
		let entry = fs::read_dir(self.root.join("rollouts"))
			.unwrap()
			.filter_map(Result::ok)
			.find(|e| e.file_name().to_string_lossy().ends_with(&suffix))
			.unwrap_or_else(|| panic!("no manifest named {name}"));
		entry.path().file_stem().unwrap().to_string_lossy().into_owned()
	}

	fn manifest(&self, id: &str) -> PathBuf {
		self.root.join("rollouts").join(format!("{id}.toml"))
	}

	fn frozen_dir(&self, id: &str) -> PathBuf {
		self.root.join("rollouts").join(id)
	}

	/// Add hand-written steps to a planned manifest, the way an author adds a
	/// backfill.
	fn append(&self, id: &str, toml: &str) {
		let path = self.manifest(id);
		// A manifest with no steps serializes them as `steps = []`, which a
		// `[[steps]]` table cannot follow.
		let mut raw = fs::read_to_string(&path).unwrap().replace("steps = []\n", "");
		raw.push('\n');
		raw.push_str(toml);
		fs::write(path, raw).unwrap();
	}

	/// Turn a planned manifest back into the pre-beta.6 form: plain paths, no
	/// frozen directory.
	fn unfreeze(&self, id: &str) {
		let path = self.manifest(id);
		let mut spec: RolloutSpec = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
		for step in &mut spec.steps {
			if let surrealkit::RolloutAction::ApplyFiles {
				files,
			} = &mut step.action
			{
				*files = files.iter().map(|f| FileRef::Path(f.path().to_string())).collect();
			}
		}
		fs::write(&path, toml::to_string_pretty(&spec).unwrap()).unwrap();
		fs::remove_dir_all(self.frozen_dir(id)).unwrap();
	}

	async fn sync(&self, db: &Surreal<Any>) {
		run_sync(
			db,
			SyncOpts {
				folder: self.folder(),
				module: Module::default_module(),
				prune: true,
				fail_fast: true,
				..SyncOpts::default()
			},
		)
		.await
		.expect("sync");
	}

	async fn baseline(&self, db: &Surreal<Any>) {
		run_baseline(db, &self.folder(), &Module::default_module()).await.expect("baseline");
	}

	async fn up(&self, db: &Surreal<Any>, complete: bool) -> anyhow::Result<surrealkit::UpReport> {
		self.up_with(db, complete, TemplateVars::default()).await
	}

	async fn up_with(
		&self,
		db: &Surreal<Any>,
		complete: bool,
		vars: TemplateVars,
	) -> anyhow::Result<surrealkit::UpReport> {
		run_up(
			db,
			&self.folder(),
			RolloutUpOpts {
				complete_newest: complete,
				..RolloutUpOpts::default()
			},
			&vars,
		)
		.await
	}

	async fn start(&self, db: &Surreal<Any>, id: &str) -> anyhow::Result<()> {
		run_start(
			db,
			&self.folder(),
			RolloutExecutionOpts::new(Some(id.to_string())),
			&TemplateVars::default(),
		)
		.await
	}

	async fn complete(&self, db: &Surreal<Any>, id: &str) -> anyhow::Result<()> {
		run_complete(
			db,
			&self.folder(),
			RolloutExecutionOpts::new(Some(id.to_string())),
			&TemplateVars::default(),
		)
		.await
	}
}

async fn status(db: &Surreal<Any>, id: &str) -> Option<String> {
	let mut res = db
		.query("SELECT VALUE status FROM __rollout WHERE record::id(id) = $id;")
		.bind(("id", id.to_string()))
		.await
		.unwrap();
	let rows: Vec<String> = res.take(0).unwrap();
	rows.into_iter().next()
}

async fn rollout_count(db: &Surreal<Any>) -> usize {
	let mut res = db.query("SELECT VALUE id FROM __rollout;").await.unwrap();
	let rows: Vec<serde_json::Value> = res.take(0).unwrap();
	rows.len()
}

async fn info(db: &Surreal<Any>, sql: &str) -> serde_json::Value {
	let mut res = db.query(sql).await.unwrap().check().unwrap();
	let value: Option<serde_json::Value> = res.take(0).unwrap();
	value.unwrap_or_default()
}

async fn table_names(db: &Surreal<Any>) -> Vec<String> {
	let mut names: Vec<String> = info(db, "INFO FOR DB;").await["tables"]
		.as_object()
		.map(|t| t.keys().filter(|k| !k.starts_with("__")).cloned().collect())
		.unwrap_or_default();
	names.sort();
	names
}

async fn field_names(db: &Surreal<Any>, table: &str) -> Vec<String> {
	let mut names: Vec<String> = info(db, &format!("INFO FOR TABLE {table};")).await["fields"]
		.as_object()
		.map(|t| t.keys().cloned().collect())
		.unwrap_or_default();
	names.sort();
	names
}

async fn catalog_keys(db: &Surreal<Any>) -> Vec<String> {
	let mut res = db.query("SELECT VALUE key FROM __entity WHERE ns = 'schema';").await.unwrap();
	let mut keys: Vec<String> = res.take(0).unwrap();
	keys.sort();
	keys
}

fn snapshot_keys(project: &Project) -> Vec<String> {
	let raw = fs::read_to_string(project.root.join("snapshots/catalog_snapshot.json")).unwrap();
	let snapshot: serde_json::Value = serde_json::from_str(&raw).unwrap();
	let mut keys: Vec<String> = snapshot["entities"]
		.as_array()
		.unwrap()
		.iter()
		.map(|e| {
			format!(
				"{}:{}:{}",
				e["kind"].as_str().unwrap(),
				e["scope"].as_str().unwrap_or(""),
				e["name"].as_str().unwrap()
			)
		})
		.collect();
	keys.sort();
	keys
}

const V1: &str = "DEFINE TABLE person SCHEMAFULL;\n\
	DEFINE FIELD name ON person TYPE string;\n\
	DEFINE FIELD nickname ON person TYPE option<string>;\n";

/// A project with a database at v1 (synced, then baselined, with two people in
/// it) and three rollouts planned after it:
///
/// - v2 adds `email`, with a backfill and an assertion written for it
/// - v3 adds `account` and drops `nickname` (a complete-phase removal)
/// - v4 adds an index on `email`
///
/// Then the schema folder moves on to an unplanned v5, which `up` must ignore.
struct Scenario {
	project: Project,
	db: Surreal<Any>,
	v2: String,
	v3: String,
	v4: String,
}

async fn scenario() -> Scenario {
	let project = Project::new();
	project.write("person.surql", V1);
	let db = mem_db().await;
	project.sync(&db).await;
	project.baseline(&db).await;
	db.query(
		"CREATE person:ann SET name = 'Ann', nickname = 'a'; CREATE person:bob SET name = 'Bob';",
	)
	.await
	.unwrap()
	.check()
	.unwrap();

	project
		.write("person.surql", &format!("{V1}DEFINE FIELD email ON person TYPE option<string>;\n"));
	let v2 = project.plan("v2_add_email").await;
	project.append(
		&v2,
		"[[steps]]\nid = \"backfill_email\"\nphase = \"start\"\nkind = \"run_sql\"\n\
		 sql = \"UPDATE person SET email = string::lowercase(name) + '@example.test' WHERE email = NONE;\"\n\n\
		 [[steps]]\nid = \"every_person_has_email\"\nphase = \"start\"\nkind = \"assert_sql\"\n\
		 sql = \"RETURN count(SELECT * FROM person WHERE email = NONE)\"\nexpect = \"0\"\n",
	);

	project.write(
		"person.surql",
		"DEFINE TABLE person SCHEMAFULL;\nDEFINE FIELD name ON person TYPE string;\nDEFINE FIELD email ON person TYPE option<string>;\n",
	);
	project.write("account.surql", "DEFINE TABLE account SCHEMALESS;\n");
	let v3 = project.plan("v3_account_drop_nickname").await;

	project.write("index.surql", "DEFINE INDEX person_email ON person FIELDS email;\n");
	let v4 = project.plan("v4_email_index").await;

	// Moves on without planning, and loses a file v2 froze.
	project.write("later.surql", "DEFINE TABLE unplanned SCHEMALESS;\n");
	project.remove("account.surql");

	Scenario {
		project,
		db,
		v2,
		v3,
		v4,
	}
}

#[tokio::test]
async fn issue_91_a_v1_database_catches_up_through_every_planned_rollout() {
	let s = scenario().await;
	let report = s.project.up(&s.db, false).await.expect("up");

	assert_eq!(report.pending, vec![s.v2.clone(), s.v3.clone(), s.v4.clone()]);
	assert_eq!(report.completed, vec![s.v2.clone(), s.v3.clone()]);
	assert_eq!(report.waiting.as_deref(), Some(s.v4.as_str()));
	assert_eq!(status(&s.db, &s.v2).await.as_deref(), Some("completed"));
	assert_eq!(status(&s.db, &s.v3).await.as_deref(), Some("completed"));
	assert_eq!(status(&s.db, &s.v4).await.as_deref(), Some("ready_to_complete"));

	// v2's backfill ran, against v2's schema.
	let mut res = s.db.query("SELECT VALUE email FROM person ORDER BY id;").await.unwrap();
	let emails: Vec<String> = res.take(0).unwrap();
	assert_eq!(emails, vec!["ann@example.test", "bob@example.test"]);
	// v3's contract phase removed nickname, and v3's account table exists even
	// though account.surql is gone from the schema folder now.
	assert_eq!(field_names(&s.db, "person").await, vec!["email", "name"]);
	assert_eq!(table_names(&s.db).await, vec!["account", "person"]);
	// v4 expanded: its index is live. The unplanned table is not.
	assert!(info(&s.db, "INFO FOR TABLE person;").await["indexes"]["person_email"].is_string());

	// Running it again changes nothing: the newest waits for the cutover.
	let again = s.project.up(&s.db, false).await.expect("up again");
	assert!(again.completed.is_empty());
	assert_eq!(again.waiting.as_deref(), Some(s.v4.as_str()));
	assert_eq!(status(&s.db, &s.v4).await.as_deref(), Some("ready_to_complete"));

	let done = s.project.up(&s.db, true).await.expect("up --complete");
	assert_eq!(done.completed, vec![s.v4.clone()]);
	assert_eq!(status(&s.db, &s.v4).await.as_deref(), Some("completed"));

	// The catalog is exactly what v4 was planned to leave, not the folder's v5.
	assert_eq!(catalog_keys(&s.db).await, snapshot_keys(&s.project));
	assert!(!table_names(&s.db).await.contains(&"unplanned".to_string()));

	let finished = s.project.up(&s.db, true).await.expect("up when up to date");
	assert!(finished.pending.is_empty());
}

#[tokio::test]
async fn plan_freezes_each_changed_file_beside_the_manifest() {
	let s = scenario().await;
	let raw = fs::read_to_string(s.project.manifest(&s.v3)).unwrap();
	assert!(raw.contains("[[steps.files]]"), "{raw}");
	let spec: RolloutSpec = toml::from_str(&raw).unwrap();
	assert!(spec.is_frozen());
	for file in spec.steps.iter().flat_map(|step| match &step.action {
		surrealkit::RolloutAction::ApplyFiles {
			files,
		} => files.clone(),
		_ => Vec::new(),
	}) {
		let FileRef::Frozen(frozen) = file else {
			panic!("plan wrote a plain path: {file:?}");
		};
		let copy = fs::read(s.project.frozen_dir(&s.v3).join(&frozen.path)).expect("frozen copy");
		assert_eq!(surrealkit::core::sha256_hex(&copy), frozen.hash);
	}
	// Only the files that changed: v3 touched person.surql and account.surql.
	let mut frozen: Vec<String> = walk(&s.project.frozen_dir(&s.v3));
	frozen.sort();
	assert_eq!(frozen, vec!["schema/account.surql", "schema/person.surql"]);
	run_lint(&s.project.folder(), RolloutExecutionOpts::new(Some(s.v3.clone())))
		.await
		.expect("lint one");
}

fn walk(root: &Path) -> Vec<String> {
	walkdir::WalkDir::new(root)
		.into_iter()
		.filter_map(Result::ok)
		.filter(|e| e.file_type().is_file())
		.map(|e| e.path().strip_prefix(root).unwrap().to_string_lossy().replace('\\', "/"))
		.collect()
}

#[tokio::test]
async fn a_tampered_frozen_file_stops_up_before_anything_starts() {
	let s = scenario().await;
	fs::write(s.project.frozen_dir(&s.v3).join("schema/account.surql"), "DEFINE TABLE hijacked;")
		.unwrap();
	let err = s.project.up(&s.db, false).await.expect_err("tampered").to_string();
	assert!(err.contains("does not match its manifest"), "{err}");
	assert_eq!(rollout_count(&s.db).await, 0, "no rollout may start when a later one cannot run");

	fs::remove_dir_all(s.project.frozen_dir(&s.v4)).unwrap();
	let err = format!("{:#}", s.project.up(&s.db, false).await.unwrap_err());
	assert!(err.contains("does not match") || err.contains("cannot be read"), "{err}");
}

#[tokio::test]
async fn up_resumes_a_failed_rollout_without_rerunning_its_completed_steps() {
	let s = scenario().await;
	s.project.append(
		&s.v3,
		"[[steps]]\nid = \"count_runs\"\nphase = \"start\"\nkind = \"run_sql\"\n\
		 sql = \"UPSERT runs:v3 SET n += 1;\"\n\n\
		 [[steps]]\nid = \"needs_flag\"\nphase = \"start\"\nkind = \"assert_sql\"\n\
		 sql = \"RETURN count(SELECT * FROM person WHERE name = 'Flag')\"\nexpect = \"1\"\n",
	);
	let err = s.project.up(&s.db, false).await.expect_err("assert fails").to_string();
	assert!(err.contains("needs_flag"), "{err}");
	assert_eq!(status(&s.db, &s.v2).await.as_deref(), Some("completed"));
	assert_eq!(status(&s.db, &s.v3).await.as_deref(), Some("failed"));

	s.db.query("CREATE person:flag SET name = 'Flag';").await.unwrap().check().unwrap();
	let report = s.project.up(&s.db, true).await.expect("resumed");
	assert_eq!(report.completed, vec![s.v3.clone(), s.v4.clone()]);
	let mut res = s.db.query("SELECT VALUE n FROM runs:v3;").await.unwrap();
	let runs: Vec<i64> = res.take(0).unwrap();
	assert_eq!(runs, vec![1], "a completed step must not run again on resume");
}

#[tokio::test]
async fn start_refuses_a_frozen_rollout_out_of_order() {
	let s = scenario().await;
	let err = s.project.start(&s.db, &s.v4).await.expect_err("out of order").to_string();
	assert!(
		err.contains("is not next") && err.contains(&s.v2) && err.contains("rollout up"),
		"{err}"
	);

	// In order it runs, although the schema folder has moved on since it was planned.
	s.project.start(&s.db, &s.v2).await.expect("start v2");
	s.project.complete(&s.db, &s.v2).await.expect("complete v2");
	s.project.start(&s.db, &s.v3).await.expect("start v3");
}

#[tokio::test]
async fn a_rolled_back_rollout_stops_up_with_advice() {
	let s = scenario().await;
	s.project.start(&s.db, &s.v2).await.expect("start v2");
	run_rollback(
		&s.db,
		&s.project.folder(),
		RolloutExecutionOpts::new(Some(s.v2.clone())),
		&TemplateVars::default(),
	)
	.await
	.expect("rollback");
	let err = s.project.up(&s.db, false).await.expect_err("rolled back").to_string();
	assert!(err.contains("was rolled back"), "{err}");
}

#[tokio::test]
async fn up_runs_everything_on_an_empty_database_when_planning_started_from_nothing() {
	let project = Project::new();
	project.write("person.surql", "DEFINE TABLE person SCHEMALESS;\n");
	let first = project.plan("first").await;
	project.write("account.surql", "DEFINE TABLE account SCHEMALESS;\n");
	let second = project.plan("second").await;

	let db = mem_db().await;
	let report = project.up(&db, true).await.expect("up");
	assert_eq!(report.completed, vec![first, second]);
	assert_eq!(table_names(&db).await, vec!["account", "person"]);
	assert_eq!(catalog_keys(&db).await, snapshot_keys(&project));
}

#[tokio::test]
async fn a_data_only_rollout_runs_in_its_place() {
	let project = Project::new();
	project.write("person.surql", "DEFINE TABLE person SCHEMALESS;\n");
	let schema = project.plan("a_schema").await;
	let data = project.plan("b_data_only").await;
	project.append(
		&data,
		"[[steps]]\nid = \"seed\"\nphase = \"start\"\nkind = \"run_sql\"\nsql = \"CREATE person:seeded;\"\n",
	);
	let db = mem_db().await;
	let report = project.up(&db, true).await.expect("up");
	assert_eq!(report.completed, vec![schema, data]);
	let mut res = db.query("SELECT VALUE id FROM person;").await.unwrap();
	let ids: Vec<serde_json::Value> = res.take(0).unwrap();
	assert_eq!(ids.len(), 1);
}

#[tokio::test]
async fn template_variables_reach_frozen_files() {
	let project = Project::new();
	project.write("t.surql", "DEFINE TABLE ${PREFIX}_items SCHEMALESS;\n");
	project.plan("templated").await;
	let db = mem_db().await;
	let vars = TemplateVars {
		vars: [("PREFIX".to_string(), "shop".to_string())].into(),
	};
	project.up_with(&db, true, vars).await.expect("up");
	assert_eq!(table_names(&db).await, vec!["shop_items"]);
}

#[tokio::test]
async fn a_legacy_manifest_runs_only_last_and_only_against_its_own_schema() {
	let s = scenario().await;
	s.project.unfreeze(&s.v3);
	let err = s.project.up(&s.db, false).await.expect_err("legacy in the middle").to_string();
	assert!(err.contains("rollout freeze"), "{err}");
	assert_eq!(rollout_count(&s.db).await, 0);
}

#[tokio::test]
async fn a_legacy_newest_manifest_still_runs_when_the_folder_matches_it() {
	let project = Project::new();
	project.write("person.surql", V1);
	let db = mem_db().await;
	project.sync(&db).await;
	project.baseline(&db).await;
	project.write("account.surql", "DEFINE TABLE account SCHEMALESS;\n");
	let legacy = project.plan("legacy").await;
	project.unfreeze(&legacy);
	let report = project.up(&db, true).await.expect("legacy newest with matching folder");
	assert_eq!(report.completed, vec![legacy]);

	// The folder moving on breaks it, as it always has.
	project.write("c.surql", "DEFINE TABLE c SCHEMALESS;\n");
	let next = project.plan("next").await;
	project.unfreeze(&next);
	project.write("other.surql", "DEFINE TABLE other;\n");
	let err = format!("{:#}", project.up(&db, true).await.unwrap_err());
	assert!(err.contains("rollout freeze") && err.contains("target schema hash mismatch"), "{err}");
}

#[tokio::test]
async fn history_from_the_old_flow_carries_on_into_frozen_rollouts() {
	let project = Project::new();
	project.write("person.surql", V1);
	let db = mem_db().await;
	project.sync(&db).await;
	project.baseline(&db).await;

	project.write("a.surql", "DEFINE TABLE a SCHEMALESS;\n");
	let old = project.plan("old_flow").await;
	project.unfreeze(&old);
	project.start(&db, &old).await.expect("legacy start");
	project.complete(&db, &old).await.expect("legacy complete");

	project.write("b.surql", "DEFINE TABLE b SCHEMALESS;\n");
	let new = project.plan("new_flow").await;
	let report = project.up(&db, true).await.expect("up");
	assert_eq!(report.completed, vec![new]);
	assert_eq!(table_names(&db).await, vec!["a", "b", "person"]);
}

#[tokio::test]
async fn freeze_converts_a_legacy_manifest_and_keeps_its_comments() {
	let project = Project::new();
	project.write("person.surql", V1);
	let db = mem_db().await;
	project.sync(&db).await;
	project.baseline(&db).await;
	project.write("a.surql", "DEFINE TABLE a SCHEMALESS;\n");
	let id = project.plan("to_freeze").await;
	project.unfreeze(&id);
	project.append(&id, "# a reviewer's note\n[[steps]]\nid = \"note\"\nphase = \"start\"\nkind = \"run_sql\"\nsql = \"RETURN 1;\"\n");

	run_freeze(&project.folder(), &id).expect("freeze");
	let raw = fs::read_to_string(project.manifest(&id)).unwrap();
	assert!(raw.contains("# a reviewer's note"), "{raw}");
	let spec: RolloutSpec = toml::from_str(&raw).unwrap();
	assert!(spec.is_frozen());
	assert_eq!(walk(&project.frozen_dir(&id)), vec!["schema/a.surql"]);
	run_freeze(&project.folder(), &id).expect("freezing again is a no-op");

	// Frozen, it no longer needs the folder to match.
	project.write("later.surql", "DEFINE TABLE later;\n");
	let report = project.up(&db, true).await.expect("up");
	assert_eq!(report.completed, vec![id]);
}

#[tokio::test]
async fn freeze_refuses_when_the_folder_has_moved_on() {
	let project = Project::new();
	project.write("person.surql", V1);
	let id = project.plan("first").await;
	project.unfreeze(&id);
	project.write("later.surql", "DEFINE TABLE later;\n");
	let err = format!("{:#}", run_freeze(&project.folder(), &id).unwrap_err());
	assert!(err.contains("Check out the commit that planned it"), "{err}");
	assert!(!project.frozen_dir(&id).exists());
}

#[tokio::test]
async fn the_rollouts_facade_reports_and_runs_the_chain() {
	let s = scenario().await;
	let rollouts = Rollouts::load(s.project.folder()).expect("load");
	let pending = rollouts.pending(&s.db).await.expect("pending");
	assert_eq!(pending.pending, vec![s.v2.clone(), s.v3.clone(), s.v4.clone()]);
	assert!(pending.position.contains("baseline"), "{}", pending.position);

	let report = rollouts.clone().complete_newest(true).up(&s.db).await.expect("up");
	assert_eq!(report.completed.len(), 3);
	let after = rollouts.pending(&s.db).await.expect("pending after");
	assert!(after.pending.is_empty());
	assert_eq!(after.applied.len(), 3);
}

#[tokio::test]
async fn cli_and_library_runs_interoperate() {
	let s = scenario().await;
	s.project.start(&s.db, &s.v2).await.expect("cli start");
	let report = Rollouts::load(s.project.folder())
		.unwrap()
		.complete_newest(true)
		.up(&s.db)
		.await
		.expect("library up");
	assert_eq!(report.completed, vec![s.v2.clone(), s.v3.clone(), s.v4.clone()]);
}

#[tokio::test]
async fn a_frozen_rollout_in_code_needs_a_folder_to_find_its_files() {
	let s = scenario().await;
	let spec: RolloutSpec =
		toml::from_str(&fs::read_to_string(s.project.manifest(&s.v2)).unwrap()).unwrap();
	let err =
		Rollout::new(spec.clone(), &[]).start(&s.db).await.expect_err("no folder").to_string();
	assert!(err.contains(".folder("), "{err}");
	assert_eq!(rollout_count(&s.db).await, 0);

	let rollout = Rollout::new(spec, &[]).folder(s.project.folder());
	rollout.start(&s.db).await.expect("start with folder");
	rollout.complete(&s.db).await.expect("complete");
	assert!(field_names(&s.db, "person").await.contains(&"email".to_string()));
}

#[tokio::test]
async fn a_code_spec_with_legacy_paths_completes_after_starting() {
	// The checksum `start` recorded was of the canonicalised spec and `complete`
	// computed it from the original, so this failed with a checksum mismatch.
	static TARGET: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "schema/legacy.surql",
		sql: "DEFINE TABLE legacy SCHEMALESS;",
	}];
	let db = mem_db().await;
	let spec = RolloutSpec::builder("20260101000000__legacy_paths")
		.step(RolloutStep::apply_schema(
			"define",
			RolloutPhase::Start,
			"DEFINE TABLE legacy SCHEMALESS;",
		))
		.step(RolloutStep::apply_files(
			"noop",
			RolloutPhase::Rollback,
			vec!["/database/schema/legacy.surql"],
		))
		.build();
	let rollout = Rollout::new(spec, TARGET);
	rollout.start(&db).await.expect("start");
	rollout.complete(&db).await.expect("complete must accept the checksum start recorded");
}

#[tokio::test]
async fn a_second_up_waits_for_the_first() {
	let s = scenario().await;
	s.db.query(
		"CREATE __entity CONTENT { ns: 'lock', key: 'up', val: { owner: 'someone-else/1', \
		 acquired_at: time::now(), expires_at: time::now() + 1h }, updated_at: time::now() };",
	)
	.await
	.unwrap()
	.check()
	.unwrap();
	let err = s.project.up(&s.db, false).await.expect_err("locked").to_string();
	assert!(err.contains("holds the 'up' lock"), "{err}");
	assert_eq!(rollout_count(&s.db).await, 0);
}

#[tokio::test]
async fn dry_run_runs_nothing() {
	let s = scenario().await;
	let report = run_up(
		&s.db,
		&s.project.folder(),
		RolloutUpOpts {
			dry_run: true,
			..RolloutUpOpts::default()
		},
		&TemplateVars::default(),
	)
	.await
	.expect("dry run");
	assert_eq!(report.pending.len(), 3);
	assert!(report.completed.is_empty());
	assert_eq!(rollout_count(&s.db).await, 0);
}

#[tokio::test]
async fn up_from_names_where_to_begin() {
	let s = scenario().await;
	let report = run_up(
		&s.db,
		&s.project.folder(),
		RolloutUpOpts {
			from: Some(s.v3.clone()),
			complete_newest: true,
			..RolloutUpOpts::default()
		},
		&TemplateVars::default(),
	)
	.await
	.expect("up --from");
	assert_eq!(report.completed, vec![s.v3.clone(), s.v4.clone()]);
	assert_eq!(status(&s.db, &s.v2).await, None);
}

#[tokio::test]
async fn lint_all_checks_the_chain_and_unplanned_changes() {
	let s = scenario().await;
	let folder = s.project.folder();
	// The scenario leaves the folder ahead of v4.
	let err = run_lint(&folder, RolloutExecutionOpts::new(None))
		.await
		.expect_err("unplanned")
		.to_string();
	assert!(err.contains("no rollout plans"), "{err}");

	// Plan the new table, keeping account so the plan is not read as a rename.
	s.project.write("account.surql", "DEFINE TABLE account SCHEMALESS;\n");
	let v5 = s.project.plan("v5_catch_up_the_plan").await;
	run_lint(&folder, RolloutExecutionOpts::new(None)).await.expect("everything planned");

	// A second plan from the same snapshots: a fork.
	let snapshots = s.project.root.join("snapshots");
	let manifest = fs::read_to_string(s.project.manifest(&v5)).unwrap();
	let forked = manifest.replace(&v5, "29990101000000__forked");
	fs::write(s.project.root.join("rollouts/29990101000000__forked.toml"), forked).unwrap();
	copy_dir(&s.project.frozen_dir(&v5), &s.project.frozen_dir("29990101000000__forked"));
	let err =
		run_lint(&folder, RolloutExecutionOpts::new(None)).await.expect_err("fork").to_string();
	assert!(err.contains("both planned from the same schema"), "{err}");
	assert!(snapshots.exists());
}

fn copy_dir(from: &Path, to: &Path) {
	for file in walk(from) {
		let dest = to.join(&file);
		fs::create_dir_all(dest.parent().unwrap()).unwrap();
		fs::copy(from.join(&file), dest).unwrap();
	}
}

#[tokio::test]
async fn plan_refuses_to_overwrite_an_existing_rollout() {
	// Two plans with the same name in the same second get the same id. Occupy
	// the ids for the next few seconds so this plan is sure to land on one.
	let project = Project::new();
	project.write("a.surql", "DEFINE TABLE a;\n");
	fs::create_dir_all(project.root.join("rollouts")).unwrap();
	let now = time::OffsetDateTime::now_utc();
	for offset in 0..5 {
		let ts = (now + time::Duration::seconds(offset))
			.format(&time::macros::format_description!("[year][month][day][hour][minute][second]"))
			.unwrap();
		fs::create_dir_all(project.root.join("rollouts").join(format!("{ts}__same"))).unwrap();
	}
	let err = project.try_plan("same", false).await.expect_err("collision").to_string();
	assert!(err.contains("already exists"), "{err}");
	assert!(
		fs::read_dir(project.root.join("rollouts")).unwrap().all(|e| !e.unwrap().path().is_file())
	);
}
