use serde_json::{Value, json};

use super::*;
use crate::check::discover::discover;
use crate::check::finding::Code;
use crate::selection::select_modules;
use crate::sync::{EmbeddedSchemaFile, Sync};

fn table(name: &str, fields: Vec<Value>) -> Value {
	json!({
		"name": name,
		"type": { "kind": "normal" },
		"schemafull": true,
		"drop": false,
		"permissions": { "select": true, "create": true, "update": true, "delete": true },
		"fields": fields,
		"indexes": [],
		"events": [],
	})
}

fn field(path: &[&str], kind: &str) -> Value {
	json!({
		"path": path,
		"kind": { "kind": kind },
		"optional": false,
		"flexible": false,
		"readonly": false,
		"inline": false,
		"permissions": { "select": true, "create": true, "update": true },
	})
}

fn with(mut value: Value, key: &str, to: Value) -> Value {
	value[key] = to;
	value
}

fn doc_at(version: &str, tables: Vec<Value>) -> SchemaDocument {
	serde_json::from_value(json!({
		"version": 2,
		"semanticDigest": "sha256:00",
		"provenance": {
			"generator": { "name": "surrealkit", "version": "0" },
			"engine": { "version": version, "mode": "embedded" },
			"adapter": "structure-4.0",
			"source": { "kind": "introspect", "namespace": "", "database": "" },
		},
		"tables": tables,
		"functions": [],
		"params": [],
		"accesses": [],
		"analyzers": [],
		"sequences": [],
		"buckets": [],
		"apis": [],
		"modules": [],
		"configs": [],
		"users": [],
		"diagnostics": [],
	}))
	.expect("a schema document")
}

fn doc(tables: Vec<Value>) -> SchemaDocument {
	doc_at("4.0.0", tables)
}

fn kinds(drifts: &[Drift]) -> Vec<(DriftKind, String, Option<String>)> {
	drifts.iter().map(|d| (d.kind, d.table.clone(), d.field.as_deref().map(field_name))).collect()
}

#[test]
fn equal_documents_have_no_drift() {
	let tables = vec![table("person", vec![field(&["name"], "string")])];
	assert!(diff(&doc(tables.clone()), &doc(tables)).is_empty());
}

#[test]
fn provenance_define_evidence_and_derived_properties_are_not_compared() {
	let mut files = vec![table("person", vec![field(&["name"], "string")])];
	files[0]["fields"][0]["value"] = json!({ "expr": "$value", "readsInput": false });
	files[0]["fields"][0]["assert"] = json!({ "expr": "true" });
	let mut evidence = files.clone();
	evidence[0]["define"] = json!("DEFINE TABLE person SCHEMAFULL");
	evidence[0]["fields"][0]["define"] = json!("DEFINE FIELD name ON person TYPE string");
	evidence[0]["fields"][0]["value"]["readsInput"] = json!(true);
	evidence[0]["fields"][0]["assert"]["recognized"] = json!([]);
	let files = doc(files);
	let mut target = doc_at("4.0.7", evidence);
	target.provenance.adapter = "info".to_string();
	target.semantic_digest = "sha256:11".to_string();
	assert!(diff(&files, &target).is_empty(), "{:?}", diff(&files, &target));
	// The clause itself is compared.
	target.tables[0].fields[0].value.as_mut().expect("value").expr = "$value + 1".to_string();
	assert_eq!(diff(&files, &target)[0].differences[0].facet, "value");
}

#[test]
fn a_missing_table_is_one_drift_not_one_per_field() {
	let files = doc(vec![
		table("person", vec![field(&["name"], "string")]),
		table("post", vec![field(&["title"], "string"), field(&["body"], "string")]),
	]);
	let target = doc(vec![table("person", vec![field(&["name"], "string")])]);
	assert_eq!(kinds(&diff(&files, &target)), [(DriftKind::TableMissing, "post".into(), None)]);
}

#[test]
fn a_table_that_differs_names_the_property() {
	let files = doc(vec![table("person", vec![])]);
	let target = doc(vec![with(table("person", vec![]), "schemafull", json!(false))]);
	let drifts = diff(&files, &target);
	assert_eq!(kinds(&drifts), [(DriftKind::TableDiffers, "person".into(), None)]);
	assert_eq!(
		drifts[0].differences,
		[Difference {
			facet: "schemafull".into(),
			files: "true".into(),
			target: "false".into(),
		}]
	);
	// A relation's `in` and `out`, and `DROP`.
	let relation = |from: &str| {
		with(
			table("likes", vec![]),
			"type",
			json!({ "kind": "relation", "in": [from], "out": ["post"], "enforced": false,
			        "lightweight": false }),
		)
	};
	let drifts = diff(&doc(vec![relation("person")]), &doc(vec![relation("user")]));
	assert_eq!(drifts[0].differences[0].facet, "type");
	let dropped = with(table("person", vec![]), "drop", json!(true));
	assert_eq!(diff(&files, &doc(vec![dropped]))[0].differences[0].facet, "drop");
}

#[test]
fn a_missing_field_and_a_field_that_differs() {
	let files = doc(vec![table(
		"person",
		vec![
			field(&["name"], "string"),
			field(&["age"], "int"),
			with(field(&["nick"], "string"), "optional", json!(true)),
			field(&["phones", "*"], "string"),
		],
	)]);
	let target = doc(vec![table(
		"person",
		vec![
			field(&["name"], "int"),
			field(&["nick"], "string"),
			with(field(&["extra"], "string"), "readonly", json!(true)),
		],
	)]);
	let drifts = diff(&files, &target);
	assert_eq!(
		kinds(&drifts),
		[
			(DriftKind::FieldMissing, "person".into(), Some("age".into())),
			(DriftKind::FieldDiffers, "person".into(), Some("name".into())),
			(DriftKind::FieldDiffers, "person".into(), Some("nick".into())),
			(DriftKind::FieldMissing, "person".into(), Some("phones[*]".into())),
			(DriftKind::FieldExtra, "person".into(), Some("extra".into())),
		]
	);
	let name = &drifts[1].differences;
	assert_eq!(name.len(), 1);
	assert_eq!(
		(name[0].facet.as_str(), name[0].files.as_str(), name[0].target.as_str()),
		("kind", r#"{"kind":"string"}"#, r#"{"kind":"int"}"#)
	);
	assert_eq!(drifts[2].differences[0].facet, "optional");
}

#[test]
fn a_default_or_value_clause_that_differs_is_a_difference() {
	let default = |expr: &str| {
		with(field(&["a"], "string"), "default", json!({ "mode": "create", "expr": expr }))
	};
	let files = doc(vec![table("t", vec![default("'x'")])]);
	let target = doc(vec![table("t", vec![default("'y'")])]);
	let drifts = diff(&files, &target);
	assert_eq!(drifts[0].differences[0].facet, "default");
	let bare = doc(vec![table("t", vec![field(&["a"], "string")])]);
	let drifts = diff(&files, &bare);
	assert_eq!(drifts[0].differences[0].target, "none", "an absent property reads `none`");
}

#[test]
fn what_only_the_target_has_is_an_extra() {
	let files = doc(vec![table("person", vec![])]);
	let target = doc(vec![
		table("audit", vec![field(&["x"], "string")]),
		table("person", vec![field(&["name"], "string")]),
	]);
	assert_eq!(
		kinds(&diff(&files, &target)),
		[
			(DriftKind::FieldExtra, "person".into(), Some("name".into())),
			(DriftKind::TableExtra, "audit".into(), None),
		]
	);
}

#[test]
fn a_long_difference_is_cut() {
	let long = "x".repeat(400);
	let files = doc(vec![with(table("t", vec![]), "comment", json!(long))]);
	let target = doc(vec![table("t", vec![])]);
	let shown = &diff(&files, &target)[0].differences[0].files;
	assert_eq!(shown.chars().count(), SHOWN + 1);
	assert!(shown.ends_with('…'));
}

fn at(name: &str, span: Range<usize>, statement: Option<usize>) -> Location {
	Location {
		path: PathBuf::from(format!("/p/{name}")),
		span,
		statement,
	}
}

struct Setup {
	header: Location,
	locator: Locator,
	managed: Managed,
	ignore: DriftIgnore,
}

fn setup() -> Setup {
	let text = "DEFINE TABLE person SCHEMAFULL;\nDEFINE FIELD name ON person TYPE string;\n";
	let source = Source {
		path: PathBuf::from("/p/a.surql"),
		text: text.to_string(),
		role: crate::check::discover::Role::Schema,
		module: Some("default".into()),
	};
	let mut managed = Managed::default();
	for (kind, scope, name) in
		[(EntityKind::Table, None, "legacy"), (EntityKind::Field, Some("legacy"), "old")]
	{
		managed.record(
			true,
			&CatalogEntity {
				kind,
				scope: scope.map(str::to_string),
				name: name.to_string(),
				source_path: String::new(),
				statement_hash: String::new(),
				file_hash: String::new(),
			},
		);
	}
	managed.record(
		false,
		&CatalogEntity {
			kind: EntityKind::Table,
			scope: None,
			name: "billing_invoice".to_string(),
			source_path: String::new(),
			statement_hash: String::new(),
			file_hash: String::new(),
		},
	);
	Setup {
		header: at("surrealkit.toml", 10..23, None),
		locator: Locator::new(&[&source], &TemplateVars::default()),
		managed,
		ignore: DriftIgnore::default(),
	}
}

fn facts<'a>(setup: &'a Setup, name: &'a str) -> TargetFacts<'a> {
	TargetFacts {
		name,
		header: &setup.header,
		locator: &setup.locator,
		managed: &setup.managed,
		ignore: &setup.ignore,
		rollout: None,
		minors: None,
	}
}

fn drift(kind: DriftKind, table: &str, field: Option<&[&str]>) -> Drift {
	Drift {
		kind,
		table: table.to_string(),
		field: field.map(|path| path.iter().map(|s| (*s).to_string()).collect()),
		differences: vec![Difference {
			facet: "kind".into(),
			files: "string".into(),
			target: "int".into(),
		}],
	}
}

#[test]
fn each_kind_is_its_code_severity_and_location() {
	let setup = setup();
	let drifts = [
		drift(DriftKind::TableMissing, "person", None),
		drift(DriftKind::TableDiffers, "person", None),
		drift(DriftKind::FieldMissing, "person", Some(&["name"])),
		drift(DriftKind::FieldDiffers, "person", Some(&["name"])),
		drift(DriftKind::TableExtra, "legacy", None),
		drift(DriftKind::TableExtra, "audit", None),
	];
	let found = findings(&drifts, &facts(&setup, "prod"));
	let summary: Vec<(String, Severity, Level)> =
		found.iter().map(|f| (f.code.to_string(), f.severity, f.default_level())).collect();
	assert_eq!(
		summary,
		[
			("K9101".into(), Severity::Error, Level::Deny),
			("K9102".into(), Severity::Error, Level::Deny),
			("K9103".into(), Severity::Error, Level::Deny),
			("K9104".into(), Severity::Error, Level::Deny),
			("K9105".into(), Severity::Warning, Level::Warn),
			("K9106".into(), Severity::Warning, Level::Warn),
		]
	);
	let text = "DEFINE TABLE person SCHEMAFULL;\nDEFINE FIELD name ON person TYPE string;\n";
	assert_eq!(&text[found[0].span.clone()], "DEFINE TABLE person SCHEMAFULL");
	assert_eq!(found[0].statement, Some(0));
	assert_eq!(&text[found[2].span.clone()], "DEFINE FIELD name ON person TYPE string");
	assert_eq!(found[3].statement, Some(1));
	for extra in &found[4..] {
		assert_eq!(extra.path, PathBuf::from("/p/surrealkit.toml"));
		assert_eq!(extra.span, 10..23, "the target's header");
		assert_eq!(extra.statement, None);
	}
	assert!(found.iter().all(|f| f.intrinsic() == f.severity));
}

#[test]
fn messages_name_the_target_and_the_entity() {
	let setup = setup();
	let drifts = [
		drift(DriftKind::TableMissing, "person", None),
		drift(DriftKind::FieldMissing, "person", Some(&["phones", "*", "number"])),
		drift(DriftKind::FieldDiffers, "person", Some(&["name"])),
		drift(DriftKind::TableExtra, "audit", None),
		drift(DriftKind::FieldExtra, "legacy", Some(&["old"])),
	];
	let messages: Vec<String> =
		findings(&drifts, &facts(&setup, "prod")).into_iter().map(|f| f.message).collect();
	assert_eq!(messages[0], "table `person` is not defined on target `prod`");
	assert_eq!(messages[1], "field `person.phones[*].number` is not defined on target `prod`");
	assert_eq!(messages[2], "field `person.name` differs on target `prod`: kind");
	assert!(messages[3].contains("table `audit`") && messages[3].contains("never managed"));
	assert!(messages[4].contains("field `legacy.old`") && messages[4].contains("no longer in"));
	// Two targets never produce identical results.
	let other: Vec<String> =
		findings(&drifts, &facts(&setup, "staging")).into_iter().map(|f| f.message).collect();
	assert!(messages.iter().zip(&other).all(|(a, b)| a != b));
}

#[test]
fn a_difference_is_two_notes_per_property() {
	let setup = setup();
	let found = findings(
		&[drift(DriftKind::FieldDiffers, "person", Some(&["name"]))],
		&facts(&setup, "prod"),
	);
	let notes: Vec<&str> = found[0].notes.iter().map(|n| n.message.as_str()).collect();
	assert_eq!(notes, ["the files say kind: string", "target `prod` has kind: int"]);
}

#[test]
fn an_extra_of_a_module_outside_the_run_is_not_drift() {
	let setup = setup();
	let found =
		findings(&[drift(DriftKind::TableExtra, "billing_invoice", None)], &facts(&setup, "prod"));
	assert!(found.is_empty(), "{found:?}");
}

#[test]
fn an_extra_with_no_catalog_is_unmanaged() {
	let mut setup = setup();
	setup.managed = Managed::default();
	let found = findings(&[drift(DriftKind::TableExtra, "legacy", None)], &facts(&setup, "prod"));
	assert_eq!(found[0].code, K9106);
}

#[test]
fn drift_ignore_skips_the_extras_it_names_on_every_target() {
	let mut setup = setup();
	setup.ignore = DriftIgnore::from_config(&AnalyzeConfig {
		drift_ignore: vec!["audit".into(), "legacy.old".into(), "person.tags[*]".into()],
		..AnalyzeConfig::default()
	})
	.expect("valid");
	let drifts = [
		drift(DriftKind::TableExtra, "audit", None),
		drift(DriftKind::FieldExtra, "audit", Some(&["x"])),
		drift(DriftKind::FieldExtra, "legacy", Some(&["old"])),
		drift(DriftKind::FieldExtra, "legacy", Some(&["new"])),
		drift(DriftKind::FieldExtra, "person", Some(&["tags", "*"])),
		drift(DriftKind::TableMissing, "audit", None),
	];
	for target in ["prod", "staging"] {
		let found = findings(&drifts, &facts(&setup, target));
		let codes: Vec<String> = found.iter().map(|f| f.code.to_string()).collect();
		assert_eq!(codes, ["K9106", "K9101"], "{target}: only legacy.new and the missing table");
	}
}

#[test]
fn drift_ignore_entries_are_validated() {
	let parse = |entries: &[&str]| {
		DriftIgnore::from_config(&AnalyzeConfig {
			drift_ignore: entries.iter().map(|e| (*e).to_string()).collect(),
			..AnalyzeConfig::default()
		})
	};
	assert!(parse(&["a", "a.b", "a.b.c", "a.b[*].c", "`odd name`.x"]).is_ok());
	for bad in ["", "  ", ".b", "a.", "a..b"] {
		let error = parse(&[bad]).expect_err(bad);
		assert!(format!("{error:#}").contains("drift_ignore"), "{bad}: {error:#}");
	}
	let ignore = parse(&["a.b"]).expect("valid");
	assert!(ignore.covers("a", Some(&["b".to_string()])));
	assert!(!ignore.covers("a", None), "a field entry is not its table");
	assert!(!ignore.covers("a", Some(&["c".to_string()])));
	let table = parse(&["a"]).expect("valid");
	assert!(table.covers("a", Some(&["c".to_string()])), "a table entry covers its fields");
}

#[test]
fn a_cross_minor_target_gets_one_k9107_hint() {
	let setup = setup();
	let mut with_minors = facts(&setup, "prod");
	with_minors.minors = Some(("4.0.1", "4.1.0"));
	let found = findings(&[drift(DriftKind::TableMissing, "person", None)], &with_minors);
	assert_eq!(found.iter().filter(|f| f.code == K9107).count(), 1);
	let hint = &found[0];
	assert_eq!(hint.code, K9107);
	assert_eq!((hint.severity, hint.default_level()), (Severity::Hint, Level::Warn));
	assert!(hint.message.contains("`prod`") && hint.message.contains("4.1.0"), "{}", hint.message);
	assert_eq!(hint.span, 10..23, "the target's header");
	assert_eq!(found.len(), 2, "the drift is reported anyway");
	assert_eq!(minor_of("4.0.7"), "4.0");
	assert_eq!(minor_of("4.1"), "4.1");
}

#[test]
fn a_rollout_in_progress_is_noted_on_every_finding_of_the_target() {
	let setup = setup();
	let mut mid = facts(&setup, "prod");
	mid.rollout = Some("r1");
	mid.minors = Some(("4.0.0", "4.1.0"));
	let found = findings(
		&[
			drift(DriftKind::TableMissing, "person", None),
			drift(DriftKind::TableExtra, "audit", None),
		],
		&mid,
	);
	assert_eq!(found.len(), 3);
	for finding in &found {
		let last = finding.notes.last().expect("a note");
		assert_eq!(last.kind, NoteKind::Note);
		assert_eq!(
			last.message,
			"rollout r1 is in progress on prod; the target is expected to differ until it \
			 completes"
		);
	}
}

#[test]
fn the_target_header_is_found_by_its_name() {
	let text = "[variables]\nA = \"b\"\n\n[target.prod]\nhost = \"x\"\n\n  [ target.\"stage\" ]  # c\n[target.other]\n";
	let range = target_header(text, "prod");
	assert_eq!(&text[range], "[target.prod]");
	let range = target_header(text, "stage");
	assert_eq!(&text[range], "[ target.\"stage\" ]  # c");
	assert_eq!(&text[target_header(text, "other")], "[target.other]");
	// A target written another way is the first line.
	assert_eq!(&text[target_header(text, "inline")], "[variables]");
}

#[test]
fn what_setup_defines_is_excluded_and_the_digest_recomputed() {
	let excluded = Excluded::from_setup(&[
		"DEFINE TABLE `my meta` SCHEMAFULL; DEFINE FIELD note ON keep TYPE string;",
		"DEFINE TABLE other_meta SCHEMAFULL;",
	]);
	let mut full = doc(vec![
		table("other_meta", vec![field(&["ns"], "string")]),
		table("my meta", vec![]),
		table("keep", vec![field(&["note"], "string"), field(&["other"], "string")]),
	]);
	excluded.strip(&mut full).expect("strip");
	let mut bare = doc(vec![table("keep", vec![field(&["other"], "string")])]);
	excluded.strip(&mut bare).expect("strip");
	assert_eq!(full.semantic_digest, bare.semantic_digest);
	assert_ne!(full.semantic_digest, "sha256:00");
	assert_eq!(full.tables.len(), 1);
	assert_eq!(full.tables[0].fields.len(), 1);
}

#[test]
fn locations_are_read_with_template_variables_substituted() {
	let text = "DEFINE TABLE ${P}_t;\nDEFINE FIELD OVERWRITE `a b`.c ON ${P}_t TYPE string;\n\
	            DEFINE INDEX i ON TABLE implicit FIELDS x;\nDEFINE TABLE ${P}_t SCHEMAFULL;\n";
	let source = Source {
		path: PathBuf::from("/p/a.surql"),
		text: text.to_string(),
		role: crate::check::discover::Role::Schema,
		module: None,
	};
	let mut vars = TemplateVars::default();
	vars.vars.insert("P".to_string(), "app".to_string());
	let locator = Locator::new(&[&source], &vars);
	let table = locator.table("app_t").expect("table");
	assert_eq!(&text[table.span.clone()], "DEFINE TABLE ${P}_t SCHEMAFULL", "the last one stands");
	assert_eq!(table.statement, Some(3));
	let field = locator.field("app_t", &["a b".into(), "c".into()]).expect("field");
	assert_eq!(field.statement, Some(1));
	let implicit = locator.table("implicit").expect("an implicitly created table");
	assert_eq!(&text[implicit.span.clone()], "DEFINE INDEX i ON TABLE implicit FIELDS x");
	assert_eq!(locator.table("nowhere").map(|l| l.statement), Some(Some(0)), "the first statement");
}

// ---------------------------------------------------------------------------------------
// A project synced to a database, and the database changed.
// ---------------------------------------------------------------------------------------

const V1: &str = "DEFINE TABLE person SCHEMAFULL;\nDEFINE FIELD name ON person TYPE string;\n\
                  DEFINE FIELD age ON person TYPE int;\nDEFINE TABLE legacy SCHEMAFULL;\n\
                  DEFINE FIELD note ON legacy TYPE string;\n";
const V2: &str = "DEFINE TABLE person SCHEMAFULL;\nDEFINE FIELD name ON person TYPE string;\n\
                  DEFINE FIELD age ON person TYPE int;\n";
const CONFIG: &str = "[target.prod]\nhost = \"mem://\"\n";

fn write(root: &Path, path: &str, text: &str) {
	let path = root.join(path);
	std::fs::create_dir_all(path.parent().expect("parent")).expect("dir");
	std::fs::write(path, text).expect("write");
}

struct Project {
	dir: tempfile::TempDir,
	config: ProjectConfig,
	vars: TemplateVars,
	ignore: DriftIgnore,
}

impl Project {
	fn new(schema: &str) -> Self {
		let dir = tempfile::tempdir().expect("tempdir");
		write(dir.path(), "surrealkit.toml", CONFIG);
		write(dir.path(), "database/schema/001_person.surql", schema);
		Self {
			dir,
			config: ProjectConfig::parse(CONFIG).expect("config"),
			vars: TemplateVars::default(),
			ignore: DriftIgnore::default(),
		}
	}

	fn root(&self) -> &Path {
		self.dir.path()
	}

	/// The drift of `db` from what is on disk now.
	async fn drift(&self, db: &Surreal<Any>) -> Vec<Finding> {
		let modules = select_modules(&self.config, &[], false, false).expect("modules");
		let sources =
			discover(&self.config, "database", self.root(), &modules, &AnalyzeConfig::default())
				.expect("discover");
		let env = Env {
			project: &self.config,
			root: self.root(),
			folder: "database",
			vars: &self.vars,
			experimental: &[],
			engine: None,
			ignore: &self.ignore,
		};
		let selected: BTreeSet<String> = modules.iter().map(|m| m.name().to_string()).collect();
		let files = files_side(&sources, &selected, &env).await.expect("files side");
		let header = Location {
			path: self.root().join("surrealkit.toml"),
			span: target_header(CONFIG, "prod"),
			statement: None,
		};
		compare(db, "prod", &header, &files, &env).await.expect("compare").findings
	}
}

async fn info(db: &Surreal<Any>) -> Value {
	crate::typegen::introspect_v2::query_json(db, "INFO FOR DB;").await.expect("INFO FOR DB")
}

static SYNCED: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
	path: "database/schema/001_person.surql",
	sql: V1,
}];

#[tokio::test]
async fn a_synced_project_has_no_drift_until_the_database_changes() {
	let project = Project::new(V1);
	let db = crate::test_db::fresh("drift_synced").await;
	Sync::embedded(SYNCED).run(&db).await.expect("sync");
	assert!(project.drift(&db).await.is_empty(), "SurrealKit's own metadata is not drift");

	// The files drop `legacy`, which the catalog still lists. The database loses a field, gains
	// an unmanaged table, and has a field's kind changed.
	write(project.root(), "database/schema/001_person.surql", V2);
	db.query(
		"REMOVE FIELD age ON person; DEFINE TABLE audit SCHEMAFULL; DEFINE FIELD note ON audit \
		 TYPE string; DEFINE FIELD OVERWRITE name ON person TYPE int;",
	)
	.await
	.and_then(|response| response.check())
	.expect("alter");

	let before = info(&db).await;
	let found = project.drift(&db).await;
	assert_eq!(info(&db).await, before, "a drift run changes nothing on the target");

	let codes: Vec<(Code, String)> = found.iter().map(|f| (f.code, f.message.clone())).collect();
	assert_eq!(
		codes,
		[
			(K9103, "field `person.age` is not defined on target `prod`".to_string()),
			(K9104, "field `person.name` differs on target `prod`: kind".to_string()),
			(K9106, "table `audit` is on target `prod` and was never managed by SurrealKit".to_string()),
			(
				K9105,
				"table `legacy` is on target `prod` and managed by SurrealKit, but it is no longer \
				 in the files"
					.to_string()
			),
		],
		"{found:#?}"
	);
	let by_code = |code: Code| found.iter().find(|f| f.code == code).expect("finding");
	let schema = std::fs::read_to_string(project.root().join("database/schema/001_person.surql"))
		.expect("read");
	let missing = by_code(K9103);
	assert!(missing.path.ends_with("database/schema/001_person.surql"));
	assert_eq!(&schema[missing.span.clone()], "DEFINE FIELD age ON person TYPE int");
	let differs = by_code(K9104);
	assert_eq!(&schema[differs.span.clone()], "DEFINE FIELD name ON person TYPE string");
	assert_eq!(differs.notes.len(), 2);
	for code in [K9105, K9106] {
		let extra = by_code(code);
		assert!(extra.path.ends_with("surrealkit.toml"));
		assert_eq!(&CONFIG[extra.span.clone()], "[target.prod]");
	}
}

#[tokio::test]
async fn drift_ignore_hides_an_unmanaged_table() {
	let mut project = Project::new(V2);
	let db = crate::test_db::fresh("drift_ignore").await;
	Sync::embedded(&[EmbeddedSchemaFile {
		path: "database/schema/001_person.surql",
		sql: V2,
	}])
	.run(&db)
	.await
	.expect("sync");
	db.query("DEFINE TABLE audit SCHEMAFULL;")
		.await
		.and_then(|response| response.check())
		.expect("alter");
	assert_eq!(project.drift(&db).await.len(), 1);
	project.ignore = DriftIgnore::from_config(&AnalyzeConfig {
		drift_ignore: vec!["audit".into()],
		..AnalyzeConfig::default()
	})
	.expect("valid");
	assert!(project.drift(&db).await.is_empty());
}

#[tokio::test]
async fn a_database_never_synced_has_only_unmanaged_extras() {
	let project = Project::new(V2);
	let db = crate::test_db::fresh("drift_unsynced").await;
	db.query(
		"DEFINE TABLE person SCHEMAFULL; DEFINE FIELD name ON person TYPE string; \
		 DEFINE FIELD age ON person TYPE int; DEFINE TABLE shared SCHEMAFULL;",
	)
	.await
	.and_then(|response| response.check())
	.expect("define");
	let found = project.drift(&db).await;
	let codes: Vec<Code> = found.iter().map(|f| f.code).collect();
	assert_eq!(codes, [K9106], "{found:#?}");
}
