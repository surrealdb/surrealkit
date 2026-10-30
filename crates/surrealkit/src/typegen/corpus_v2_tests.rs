//! The typegen golden corpus through schema document v2.
//!
//! - Every captured engine minor is read through its STRUCTURE adapter with no error
//!   diagnostic, and the exact set of warnings and infos is pinned.
//! - The plain-`INFO` fallback reader produces the same semantic digest as the STRUCTURE
//!   adapter on every captured minor (`info_and_structure_agree_*`).
//! - Every entry of the contract's hand-written `example.v2.json` equals the generated one.
//! - 3.2.4 and 3.3.0 differ only by the 3.3-only relations.
//! - A live engine with the corpus applied agrees with its snapshot.

use serde_json::Value;

use super::corpus_tests::{corpus_dir, corpus_modules, read_json};
use super::diagnostics::Severity;
use super::document::{EngineMode, SchemaDocument, Source};
use super::introspect_v2::{Origin, Snapshot, assemble, assemble_from_info};

/// The captured `INFO` and `INFO … STRUCTURE` output for one engine version.
pub(crate) fn fixture_snapshot(version: &str) -> Snapshot {
	let dir = corpus_dir().join("introspection").join(version);
	let pairs = |file: &str| -> Vec<(String, Value)> {
		read_json(&dir.join(file))
			.as_object()
			.expect("an object of tables")
			.iter()
			.map(|(k, v)| (k.clone(), v.clone()))
			.collect()
	};
	Snapshot {
		version: version.to_string(),
		build: Some(format!("surrealdb-{version}")),
		db_info: read_json(&dir.join("db.info.json")),
		db_structure: Some(read_json(&dir.join("db.structure.json"))),
		tables_info: pairs("tables.info.json"),
		tables_structure: pairs("tables.structure.json"),
	}
}

fn introspect_origin() -> Origin {
	Origin {
		source: Source::Introspect {
			namespace: "corpus".into(),
			database: "corpus".into(),
		},
		mode: EngineMode::Remote,
		capabilities: None,
		diagnostics: Vec::new(),
	}
}

pub(crate) fn fixture_document(version: &str) -> SchemaDocument {
	assemble(&fixture_snapshot(version), introspect_origin()).expect("assemble")
}

/// The semantic projection, pretty-printed, for readable diffs.
fn semantic(doc: &SchemaDocument) -> String {
	let value = super::canonical::projection(&serde_json::to_value(doc).expect("json"));
	serde_json::to_string_pretty(&value).expect("json")
}

fn diagnostic_lines(doc: &SchemaDocument) -> Vec<String> {
	doc.diagnostics
		.iter()
		.map(|d| {
			let at = match (&d.table, &d.field, &d.function) {
				(Some(t), Some(f), _) => format!("{t}.{}", f.join(".")),
				(Some(t), None, _) => t.clone(),
				(None, _, Some(f)) => f.clone(),
				_ => "-".to_string(),
			};
			format!("{} {} {at}", d.severity, d.code)
		})
		.collect()
}

/// The error codes the corpus acknowledges (`corpus.json` `acknowledge`), as
/// `[typegen] acknowledge` would: each one is there on purpose, with its reason.
pub(crate) fn corpus_acknowledged() -> Vec<String> {
	read_json(&corpus_dir().join("corpus.json"))["acknowledge"]
		.as_object()
		.map(|codes| codes.keys().cloned().collect())
		.unwrap_or_default()
}

fn assert_no_errors(label: &str, doc: &SchemaDocument) {
	let acknowledged = corpus_acknowledged();
	let errors: Vec<String> = doc
		.diagnostics
		.iter()
		.filter(|d| d.severity == Severity::Error && !acknowledged.contains(&d.code))
		.map(|d| d.to_string())
		.collect();
	assert!(errors.is_empty(), "{label}: error diagnostics:\n{}", errors.join("\n"));
}

#[test]
fn the_corpus_reads_cleanly_through_both_structure_adapters() {
	for (version, adapter) in [("3.2.4", "structure-3.2"), ("3.3.0", "structure-3.3")] {
		let doc = fixture_document(version);
		assert_no_errors(version, &doc);
		assert_eq!(doc.provenance.adapter, adapter);
		assert!(!semantic(&doc).contains("\"unknown\""), "{version}: an unknown kind");
	}
}

/// The 3.3.0 document's content is pinned once, by `compiled/corpus.v2.json`: compiling the
/// corpus gives the same semantic content (`compile::corpus` checks that), so here its digest
/// must equal that file's, and only its diagnostics, which differ between the readers, are
/// pinned.
#[test]
fn the_3_3_0_document_is_pinned() {
	let doc = fixture_document("3.3.0");
	let compiled = read_json(&corpus_dir().join("compiled/corpus.v2.json"));
	assert_eq!(compiled["semanticDigest"].as_str(), Some(doc.semantic_digest.as_str()));
	let mut text = String::from("diagnostics:\n");
	text.push_str(&diagnostic_lines(&doc).join("\n"));
	text.push_str(&format!("\n\n{}\n", doc.semantic_digest));
	insta::assert_snapshot!("corpus_v2_3_3_0", text);
}

#[test]
fn info_and_structure_agree_on_3_2_4() {
	let structured = fixture_document("3.2.4");
	let fallback = assemble_from_info(&fixture_snapshot("3.2.4")).expect("info");
	assert_eq!(semantic(&fallback), semantic(&structured));
	assert_eq!(fallback.semantic_digest, structured.semantic_digest);
	assert_eq!(fallback.provenance.adapter, "info");
}

#[test]
fn info_and_structure_agree_on_3_3_0() {
	let structured = fixture_document("3.3.0");
	let fallback = assemble_from_info(&fixture_snapshot("3.3.0")).expect("info");
	assert_eq!(semantic(&fallback), semantic(&structured));
	assert_eq!(fallback.semantic_digest, structured.semantic_digest);
	// The fallback says it is one, at error severity, and nothing else differs.
	let extra: Vec<String> = diagnostic_lines(&fallback)
		.into_iter()
		.filter(|l| !diagnostic_lines(&structured).contains(l))
		.collect();
	assert_eq!(extra, vec!["error structure-unsupported-engine -".to_string()]);
}

#[test]
fn an_unsupported_engine_minor_falls_back_to_info_with_an_error() {
	let mut snapshot = fixture_snapshot("3.3.0");
	snapshot.version = "3.4.0".into();
	snapshot.db_structure = None;
	let doc = assemble(&snapshot, introspect_origin()).unwrap();
	assert_eq!(doc.provenance.adapter, "info");
	assert!(doc.has_errors());
	assert_eq!(doc.semantic_digest, fixture_document("3.3.0").semantic_digest);
}

/// The contract's example document is a hand-written subset, cross-checked by hand against
/// the snapshots. Every entry in it must equal the generated one.
#[test]
fn every_entry_of_the_contract_example_matches_the_generated_document() {
	let example = read_json(&corpus_dir().join("example.v2.json"));
	let generated = serde_json::to_value(fixture_document("3.3.0")).unwrap();
	let key = |collection: &str| {
		if collection == "apis" {
			"path"
		} else if collection == "configs" {
			"type"
		} else {
			"name"
		}
	};
	for collection in [
		"tables",
		"functions",
		"params",
		"accesses",
		"analyzers",
		"sequences",
		"buckets",
		"apis",
		"modules",
		"configs",
		"users",
	] {
		for entry in example[collection].as_array().unwrap() {
			let k = key(collection);
			let mut found = generated[collection]
				.as_array()
				.unwrap()
				.iter()
				.find(|g| g[k] == entry[k])
				.unwrap_or_else(|| {
					panic!("{collection} {} missing from the generated document", entry[k])
				})
				.clone();
			if collection == "tables" {
				// The example lists a subset of each table's fields, indexes and events.
				strip_define(&mut found);
				for part in ["fields", "indexes", "events"] {
					let want = entry[part].as_array().unwrap();
					let have = found[part].as_array().unwrap();
					for item in want {
						let id = if part == "fields" {
							"path"
						} else {
							"name"
						};
						let got = have.iter().find(|h| h[id] == item[id]).unwrap_or_else(|| {
							panic!("{} {part} {} missing", entry["name"], item[id])
						});
						assert_eq!(got, item, "{} {part} {}", entry["name"], item[id]);
					}
					found[part] = entry[part].clone();
				}
			}
			assert_eq!(&found, entry, "{collection} {}", entry[k]);
		}
	}
	// Each of the example's diagnostics is generated (reasons included).
	let have = generated["diagnostics"].as_array().unwrap();
	for d in example["diagnostics"].as_array().unwrap() {
		assert!(have.contains(d), "diagnostic not generated: {d}");
	}
}

fn strip_define(table: &mut Value) {
	if let Value::Object(map) = table {
		map.remove("define");
		if let Some(Value::Array(fields)) = map.get_mut("fields") {
			for f in fields {
				if let Value::Object(f) = f {
					f.remove("define");
				}
			}
		}
	}
}

#[test]
fn engine_minors_differ_only_by_the_3_3_relations() {
	let mut new = fixture_document("3.3.0");
	let old = fixture_document("3.2.4");
	new.tables.retain(|t| t.name != "knows" && t.name != "rates");
	let reproject = |doc: &SchemaDocument| {
		super::canonical::semantic_digest(&serde_json::to_value(doc).unwrap()).unwrap()
	};
	assert_eq!(semantic(&new), semantic(&old));
	assert_eq!(reproject(&new), old.semantic_digest);
}

#[test]
fn the_document_round_trips_through_strict_decoding() {
	let doc = fixture_document("3.3.0");
	let json = serde_json::to_string_pretty(&doc).unwrap();
	let back: SchemaDocument = serde_json::from_str(&json).unwrap();
	assert_eq!(back, doc);
	// An unknown property is refused, not ignored.
	let mut value: Value = serde_json::from_str(&json).unwrap();
	value["tables"][0]["surprise"] = Value::Bool(true);
	assert!(serde_json::from_value::<SchemaDocument>(value).is_err());
}

#[test]
fn identical_inputs_give_identical_bytes() {
	let a = serde_json::to_string_pretty(&fixture_document("3.3.0")).unwrap();
	let b = serde_json::to_string_pretty(&fixture_document("3.3.0")).unwrap();
	assert_eq!(a, b);
}

/// Apply the corpus modules the engine accepts to a fresh database.
pub(crate) async fn apply_corpus(
	db: &surrealdb::Surreal<surrealdb::engine::any::Any>,
	experimental: bool,
) -> String {
	let version = db.version().await.expect("engine version");
	let minor = format!("{}.{}", version.major, version.minor);
	for module in corpus_modules() {
		if module.engines.as_ref().is_some_and(|e| !e.contains(&minor)) {
			continue;
		}
		if module.experimental && !experimental {
			continue;
		}
		let sql = std::fs::read_to_string(corpus_dir().join(&module.path)).expect("module");
		db.query(sql.as_str())
			.await
			.and_then(|r| r.check())
			.unwrap_or_else(|err| panic!("applying {}: {err}", module.path));
	}
	format!("{}.{}.{}", version.major, version.minor, version.patch)
}

/// Whether a server at `SURREALKIT_TEST_URL` was started with the experimental capabilities
/// the gated corpus modules need (`--allow-experimental=files,surrealism`).
pub(crate) fn remote_has_experimental() -> bool {
	std::env::var("SURREALKIT_TEST_EXPERIMENTAL").is_ok_and(|v| v == "1")
}

/// `[typegen] schema_version = 2` changes the JSON and nothing else: the TypeScript is the
/// v1 emitter's, byte for byte, and `--check` compares the v2 bytes.
#[tokio::test]
async fn typegen_v2_writes_the_v2_document_and_unchanged_typescript() {
	use super::{SchemaVersion, TypegenOpts, run_typegen};
	let db = crate::test_db::fresh("typegen_v2_cmd").await;
	db.query(
		"DEFINE TABLE person SCHEMAFULL; DEFINE FIELD name ON person TYPE string; DEFINE FIELD \
		 nick ON person TYPE option<string> ASSERT string::len($value) <= 20;",
	)
	.await
	.and_then(|r| r.check())
	.expect("schema");
	let tmp = tempfile::TempDir::new().unwrap();
	let opts = |version: SchemaVersion, dir: &str, check: bool| TypegenOpts {
		out: Some(tmp.path().join(dir).join("schema.json")),
		pretty: true,
		ts_out: Some(tmp.path().join(dir).join("types")),
		strict: true,
		check,
		omit_generated_at: true,
		schema_version: version,
		..TypegenOpts::default()
	};
	run_typegen(&db, "./database", opts(SchemaVersion::V1, "v1", false)).await.expect("v1");
	run_typegen(&db, "./database", opts(SchemaVersion::V2, "v2", false)).await.expect("v2");
	let read = |p: &str| std::fs::read_to_string(tmp.path().join(p)).unwrap();
	assert_eq!(read("v1/types/index.ts"), read("v2/types/index.ts"));
	let doc: SchemaDocument = serde_json::from_str(&read("v2/schema.json")).expect("a v2 document");
	assert_eq!(doc.version, 2);
	assert!(doc.tables.iter().any(|t| t.name == "person"));
	assert!(read("v1/schema.json").contains("\"version\": 1"));
	run_typegen(&db, "./database", opts(SchemaVersion::V2, "v2", true)).await.expect("up to date");
	std::fs::write(tmp.path().join("v2/schema.json"), "{}").unwrap();
	assert!(run_typegen(&db, "./database", opts(SchemaVersion::V2, "v2", true)).await.is_err());
}

/// `semantics/access.v3.3.surql` on a live 3.3 engine: every key STRUCTURE reports for an
/// access is known, and the STRUCTURE and plain-`INFO` readers give the same document and the
/// same warnings for the clauses the document does not carry (AUDIENCE, CONTEXT).
#[tokio::test]
async fn access_definitions_read_alike_through_both_readers_on_3_3() {
	let db = crate::test_db::fresh("typegen_access").await;
	let version = db.version().await.expect("engine version");
	if (version.major, version.minor) != (3, 3) {
		// 3.2 has neither AUDIENCE nor CONTEXT.
		return;
	}
	let sql =
		std::fs::read_to_string(corpus_dir().join("semantics/access.v3.3.surql")).expect("module");
	db.query(sql.as_str()).await.and_then(|r| r.check()).expect("applying the access module");
	let snapshot = super::introspect_v2::capture(&db).await.expect("capture");
	let structured = assemble(&snapshot, introspect_origin()).expect("structure");
	let fallback = assemble_from_info(&snapshot).expect("info");
	assert_no_errors("structure", &structured);
	assert_eq!(semantic(&fallback), semantic(&structured));
	let mut from_structure = diagnostic_lines(&structured);
	from_structure.sort();
	let mut from_info: Vec<String> = diagnostic_lines(&fallback)
		.into_iter()
		.filter(|l| l != "error structure-unsupported-engine -")
		.collect();
	from_info.sort();
	assert_eq!(from_info, from_structure);
	let unsupported =
		from_structure.iter().filter(|l| *l == "warning definition-unsupported -").count();
	// issuer_jwt: AUDIENCE; remote_audience: AUDIENCE; members: AUDIENCE, CONTEXT.
	assert_eq!(unsupported, 4, "{from_structure:?}");
	for name in ["issuer_jwt", "members"] {
		let access = structured.accesses.iter().find(|a| a.name == name).expect("access");
		assert_eq!(access.authenticate.as_deref(), Some("{ RETURN $auth }"));
	}
	let remote = structured.accesses.iter().find(|a| a.name == "remote_audience").expect("access");
	assert_eq!(
		serde_json::to_value(&remote.verify).expect("json")["url"],
		"https://example.com/.well-known/jwks.json"
	);
}

/// `semantics/objects.surql` on a live engine: every object position the document reports
/// as `object-without-fields` refuses a keyed write and every other one keeps its keys
/// (which is why it is an error), `field-name-run-together` flags only the name run into
/// an object's, and the STRUCTURE and plain-`INFO` readers agree.
#[tokio::test]
async fn objects_without_fields_are_errors_because_the_engine_refuses_their_keys() {
	use super::diagnostics::codes;
	let db = crate::test_db::fresh("typegen_objects").await;
	let sql =
		std::fs::read_to_string(corpus_dir().join("semantics/objects.surql")).expect("module");
	db.query(sql.as_str()).await.and_then(|r| r.check()).expect("applying the objects module");
	let snapshot = super::introspect_v2::capture(&db).await.expect("capture");
	let structured = assemble(&snapshot, introspect_origin()).expect("structure");
	let fallback = assemble_from_info(&snapshot).expect("info");
	let reported = |doc: &SchemaDocument, code: &str| -> Vec<String> {
		let mut found: Vec<String> = doc
			.diagnostics
			.iter()
			.filter(|d| d.code == code)
			.map(|d| format!("{} {}.{}", d.severity, d.table.as_deref().unwrap_or("-"), d.source))
			.collect();
		found.sort();
		found
	};
	let objects = [
		"error shapes.items.*",
		"error shapes.nullable",
		"error shapes.parent.inner",
		"error shapes.plain",
	];
	let run_together = ["warning shapes.envelopealgorithm"];
	for doc in [&structured, &fallback] {
		assert_eq!(reported(doc, codes::OBJECT_WITHOUT_FIELDS), objects);
		assert_eq!(reported(doc, codes::FIELD_NAME_RUN_TOGETHER), run_together);
	}

	let write = |content: &'static str| {
		let db = db.clone();
		async move { db.query(format!("CREATE shapes CONTENT {content}")).await.and_then(|r| r.check()) }
	};
	for content in [
		"{ plain: { k: 1 } }",
		"{ nullable: { k: 1 } }",
		"{ items: [{ k: 1 }] }",
		"{ parent: { inner: { k: 1 } } }",
	] {
		let refused = write(content).await.expect_err(content).to_string();
		assert!(refused.contains("no such field exists"), "{content}: {refused}");
	}
	for content in [
		"{ plain: {} }",
		"{ flex: { k: 1 } }",
		"{ withkid: { k: 'v' } }",
		"{ rows: [{ k: 'v' }] }",
		"{ openrows: [{ k: 1 }] }",
	] {
		write(content).await.unwrap_or_else(|err| panic!("{content}: {err}"));
	}
	let mut kept = db
		.query("CREATE loose CONTENT { plain: { k: 1 } } RETURN VALUE plain.k")
		.await
		.and_then(|r| r.check())
		.expect("a SCHEMALESS object keeps its keys");
	let k: Option<i64> = kept.take(0).expect("one value");
	assert_eq!(k, Some(1));
}

#[tokio::test]
async fn a_live_engine_agrees_with_its_snapshot_v2() {
	let remote = crate::test_db::is_remote();
	let experimental = !remote || remote_has_experimental();
	let db = if remote {
		crate::test_db::fresh("typegen_corpus_v2").await
	} else {
		crate::test_db::fresh_experimental("typegen_corpus_v2").await
	};
	let exact = apply_corpus(&db, experimental).await;
	let doc = super::introspect_v2::introspect(&db, EngineMode::Remote).await.expect("introspect");
	assert_no_errors(&format!("live {exact}"), &doc);
	if !corpus_dir().join("introspection").join(&exact).is_dir() || !experimental {
		// No snapshot for this exact engine, or a subset of the corpus: the strict bar is it.
		return;
	}
	let snapshot = fixture_document(&exact);
	assert_eq!(semantic(&doc), semantic(&snapshot), "live {exact} differs from its snapshot");
	assert_eq!(doc.semantic_digest, snapshot.semantic_digest);
}

/// Authentication policy is semantic content, including when it is a RETURN statement.
/// A released server must agree with the hermetic compiler through both INFO readers.
#[tokio::test]
async fn access_authenticate_changes_the_semantic_digest() {
	use super::introspect_v2::capture;
	use crate::compile::{CompileModule, CompileOpts, compile};
	use crate::schema_state::SchemaFile;
	let mut digests = Vec::new();
	for expression in ["RETURN $auth", "RETURN NONE", "{ RETURN $auth }", "SELECT * FROM principal"]
	{
		let sql = format!(
			"DEFINE TABLE principal SCHEMAFULL; \
			 DEFINE ACCESS sample ON DATABASE TYPE RECORD SIGNIN (RETURN principal:one) \
			 WITH JWT ALGORITHM HS512 KEY 'synthetic-compile-only-key-never-used' \
			 AUTHENTICATE ({expression}) DURATION FOR TOKEN 15m, FOR SESSION 1h;"
		);
		let module = CompileModule {
			name: "access".into(),
			files: vec![SchemaFile {
				path: "schema/access.surql".into(),
				hash: crate::core::sha256_hex(sql.as_bytes()),
				sql,
			}],
		};
		let document = compile(std::slice::from_ref(&module), &CompileOpts::default())
			.await
			.expect("compile")
			.document;
		assert!(document.diagnostics.is_empty(), "{:?}", document.diagnostics);
		assert_eq!(document.accesses[0].authenticate.as_deref(), Some(expression));
		crate::typegen::json_schema_tests::assert_valid(
			"compiled authentication policy",
			&serde_json::to_value(&document).unwrap(),
		);
		let repeated = compile(std::slice::from_ref(&module), &CompileOpts::default())
			.await
			.expect("repeat compile")
			.document;
		assert_eq!(
			serde_json::to_value(&document).unwrap(),
			serde_json::to_value(repeated).unwrap()
		);
		if crate::test_db::is_remote() {
			let db = crate::test_db::fresh("compile_authenticate_vs_live").await;
			db.query(module.files[0].sql.as_str()).await.and_then(|r| r.check()).expect("apply");
			let snapshot = capture(&db).await.expect("capture");
			let live = super::introspect_v2::introspect(&db, super::document::EngineMode::Remote)
				.await
				.expect("structure");
			let fallback =
				crate::typegen::introspect_v2::assemble_from_info(&snapshot).expect("plain INFO");
			assert_eq!(document.semantic_digest, live.semantic_digest);
			assert_eq!(document.semantic_digest, fallback.semantic_digest);
		}
		digests.push(document.semantic_digest);
	}
	assert_ne!(digests[0], digests[1]);
}
