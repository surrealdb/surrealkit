//! The typegen golden corpus (`tests/fixtures/typegen_corpus`) through the v1
//! generator.
//!
//! Two sources feed the same assembly code:
//!
//! - the plain `INFO` snapshots captured from SurrealDB 3.2.4 and 3.3.0, so every engine minor
//!   the corpus documents is covered on every machine, offline;
//! - a live engine — the embedded 3.3 engine, or the server at `SURREALKIT_TEST_URL` — with the
//!   corpus applied, which must agree with the snapshot for its version.
//!
//! The bar is the strict one: no `unknown` kind anywhere, and no diagnostic of
//! error severity. What genuinely is not modelled is reported, and the exact set
//! of those diagnostics is pinned here.

use std::path::{Path, PathBuf};

use serde_json::Value;

use super::diagnostics::{Diagnostic, Severity, codes};
use super::introspect::assemble;
use super::types::{FieldType, SchemaTypes};

pub(crate) fn corpus_dir() -> PathBuf {
	Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/typegen_corpus")
}

pub(crate) fn read_json(path: &Path) -> Value {
	let text =
		std::fs::read_to_string(path).unwrap_or_else(|err| panic!("{}: {err}", path.display()));
	serde_json::from_str(&text).unwrap_or_else(|err| panic!("{}: {err}", path.display()))
}

/// The v1 document and diagnostics for one engine version's `INFO` snapshots.
fn from_snapshot(version: &str) -> (SchemaTypes, Vec<Diagnostic>) {
	let dir = corpus_dir().join("introspection").join(version);
	let db_info = read_json(&dir.join("db.info.json"));
	let tables_info = read_json(&dir.join("tables.info.json"));
	let tables: Vec<(String, Value)> = tables_info
		.as_object()
		.expect("tables.info.json is an object")
		.iter()
		.map(|(name, info)| (name.clone(), info.clone()))
		.collect();
	assemble(&db_info, &tables, Some("corpus".into()), Some("corpus".into()))
}

/// Every kind in the document: field kinds and function argument and return kinds.
fn all_kinds(doc: &SchemaTypes) -> Vec<(String, &FieldType)> {
	let mut kinds = Vec::new();
	for table in &doc.tables {
		for field in &table.fields {
			kinds.push((format!("{}.{}", table.name, field.name), &field.r#type));
		}
	}
	for function in &doc.functions {
		for arg in &function.args {
			kinds.push((format!("{}(${})", function.name, arg.name), &arg.r#type));
		}
		if let Some(returns) = &function.returns {
			kinds.push((format!("{} -> ", function.name), returns));
		}
	}
	kinds
}

/// Whether `kind` holds a kind that failed to parse. A field without `TYPE` is `Unknown` with
/// an empty source, which is what v1 has always written for it (and is diagnosed as
/// `field-kind-undeclared`); it is not a parse failure.
fn contains_unknown(kind: &FieldType) -> bool {
	match kind {
		FieldType::Unknown {
			source,
		} => !source.is_empty(),
		FieldType::Option {
			inner,
		}
		| FieldType::Array {
			inner,
			..
		}
		| FieldType::Set {
			inner,
			..
		} => contains_unknown(inner),
		FieldType::Union {
			variants: items,
		}
		| FieldType::Tuple {
			items,
		} => items.iter().any(contains_unknown),
		FieldType::Object {
			fields,
		} => fields.iter().any(|f| contains_unknown(&f.r#type)),
		FieldType::Primitive {
			..
		}
		| FieldType::Record {
			..
		}
		| FieldType::Table {
			..
		}
		| FieldType::File {
			..
		}
		| FieldType::Geometry {
			..
		}
		| FieldType::Literal {
			..
		} => false,
	}
}

/// Fail with every `unknown` kind and error diagnostic listed.
fn assert_strict_clean(label: &str, doc: &SchemaTypes, diagnostics: &[Diagnostic]) {
	let unknown: Vec<String> = all_kinds(doc)
		.into_iter()
		.filter(|(_, kind)| contains_unknown(kind))
		.map(|(at, kind)| format!("{at}: {kind:?}"))
		.collect();
	assert!(unknown.is_empty(), "{label}: unknown kinds:\n{}", unknown.join("\n"));
	let errors: Vec<String> = diagnostics
		.iter()
		.filter(|d| d.severity == Severity::Error)
		.map(|d| d.to_string())
		.collect();
	assert!(errors.is_empty(), "{label}: error diagnostics:\n{}", errors.join("\n"));
}

/// One line per field and function: the document's semantic content in a form
/// that diffs readably. `define` strings are left out (they are evidence, and
/// identical to the snapshots by construction).
fn projection(doc: &SchemaTypes) -> Vec<String> {
	let mut lines = Vec::new();
	for table in &doc.tables {
		lines.push(format!(
			"table {} kind={} schemafull={}",
			table.name,
			table.kind.as_deref().unwrap_or("-"),
			table.schemafull.map_or("-".to_string(), |s| s.to_string())
		));
		for field in &table.fields {
			let mut flags = Vec::new();
			for (on, flag) in [
				(field.optional, "optional"),
				(field.flexible, "flexible"),
				(field.readonly, "readonly"),
				(field.has_default, "default"),
			] {
				if on {
					flags.push(flag);
				}
			}
			lines.push(format!(
				"  {}.{}: {} [{}]",
				table.name,
				field.name,
				serde_json::to_string(&field.r#type).expect("serialise kind"),
				flags.join(",")
			));
		}
	}
	for function in &doc.functions {
		let args: Vec<String> = function
			.args
			.iter()
			.map(|a| {
				format!(
					"${}{}: {}",
					a.name,
					if a.optional {
						"?"
					} else {
						""
					},
					serde_json::to_string(&a.r#type).expect("serialise kind")
				)
			})
			.collect();
		let returns = function
			.returns
			.as_ref()
			.map_or("-".to_string(), |r| serde_json::to_string(r).expect("serialise kind"));
		lines.push(format!("fn {}({}) -> {}", function.name, args.join(", "), returns));
	}
	lines
}

fn diagnostic_lines(diagnostics: &[Diagnostic]) -> Vec<String> {
	diagnostics
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

#[test]
fn the_corpus_parses_with_no_unknown_kinds_on_3_3_0() {
	let (doc, diagnostics) = from_snapshot("3.3.0");
	assert_strict_clean("3.3.0", &doc, &diagnostics);
	let mut text = projection(&doc).join("\n");
	text.push_str("\n\ndiagnostics:\n");
	text.push_str(&diagnostic_lines(&diagnostics).join("\n"));
	insta::assert_snapshot!("corpus_v1_3_3_0", text);
}

#[test]
fn the_corpus_parses_with_no_unknown_kinds_on_3_2_4() {
	let (doc, diagnostics) = from_snapshot("3.2.4");
	assert_strict_clean("3.2.4", &doc, &diagnostics);
}

#[test]
fn engine_minors_agree_except_for_3_3_only_relations() {
	let old = projection(&from_snapshot("3.2.4").0);
	let new = projection(&from_snapshot("3.3.0").0);
	for line in &old {
		assert!(new.contains(line), "3.2.4 line missing on 3.3.0: {line}");
	}
	let extra: Vec<&String> = new.iter().filter(|l| !old.contains(l)).collect();
	for line in &extra {
		let line = line.trim_start();
		assert!(
			["table knows ", "table rates ", "knows.", "rates."]
				.iter()
				.any(|p| line.starts_with(p)),
			"only the 3.3-only LIGHTWEIGHT/INLINE relations may differ, got: {line}"
		);
	}
	assert!(!extra.is_empty(), "3.3.0 has the knows/rates relations");
}

/// Everything the corpus defines that v1 genuinely does not model, and nothing
/// else: no error, and each warning and info accounted for.
#[test]
fn only_genuinely_unmodelled_constructs_are_diagnosed() {
	let (_, diagnostics) = from_snapshot("3.3.0");
	let lines = diagnostic_lines(&diagnostics);
	assert_eq!(
		lines,
		vec![
			format!("warning {} -", codes::DEFINITION_UNSUPPORTED),
			format!("warning {} person.display", codes::FIELD_COMPUTED_KIND_UNDECLARED),
			format!("warning {} person.referenced_by", codes::FIELD_COMPUTED_KIND_UNDECLARED),
			format!("warning {} team.members", codes::FIELD_COMPUTED_KIND_UNDECLARED),
			format!("info {} person.notes", codes::FIELD_KIND_UNDECLARED),
		]
	);
	let module = diagnostics.iter().find(|d| d.code == codes::DEFINITION_UNSUPPORTED).unwrap();
	assert!(module.source.starts_with("DEFINE MODULE mod::demo"), "{module:?}");
}

/// Kinds the old parser turned into `unknown`, now structured.
#[test]
fn previously_unknown_kinds_are_structured() {
	let (doc, _) = from_snapshot("3.3.0");
	let kind = |table: &str, field: &str| {
		let t = doc.tables.iter().find(|t| t.name == table).unwrap();
		serde_json::to_value(&t.fields.iter().find(|f| f.name == field).unwrap().r#type).unwrap()
	};
	assert_eq!(kind("kinds", "k_regex"), serde_json::json!({"kind": "primitive", "name": "regex"}));
	assert_eq!(kind("kinds", "k_range"), serde_json::json!({"kind": "primitive", "name": "range"}));
	assert_eq!(
		kind("kinds", "k_table"),
		serde_json::json!({"kind": "table", "tables": ["person", "team"]})
	);
	assert_eq!(
		kind("kinds", "k_file_buckets"),
		serde_json::json!({"kind": "file", "buckets": ["avatars", "docs"]})
	);
	assert_eq!(kind("kinds", "k_tuple")["kind"], "tuple");
	assert_eq!(kind("kinds", "k_object_literal")["kind"], "object");
	assert_eq!(
		kind("kinds", "k_literal_numbers"),
		serde_json::json!({"kind": "union", "variants": [
			{"kind": "literal", "value": -5},
			{"kind": "literal", "value": 0},
			{"kind": "literal", "value": 1.5, "type": "float"},
			{"kind": "literal", "value": "10", "type": "decimal"},
		]})
	);
	assert_eq!(
		kind("kinds", "k_object_flexible"),
		serde_json::json!({"kind": "primitive", "name": "object"})
	);
	assert_eq!(
		kind("id_object", "id"),
		serde_json::json!({"kind": "object", "fields": [
			{"name": "n", "type": {"kind": "primitive", "name": "int"}},
			{"name": "region", "type": {"kind": "primitive", "name": "string"}},
		]})
	);
	let person = doc.tables.iter().find(|t| t.name == "person").unwrap();
	let address = person.fields.iter().find(|f| f.name == "address").unwrap();
	assert!(address.flexible, "FLEXIBLE after the kind is read");
	assert_eq!(address.raw_type.as_deref(), Some("object"));
	let weight = doc
		.tables
		.iter()
		.find(|t| t.name == "rates")
		.unwrap()
		.fields
		.iter()
		.find(|f| f.name == "weight")
		.unwrap();
	assert!(weight.optional);
	assert_eq!(weight.raw_type.as_deref(), Some("none | float"), "INLINE ends the kind");
}

// ---------------------------------------------------------------------------
// Live engine
// ---------------------------------------------------------------------------

/// A corpus module from `corpus.json`.
pub(crate) struct Module {
	pub path: String,
	pub engines: Option<Vec<String>>,
	pub experimental: bool,
}

pub(crate) fn corpus_modules() -> Vec<Module> {
	let corpus = read_json(&corpus_dir().join("corpus.json"));
	corpus["modules"]
		.as_array()
		.expect("modules")
		.iter()
		.map(|m| Module {
			path: m["path"].as_str().expect("path").to_string(),
			engines: m["engines"]
				.as_array()
				.map(|e| e.iter().map(|v| v.as_str().expect("engine").to_string()).collect()),
			experimental: m["experimental"].as_array().is_some_and(|e| !e.is_empty()),
		})
		.collect()
}

#[tokio::test]
async fn a_live_engine_agrees_with_its_snapshot() {
	let remote = crate::test_db::is_remote();
	// The embedded engine takes experimental capabilities from the client; a
	// server has them only if it was started with them, which CI's are not, so
	// gated modules are applied only to the embedded engine.
	let db = if remote {
		crate::test_db::fresh("typegen_corpus").await
	} else {
		crate::test_db::fresh_experimental("typegen_corpus").await
	};
	let version = db.version().await.expect("engine version");
	let minor = format!("{}.{}", version.major, version.minor);
	let exact = format!("{}.{}.{}", version.major, version.minor, version.patch);

	for module in corpus_modules() {
		if module.engines.as_ref().is_some_and(|e| !e.contains(&minor)) {
			continue;
		}
		if module.experimental && remote {
			continue;
		}
		let sql = std::fs::read_to_string(corpus_dir().join(&module.path)).expect("module");
		db.query(sql.as_str())
			.await
			.and_then(|r| r.check())
			.unwrap_or_else(|err| panic!("applying {} to {exact}: {err}", module.path));
	}

	let (doc, diagnostics) = super::introspect::introspect(&db).await.expect("introspect");
	assert_strict_clean(&format!("live {exact}"), &doc, &diagnostics);

	// The command itself, in strict mode: warnings are logged, nothing fails, and
	// neither output contains an unknown kind.
	let tmp = tempfile::TempDir::new().expect("temp dir");
	super::run_typegen(
		&db,
		"./database",
		super::TypegenOpts {
			out: Some(tmp.path().join("schema.json")),
			pretty: true,
			ts_out: Some(tmp.path().join("types")),
			strict: true,
			..super::TypegenOpts::default()
		},
	)
	.await
	.unwrap_or_else(|err| panic!("strict typegen over the corpus on {exact}: {err:#}"));
	let json = std::fs::read_to_string(tmp.path().join("schema.json")).expect("json");
	// A field without TYPE is `unknown` with an empty source, as v1 always wrote it; any other
	// `unknown` is a kind that failed to parse.
	fn parse_failures(v: &serde_json::Value) -> usize {
		match v {
			serde_json::Value::Object(map) => {
				let failed = map.get("kind").and_then(serde_json::Value::as_str) == Some("unknown")
					&& map.get("source").and_then(serde_json::Value::as_str) != Some("");
				usize::from(failed) + map.values().map(parse_failures).sum::<usize>()
			}
			serde_json::Value::Array(items) => items.iter().map(parse_failures).sum(),
			_ => 0,
		}
	}
	let parsed: serde_json::Value = serde_json::from_str(&json).expect("json output");
	assert_eq!(parse_failures(&parsed), 0, "unknown kind in strict output:\n{json}");
	let ts = std::fs::read_to_string(tmp.path().join("types/index.ts")).expect("ts");
	assert!(ts.contains("export interface Kinds {"), "{ts}");

	let snapshot_dir = corpus_dir().join("introspection").join(&exact);
	if !snapshot_dir.is_dir() {
		// No snapshot for this exact engine: the strict bar above is the check.
		return;
	}
	let live = projection(&doc);
	let snapshot = projection(&from_snapshot(&exact).0);
	if remote {
		// Without the experimental modules the live schema is a subset.
		for line in &live {
			assert!(snapshot.contains(line), "live {exact} disagrees with its snapshot: {line}");
		}
	} else {
		assert_eq!(live, snapshot, "the embedded engine must match the {exact} snapshot exactly");
	}
}

/// A field named like a clause keyword, read by other fields' VALUE, ASSERT and PERMISSIONS:
/// the engine renders it unquoted, and none of those mentions may become a TYPE clause.
#[tokio::test]
async fn a_field_named_type_does_not_retype_the_fields_that_read_it() {
	let db = crate::test_db::fresh("typegen_field_named_type").await;
	db.query(
		"DEFINE TABLE t SCHEMAFULL;
		 DEFINE FIELD type ON t TYPE string;
		 DEFINE FIELD value ON t TYPE int;
		 DEFINE FIELD label ON t TYPE string VALUE type + '!';
		 DEFINE FIELD amount ON t TYPE int ASSERT type != 'refund' OR $value > 0;
		 DEFINE FIELD secret ON t TYPE option<string> PERMISSIONS FOR select WHERE type = 'public';
		 DEFINE FIELD doubled ON t TYPE int VALUE value * 2;
		 DEFINE FIELD echo ON t COMPUTED type;",
	)
	.await
	.and_then(|r| r.check())
	.expect("apply the schema");
	let (doc, _) = super::introspect::introspect(&db).await.expect("introspect");
	let t = doc.tables.iter().find(|t| t.name == "t").expect("table t");
	let raw = |name: &str| {
		let field = t.fields.iter().find(|f| f.name == name).expect(name);
		let kind = serde_json::to_value(&field.r#type).expect("kind");
		(field.raw_type.clone(), kind["kind"].as_str().unwrap_or_default().to_string())
	};
	assert_eq!(raw("label"), (Some("string".into()), "primitive".into()));
	assert_eq!(raw("amount"), (Some("int".into()), "primitive".into()));
	assert_eq!(raw("doubled"), (Some("int".into()), "primitive".into()));
	assert_eq!(raw("secret").0.as_deref(), Some("none | string"));
	assert_eq!(raw("echo").0, None, "COMPUTED without TYPE has no kind");
}
