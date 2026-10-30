//! Live-database schema introspection.
//!
//! Runs `INFO FOR DB` and a per-table `INFO FOR TABLE`, then assembles a
//! [`SchemaTypes`] document. The `INFO` statements return maps of
//! `name -> "DEFINE … statement"`; the raw statement is always preserved on
//! each element so the output is lossless even for kinds we do not deeply
//! parse yet.
//!
//! Assembly ([`assemble`]) is pure: it takes the `INFO` results as JSON, so the
//! golden corpus snapshots in `tests/fixtures/typegen_corpus/introspection` run
//! through exactly the code a live database does. Everything it cannot
//! represent becomes a [`Diagnostic`] rather than a silent `unknown`.

use anyhow::{Context, Result};
use serde_json::Value;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb_types::SurrealValue;

use super::define;
use super::diagnostics::{Diagnostic, Severity, codes};
use super::scan;
use super::signature::parse_function;
use super::type_parser::{parse_type_checked, unwrap_optional};
use super::types::{FieldDef, FieldType, FunctionDef, NamedDef, ParamDef, SchemaTypes, TableDef};

/// The `INFO FOR DB` categories the v1 document models.
const DB_CATEGORIES: &[&str] = &[
	"accesses",
	"analyzers",
	"apis",
	"buckets",
	"configs",
	"functions",
	"models",
	"params",
	"sequences",
	"tables",
	"users",
];

/// The `INFO FOR TABLE` categories the v1 document models (`events`, `fields`,
/// `indexes`) or deliberately ignores: `lives` are running live queries and
/// `tables` the views computed from this one, neither of them schema to type.
const TABLE_CATEGORIES: &[&str] = &["events", "fields", "indexes", "lives", "tables"];

/// Run a query and decode the first result set into a [`serde_json::Value`].
///
/// Mirrors the conversion used by the test runner (`tester/runner.rs`).
async fn query_json(db: &Surreal<Any>, sql: &str) -> Result<Value> {
	let mut response = db.query(sql).await?.check()?;
	let raw: surrealdb_types::Value = response.take(0)?;
	Value::from_value(raw).context("converting the INFO result to JSON")
}

/// Introspect the connected database into a [`SchemaTypes`] document, with a
/// [`Diagnostic`] for everything it could not represent.
///
/// `generated_at` is left empty here — the caller stamps it so this function
/// stays deterministic and easy to test.
pub async fn introspect(db: &Surreal<Any>) -> Result<(SchemaTypes, Vec<Diagnostic>)> {
	let info = query_json(db, "INFO FOR DB;").await.context("INFO FOR DB")?;
	let mut tables = Vec::new();
	for (name, _) in map_entries(&info, "tables") {
		let sql = format!("INFO FOR TABLE `{}`;", name.replace('\\', "\\\\").replace('`', "\\`"));
		let table_info =
			query_json(db, &sql).await.with_context(|| format!("INFO FOR TABLE {name}"))?;
		tables.push((name, table_info));
	}
	let (namespace, database) = session_ns_db(db).await;
	Ok(assemble(&info, &tables, namespace, database))
}

/// Build the document from `INFO FOR DB` and each table's `INFO FOR TABLE`
/// (keyed by table name). Pure; the output is sorted and deterministic.
pub(super) fn assemble(
	db_info: &Value,
	table_infos: &[(String, Value)],
	namespace: Option<String>,
	database: Option<String>,
) -> (SchemaTypes, Vec<Diagnostic>) {
	let mut diagnostics = Vec::new();

	let functions = map_entries(db_info, "functions")
		.into_iter()
		.map(|(_, define)| build_function(define, &mut diagnostics))
		.collect::<Vec<_>>();

	let params = map_entries(db_info, "params")
		.into_iter()
		.map(|(name, define)| ParamDef {
			name: name.trim_start_matches('$').to_string(),
			define,
		})
		.collect();

	let mut tables = Vec::new();
	for (name, define) in map_entries(db_info, "tables") {
		let empty = Value::Null;
		let info = table_infos.iter().find(|(n, _)| *n == name).map_or(&empty, |(_, info)| info);
		tables.push(build_table(&name, define, info, &mut diagnostics));
	}

	unsupported_categories(db_info, DB_CATEGORIES, None, &mut diagnostics);

	let mut doc = SchemaTypes {
		version: 1,
		generated_at: String::new(),
		namespace,
		database,
		tables,
		functions,
		params,
		analyzers: named_defs(db_info, "analyzers"),
		accesses: named_defs(db_info, "accesses"),
		apis: named_defs(db_info, "apis"),
		buckets: named_defs(db_info, "buckets"),
		sequences: named_defs(db_info, "sequences"),
		configs: named_defs(db_info, "configs"),
		models: named_defs(db_info, "models"),
		users: named_defs(db_info, "users"),
	};

	sort_doc(&mut doc);
	diagnostics.sort();
	(doc, diagnostics)
}

/// The namespace and database the session is using. Not fatal when it fails:
/// the schema itself was already introspected, and both fields are optional.
async fn session_ns_db(db: &Surreal<Any>) -> (Option<String>, Option<String>) {
	match query_json(db, "RETURN [session::ns(), session::db()];").await {
		Ok(value) => (
			value.get(0).and_then(Value::as_str).map(str::to_string),
			value.get(1).and_then(Value::as_str).map(str::to_string),
		),
		Err(err) => {
			log::debug!("typegen: could not read the session namespace/database: {err:#}");
			(None, None)
		}
	}
}

fn build_table(
	name: &str,
	define: String,
	info: &Value,
	diagnostics: &mut Vec<Diagnostic>,
) -> TableDef {
	let fields = map_entries(info, "fields")
		.into_iter()
		.map(|(field_name, field_def)| build_field(name, field_name, field_def, diagnostics))
		.collect();
	unsupported_categories(info, TABLE_CATEGORIES, Some(name), diagnostics);

	TableDef {
		name: name.to_string(),
		schemafull: schemafull(&define),
		kind: table_kind(&define),
		define,
		fields,
		events: named_defs(info, "events"),
		indexes: named_defs(info, "indexes"),
	}
}

fn build_field(
	table: &str,
	name: String,
	define: String,
	diagnostics: &mut Vec<Diagnostic>,
) -> FieldDef {
	let clauses = define::parse_field(&define);
	let path = scan::field_path(&name);
	let raw_type = clauses.kind.map(str::to_string);
	let parsed = match &raw_type {
		Some(src) => {
			let (kind, problems) = parse_type_checked(src);
			for problem in problems {
				diagnostics.push(
					Diagnostic::new(
						Severity::Error,
						codes::KIND_UNRECOGNIZED,
						src.as_str(),
						problem.reason,
					)
					.on_table(table)
					.on_field(path.clone())
					.with_span(problem.span),
				);
			}
			kind
		}
		None => {
			// No TYPE clause: the field holds any value. v1 has always written such a field
			// as `unknown` with an empty source (and `unknown` in TypeScript), which is the
			// safer TypeScript type for "anything"; schema document v2 says `any`.
			let (severity, code, reason) = if clauses.computed.is_some() {
				(
					Severity::Warning,
					codes::FIELD_COMPUTED_KIND_UNDECLARED,
					"COMPUTED without TYPE: the computed value can be anything",
				)
			} else {
				(Severity::Info, codes::FIELD_KIND_UNDECLARED, "no TYPE: the field holds any value")
			};
			diagnostics.push(
				Diagnostic::new(severity, code, define.as_str(), reason)
					.on_table(table)
					.on_field(path),
			);
			FieldType::Unknown {
				source: String::new(),
			}
		}
	};
	// SurrealDB normalises `option<T>` to `none | T`; collapse it into the flag.
	let (ty, optional) = unwrap_optional(parsed);
	FieldDef {
		name,
		optional,
		flexible: clauses.flexible,
		readonly: clauses.readonly,
		has_default: clauses.default.is_some(),
		raw_type,
		r#type: ty,
		define,
	}
}

fn build_function(define: String, diagnostics: &mut Vec<Diagnostic>) -> FunctionDef {
	let sig = parse_function(&define);
	let name = sig.name.unwrap_or_else(|| function_name_from_key(&define));
	for (text, problem) in sig.problems {
		diagnostics.push(
			Diagnostic::new(Severity::Error, codes::KIND_UNRECOGNIZED, text, problem.reason)
				.on_function(&name)
				.with_span(problem.span),
		);
	}
	FunctionDef {
		name,
		define,
		args: sig.args,
		returns: sig.returns,
	}
}

/// A `definition-unsupported` warning for every entry of an `INFO` category
/// outside `known`, so nothing the database defines is dropped without a word.
fn unsupported_categories(
	info: &Value,
	known: &[&str],
	table: Option<&str>,
	diagnostics: &mut Vec<Diagnostic>,
) {
	let Some(map) = info.as_object() else {
		return;
	};
	for category in map.keys() {
		if known.contains(&category.as_str()) {
			continue;
		}
		for (_, define) in map_entries(info, category) {
			let level = if table.is_some() {
				"INFO FOR TABLE"
			} else {
				"INFO FOR DB"
			};
			let mut diagnostic = Diagnostic::new(
				Severity::Warning,
				codes::DEFINITION_UNSUPPORTED,
				define,
				format!("{level} category `{category}` is not part of the typegen document"),
			);
			if let Some(table) = table {
				diagnostic = diagnostic.on_table(table);
			}
			diagnostics.push(diagnostic);
		}
	}
}

/// Read a `{ name: "DEFINE …" }` sub-map from an `INFO` result as `(name,
/// define)` pairs. Returns an empty vec if the key is missing or not an object.
fn map_entries(info: &Value, key: &str) -> Vec<(String, String)> {
	let Some(Value::Object(map)) = info.get(key) else {
		return Vec::new();
	};
	map.iter().map(|(k, v)| (k.clone(), as_define_string(v))).collect()
}

fn named_defs(info: &Value, key: &str) -> Vec<NamedDef> {
	let mut defs: Vec<NamedDef> = map_entries(info, key)
		.into_iter()
		.map(|(name, define)| NamedDef {
			name,
			define,
		})
		.collect();
	defs.sort_by(|a, b| a.name.cmp(&b.name));
	defs
}

/// Coerce an `INFO` map value into a definition string. Values are normally
/// `DEFINE …` strings; anything else is JSON-encoded so nothing is lost.
fn as_define_string(v: &Value) -> String {
	match v {
		Value::String(s) => s.clone(),
		other => other.to_string(),
	}
}

fn schemafull(define: &str) -> Option<bool> {
	if scan::has_top_level_keyword(define, "SCHEMAFULL") {
		Some(true)
	} else if scan::has_top_level_keyword(define, "SCHEMALESS") {
		Some(false)
	} else {
		None
	}
}

/// The table's `TYPE` (`NORMAL`, `RELATION` or `ANY`), read from the word after
/// the `TYPE` keyword so a `RELATION` in a comment or view query cannot be
/// mistaken for it.
fn table_kind(define: &str) -> Option<String> {
	let kind = scan::word_after_keyword(define, "TYPE")?.to_ascii_uppercase();
	matches!(kind.as_str(), "NORMAL" | "RELATION" | "ANY").then_some(kind)
}

fn function_name_from_key(define: &str) -> String {
	define
		.find("fn::")
		.map(|i| {
			let rest = &define[i..];
			let end = rest.find('(').unwrap_or(rest.len());
			rest[..end].trim().to_string()
		})
		.unwrap_or_default()
}

fn sort_doc(doc: &mut SchemaTypes) {
	doc.tables.sort_by(|a, b| a.name.cmp(&b.name));
	for t in &mut doc.tables {
		t.fields.sort_by(|a, b| a.name.cmp(&b.name));
	}
	doc.functions.sort_by(|a, b| a.name.cmp(&b.name));
	doc.params.sort_by(|a, b| a.name.cmp(&b.name));
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn table_kind_detects_any_without_false_positives() {
		assert_eq!(table_kind("DEFINE TABLE u TYPE ANY SCHEMALESS"), Some("ANY".to_string()));
		assert_eq!(table_kind("DEFINE TABLE u TYPE RELATION"), Some("RELATION".to_string()));
		assert_eq!(table_kind("DEFINE TABLE u TYPE NORMAL"), Some("NORMAL".to_string()));
		// `ANY` only inside a comment string must not be picked up as the kind.
		assert_eq!(table_kind("DEFINE TABLE u SCHEMAFULL COMMENT 'accepts ANY value'"), None);
	}

	#[test]
	fn escaped_quotes_in_a_table_comment_do_not_leak_keywords() {
		// The engine renders a comment containing `'` with double quotes and
		// escapes inner double quotes; the old scanner ended the string at `\"`.
		let define = r#"DEFINE TABLE t TYPE NORMAL SCHEMAFULL COMMENT "it's a \"RELATION\" SCHEMALESS" PERMISSIONS NONE"#;
		assert_eq!(table_kind(define), Some("NORMAL".to_string()));
		assert_eq!(schemafull(define), Some(true));
	}

	#[test]
	fn a_view_query_does_not_change_the_table_kind() {
		let define = "DEFINE TABLE adults TYPE ANY SCHEMALESS AS SELECT name, age FROM person WHERE \
		              age >= 18 PERMISSIONS NONE";
		assert_eq!(table_kind(define), Some("ANY".to_string()));
		assert_eq!(schemafull(define), Some(false));
	}

	fn info(json: serde_json::Value) -> Value {
		json
	}

	#[test]
	fn unmodelled_categories_are_diagnosed_not_dropped() {
		let db = info(serde_json::json!({
			"tables": {},
			"modules": { "mod::demo": "DEFINE MODULE mod::demo FROM f\"avatars:/demo.wasm\" UNSIGNED PERMISSIONS FULL" },
			"models": {},
		}));
		let (_, diagnostics) = assemble(&db, &[], None, None);
		assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
		assert_eq!(diagnostics[0].code, codes::DEFINITION_UNSUPPORTED);
		assert_eq!(diagnostics[0].severity, Severity::Warning);
		assert!(diagnostics[0].source.starts_with("DEFINE MODULE mod::demo"));
	}

	#[test]
	fn untyped_fields_stay_unknown_with_a_diagnostic() {
		let db =
			info(serde_json::json!({ "tables": { "p": "DEFINE TABLE p TYPE NORMAL SCHEMAFULL" } }));
		let table = info(serde_json::json!({
			"fields": {
				"notes": "DEFINE FIELD notes ON p PERMISSIONS FULL",
				"display": "DEFINE FIELD display ON p COMPUTED string::concat(a, b) PERMISSIONS FULL",
			},
			"events": {}, "indexes": {}, "lives": {}, "tables": {},
		}));
		let (doc, diagnostics) = assemble(&db, &[("p".into(), table)], None, None);
		for field in &doc.tables[0].fields {
			// What v1 has always written for a field without TYPE.
			assert_eq!(
				field.r#type,
				FieldType::Unknown {
					source: String::new()
				},
				"{field:?}"
			);
			assert_eq!(field.raw_type, None);
		}
		let codes: Vec<(&str, Severity)> =
			diagnostics.iter().map(|d| (d.code.as_str(), d.severity)).collect();
		assert_eq!(
			codes,
			vec![
				(codes::FIELD_COMPUTED_KIND_UNDECLARED, Severity::Warning),
				(codes::FIELD_KIND_UNDECLARED, Severity::Info),
			]
		);
	}

	#[test]
	fn unrecognised_kinds_are_errors_with_their_location() {
		let db = info(serde_json::json!({
			"tables": { "p": "DEFINE TABLE p TYPE NORMAL SCHEMAFULL" },
			"functions": { "f": "DEFINE FUNCTION fn::f($a: frob) { RETURN $a } PERMISSIONS FULL" },
		}));
		let table = info(serde_json::json!({
			"fields": { "`odd.x`": "DEFINE FIELD `odd.x` ON p TYPE array<zzz> PERMISSIONS FULL" },
		}));
		let (_, diagnostics) = assemble(&db, &[("p".into(), table)], None, None);
		assert_eq!(diagnostics.len(), 2, "{diagnostics:?}");
		let field = diagnostics.iter().find(|d| d.table.is_some()).unwrap();
		assert_eq!(field.code, codes::KIND_UNRECOGNIZED);
		assert_eq!(field.severity, Severity::Error);
		assert_eq!(field.field.as_deref(), Some(&["odd.x".to_string()][..]));
		assert_eq!(field.source, "array<zzz>");
		let span = field.span.unwrap();
		assert_eq!(&field.source[span.start..span.end], "zzz");
		let function = diagnostics.iter().find(|d| d.function.is_some()).unwrap();
		assert_eq!(function.function.as_deref(), Some("fn::f"));
		assert_eq!(function.source, "frob");
	}
}
