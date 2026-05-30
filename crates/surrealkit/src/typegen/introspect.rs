//! Live-database schema introspection.
//!
//! Runs `INFO FOR DB` and a per-table `INFO FOR TABLE`, then assembles a
//! [`SchemaTypes`] document. The `INFO` statements return maps of
//! `name -> "DEFINE … statement"`; the raw statement is always preserved on
//! each element so the output is lossless even for kinds we do not deeply
//! parse yet.

use anyhow::{Context, Result};
use serde_json::Value;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb_types::SurrealValue;

use super::signature::parse_function;
use super::type_parser::{extract_type_clause, parse_type, unwrap_optional};
use super::types::{FieldDef, FieldType, FunctionDef, NamedDef, ParamDef, SchemaTypes, TableDef};

/// Run a query and decode the first result set into a [`serde_json::Value`].
///
/// Mirrors the conversion used by the test runner (`tester/runner.rs`).
async fn query_json(db: &Surreal<Any>, sql: &str) -> Result<Value> {
	let mut response = db.query(sql).await?.check()?;
	let raw: surrealdb_types::Value = response.take(0)?;
	Ok(Value::from_value(raw).unwrap_or(Value::Null))
}

/// Introspect the connected database into a [`SchemaTypes`] document.
///
/// `namespace`/`database` and `generated_at` are left empty here — the caller
/// fills them in so this function stays deterministic and easy to test.
pub async fn introspect(db: &Surreal<Any>) -> Result<SchemaTypes> {
	let info = query_json(db, "INFO FOR DB;").await.context("INFO FOR DB")?;

	let functions = map_entries(&info, "functions")
		.into_iter()
		.map(|(_, define)| {
			let sig = parse_function(&define);
			FunctionDef {
				name: sig.name.unwrap_or_else(|| function_name_from_key(&define)),
				define,
				args: sig.args,
				returns: sig.returns,
			}
		})
		.collect::<Vec<_>>();

	let params = map_entries(&info, "params")
		.into_iter()
		.map(|(name, define)| ParamDef {
			name: name.trim_start_matches('$').to_string(),
			define,
		})
		.collect();

	let mut tables = Vec::new();
	for (name, define) in map_entries(&info, "tables") {
		tables.push(introspect_table(db, &name, define).await?);
	}

	let mut doc = SchemaTypes {
		version: 1,
		generated_at: String::new(),
		namespace: None,
		database: None,
		tables,
		functions,
		params,
		analyzers: named_defs(&info, "analyzers"),
		accesses: named_defs(&info, "accesses"),
		apis: named_defs(&info, "apis"),
		buckets: named_defs(&info, "buckets"),
		sequences: named_defs(&info, "sequences"),
		configs: named_defs(&info, "configs"),
		models: named_defs(&info, "models"),
		users: named_defs(&info, "users"),
	};

	sort_doc(&mut doc);
	Ok(doc)
}

async fn introspect_table(db: &Surreal<Any>, name: &str, define: String) -> Result<TableDef> {
	let info = query_json(db, &format!("INFO FOR TABLE `{name}`;"))
		.await
		.with_context(|| format!("INFO FOR TABLE {name}"))?;

	let fields = map_entries(&info, "fields")
		.into_iter()
		.map(|(field_name, field_def)| build_field(field_name, field_def))
		.collect();

	Ok(TableDef {
		name: name.to_string(),
		schemafull: schemafull(&define),
		kind: table_kind(&define),
		define,
		fields,
		events: named_defs(&info, "events"),
		indexes: named_defs(&info, "indexes"),
	})
}

fn build_field(name: String, define: String) -> FieldDef {
	let raw_type = extract_type_clause(&define);
	let parsed = raw_type.as_deref().map(parse_type).unwrap_or(FieldType::Unknown {
		source: String::new(),
	});
	// SurrealDB normalises `option<T>` to `none | T`; collapse it into the flag.
	let (ty, optional) = unwrap_optional(parsed);
	let upper = define.to_ascii_uppercase();
	FieldDef {
		name,
		optional,
		flexible: contains_keyword(&upper, "FLEXIBLE"),
		readonly: contains_keyword(&upper, "READONLY"),
		has_default: contains_keyword(&upper, "DEFAULT"),
		raw_type,
		r#type: ty,
		define,
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
	let upper = define.to_ascii_uppercase();
	if contains_keyword(&upper, "SCHEMAFULL") {
		Some(true)
	} else if contains_keyword(&upper, "SCHEMALESS") {
		Some(false)
	} else {
		None
	}
}

fn table_kind(define: &str) -> Option<String> {
	let upper = define.to_ascii_uppercase();
	if contains_keyword(&upper, "RELATION") {
		Some("RELATION".to_string())
	} else if contains_keyword(&upper, "NORMAL") {
		Some("NORMAL".to_string())
	} else if upper.contains("TYPE ANY") {
		Some("ANY".to_string())
	} else {
		None
	}
}

/// Whole-word, ASCII-case-insensitive keyword check (input already uppercased).
fn contains_keyword(haystack_upper: &str, keyword: &str) -> bool {
	haystack_upper.split(|c: char| !c.is_ascii_alphanumeric() && c != '_').any(|w| w == keyword)
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
