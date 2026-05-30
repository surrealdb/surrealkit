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
	FieldDef {
		name,
		optional,
		flexible: has_clause_keyword(&define, "FLEXIBLE"),
		readonly: has_clause_keyword(&define, "READONLY"),
		has_default: has_clause_keyword(&define, "DEFAULT"),
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
	if has_clause_keyword(define, "SCHEMAFULL") {
		Some(true)
	} else if has_clause_keyword(define, "SCHEMALESS") {
		Some(false)
	} else {
		None
	}
}

fn table_kind(define: &str) -> Option<String> {
	if has_clause_keyword(define, "RELATION") {
		Some("RELATION".to_string())
	} else if has_clause_keyword(define, "NORMAL") {
		Some("NORMAL".to_string())
	} else if has_clause_keyword(define, "ANY") {
		Some("ANY".to_string())
	} else {
		None
	}
}

/// Whole-word, ASCII-case-insensitive keyword check that only matches at the
/// top level (bracket depth 0) and outside string literals.
///
/// This avoids false positives from keywords that appear inside `ASSERT` /
/// `VALUE` expressions, string defaults, or comments — e.g. a field defined as
/// `TYPE string ASSERT $value != 'DEFAULT'` must not be reported as having a
/// `DEFAULT` clause.
fn has_clause_keyword(stmt: &str, keyword: &str) -> bool {
	let chars: Vec<char> = stmt.chars().collect();
	let n = chars.len();
	let mut i = 0;
	let mut depth: i32 = 0;
	let mut quote: Option<char> = None;

	while i < n {
		let c = chars[i];
		if let Some(q) = quote {
			if c == q {
				quote = None;
			}
			i += 1;
			continue;
		}
		match c {
			'\'' | '"' => {
				quote = Some(c);
				i += 1;
				continue;
			}
			'<' | '(' | '[' | '{' => {
				depth += 1;
				i += 1;
				continue;
			}
			'>' | ')' | ']' | '}' => {
				depth -= 1;
				i += 1;
				continue;
			}
			_ => {}
		}

		if c.is_ascii_alphabetic() {
			let start = i;
			let mut j = i;
			while j < n && (chars[j].is_ascii_alphanumeric() || chars[j] == '_') {
				j += 1;
			}
			if depth == 0
				&& chars[start..j].iter().collect::<String>().eq_ignore_ascii_case(keyword)
			{
				return true;
			}
			i = j;
			continue;
		}
		i += 1;
	}
	false
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
	fn clause_keyword_matches_whole_word_at_top_level() {
		assert!(has_clause_keyword("DEFINE FIELD x ON t TYPE string READONLY", "READONLY"));
		assert!(has_clause_keyword("DEFINE FIELD x ON t TYPE string DEFAULT 'a'", "DEFAULT"));
		assert!(has_clause_keyword("DEFINE FIELD x ON t FLEXIBLE TYPE object", "FLEXIBLE"));
		// Case-insensitive.
		assert!(has_clause_keyword("define field x on t type string readonly", "READONLY"));
	}

	#[test]
	fn clause_keyword_ignores_string_literals_and_nested_scopes() {
		// `DEFAULT` only inside a string literal must not count as a DEFAULT clause.
		assert!(!has_clause_keyword(
			"DEFINE FIELD x ON t TYPE string ASSERT $value != 'DEFAULT'",
			"DEFAULT"
		));
		// Keyword nested inside an assertion block (depth > 0) must not match.
		assert!(!has_clause_keyword(
			"DEFINE FIELD x ON t TYPE string ASSERT { READONLY }",
			"READONLY"
		));
		// Substring of a larger identifier must not match.
		assert!(!has_clause_keyword("DEFINE FIELD readonly_at ON t TYPE string", "READONLY"));
	}

	#[test]
	fn table_kind_detects_any_without_false_positives() {
		assert_eq!(table_kind("DEFINE TABLE u TYPE ANY SCHEMALESS"), Some("ANY".to_string()));
		assert_eq!(table_kind("DEFINE TABLE u TYPE RELATION"), Some("RELATION".to_string()));
		assert_eq!(table_kind("DEFINE TABLE u TYPE NORMAL"), Some("NORMAL".to_string()));
		// `ANY` only inside a comment string must not be picked up as the kind.
		assert_eq!(table_kind("DEFINE TABLE u SCHEMAFULL COMMENT 'accepts ANY value'"), None);
	}
}
