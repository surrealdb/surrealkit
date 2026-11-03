//! The typed result of each query, in schema document v2's vocabulary.
//!
//! A [`QueryEntry`] is one query: its text, where host code sends it, the type of each
//! statement's slot in the response, and the `$name` parameters it reads. Its kinds are v2
//! [`Kind`]s, and its JSON form follows the v2 conventions: camelCase members, no
//! user-controlled object keys, optional members omitted, never `null`.
//!
//! [`from_types_document`] builds the entries from the engine's [`TypesDocument`].

use serde::{Deserialize, Serialize};

pub use super::document::Certainty;
use super::document::{FieldStep, KindValue, QueryParam, StatementType, TypesDocument};
use super::v2;
use crate::typegen::document::Kind;

/// One query and the typed result of each of its statements.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueryEntry {
	/// The name the query was given, or its exact text when it has none.
	pub key: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub name: Option<String>,
	/// The exact text host code passes to the client.
	pub text: String,
	/// Where host code sends the query. May be empty.
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub sources: Vec<SourceRef>,
	pub statements: Vec<StatementEntry>,
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub params: Vec<ParamEntry>,
}

/// A place in host code that sends a query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SourceRef {
	pub path: String,
	pub line: u32,
	pub column: u32,
}

/// The type of one top-level statement's slot in the response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StatementEntry {
	/// Absent for a statement with no slot in the response (`BEGIN`, `COMMIT`, `CANCEL`).
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub kind: Option<Kind>,
	/// The top-level kind admitted NONE: v2's rule for a top position folds it into this flag.
	pub optional: bool,
	pub certainty: Certainty,
	/// Whether the engine reports the kind as the exact shape of the result.
	pub exact: bool,
	/// Positions inside `kind` where an object admits undeclared keys. Each is a path of v2
	/// segments: `[]` is the kind itself, `"*"` an array or set element, any other segment an
	/// object literal field.
	#[serde(default)]
	pub open: Vec<Vec<String>>,
}

/// A `$name` a query reads without binding it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ParamEntry {
	pub name: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub kind: Option<Kind>,
	/// The kind admitted NONE (folded as for a statement).
	pub optional: bool,
	/// Whether the caller must bind it.
	pub required: bool,
}

/// The v2 spelling of a path step into a collection's elements.
pub const ELEMENT_SEGMENT: &str = "*";

/// The entries for the queries of an engine document, and the SurrealQL spelling of every kind
/// v2 cannot represent (deduplicated, in the order met), for the caller to report.
///
/// Entries carry no `sources`: the engine document does not know where host code sends a query.
pub fn from_types_document(doc: &TypesDocument) -> (Vec<QueryEntry>, Vec<String>) {
	let mut unrepresented: Vec<String> = Vec::new();
	let mut note = |spellings: Vec<String>| {
		for spelling in spellings {
			if !unrepresented.contains(&spelling) {
				unrepresented.push(spelling);
			}
		}
	};
	let mut entries = Vec::with_capacity(doc.queries.len());
	for query in &doc.queries {
		let mut statements = Vec::with_capacity(query.statements.len());
		for statement in &query.statements {
			let (entry, spellings) = statement_entry(statement);
			note(spellings);
			statements.push(entry);
		}
		let mut params = Vec::with_capacity(query.params.len());
		for param in &query.params {
			let (entry, spellings) = param_entry(param);
			note(spellings);
			params.push(entry);
		}
		entries.push(QueryEntry {
			key: query.name.clone().unwrap_or_else(|| query.text.clone()),
			name: query.name.clone(),
			text: query.text.clone(),
			sources: Vec::new(),
			statements,
			params,
		});
	}
	(entries, unrepresented)
}

fn statement_entry(statement: &StatementType) -> (StatementEntry, Vec<String>) {
	let (kind, optional, unrepresented) = top_kind(statement.kind_value.as_ref());
	let exact =
		statement.support.get("exact").and_then(serde_json::Value::as_bool).unwrap_or(false);
	let entry = StatementEntry {
		kind,
		optional,
		certainty: statement.certainty,
		exact,
		open: statement.open.iter().map(|path| path.iter().map(segment).collect()).collect(),
	};
	(entry, unrepresented)
}

fn param_entry(param: &QueryParam) -> (ParamEntry, Vec<String>) {
	let (kind, optional, unrepresented) = top_kind(param.kind_value.as_ref());
	let entry = ParamEntry {
		name: param.name.clone(),
		kind,
		optional,
		required: param.required,
	};
	(entry, unrepresented)
}

fn top_kind(kind: Option<&KindValue>) -> (Option<Kind>, bool, Vec<String>) {
	match kind {
		Some(kind) => {
			let converted = v2::top(kind);
			(Some(converted.kind), converted.optional, converted.unrepresented)
		}
		None => (None, false, Vec::new()),
	}
}

fn segment(step: &FieldStep) -> String {
	match step {
		FieldStep::Field(name) => name.clone(),
		FieldStep::Element => ELEMENT_SEGMENT.to_string(),
	}
}

#[cfg(test)]
mod tests {
	use serde_json::{Value, json};

	use super::*;

	fn entries(queries: Value) -> (Vec<QueryEntry>, Vec<String>) {
		let doc = json!({ "format": "surrealdb.types", "version": 1, "queries": queries });
		from_types_document(&TypesDocument::from_json(&doc.to_string()).expect("document"))
	}

	#[test]
	fn the_key_is_the_name_or_the_text() {
		let (entries, _) = entries(json!([
			{ "name": "older", "text": "SELECT 1", "statements": [] },
			{ "name": null, "text": "SELECT 2", "statements": [] },
		]));
		assert_eq!((entries[0].key.as_str(), entries[0].name.as_deref()), ("older", Some("older")));
		assert_eq!((entries[1].key.as_str(), entries[1].name.as_deref()), ("SELECT 2", None));
		assert!(entries.iter().all(|entry| entry.sources.is_empty()));
	}

	#[test]
	fn a_statement_folds_none_into_optional_and_reads_exactness_and_open_paths() {
		let (entries, unrepresented) = entries(json!([{ "text": "q", "statements": [
			{ "kind_value": { "Either": ["None", { "Literal": { "Object": { "a": "Int" } } }] },
				"certainty": "fact", "support": { "exact": true }, "open": [[]] },
			{ "kind_value": { "Array": ["Any", null] }, "certainty": "hint",
				"open": [[{ "element": true }, { "field": "a" }]] },
			{ "kind_value": null, "certainty": "fact" },
		] }]));
		assert!(unrepresented.is_empty());
		let statements = &entries[0].statements;
		assert!(statements[0].optional && statements[0].exact);
		assert_eq!(statements[0].open, vec![Vec::<String>::new()]);
		assert_eq!(
			serde_json::to_value(&statements[0].kind).expect("json"),
			json!({ "kind": "objectLiteral", "fields": [
				{ "name": "a", "kind": { "kind": "int" }, "optional": false }
			] })
		);
		assert!(!statements[1].exact && statements[1].certainty == Certainty::Hint);
		assert_eq!(statements[1].open, vec![vec!["*".to_string(), "a".to_string()]]);
		assert!(statements[2].kind.is_none() && !statements[2].optional);
	}

	#[test]
	fn params_convert_and_unrepresented_spellings_are_listed_once() {
		let (entries, unrepresented) = entries(json!([
			{ "text": "q1", "statements": [
				{ "kind_value": { "Either": ["Date", "Int"] }, "certainty": "fact" }
			], "params": [
				{ "name": "a", "kind_value": { "Either": ["None", "String"] }, "required": true },
				{ "name": "b", "required": false },
				{ "name": "c", "kind_value": { "Named": "money" }, "required": true },
			] },
			{ "text": "q2", "statements": [
				{ "kind_value": "Date", "certainty": "fact" }
			] },
		]));
		assert_eq!(unrepresented, ["date", "money"]);
		let params = &entries[0].params;
		assert!(params[0].optional && params[0].required);
		assert_eq!(params[0].kind, Some(Kind::String));
		assert!(params[1].kind.is_none() && !params[1].required);
	}

	#[test]
	fn an_entry_round_trips_and_omits_what_is_absent() {
		let entry = QueryEntry {
			key: "RETURN 1".into(),
			name: None,
			text: "RETURN 1".into(),
			sources: Vec::new(),
			statements: vec![StatementEntry {
				kind: Some(Kind::Int),
				optional: false,
				certainty: Certainty::Fact,
				exact: true,
				open: Vec::new(),
			}],
			params: Vec::new(),
		};
		let json = serde_json::to_value(&entry).expect("json");
		assert_eq!(
			json,
			json!({
				"key": "RETURN 1", "text": "RETURN 1",
				"statements": [{
					"kind": { "kind": "int" }, "optional": false,
					"certainty": "fact", "exact": true, "open": []
				}]
			})
		);
		assert_eq!(serde_json::from_value::<QueryEntry>(json).expect("reads"), entry);
	}
}
