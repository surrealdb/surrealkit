//! Building the `queries` section's entries from the engine's [`TypesDocument`].
//!
//! The entry model ([`QueryEntry`] and the types it holds) is schema document v2's, defined in
//! [`crate::typegen::document`]; its kinds are v2 [`Kind`]s, and its JSON form follows the v2
//! conventions: camelCase members, no user-controlled object keys, optional members omitted,
//! never `null`.
//!
//! [`from_types_document`] builds the entries, and the `types` their `named` kinds refer to,
//! from the engine's [`TypesDocument`].

pub use super::document::Certainty;
use super::document::{FieldStep, KindValue, QueryParam, StatementType, TypesDocument};
use super::v2;
use crate::typegen::document::{Kind, TypeDef};
pub use crate::typegen::document::{
	QueryEntry, QueryOrigin, QueryParameter, QuerySource, QueryStatement,
};

/// The v2 spelling of a path step into a collection's elements.
pub const ELEMENT_SEGMENT: &str = "*";

/// An engine document in v2's vocabulary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Converted {
	/// One entry per query, in the engine document's order. Entries carry no `sources`: the
	/// engine document does not know where a query comes from.
	pub queries: Vec<QueryEntry>,
	/// The `DEFINE TYPE`s, sorted by name.
	pub types: Vec<TypeDef>,
	/// The `source` of every `unknown` kind, deduplicated, in the order met, for the caller to
	/// report.
	pub unrepresented: Vec<String>,
}

/// Converts the queries and types of an engine document.
pub fn from_types_document(doc: &TypesDocument) -> Converted {
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
	let mut types: Vec<TypeDef> = doc
		.types
		.iter()
		.map(|def| {
			let converted = v2::nested(&def.kind_value);
			note(converted.unrepresented);
			TypeDef {
				name: def.name.clone(),
				kind: converted.kind,
				comment: None,
			}
		})
		.collect();
	types.sort_by(|a, b| a.name.cmp(&b.name));
	Converted {
		queries: entries,
		types,
		unrepresented,
	}
}

fn statement_entry(statement: &StatementType) -> (QueryStatement, Vec<String>) {
	let (kind, optional, unrepresented) = top_kind(statement.kind_value.as_ref());
	let exact =
		statement.support.get("exact").and_then(serde_json::Value::as_bool).unwrap_or(false);
	let entry = QueryStatement {
		kind,
		optional,
		certainty: statement.certainty,
		exact,
		open: statement.open.iter().map(|path| path.iter().map(segment).collect()).collect(),
	};
	(entry, unrepresented)
}

fn param_entry(param: &QueryParam) -> (QueryParameter, Vec<String>) {
	let (kind, optional, unrepresented) = top_kind(param.kind_value.as_ref());
	let entry = QueryParameter {
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
		let converted =
			from_types_document(&TypesDocument::from_json(&doc.to_string()).expect("document"));
		(converted.queries, converted.unrepresented)
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
				{ "kind_value": { "Either": [{ "Hologram": 1 }, "Int"] }, "certainty": "fact" }
			], "params": [
				{ "name": "a", "kind_value": { "Either": ["None", "String"] }, "required": true },
				{ "name": "b", "required": false },
				{ "name": "c", "kind_value": { "Quux": [] }, "required": true },
			] },
			{ "text": "q2", "statements": [
				{ "kind_value": { "Hologram": 1 }, "certainty": "fact" }
			] },
		]));
		assert_eq!(unrepresented, [r#"{"Hologram":1}"#, r#"{"Quux":[]}"#]);
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
			statements: vec![QueryStatement {
				kind: Some(Kind::int()),
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
