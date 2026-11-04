//! The engine's `TypesDocument`: the schema of a database plus the typed result of each query.
//!
//! A serde mirror of the document `Datastore::types_document` and the `types` RPC method
//! produce. Members this reader does not know are ignored, so an additive engine change does not
//! break it; a `format` it does not know or a `version` above [`VERSION`] is refused.

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use super::kind::{KindValue, LiteralValue};
pub use crate::typegen::document::{Certainty, LiveAction, LiveExit};

/// The `format` member of every document this reader accepts.
pub const FORMAT: &str = "surrealdb.types";

/// The newest document `version` this reader understands.
pub const VERSION: u32 = 1;

/// A database's types and the typed result of each query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TypesDocument {
	pub format: String,
	pub version: u32,
	/// The version of the engine's core crate.
	#[serde(default)]
	pub engine: Option<String>,
	#[serde(default)]
	pub namespace: Option<String>,
	#[serde(default)]
	pub database: Option<String>,
	#[serde(default)]
	pub tables: Vec<TableTypes>,
	#[serde(default)]
	pub functions: Vec<FunctionTypes>,
	/// `DEFINE TYPE` definitions.
	#[serde(default)]
	pub types: Vec<NamedType>,
	/// `DEFINE PARAM` definitions.
	#[serde(default)]
	pub params: Vec<ParamTypes>,
	#[serde(default)]
	pub queries: Vec<QueryTypes>,
}

impl TypesDocument {
	/// Reads a document from JSON. Refuses a `format` other than [`FORMAT`] and a `version` above
	/// [`VERSION`], before looking at anything else in it.
	pub fn from_json(text: &str) -> Result<Self> {
		let raw: Value = serde_json::from_str(text).context("the types document is not JSON")?;
		let format = raw.get("format").and_then(Value::as_str).unwrap_or("<missing>");
		if format != FORMAT {
			bail!("unsupported types document format `{format}`; this reader reads `{FORMAT}`");
		}
		let Some(version) = raw.get("version").and_then(Value::as_u64) else {
			bail!("the `{FORMAT}` document has no `version`; this reader reads up to {VERSION}");
		};
		if version > u64::from(VERSION) {
			bail!(
				"the `{FORMAT}` document is version {version}, newer than the version {VERSION} \
				 this reader reads; upgrade SurrealKit"
			);
		}
		serde_json::from_value(raw).context("the types document is malformed")
	}
}

/// One table: its fields and what kind of table it is.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableTypes {
	pub name: String,
	#[serde(default)]
	pub schemafull: bool,
	#[serde(default)]
	pub drop: bool,
	#[serde(default)]
	pub view: bool,
	#[serde(default)]
	pub relation: Option<RelationTypes>,
	#[serde(default)]
	pub fields: Vec<FieldTypes>,
}

/// The tables a relation connects. An empty list admits any table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelationTypes {
	#[serde(rename = "in", default)]
	pub in_: Vec<String>,
	#[serde(default)]
	pub out: Vec<String>,
}

/// One `DEFINE FIELD`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldTypes {
	pub path: Vec<String>,
	pub steps: Vec<FieldStep>,
	pub kind: String,
	pub kind_value: KindValue,
	pub certainty: Certainty,
	#[serde(default)]
	pub optional: bool,
	#[serde(default)]
	pub computed: bool,
	#[serde(default)]
	pub readonly: bool,
	#[serde(default)]
	pub has_default: bool,
	#[serde(default)]
	pub reference: bool,
	#[serde(default)]
	pub flexible: bool,
}

/// One step into a value: an object member or a collection element (`[*]`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawStep", into = "RawStep")]
pub enum FieldStep {
	Field(String),
	Element,
}

/// The JSON spelling of a [`FieldStep`]: `{"field": "name"}` or `{"element": true}`.
#[derive(Serialize, Deserialize)]
struct RawStep {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	field: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	element: Option<bool>,
}

impl TryFrom<RawStep> for FieldStep {
	type Error = String;

	fn try_from(raw: RawStep) -> Result<Self, Self::Error> {
		match raw {
			RawStep {
				field: Some(name),
				element: None,
			} => Ok(FieldStep::Field(name)),
			RawStep {
				field: None,
				element: Some(true),
			} => Ok(FieldStep::Element),
			_ => Err("a step is `{\"field\": name}` or `{\"element\": true}`".to_string()),
		}
	}
}

impl From<FieldStep> for RawStep {
	fn from(step: FieldStep) -> Self {
		match step {
			FieldStep::Field(name) => RawStep {
				field: Some(name),
				element: None,
			},
			FieldStep::Element => RawStep {
				field: None,
				element: Some(true),
			},
		}
	}
}

/// A `DEFINE FUNCTION`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionTypes {
	pub name: String,
	#[serde(default)]
	pub args: Vec<ArgTypes>,
	#[serde(default)]
	pub returns: Option<String>,
	#[serde(default)]
	pub returns_value: Option<KindValue>,
	#[serde(default)]
	pub returns_certainty: Option<Certainty>,
	#[serde(default)]
	pub returns_declared: bool,
}

/// One argument of a function.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArgTypes {
	pub name: String,
	pub kind: String,
	pub kind_value: KindValue,
}

/// A `DEFINE TYPE`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NamedType {
	pub name: String,
	pub kind: String,
	pub kind_value: KindValue,
}

/// A `DEFINE PARAM`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParamTypes {
	pub name: String,
	#[serde(default)]
	pub kind: Option<String>,
	#[serde(default)]
	pub kind_value: Option<KindValue>,
}

/// One query and the typed result of each of its statements.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryTypes {
	/// The name the query was given, if any.
	#[serde(default)]
	pub name: Option<String>,
	pub text: String,
	#[serde(default)]
	pub statements: Vec<StatementType>,
	#[serde(default)]
	pub params: Vec<QueryParam>,
	/// Why the engine could not type this query alone. The query then has no `statements` and
	/// no `params`.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub error: Option<QueryErrorType>,
}

/// The engine's refusal to type one query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryErrorType {
	pub code: String,
	pub message: String,
	/// Kept opaque: a byte range in the query text as `{ "start": …, "end": … }`, with the
	/// members this reader does not use.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub span: Option<Value>,
}

/// The type of one top-level statement's slot in the response. Every statement has one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatementType {
	/// `None` when the engine did not type the statement; it is then approximate.
	#[serde(default)]
	pub kind: Option<String>,
	#[serde(default)]
	pub kind_value: Option<KindValue>,
	pub certainty: Certainty,
	/// Kept opaque.
	#[serde(default)]
	pub support: Value,
	/// Object positions inside `kind_value` that admit undeclared keys. `[]` is the kind itself.
	#[serde(default)]
	pub open: Vec<Vec<FieldStep>>,
	/// Kept opaque.
	#[serde(default)]
	pub span: Value,
	/// What a live subscription to the statement delivers.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub live: Option<LiveType>,
	/// Why `LIVE SELECT` cannot run the statement as written.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub live_refusal: Option<LiveRefusalType>,
}

/// The live subscription of a statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LiveType {
	pub diff: bool,
	/// The `LIVE SELECT` text to subscribe with.
	pub query: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub exit: Option<LiveExit>,
	/// `LIVE SELECT id FROM <targets>`, without the `WHERE`. Only with `exit: "silent"`.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub watch: Option<String>,
	/// One per action.
	#[serde(default)]
	pub notifications: Vec<LiveNotificationType>,
}

/// What a live subscription delivers for one action.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LiveNotificationType {
	pub action: LiveAction,
	pub record: String,
	pub record_value: KindValue,
	pub result: String,
	pub result_value: KindValue,
	pub certainty: Certainty,
	/// Object positions inside `result_value` that admit undeclared keys.
	#[serde(default)]
	pub open: Vec<Vec<FieldStep>>,
}

/// Why `LIVE SELECT` cannot run a `SELECT` as written.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LiveRefusalType {
	pub code: String,
	pub message: String,
	#[serde(default)]
	pub clauses: Vec<String>,
	#[serde(default)]
	pub windowable: bool,
}

/// A `$name` a query reads without binding it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryParam {
	pub name: String,
	#[serde(default)]
	pub kind: Option<String>,
	#[serde(default)]
	pub kind_value: Option<KindValue>,
	#[serde(default)]
	pub certainty: Option<Certainty>,
	#[serde(default)]
	pub required: bool,
}

#[cfg(test)]
mod tests {
	use serde_json::json;

	use super::*;

	fn minimal(format: &str, version: u32) -> String {
		json!({ "format": format, "version": version }).to_string()
	}

	#[test]
	fn reads_a_minimal_document() {
		let doc = TypesDocument::from_json(&minimal(FORMAT, VERSION)).expect("reads");
		assert_eq!(doc.format, FORMAT);
		assert!(doc.tables.is_empty() && doc.queries.is_empty() && doc.types.is_empty());
	}

	#[test]
	fn a_session_without_namespace_or_database_reads_as_none() {
		let raw = json!({
			"format": FORMAT, "version": 1, "engine": "4.1.0", "namespace": null, "database": null,
		});
		let doc = TypesDocument::from_json(&raw.to_string()).expect("reads");
		assert_eq!(doc.engine.as_deref(), Some("4.1.0"));
		assert!(doc.namespace.is_none() && doc.database.is_none());
	}

	#[test]
	fn refuses_another_format() {
		let err = TypesDocument::from_json(&minimal("other.types", 1)).expect_err("refused");
		let text = format!("{err:#}");
		assert!(text.contains("other.types") && text.contains(FORMAT), "{text}");
	}

	#[test]
	fn refuses_a_newer_version() {
		let err = TypesDocument::from_json(&minimal(FORMAT, VERSION + 1)).expect_err("refused");
		let text = format!("{err:#}");
		assert!(text.contains("version 2") && text.contains("version 1"), "{text}");
	}

	#[test]
	fn ignores_members_it_does_not_know() {
		let raw = json!({
			"format": FORMAT, "version": 1, "future": [1],
			"tables": [{ "name": "t", "fields": [], "future": true }],
		});
		let doc = TypesDocument::from_json(&raw.to_string()).expect("reads");
		assert_eq!(doc.tables.len(), 1);
	}

	#[test]
	fn steps_and_open_paths_read_the_structured_form() {
		let raw = json!({
			"format": FORMAT, "version": 1,
			"queries": [{
				"name": null,
				"text": "SELECT * FROM t",
				"statements": [{
					"kind": "array<object>", "kind_value": { "Array": ["Object", null] },
					"certainty": "hint", "support": { "exact": true },
					"open": [[{ "element": true }], [{ "element": true }, { "field": "a" }]],
					"span": {}
				}],
			}],
		});
		let doc = TypesDocument::from_json(&raw.to_string()).expect("reads");
		let open = &doc.queries[0].statements[0].open;
		assert_eq!(
			open,
			&vec![vec![FieldStep::Element], vec![FieldStep::Element, FieldStep::Field("a".into())]]
		);
		let back = serde_json::to_value(&doc).expect("serialises");
		assert_eq!(
			back["queries"][0]["statements"][0]["open"],
			raw["queries"][0]["statements"][0]["open"]
		);
	}

	#[test]
	fn open_defaults_to_empty() {
		let raw = json!({
			"format": FORMAT, "version": 1,
			"queries": [{ "text": "RETURN 1", "statements": [
				{ "kind": "int", "kind_value": "Int", "certainty": "fact" }
			] }],
		});
		let doc = TypesDocument::from_json(&raw.to_string()).expect("reads");
		assert!(doc.queries[0].statements[0].open.is_empty());
	}

	#[test]
	fn a_malformed_step_is_an_error() {
		let raw = json!({
			"format": FORMAT, "version": 1,
			"tables": [{ "name": "t", "fields": [{
				"path": ["a"], "steps": [{ "elements": true }], "kind": "int",
				"kind_value": "Int", "certainty": "fact"
			}] }],
		});
		assert!(TypesDocument::from_json(&raw.to_string()).is_err());
	}

	#[test]
	fn live_typing_and_a_query_error_read_in_the_engines_shape() {
		let raw = json!({
			"format": FORMAT, "version": 1,
			"queries": [
				{ "text": "SELECT * FROM t WHERE a = 1", "statements": [{
					"kind": "array<object>", "kind_value": { "Array": ["Object", null] },
					"certainty": "fact",
					"live": {
						"diff": false, "query": "LIVE SELECT * FROM t WHERE a = 1", "exit": "silent",
						"watch": "LIVE SELECT id FROM t", "future": 1,
						"notifications": [{
							"action": "UPDATE", "record": "record<t>", "record_value": { "Record": ["t"] },
							"result": "object", "result_value": "Object", "certainty": "hint",
							"open": [[{ "field": "a" }, { "element": true }]], "future": 1
						}]
					},
					"live_refusal": {
						"code": "E4009", "message": "no", "clauses": ["ORDER BY"], "windowable": true
					}
				}] },
				{ "text": "SELEC", "statements": [], "params": [],
					"error": { "code": "S0001", "message": "bad", "span": { "start": 0, "end": 5 } } },
				{ "text": "RETURN 1", "statements": [] },
			],
		});
		let doc = TypesDocument::from_json(&raw.to_string()).expect("reads");
		let statement = &doc.queries[0].statements[0];
		let live = statement.live.as_ref().expect("live");
		assert_eq!(live.exit, Some(LiveExit::Silent));
		assert_eq!(live.watch.as_deref(), Some("LIVE SELECT id FROM t"));
		let note = &live.notifications[0];
		assert_eq!((note.action, note.certainty), (LiveAction::Update, Certainty::Hint));
		assert_eq!(note.record_value, KindValue::Record(vec!["t".into()]));
		assert_eq!(note.open, vec![vec![FieldStep::Field("a".into()), FieldStep::Element]]);
		let refusal = statement.live_refusal.as_ref().expect("refusal");
		assert!(refusal.windowable && refusal.clauses == ["ORDER BY"]);
		let error = doc.queries[1].error.as_ref().expect("error");
		assert_eq!((error.code.as_str(), error.message.as_str()), ("S0001", "bad"));
		assert!(doc.queries[2].error.is_none() && doc.queries[2].statements.is_empty());
		let back = serde_json::to_value(&doc).expect("serialises");
		assert!(back["queries"][2].get("error").is_none(), "an absent error is not written");
	}

	#[test]
	fn an_action_the_reader_does_not_know_is_an_error() {
		let raw = json!({
			"format": FORMAT, "version": 1,
			"queries": [{ "text": "q", "statements": [{
				"certainty": "fact",
				"live": { "diff": false, "query": "LIVE SELECT * FROM t", "notifications": [{
					"action": "MOVE", "record": "none", "record_value": "None",
					"result": "none", "result_value": "None", "certainty": "fact"
				}] }
			}] }],
		});
		assert!(TypesDocument::from_json(&raw.to_string()).is_err());
	}
}
