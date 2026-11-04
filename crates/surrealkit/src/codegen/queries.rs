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
use super::document::{
	FieldStep, KindValue, LiveNotificationType, LiveRefusalType, LiveType, QueryErrorType,
	QueryParam, QueryTypes, StatementType, TypesDocument,
};
use super::v2;
use crate::typegen::Span;
use crate::typegen::document::{Kind, TypeDef};
pub use crate::typegen::document::{
	LiveAction, LiveExit, LiveNotification, LiveRefusal, QueryEntry, QueryError, QueryLive,
	QueryOrigin, QueryParameter, QuerySource, QueryStatement,
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
		let (entry, spellings) = convert_query(query);
		note(spellings);
		entries.push(entry);
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

/// Converts one query of an engine document, with the `source` of each `unknown` kind in it,
/// in the order met.
pub fn convert_query(query: &QueryTypes) -> (QueryEntry, Vec<String>) {
	let mut unrepresented: Vec<String> = Vec::new();
	let mut note = |spellings: Vec<String>| {
		for spelling in spellings {
			if !unrepresented.contains(&spelling) {
				unrepresented.push(spelling);
			}
		}
	};
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
	let entry = QueryEntry {
		key: query.name.clone().unwrap_or_else(|| query.text.clone()),
		name: query.name.clone(),
		text: query.text.clone(),
		sources: Vec::new(),
		statements,
		params,
		error: query.error.as_ref().map(|error| query_error(error, &query.text)),
	};
	(entry, unrepresented)
}

/// The engine's refusal as v2 states it. The engine's span is kept when it is a byte range
/// within `text` on character boundaries, and left out otherwise.
fn query_error(error: &QueryErrorType, text: &str) -> QueryError {
	let bound = |name: &str| {
		let at = error.span.as_ref()?.get(name)?.as_u64()?;
		usize::try_from(at).ok().filter(|at| *at <= text.len() && text.is_char_boundary(*at))
	};
	let span = match (bound("start"), bound("end")) {
		(Some(start), Some(end)) if start <= end => Some(Span {
			start,
			end,
		}),
		_ => None,
	};
	QueryError {
		code: error.code.clone(),
		message: error.message.clone(),
		span,
	}
}

fn statement_entry(statement: &StatementType) -> (QueryStatement, Vec<String>) {
	let (kind, optional, mut unrepresented) = top_kind(statement.kind_value.as_ref());
	let exact =
		statement.support.get("exact").and_then(serde_json::Value::as_bool).unwrap_or(false);
	let live = statement.live.as_ref().map(|live| {
		let (live, spellings) = live_entry(live);
		unrepresented.extend(spellings);
		live
	});
	let entry = QueryStatement {
		kind,
		optional,
		certainty: statement.certainty,
		exact,
		open: paths(&statement.open),
		live,
		live_refusal: statement.live_refusal.as_ref().map(refusal),
	};
	(entry, unrepresented)
}

fn paths(open: &[Vec<FieldStep>]) -> Vec<Vec<String>> {
	open.iter().map(|path| path.iter().map(segment).collect()).collect()
}

/// A statement's live subscription, with the `source` of each `unknown` kind in it. Both kinds
/// of a notification are value positions: a NONE in one becomes an `option`. The notifications
/// are in the order CREATE, UPDATE, DELETE, KILLED.
fn live_entry(live: &LiveType) -> (QueryLive, Vec<String>) {
	let mut unrepresented = Vec::new();
	let mut notifications: Vec<LiveNotification> =
		live.notifications.iter().map(|n| notification(n, &mut unrepresented)).collect();
	notifications.sort_by_key(|notification| notification.action);
	let entry = QueryLive {
		diff: live.diff,
		query: live.query.clone(),
		exit: live.exit,
		watch: live.watch.clone(),
		notifications,
	};
	(entry, unrepresented)
}

fn notification(
	notification: &LiveNotificationType,
	unrepresented: &mut Vec<String>,
) -> LiveNotification {
	let record = v2::nested(&notification.record_value);
	let result = v2::nested(&notification.result_value);
	unrepresented.extend(record.unrepresented);
	unrepresented.extend(result.unrepresented);
	LiveNotification {
		action: notification.action,
		record: record.kind,
		result: result.kind,
		certainty: notification.certainty,
		open: paths(&notification.open),
	}
}

fn refusal(refusal: &LiveRefusalType) -> LiveRefusal {
	LiveRefusal {
		code: refusal.code.clone(),
		message: refusal.message.clone(),
		clauses: refusal.clauses.clone(),
		windowable: refusal.windowable,
	}
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
				live: None,
				live_refusal: None,
			}],
			params: Vec::new(),
			error: None,
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

	#[test]
	fn live_notifications_are_value_kinds_in_a_fixed_order() {
		let note = |action: &str, result: Value| {
			json!({ "action": action, "record": "r", "record_value": { "Record": ["t"] },
				"result": "r", "result_value": result, "certainty": "fact", "open": [[{ "element": true }]] })
		};
		let none_or_int = json!({ "Either": ["None", "Int"] });
		let (entries, unrepresented) =
			entries(json!([{ "text": "SELECT * FROM t", "statements": [{
			"kind_value": "Int", "certainty": "fact",
			"live": { "diff": true, "query": "LIVE SELECT DIFF FROM t", "notifications": [
				note("KILLED", json!("None")),
				note("DELETE", none_or_int.clone()),
				note("CREATE", json!({ "Hologram": 1 })),
				note("UPDATE", none_or_int),
			] }
		}] }]));
		assert_eq!(unrepresented, [r#"{"Hologram":1}"#]);
		let live = entries[0].statements[0].live.as_ref().expect("live");
		assert!(live.diff && live.exit.is_none() && live.watch.is_none());
		let actions: Vec<LiveAction> = live.notifications.iter().map(|n| n.action).collect();
		assert_eq!(
			actions,
			[LiveAction::Create, LiveAction::Update, LiveAction::Delete, LiveAction::Killed]
		);
		assert_eq!(
			live.notifications[1].result,
			Kind::Option {
				item: Box::new(Kind::int())
			}
		);
		assert_eq!(live.notifications[3].result, Kind::None);
		assert_eq!(live.notifications[1].open, vec![vec!["*".to_string()]]);
		assert_eq!(
			live.notifications[1].record,
			Kind::Record {
				tables: vec!["t".into()]
			}
		);
	}

	#[test]
	fn a_query_error_keeps_a_span_only_inside_the_text() {
		let error = |span: Value| {
			let (entries, _) = entries(json!([{ "text": "SELEC é", "statements": [],
				"error": { "code": "S0001", "message": "bad", "span": span } }]));
			entries[0].error.clone().expect("error")
		};
		let valid = error(json!({ "start": 0, "end": 5, "line": 1, "column": 1 }));
		assert_eq!(
			valid.span,
			Some(Span {
				start: 0,
				end: 5
			})
		);
		// Past the end, backwards, inside a character (`é` is bytes 6 and 7), or not numbers.
		for span in [
			json!({ "start": 0, "end": 99 }),
			json!({ "start": 5, "end": 1 }),
			json!({ "start": 0, "end": 7 }),
			json!({ "start": "0", "end": 1 }),
			json!({ "start": 0 }),
			json!(null),
		] {
			assert_eq!(error(span.clone()).span, None, "{span}");
		}
		let (entries, _) = entries(json!([{ "text": "SELEC", "statements": [],
			"error": { "code": "S0001", "message": "bad" } }]));
		let error = entries[0].error.as_ref().expect("error");
		assert!(error.span.is_none());
		let json = serde_json::to_value(&entries[0]).expect("json");
		assert_eq!(
			json,
			json!({ "key": "SELEC", "text": "SELEC", "statements": [],
				"error": { "code": "S0001", "message": "bad" } })
		);
	}
}
