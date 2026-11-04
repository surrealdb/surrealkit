//! One query in, its TypeScript type out: what `surrealkit types` and the `query_types` MCP
//! tool answer with.
//!
//! The query is never executed. The engine types it ([`engine::types_document`], the seam
//! `compile` uses) against a schema: the project's, compiled on an embedded engine exactly as
//! `compile` compiles it ([`Source::Project`]), or a connected database's ([`Source::Live`]).
//! [`report`] turns the engine's [`TypesDocument`] for that query into a [`Report`]: each
//! statement's TypeScript type and SurrealQL kind, the parameters' kinds, and a snippet with the
//! types ready to paste. Rendering is [`typescript`]'s, so a type here is the type the per-query
//! module gives the same query.
//!
//! A [`Request`] may carry a [`ParamHint`] per parameter: a kind, or an example value to derive
//! one from. A hint fills in a parameter the engine could not infer a kind for. It never
//! overrides the engine.

use std::collections::BTreeMap;

use anyhow::{Result, bail};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;

use super::document::{Certainty, TypesDocument};
use super::engine;
use super::queries::{self, QueryEntry};
use super::typescript::{
	QueryRender, RenderOptions, params_text, render_query, string_literal, tuple_text,
};
use crate::compile::{self, collect_module};
use crate::compile_cmd::Plan;
use crate::typegen::document::Kind;
use crate::typegen::kind_v2::parse_top_exact;

/// The name a request without one gets.
pub const DEFAULT_NAME: &str = "Query";

/// What a call fails with when the engine has no query typing.
const CANNOT_TYPE: &str = "the engine cannot type queries (SurrealDB 4.1 or later is needed)";

/// The query to type, and what the caller knows about its parameters.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
	/// The SurrealQL text, as a client would send it.
	pub query: String,
	/// The name of the query, and the stem of the types in the snippet: an identifier
	/// (`[A-Za-z_][A-Za-z0-9_]*`). [`DEFAULT_NAME`] when absent.
	#[serde(
		default,
		skip_serializing_if = "Option::is_none",
		deserialize_with = "deserialize_name"
	)]
	pub name: Option<String>,
	/// What the caller knows about parameters, by name without the `$`.
	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub params: BTreeMap<String, ParamHint>,
}

impl Request {
	/// A request for `query` with no name and no hints.
	pub fn new(query: impl Into<String>) -> Self {
		Self {
			query: query.into(),
			name: None,
			params: BTreeMap::new(),
		}
	}

	/// Checks what [`types`] relies on: a query, a name that is an identifier, and declared kinds
	/// that are SurrealQL kinds.
	///
	/// # Errors
	///
	/// Fails with the first thing that is wrong.
	pub fn validate(&self) -> Result<()> {
		if self.query.trim().is_empty() {
			bail!("the query is empty");
		}
		if let Some(name) = &self.name {
			check_name(name).map_err(anyhow::Error::msg)?;
		}
		for (param, hint) in &self.params {
			if let ParamHint::Kind(text) = hint
				&& parse_top_exact(text.trim()).is_none()
			{
				bail!("param `{param}`: `{}` is not a SurrealQL kind", text.trim());
			}
		}
		Ok(())
	}

	/// The name of the snippet's types.
	fn type_name(&self) -> &str {
		self.name.as_deref().unwrap_or(DEFAULT_NAME)
	}
}

/// Whether `name` is `[A-Za-z_][A-Za-z0-9_]*`.
fn check_name(name: &str) -> std::result::Result<(), String> {
	let mut chars = name.chars();
	let valid = matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
		&& chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
	if valid {
		Ok(())
	} else {
		Err(format!("the name `{name}` is not an identifier ([A-Za-z_][A-Za-z0-9_]*)"))
	}
}

fn deserialize_name<'de, D: Deserializer<'de>>(
	deserializer: D,
) -> std::result::Result<Option<String>, D::Error> {
	let name = Option::<String>::deserialize(deserializer)?;
	if let Some(name) = &name {
		check_name(name).map_err(serde::de::Error::custom)?;
	}
	Ok(name)
}

/// What a caller says about one parameter: a SurrealQL kind, or an example value.
///
/// Its JSON form is `{ "kind": "<SurrealQL kind>" }` or `{ "example": <JSON value> }`, with
/// exactly one of the two keys. An example's kind is derived: an integer is `int`, any other
/// number `float`, a string `string`, a boolean `bool`, `null` `null`, an array `array<K>` when
/// every element has the kind `K` (else `array<any>`), and an object `object`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawHint", into = "RawHint")]
pub enum ParamHint {
	/// A declared kind, as SurrealQL spells it: `int`, `option<string>`, `record<person>`.
	Kind(String),
	/// An example value.
	Example(Value),
}

impl ParamHint {
	/// The hint of a JSON form's two members.
	///
	/// # Errors
	///
	/// Fails unless exactly one is present.
	pub fn from_parts(
		kind: Option<String>,
		example: Option<Value>,
	) -> std::result::Result<Self, String> {
		match (kind, example) {
			(Some(kind), None) => Ok(Self::Kind(kind)),
			(None, Some(example)) => Ok(Self::Example(example)),
			(Some(_), Some(_)) => Err("a param hint is `kind` or `example`, not both".to_string()),
			(None, None) => Err("a param hint needs `kind` or `example`".to_string()),
		}
	}

	/// The kind as SurrealQL spells it, its normalised form, and whether it admits NONE. `None`
	/// when a declared kind does not parse.
	fn resolve(&self) -> Option<(String, Kind, bool)> {
		let text = match self {
			Self::Kind(text) => text.trim().to_string(),
			Self::Example(value) => example_kind(value),
		};
		let (kind, optional) = parse_top_exact(&text)?;
		Some((text, kind, optional))
	}

	/// Which `source` a parameter typed from this hint has.
	fn source(&self) -> ParamSource {
		match self {
			Self::Kind(_) => ParamSource::Declared,
			Self::Example(_) => ParamSource::Example,
		}
	}
}

/// The SurrealQL kind of an example value.
fn example_kind(value: &Value) -> String {
	match value {
		Value::Null => "null".to_string(),
		Value::Bool(_) => "bool".to_string(),
		Value::Number(number) if number.is_i64() => "int".to_string(),
		Value::Number(_) => "float".to_string(),
		Value::String(_) => "string".to_string(),
		Value::Object(_) => "object".to_string(),
		Value::Array(items) => {
			let kinds: Vec<String> = items.iter().map(example_kind).collect();
			match kinds.split_first() {
				Some((first, rest)) if rest.iter().all(|kind| kind == first) => {
					format!("array<{first}>")
				}
				_ => "array<any>".to_string(),
			}
		}
	}
}

/// A param hint's JSON form.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawHint {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	kind: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none", deserialize_with = "present")]
	example: Option<Value>,
}

/// Reads a member that is there, `null` included, as `Some`: `Option<Value>` alone reads a
/// `null` as absent.
pub(crate) fn present<'de, D: Deserializer<'de>>(
	deserializer: D,
) -> std::result::Result<Option<Value>, D::Error> {
	Value::deserialize(deserializer).map(Some)
}

impl TryFrom<RawHint> for ParamHint {
	type Error = String;

	fn try_from(raw: RawHint) -> std::result::Result<Self, Self::Error> {
		Self::from_parts(raw.kind, raw.example)
	}
}

impl From<ParamHint> for RawHint {
	fn from(hint: ParamHint) -> Self {
		match hint {
			ParamHint::Kind(kind) => Self {
				kind: Some(kind),
				example: None,
			},
			ParamHint::Example(example) => Self {
				kind: None,
				example: Some(example),
			},
		}
	}
}

/// Reads a `--param NAME=KIND` flag: a parameter's name, without the `$`, and its declared kind.
///
/// # Errors
///
/// Fails when the flag has no `=`, or an empty name or kind.
pub fn parse_param_flag(flag: &str) -> Result<(String, ParamHint)> {
	let Some((name, kind)) = flag.split_once('=') else {
		bail!("--param expects NAME=KIND, got `{flag}`");
	};
	let (name, kind) = (name.trim().trim_start_matches('$'), kind.trim());
	if name.is_empty() || kind.is_empty() {
		bail!("--param expects NAME=KIND, got `{flag}`");
	}
	Ok((name.to_string(), ParamHint::Kind(kind.to_string())))
}

/// The schema a query is typed against.
pub enum Source<'a> {
	/// The project's schema, compiled on an embedded engine as `compile` compiles it: the
	/// plan's modules, template variables and experimental capabilities. `folder` is the project
	/// folder the modules' files are read from.
	Project {
		plan: &'a Plan,
		folder: &'a str,
	},
	/// A connected database's schema.
	Live(&'a Surreal<Any>),
}

/// Types `request` against `source` without executing it, and reports the result.
///
/// A [`Source::Project`] is compiled first, and the diagnostics of that compile are not
/// reported: the query is typed against whatever schema the engine holds.
///
/// # Errors
///
/// Fails when the request is invalid ([`Request::validate`]), when the schema cannot be
/// compiled or the target cannot be reached, when the engine cannot type queries (SurrealDB
/// before 4.1), and with the engine's message when it refuses the query.
pub async fn types(request: &Request, source: Source<'_>, opts: &RenderOptions) -> Result<Report> {
	request.validate()?;
	let inputs = [(request.name.clone(), request.query.clone())];
	let document = match source {
		Source::Live(db) => engine::types_document(db, &inputs).await?,
		Source::Project {
			plan,
			folder,
		} => {
			let modules = plan
				.modules
				.iter()
				.map(|module| collect_module(&plan.project, folder, module))
				.collect::<Result<Vec<_>>>()?;
			let built = compile::build(&modules, &plan.compile).await?;
			engine::types_document(&built.db, &inputs).await?
		}
	};
	let Some(document) = document else {
		bail!(CANNOT_TYPE);
	};
	if document.queries.is_empty() {
		bail!("the engine returned no typing for the query");
	}
	Ok(report(request, &document, opts))
}

/// What the static check of one query reads: the project, its folder and template variables, and
/// the modules the schema is made of (a target's, or `--schema`'s). Nothing connects.
#[cfg(feature = "analyze")]
pub struct CheckScope<'a> {
	pub project: &'a crate::project::ProjectConfig,
	/// The project root, which the schema files are found under.
	pub root: &'a std::path::Path,
	pub folder: &'a str,
	pub vars: &'a crate::variables::TemplateVars,
	pub target: Option<String>,
	pub schema: Vec<String>,
}

/// Fills `report.findings` with the query's static-check diagnostics (`check::query`). When the
/// check cannot run, `findings` stays absent and a note says why: the typing stands on its own.
#[cfg(feature = "analyze")]
pub async fn check(report: &mut Report, request: &Request, scope: CheckScope<'_>) {
	let check_request = crate::check::query::QueryCheckRequest {
		text: request.query.clone(),
		name: request.name.clone(),
		target: scope.target,
		schema: scope.schema,
	};
	let checked = crate::check::query::check_query(
		scope.project,
		scope.root,
		scope.folder,
		scope.vars,
		&check_request,
	)
	.await;
	let findings = checked.and_then(|found| {
		found
			.diagnostics
			.iter()
			.map(|finding| serde_json::to_value(finding).map_err(Into::into))
			.collect::<Result<Vec<Value>>>()
	});
	match findings {
		Ok(findings) => report.findings = Some(findings),
		Err(err) => report.notes.push(format!("the static check did not run: {err:#}")),
	}
}

/// What a parameter's type comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ParamSource {
	/// The engine inferred it from how the query uses the parameter (or could not, and the
	/// type is `unknown`).
	Engine,
	/// The request declared a kind.
	Declared,
	/// The request gave an example value.
	Example,
}

/// The typing of one query: the JSON both `surrealkit types --json` and the `query_types` tool
/// return. Optional members are left out, never `null`.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Report {
	/// The request's name, or [`DEFAULT_NAME`].
	pub name: String,
	/// The query text.
	pub text: String,
	/// One per statement, in source order. `BEGIN` and `COMMIT` are statements.
	pub statements: Vec<StatementReport>,
	/// The parameters the query reads, in the engine's order. Those a host-language template
	/// fills (`__host…`) are not listed.
	pub params: Vec<ParamReport>,
	/// TypeScript to paste: the SDK imports it needs, `export type <Name> = [...]` (one
	/// element per statement) and `export type <Name>Params = {...}`.
	pub snippet: String,
	/// What the caller should know: a hint the query does not use, a declared kind the engine
	/// disagrees with.
	#[serde(skip_serializing_if = "Vec::is_empty")]
	pub notes: Vec<String>,
	/// The query's static-check diagnostics, as `check --json` reports them; absent when the
	/// check did not run.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub findings: Option<Vec<Value>>,
}

/// One statement's slot in the response.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatementReport {
	pub index: usize,
	/// The slot's type, as the per-query module renders it: `unknown` when the engine did not
	/// type the statement, `| undefined` when it may be NONE.
	pub typescript: String,
	/// The engine's SurrealQL spelling of the kind; absent when the statement is untyped.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub kind: Option<String>,
	/// `fact` is guaranteed by an engine invariant, `hint` is declared but unproven.
	pub certainty: Certainty,
	/// Whether the engine reports the kind as the exact shape of the result.
	pub exact: bool,
	/// The engine's account of how far its typing reaches, as the engine states it.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub support: Option<Value>,
	/// Positions where an object admits keys the kind does not list, as paths of v2 segments
	/// (`*` is an array or set element).
	pub open: Vec<Vec<String>>,
	/// The statement's span in the query, as the engine states it.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub span: Option<Value>,
}

/// One parameter of the query.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ParamReport {
	/// Without the `$`.
	pub name: String,
	/// The value's type, as it appears in `<Name>Params`.
	pub typescript: String,
	/// The SurrealQL kind; absent when nothing gave one.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub kind: Option<String>,
	/// Whether the caller must bind it.
	pub required: bool,
	pub source: ParamSource,
}

/// Builds the [`Report`] of `request` from the engine's `document` for it, with the rendering
/// choices of `opts`. Pure: nothing is executed or read. `request` is expected to have passed
/// [`Request::validate`].
pub fn report(request: &Request, document: &TypesDocument, opts: &RenderOptions) -> Report {
	let name = request.type_name().to_string();
	let converted = queries::from_types_document(document);
	let raw = document.queries.first();
	let mut entry = converted.queries.into_iter().next().unwrap_or_else(|| QueryEntry {
		key: request.query.clone(),
		name: request.name.clone(),
		text: request.query.clone(),
		sources: Vec::new(),
		statements: Vec::new(),
		params: Vec::new(),
	});

	let mut notes = Vec::new();
	if !converted.unrepresented.is_empty() {
		notes.push(format!(
			"the engine reported kinds this version of SurrealKit cannot represent, typed as \
			 `unknown`: {}",
			converted.unrepresented.join(", ")
		));
	}

	// Hints fill the parameters the engine left without a kind. The engine's own kind wins.
	let mut kinds: BTreeMap<String, (Option<String>, ParamSource)> = BTreeMap::new();
	let engine_params = raw.map_or(&[][..], |query| query.params.as_slice());
	for (index, param) in entry.params.iter().enumerate() {
		let text = engine_params.get(index).and_then(|param| param.kind.clone());
		kinds.insert(param.name.clone(), (text, ParamSource::Engine));
	}
	for (param, hint) in &request.params {
		let Some(slot) = entry.params.iter_mut().find(|slot| &slot.name == param) else {
			notes.push(format!("`${param}` has a hint, but the query does not read it"));
			continue;
		};
		let Some((text, kind, optional)) = hint.resolve() else {
			notes.push(format!("the hint for `${param}` is not a SurrealQL kind, and is ignored"));
			continue;
		};
		match &slot.kind {
			Some(engine) => {
				if (engine, slot.optional) != (&kind, optional) {
					let engine_text = kinds
						.get(param)
						.and_then(|(text, _)| text.as_deref())
						.unwrap_or("the engine's kind");
					notes.push(format!(
						"`${param}` is declared as `{text}`, but the engine infers \
						 `{engine_text}`; the engine's kind is used"
					));
				}
			}
			None => {
				slot.kind = Some(kind);
				slot.optional = optional;
				kinds.insert(param.clone(), (Some(text), hint.source()));
			}
		}
	}

	let render = render_query(&entry, &converted.types, opts);
	let statements = statements(&entry, raw, &render);
	let params = params(&render, &kinds);
	let snippet = snippet(&name, &render, opts);
	Report {
		name,
		text: entry.text,
		statements,
		params,
		snippet,
		notes,
		findings: None,
	}
}

fn statements(
	entry: &QueryEntry,
	raw: Option<&super::document::QueryTypes>,
	render: &QueryRender,
) -> Vec<StatementReport> {
	let raw_statements = raw.map_or(&[][..], |query| query.statements.as_slice());
	entry
		.statements
		.iter()
		.enumerate()
		.map(|(index, statement)| {
			let engine = raw_statements.get(index);
			StatementReport {
				index,
				typescript: render.slots.get(index).cloned().unwrap_or_default(),
				kind: engine.and_then(|statement| statement.kind.clone()),
				certainty: statement.certainty,
				exact: statement.exact,
				support: engine
					.map(|statement| statement.support.clone())
					.filter(|value| !value.is_null()),
				open: statement.open.clone(),
				span: engine
					.map(|statement| statement.span.clone())
					.filter(|value| !value.is_null()),
			}
		})
		.collect()
}

fn params(
	render: &QueryRender,
	kinds: &BTreeMap<String, (Option<String>, ParamSource)>,
) -> Vec<ParamReport> {
	render
		.params
		.iter()
		.map(|param| {
			let (kind, source) =
				kinds.get(&param.name).cloned().unwrap_or((None, ParamSource::Engine));
			ParamReport {
				name: param.name.clone(),
				typescript: param.typescript.clone(),
				kind,
				required: param.required,
				source,
			}
		})
		.collect()
}

/// The snippet: the SDK names it uses, and the two types.
fn snippet(name: &str, render: &QueryRender, opts: &RenderOptions) -> String {
	let mut out = String::new();
	if !render.imports.is_empty() {
		out.push_str(&format!(
			"import type {{ {} }} from {};\n\n",
			render.imports.join(", "),
			string_literal(&opts.sdk_package)
		));
	}
	out.push_str(&format!("export type {name} = {};\n", tuple_text(&render.slots)));
	out.push_str(&format!("export type {name}Params = {};\n", params_text(&render.params)));
	out
}

#[cfg(test)]
mod tests {
	use serde_json::json;

	use super::*;

	fn request(raw: Value) -> std::result::Result<Request, serde_json::Error> {
		serde_json::from_value(raw)
	}

	#[test]
	fn a_hint_is_a_kind_or_an_example() {
		let request = request(json!({
			"query": "RETURN $a + $b",
			"params": { "a": { "kind": "int" }, "b": { "example": 1.5 }, "c": { "example": null } },
		}))
		.expect("reads");
		assert_eq!(request.params["a"], ParamHint::Kind("int".into()));
		assert_eq!(request.params["b"], ParamHint::Example(json!(1.5)));
		assert_eq!(request.params["c"], ParamHint::Example(Value::Null));
		let back = serde_json::to_value(&request).expect("writes");
		assert_eq!(back["params"]["c"], json!({ "example": null }));
		assert_eq!(back["params"]["a"], json!({ "kind": "int" }));
	}

	#[test]
	fn a_hint_with_both_keys_neither_or_another_is_refused() {
		for raw in [
			json!({ "kind": "int", "example": 1 }),
			json!({}),
			json!({ "kind": "int", "type": "int" }),
			json!({ "example": 1, "x": 1 }),
			json!({ "kind": 1 }),
		] {
			let result = request(json!({ "query": "RETURN $a", "params": { "a": raw } }));
			assert!(result.is_err(), "{raw}");
		}
		let err =
			request(json!({ "query": "q", "params": { "a": { "kind": "int", "example": 1 } } }))
				.expect_err("refused");
		assert!(err.to_string().contains("not both"), "{err}");
	}

	#[test]
	fn a_name_must_be_an_identifier() {
		assert!(request(json!({ "query": "q", "name": "ListPeople_2" })).is_ok());
		assert!(request(json!({ "query": "q", "name": "_x" })).is_ok());
		for name in ["", "2fast", "has space", "a-b", "$x", "naïve"] {
			assert!(request(json!({ "query": "q", "name": name })).is_err(), "{name:?}");
			let mut direct = Request::new("q");
			direct.name = Some(name.to_string());
			assert!(direct.validate().is_err(), "{name:?}");
		}
	}

	#[test]
	fn a_request_refuses_unknown_members_an_empty_query_and_a_bad_kind() {
		assert!(request(json!({ "query": "q", "nmae": "x" })).is_err());
		assert!(request(json!({ "name": "x" })).is_err());
		assert!(Request::new("  \n").validate().is_err());
		let mut bad = Request::new("RETURN $a");
		bad.params.insert("a".into(), ParamHint::Kind("array<".into()));
		let err = bad.validate().expect_err("refused");
		assert!(err.to_string().contains("param `a`"), "{err}");
	}

	#[test]
	fn an_example_gives_its_kind() {
		let kind = |value: Value| ParamHint::Example(value).resolve().expect("kind").0;
		assert_eq!(kind(json!(3)), "int");
		assert_eq!(kind(json!(-3)), "int");
		assert_eq!(kind(json!(3.5)), "float");
		assert_eq!(kind(json!(1.0)), "float");
		assert_eq!(kind(json!("a")), "string");
		assert_eq!(kind(json!(true)), "bool");
		assert_eq!(kind(Value::Null), "null");
		assert_eq!(kind(json!({ "a": 1 })), "object");
		assert_eq!(kind(json!([])), "array<any>");
		assert_eq!(kind(json!([1, 2])), "array<int>");
		assert_eq!(kind(json!([1, 2.5])), "array<any>");
		assert_eq!(kind(json!([[1], [2]])), "array<array<int>>");
		assert_eq!(kind(json!(["a", 1])), "array<any>");
	}

	#[test]
	fn a_param_flag_is_a_name_and_a_kind() {
		let (name, hint) = parse_param_flag("min=int").expect("parses");
		assert_eq!((name.as_str(), hint), ("min", ParamHint::Kind("int".into())));
		let (name, hint) = parse_param_flag("$who = record<person>").expect("parses");
		assert_eq!((name.as_str(), hint), ("who", ParamHint::Kind("record<person>".into())));
		// The first `=` splits: a kind may hold another.
		assert!(parse_param_flag("x=a=b").is_ok());
		for bad in ["min", "=int", "min=", " = "] {
			assert!(parse_param_flag(bad).is_err(), "{bad:?}");
		}
	}
}
