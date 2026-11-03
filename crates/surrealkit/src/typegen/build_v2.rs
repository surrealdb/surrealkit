//! Assembly of schema document v2 from what an adapter read.
//!
//! The two adapters — [`super::structure`] over `INFO … STRUCTURE` and [`super::info_v2`] over
//! plain `INFO` — differ only in how they read the engine. Both produce a [`RawSchema`]: engine
//! text, split into clauses but not yet interpreted. This module interprets it once, for both:
//! it parses kinds, derives `readsInput` and recognised assertions, lifts `id` into `idKey`,
//! raises every diagnostic of contract section 14, orders every collection (section 12) and
//! computes the semantic digest (section 13). Whatever the adapters agree on, the documents
//! agree on.

use std::collections::BTreeSet;

use anyhow::Result;

use super::canonical::{canonical_of, semantic_digest};
use super::diagnostics::{Diagnostic, Severity, codes};
use super::document::{
	Access, Analyzer, Api, AssertClause, Bucket, Changefeed, Config, DefaultClause, DefaultMode,
	Event, ExprClause, Field, FieldPermissions, Fulltext, Function, FunctionArg, Index, IndexType,
	Kind, ModuleDef, OnDelete, Param, Permission, Provenance, Reference, SCHEMA_DOCUMENT_VERSION,
	SchemaDocument, Sequence, Table, TablePermissions, TableType, User, ValueClause, Vector, View,
};
use super::kind_v2::{ParsedKind, parse_top};
use super::refine::{has_fallback, reads_input, recognize};
use super::scan::{field_path, tokenize};

/// A table as an adapter read it.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct RawTable {
	pub name: String,
	pub table_type: TableType,
	pub schemafull: bool,
	pub drop: bool,
	/// The view's `SELECT`, without `AS `.
	pub view: Option<String>,
	pub changefeed: Option<Changefeed>,
	pub permissions: TablePermissions,
	pub comment: Option<String>,
	pub define: Option<String>,
	pub fields: Vec<RawField>,
	pub indexes: Vec<RawIndex>,
	pub events: Vec<Event>,
}

/// A field as an adapter read it.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct RawField {
	/// The engine's field name (`address.city`, `` `odd.name` ``, `tags.*`).
	pub name: String,
	/// The `TYPE` text.
	pub kind: Option<String>,
	pub flexible: bool,
	pub readonly: bool,
	pub inline: bool,
	/// The `DEFAULT` expression and whether it is `DEFAULT ALWAYS`.
	pub default: Option<(String, bool)>,
	pub value: Option<String>,
	pub computed: Option<String>,
	pub assert: Option<String>,
	/// The engine's `ON DELETE` text: `IGNORE`, `CASCADE`, `THEN (…)`, … .
	pub reference: Option<String>,
	pub permissions: FieldPermissions,
	pub comment: Option<String>,
	pub define: Option<String>,
}

/// An index as an adapter read it.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct RawIndex {
	pub name: String,
	/// Column idioms as the engine renders them.
	pub columns: Vec<String>,
	/// The engine's index clause (`""` for a standard index).
	pub clause: String,
	pub comment: Option<String>,
}

/// A function as an adapter read it.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct RawFunction {
	/// Without its `fn::` prefix.
	pub name: String,
	/// `(name without $, kind text)`, in declared order.
	pub args: Vec<(String, String)>,
	pub returns: Option<String>,
	pub block: String,
	pub permissions: Permission,
	pub comment: Option<String>,
}

/// Everything an adapter read.
#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct RawSchema {
	pub tables: Vec<RawTable>,
	pub functions: Vec<RawFunction>,
	pub params: Vec<Param>,
	pub accesses: Vec<Access>,
	pub analyzers: Vec<Analyzer>,
	pub sequences: Vec<Sequence>,
	pub buckets: Vec<Bucket>,
	pub apis: Vec<Api>,
	pub modules: Vec<ModuleDef>,
	pub configs: Vec<Config>,
	pub users: Vec<User>,
	/// What the adapter itself could not read.
	pub diagnostics: Vec<Diagnostic>,
}

/// Whether `table` is one of SurrealKit's own metadata tables (`__entity`, `__rollout`,
/// `__seed`, defined by `setup.surql`): every `__`-prefixed table, the rule the TypeScript
/// interface emitter has always applied. They exist in every synced database and never in a
/// compiled one, so leaving them out is what makes a compile and an introspection of a
/// database synced from the same files agree.
pub(crate) fn is_surrealkit_metadata(table: &str) -> bool {
	table.starts_with("__")
}

/// Interpret `raw` into a finished document: sorted, diagnosed and digested.
pub(super) fn build(raw: RawSchema, provenance: Provenance) -> Result<SchemaDocument> {
	let mut diagnostics = raw.diagnostics;
	let mut tables: Vec<Table> = raw
		.tables
		.into_iter()
		.filter(|t| !is_surrealkit_metadata(&t.name))
		.map(|t| build_table(t, &mut diagnostics))
		.collect();
	tables.sort_by(|a, b| a.name.cmp(&b.name));

	let mut functions: Vec<Function> =
		raw.functions.into_iter().map(|f| build_function(f, &mut diagnostics)).collect();
	functions.sort_by(|a, b| a.name.cmp(&b.name));

	let mut apis = raw.apis;
	for api in &mut apis {
		order_api(api, &mut diagnostics);
	}
	apis.sort_by(|a, b| a.path.cmp(&b.path));

	let mut configs = raw.configs;
	for config in &mut configs {
		if let Config::Api {
			middleware,
			..
		} = config
		{
			middleware.sort_by(|a, b| a.name.cmp(&b.name));
		}
	}
	configs.sort_by_cached_key(|c| match c {
		Config::Graphql {
			..
		} => "graphql",
		Config::Api {
			..
		} => "api",
	});

	let mut users = raw.users;
	for user in &mut users {
		user.roles.sort();
		user.roles.dedup();
	}

	let mut doc = SchemaDocument {
		version: SCHEMA_DOCUMENT_VERSION,
		semantic_digest: String::new(),
		provenance,
		tables,
		functions,
		params: sorted_by_name(raw.params, |p| &p.name),
		accesses: sorted_by_name(raw.accesses, |a| &a.name),
		analyzers: sorted_by_name(raw.analyzers, |a| &a.name),
		sequences: sorted_by_name(raw.sequences, |s| &s.name),
		buckets: sorted_by_name(raw.buckets, |b| &b.name),
		apis,
		modules: sorted_by_name(raw.modules, |m| &m.name),
		configs,
		users: sorted_by_name(users, |u| &u.name),
		diagnostics: Vec::new(),
	};
	doc.diagnostics = sort_diagnostics(diagnostics);
	doc.semantic_digest = semantic_digest(&serde_json::to_value(&doc)?)?;
	Ok(doc)
}

fn sorted_by_name<T>(mut items: Vec<T>, name: impl Fn(&T) -> &String) -> Vec<T> {
	items.sort_by(|a, b| name(a).cmp(name(b)));
	items
}

/// Diagnostics in canonical-JSON byte order, without duplicates.
pub(super) fn sort_diagnostics(diagnostics: Vec<Diagnostic>) -> Vec<Diagnostic> {
	let mut keyed: Vec<(String, Diagnostic)> =
		diagnostics.into_iter().map(|d| (canonical_of(&d).unwrap_or_default(), d)).collect();
	keyed.sort_by(|a, b| a.0.cmp(&b.0));
	keyed.dedup_by(|a, b| a.0 == b.0);
	keyed.into_iter().map(|(_, d)| d).collect()
}

/// A [`Diagnostic`] for every problem in a parsed kind.
fn kind_problems(
	parsed: &ParsedKind,
	text: &str,
	at: impl Fn(Diagnostic) -> Diagnostic,
	diagnostics: &mut Vec<Diagnostic>,
) {
	for problem in &parsed.problems {
		diagnostics.push(at(Diagnostic::new(
			Severity::Error,
			codes::KIND_UNRECOGNIZED,
			text,
			problem.reason.clone(),
		)
		.with_span(problem.span.clone())));
	}
}

fn build_table(raw: RawTable, diagnostics: &mut Vec<Diagnostic>) -> Table {
	let name = raw.name;
	if raw.view.is_some() {
		diagnostics.push(
			Diagnostic::new(
				Severity::Info,
				codes::TABLE_VIEW_SHAPE,
				raw.view.clone().unwrap_or_default(),
				"A view's row shape comes from its SELECT and is not derived; it has no write \
				 contracts.",
			)
			.on_table(&name),
		);
	}
	if let TableType::Relation {
		lightweight: true,
		..
	} = &raw.table_type
	{
		diagnostics.push(
			Diagnostic::new(
				Severity::Info,
				codes::TABLE_LIGHTWEIGHT_RELATION,
				name.clone(),
				"A lightweight relation stores no edge content: no content schemas, `relate` \
				 only.",
			)
			.on_table(&name),
		);
	}

	let mut id_key = None;
	let mut fields = Vec::new();
	for raw_field in raw.fields {
		let path = field_path(&raw_field.name);
		if path == ["id"] {
			id_key = build_id_key(&name, raw_field, diagnostics);
			continue;
		}
		fields.push(build_field(&name, path, raw_field, diagnostics));
	}
	fields.sort_by(|a, b| a.path.cmp(&b.path));
	implicit_parents(&name, &fields, diagnostics);
	if raw.schemafull {
		objects_without_fields(&name, &fields, diagnostics);
	}
	run_together_names(&name, &fields, diagnostics);

	let mut indexes: Vec<Index> = raw.indexes.into_iter().map(build_index).collect();
	indexes.sort_by(|a, b| a.name.cmp(&b.name));
	let mut events = raw.events;
	events.sort_by(|a, b| a.name.cmp(&b.name));

	Table {
		name,
		table_type: raw.table_type,
		schemafull: raw.schemafull,
		drop: raw.drop,
		view: raw.view.map(|select| View {
			select,
		}),
		changefeed: raw.changefeed,
		permissions: raw.permissions,
		id_key,
		comment: raw.comment,
		fields,
		indexes,
		events,
		define: raw.define,
	}
}

/// The key kind declared by `DEFINE FIELD id`. Clauses other than `TYPE` are ignored with a
/// warning. Without `TYPE` the key is `any`, with the info `field-kind-undeclared`.
fn build_id_key(table: &str, raw: RawField, diagnostics: &mut Vec<Diagnostic>) -> Option<Kind> {
	let extra = raw.flexible
		|| raw.readonly
		|| raw.inline
		|| raw.default.is_some()
		|| raw.value.is_some()
		|| raw.computed.is_some()
		|| raw.assert.is_some()
		|| raw.reference.is_some()
		|| raw.comment.is_some()
		|| raw.permissions != FieldPermissions::full();
	let source = raw.define.clone().unwrap_or_else(|| raw.name.clone());
	let at = |d: Diagnostic| d.on_table(table).on_field(vec!["id".to_string()]);
	let Some(text) = raw.kind else {
		diagnostics.push(at(Diagnostic::new(
			Severity::Info,
			codes::FIELD_KIND_UNDECLARED,
			source.clone(),
			"`DEFINE FIELD id` has no TYPE clause; the record key can be any key kind.",
		)));
		if extra {
			diagnostics.push(at(Diagnostic::new(
				Severity::Warning,
				codes::FIELD_ID_CLAUSES_IGNORED,
				source,
				"`DEFINE FIELD id` carries clauses besides TYPE; only the key kind is used.",
			)));
		}
		return Some(Kind::Any);
	};
	let parsed = parse_top(&text);
	kind_problems(&parsed, &text, at, diagnostics);
	if extra || parsed.optional {
		diagnostics.push(at(Diagnostic::new(
			Severity::Warning,
			codes::FIELD_ID_CLAUSES_IGNORED,
			source,
			"`DEFINE FIELD id` carries clauses besides TYPE; only the key kind is used.",
		)));
	}
	Some(parsed.kind)
}

fn build_field(
	table: &str,
	path: Vec<String>,
	raw: RawField,
	diagnostics: &mut Vec<Diagnostic>,
) -> Field {
	let at = |d: Diagnostic| d.on_table(table).on_field(path.clone());
	let (kind, optional) = match &raw.kind {
		Some(text) => {
			let parsed = parse_top(text);
			kind_problems(&parsed, text, at, diagnostics);
			(Some(parsed.kind), parsed.optional)
		}
		None => {
			match &raw.computed {
				Some(expr) => diagnostics.push(at(Diagnostic::new(
					Severity::Warning,
					codes::FIELD_COMPUTED_KIND_UNDECLARED,
					expr.as_str(),
					"COMPUTED field has no TYPE clause; its value kind is unknown and is typed as \
					 `any`.",
				))),
				None => diagnostics.push(at(Diagnostic::new(
					Severity::Info,
					codes::FIELD_KIND_UNDECLARED,
					raw.define.clone().unwrap_or_else(|| raw.name.clone()),
					"Field has no TYPE clause; it holds any value.",
				))),
			}
			(None, false)
		}
	};

	let value = raw.value.map(|expr| {
		let reads = reads_input(&expr);
		if reads && has_fallback(&expr) {
			diagnostics.push(at(Diagnostic::new(
				Severity::Warning,
				codes::FIELD_VALUE_INPUT_CONDITIONAL,
				expr.as_str(),
				"VALUE reads its input but supplies its own fallback; the conservative contract \
				 (required) applies unless the project overrides it.",
			)));
		}
		ValueClause {
			expr,
			reads_input: reads,
		}
	});

	let assert = raw.assert.map(|expr| {
		let recognized = recognize(&expr);
		if recognized.is_none() {
			diagnostics.push(at(Diagnostic::new(
				Severity::Info,
				codes::ASSERT_UNRECOGNIZED,
				expr.as_str(),
				"ASSERT is outside the recognised subset (membership, numeric range, string/array \
				 length, email); kept as an expression only.",
			)));
		}
		AssertClause {
			expr,
			recognized,
		}
	});

	Field {
		kind,
		optional,
		flexible: raw.flexible,
		readonly: raw.readonly,
		inline: raw.inline,
		default: raw.default.map(|(expr, always)| DefaultClause {
			mode: if always {
				DefaultMode::Always
			} else {
				DefaultMode::Create
			},
			expr,
		}),
		value,
		computed: raw.computed.map(|expr| ExprClause {
			expr,
		}),
		assert,
		reference: raw.reference.map(|text| reference(&text, at, diagnostics)),
		permissions: raw.permissions,
		comment: raw.comment,
		define: raw.define,
		path,
	}
}

/// `REFERENCE` from the engine's `ON DELETE` text. No text means the default, `IGNORE`.
///
/// Text that is none of the known actions is the error `field-reference-unrecognized`, not a
/// silent `IGNORE`; the document still needs an action, and records `ignore` beside the error.
fn reference(
	text: &str,
	at: impl Fn(Diagnostic) -> Diagnostic,
	diagnostics: &mut Vec<Diagnostic>,
) -> Reference {
	let text = text.trim();
	let text = text.strip_prefix("ON DELETE").map_or(text, str::trim);
	// The action keyword: leading letters, so `THEN{ … }` and `THEN (…)` both read as THEN.
	let word_len = text.bytes().take_while(u8::is_ascii_alphabetic).count();
	let (word, rest) = text.split_at(word_len);
	let on_delete = match word.to_ascii_uppercase().as_str() {
		"" if text.is_empty() => OnDelete::Ignore,
		"IGNORE" if rest.trim().is_empty() => OnDelete::Ignore,
		"REJECT" if rest.trim().is_empty() => OnDelete::Reject,
		"CASCADE" if rest.trim().is_empty() => OnDelete::Cascade,
		"UNSET" if rest.trim().is_empty() => OnDelete::Unset,
		"THEN" if !rest.trim().is_empty() => OnDelete::Then,
		_ => {
			diagnostics.push(at(Diagnostic::new(
				Severity::Error,
				codes::FIELD_REFERENCE_UNRECOGNIZED,
				text,
				"REFERENCE ON DELETE is none of IGNORE, REJECT, CASCADE, UNSET or THEN <expr>; \
				 the document records `ignore` but does not describe the reference.",
			)));
			OnDelete::Ignore
		}
	};
	Reference {
		on_delete,
		expr: (on_delete == OnDelete::Then).then(|| rest.trim().to_string()),
	}
}

/// An info diagnostic for every field whose parent path is not declared.
fn implicit_parents(table: &str, fields: &[Field], diagnostics: &mut Vec<Diagnostic>) {
	let declared: BTreeSet<&[String]> = fields.iter().map(|f| f.path.as_slice()).collect();
	for field in fields {
		if field.path.len() < 2 {
			continue;
		}
		let parent = &field.path[..field.path.len() - 1];
		if !declared.contains(parent) {
			diagnostics.push(
				Diagnostic::new(
					Severity::Info,
					codes::FIELD_PARENT_IMPLICIT,
					parent.join("."),
					"The parent path is not declared; it is an implicit object, required when any \
					 child is required.",
				)
				.on_table(table)
				.on_field(field.path.clone()),
			);
		}
	}
}

/// `kind` without the `option<…>` and `| null` around it, when that leaves a single kind.
fn without_nullable(kind: &Kind) -> &Kind {
	match kind {
		Kind::Option {
			item,
		} => without_nullable(item),
		Kind::Union {
			variants,
		} => {
			let mut rest = variants.iter().filter(|v| !matches!(v, Kind::Null | Kind::None));
			match (rest.next(), rest.next()) {
				(Some(only), None) => without_nullable(only),
				_ => kind,
			}
		}
		_ => kind,
	}
}

/// The field holds an object (possibly NONE or NULL instead).
fn holds_object(field: &Field) -> bool {
	field.kind.as_ref().is_some_and(|k| matches!(without_nullable(k), Kind::Object))
}

/// The field holds an array or set of objects (possibly NONE or NULL instead).
fn holds_object_elements(field: &Field) -> bool {
	field.kind.as_ref().is_some_and(|k| match without_nullable(k) {
		Kind::Array {
			item,
			..
		}
		| Kind::Set {
			item,
			..
		} => matches!(without_nullable(item), Kind::Object),
		_ => false,
	})
}

/// An error for every object position of a SCHEMAFULL table that is not FLEXIBLE and has no
/// declared fields: SurrealDB 3.2 and 3.3 refuse every undeclared key of such an object, so
/// it can hold only `{}`. An array or set of objects whose element is declared (`a.*`) is
/// judged by that element field.
fn objects_without_fields(table: &str, fields: &[Field], diagnostics: &mut Vec<Diagnostic>) {
	let has_fields_under = |prefix: &[String]| {
		fields.iter().any(|f| f.path.len() > prefix.len() && f.path.starts_with(prefix))
	};
	let flexible = |path: &[String]| fields.iter().any(|f| f.path == path && f.flexible);
	// The engine synthesises `a.*` for `array<object>`; a FLEXIBLE array opens its elements.
	let open = |field: &Field| {
		field.flexible
			|| field.path.last().is_some_and(|last| last == "*")
				&& flexible(&field.path[..field.path.len() - 1])
	};
	for field in fields.iter().filter(|f| !open(f)) {
		let position = if holds_object(field) {
			field.path.clone()
		} else if holds_object_elements(field) {
			let element = [field.path.clone(), vec!["*".to_string()]].concat();
			if fields.iter().any(|f| f.path == element) {
				continue;
			}
			element
		} else {
			continue;
		};
		if has_fields_under(&position) {
			continue;
		}
		let shown = position.join(".");
		diagnostics.push(
			Diagnostic::new(
				Severity::Error,
				codes::OBJECT_WITHOUT_FIELDS,
				shown.clone(),
				format!(
					"On a SCHEMAFULL table an object that is not FLEXIBLE holds only {{}}: \
					 SurrealDB refuses every undeclared key of it (`Found field '{shown}.<key>', \
					 but no such field exists`). Declare its fields, make it FLEXIBLE, or \
					 acknowledge this code with a reason."
				),
			)
			.on_table(table)
			.on_field(field.path.clone()),
		);
	}
}

/// A warning for a top-level field whose name is the name of an object field beside it run
/// straight into a lowercase word (`envelopealgorithm` next to `envelope`): likely a child
/// path written without its dot. Separated (`k_object_literal`) and camelCase names are left
/// alone; the inference is heuristic, so never an error.
fn run_together_names(table: &str, fields: &[Field], diagnostics: &mut Vec<Diagnostic>) {
	let objects: Vec<&str> = fields
		.iter()
		.filter(|f| f.path.len() == 1 && holds_object(f))
		.map(|f| f.path[0].as_str())
		.collect();
	for field in fields.iter().filter(|f| f.path.len() == 1) {
		let name = field.path[0].as_str();
		for object in &objects {
			let Some(child) = name.strip_prefix(object) else {
				continue;
			};
			if child.starts_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit()) {
				diagnostics.push(
					Diagnostic::new(
						Severity::Warning,
						codes::FIELD_NAME_RUN_TOGETHER,
						name,
						format!(
							"`{name}` begins with the object field `{object}`; if it is meant as \
							 the child `{object}.{child}`, DEFINE FIELD needs the dot."
						),
					)
					.on_table(table)
					.on_field(field.path.clone()),
				);
			}
		}
	}
}

/// Interpret an index clause: its type and the structured parts of fulltext, vector and
/// count indexes. Shared by both adapters, so they agree by construction.
fn build_index(raw: RawIndex) -> Index {
	let clause = raw.clause.trim().to_string();
	let tokens = tokenize(&clause);
	let word = |i: usize| tokens.get(i).map(|t| t.text(&clause).to_ascii_uppercase());
	let first = word(0).unwrap_or_default();
	let index_type = match first.as_str() {
		"UNIQUE" => IndexType::Unique,
		"FULLTEXT" | "SEARCH" => IndexType::Fulltext,
		"HNSW" => IndexType::Hnsw,
		"DISKANN" => IndexType::Diskann,
		"COUNT" => IndexType::Count,
		_ => IndexType::Standard,
	};
	// The word after keyword `kw`, anywhere in the clause.
	let after = |kw: &str| {
		tokens
			.iter()
			.position(|t| t.is_word(&clause, kw))
			.and_then(|i| tokens.get(i + 1))
			.map(|t| t.text(&clause).to_string())
	};
	let fulltext = (index_type == IndexType::Fulltext).then(|| Fulltext {
		analyzer: after("ANALYZER").unwrap_or_default(),
		highlights: tokens.iter().any(|t| t.is_word(&clause, "HIGHLIGHTS")),
	});
	let vector = matches!(index_type, IndexType::Hnsw | IndexType::Diskann).then(|| Vector {
		dimension: after("DIMENSION").and_then(|d| d.parse().ok()).unwrap_or(0),
		distance: after("DIST").unwrap_or_default(),
		element_type: after("TYPE").unwrap_or_else(|| "F32".to_string()),
	});
	let where_clause = (index_type == IndexType::Count)
		.then(|| tokens.iter().find(|t| t.is_word(&clause, "WHERE")))
		.flatten()
		.map(|t| clause[t.end..].trim().to_string())
		.filter(|w| !w.is_empty());
	Index {
		name: raw.name,
		columns: raw.columns.iter().map(|c| field_path(c)).collect(),
		index_type,
		clause,
		fulltext,
		vector,
		where_clause,
		comment: raw.comment,
	}
}

fn build_function(raw: RawFunction, diagnostics: &mut Vec<Diagnostic>) -> Function {
	let name = if raw.name.starts_with("fn::") {
		raw.name
	} else {
		format!("fn::{}", raw.name)
	};
	let at = |d: Diagnostic| d.on_function(&name);
	let mut args: Vec<FunctionArg> = raw
		.args
		.iter()
		.map(|(arg, text)| {
			let parsed = parse_top(text);
			kind_problems(&parsed, text, at, diagnostics);
			FunctionArg {
				name: arg.trim_start_matches('$').to_string(),
				kind: parsed.kind,
				optional: parsed.optional,
				omittable: false,
			}
		})
		.collect();
	// Only the maximal trailing run of optional arguments may be left out.
	for arg in args.iter_mut().rev() {
		if !arg.optional {
			break;
		}
		arg.omittable = true;
	}
	if let Some(required) = args.iter().rposition(|a| !a.optional) {
		for arg in &args[..required] {
			if arg.optional {
				diagnostics.push(at(Diagnostic::new(
					Severity::Warning,
					codes::FUNCTION_OPTIONAL_ARG_NOT_TRAILING,
					arg.name.as_str(),
					format!(
						"Optional argument `${}` precedes the required argument `${}`; callers \
						 cannot omit it and must pass NONE explicitly.",
						arg.name, args[required].name
					),
				)));
			}
		}
	}
	let returns = raw.returns.map(|text| {
		let parsed = parse_top(&text);
		kind_problems(&parsed, &text, at, diagnostics);
		if parsed.optional {
			match parsed.kind {
				Kind::None => Kind::None,
				kind => Kind::Option {
					item: Box::new(kind),
				},
			}
		} else {
			parsed.kind
		}
	});
	if returns.is_none() {
		diagnostics.push(at(Diagnostic::new(
			Severity::Info,
			codes::FUNCTION_RETURN_UNDECLARED,
			raw.block.as_str(),
			"Function declares no return kind; its result is typed as unknown.",
		)));
	}
	Function {
		args,
		returns,
		block: raw.block,
		permissions: raw.permissions,
		comment: raw.comment,
		name,
	}
}

/// Sort an API's methods, middleware and actions, and note lost middleware order.
fn order_api(api: &mut Api, diagnostics: &mut Vec<Diagnostic>) {
	let mut multiple = api.middleware.len() > 1;
	api.middleware.sort_by(|a, b| a.name.cmp(&b.name));
	for action in &mut api.actions {
		multiple |= action.middleware.len() > 1;
		action.methods.sort();
		action.methods.dedup();
		action.middleware.sort_by(|a, b| a.name.cmp(&b.name));
	}
	api.actions.sort_by_cached_key(|a| canonical_of(a).unwrap_or_default());
	if multiple {
		diagnostics.push(Diagnostic::new(
			Severity::Info,
			codes::API_MIDDLEWARE_ORDER,
			api.path.clone(),
			"More than one middleware is declared; their execution order is not preserved \
			 through INFO STRUCTURE.",
		));
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn raw(name: &str, kind: Option<&str>) -> RawField {
		RawField {
			name: name.into(),
			kind: kind.map(str::to_string),
			flexible: false,
			readonly: false,
			inline: false,
			default: None,
			value: None,
			computed: None,
			assert: None,
			reference: None,
			permissions: FieldPermissions::full(),
			comment: None,
			define: Some(format!("DEFINE FIELD {name} ON t")),
		}
	}

	fn on_delete(text: &str) -> (Reference, Vec<Diagnostic>) {
		let mut diagnostics = Vec::new();
		let r = reference(text, |d| d.on_table("t"), &mut diagnostics);
		(r, diagnostics)
	}

	#[test]
	fn known_on_delete_actions_are_read() {
		for (text, want) in [
			("", OnDelete::Ignore),
			("ON DELETE IGNORE", OnDelete::Ignore),
			("ON DELETE REJECT", OnDelete::Reject),
			("ON DELETE CASCADE", OnDelete::Cascade),
			("ON DELETE UNSET", OnDelete::Unset),
		] {
			let (r, diagnostics) = on_delete(text);
			assert_eq!(r.on_delete, want, "{text}");
			assert_eq!(r.expr, None, "{text}");
			assert!(diagnostics.is_empty(), "{text}: {diagnostics:?}");
		}
		for (text, expr) in [
			("ON DELETE THEN (UPDATE x SET y = 1)", "(UPDATE x SET y = 1)"),
			("THEN{ 1 }", "{ 1 }"),
		] {
			let (r, diagnostics) = on_delete(text);
			assert_eq!(r.on_delete, OnDelete::Then, "{text}");
			assert_eq!(r.expr.as_deref(), Some(expr), "{text}");
			assert!(diagnostics.is_empty(), "{text}: {diagnostics:?}");
		}
	}

	#[test]
	fn an_unknown_on_delete_action_is_an_error_not_a_silent_ignore() {
		for text in ["ON DELETE ARCHIVE", "ON DELETE CASCADE NOW", "ON DELETE THEN", "ON DELETE 1"]
		{
			let (r, diagnostics) = on_delete(text);
			assert_eq!(r.on_delete, OnDelete::Ignore, "{text}");
			assert_eq!(diagnostics.len(), 1, "{text}: {diagnostics:?}");
			assert_eq!(diagnostics[0].code, codes::FIELD_REFERENCE_UNRECOGNIZED);
			assert_eq!(diagnostics[0].severity, Severity::Error);
			assert_eq!(diagnostics[0].table.as_deref(), Some("t"));
		}
	}

	#[test]
	fn an_untyped_id_field_is_an_any_key_with_a_diagnostic() {
		let mut diagnostics = Vec::new();
		assert_eq!(build_id_key("t", raw("id", None), &mut diagnostics), Some(Kind::Any));
		assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
		assert_eq!(diagnostics[0].code, codes::FIELD_KIND_UNDECLARED);
		assert_eq!(diagnostics[0].field.as_deref(), Some(&["id".to_string()][..]));

		let mut with_clause = raw("id", None);
		with_clause.assert = Some("$value != NONE".into());
		let mut diagnostics = Vec::new();
		assert_eq!(build_id_key("t", with_clause, &mut diagnostics), Some(Kind::Any));
		let codes: Vec<&str> = diagnostics.iter().map(|d| d.code.as_str()).collect();
		assert_eq!(codes, [codes::FIELD_KIND_UNDECLARED, codes::FIELD_ID_CLAUSES_IGNORED]);
	}
}
