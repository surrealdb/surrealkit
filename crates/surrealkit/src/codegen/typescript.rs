//! The per-query result types of schema document v2, rendered as a TypeScript module for the
//! surrealdb JS SDK.
//!
//! The module holds a `Queries` registry keyed by exact query text, `QueryNames`, and `Typed<T>`,
//! which gives a client's `query()` the registry's result type for known query text. Table types
//! are not part of it: the built-in typegen output owns them.
//!
//! The module is not only types: `liveQueries` is a runtime value, the live subscription facts
//! of each `SELECT` keyed like `Queries`, and `LiveQueries` types it together with the
//! notification each subscription delivers. A query the engine could not type is in neither
//! registry. With `register` set, the module also augments `Register` of the named bindings
//! package with `Queries` and `LiveQueries`.
//!
//! `Queries` is a type alias, not an interface: an interface never satisfies an index signature,
//! so a consumer constraining a registry to `Record<string, …>` needs the alias.
//!
//! # Optionality has two spellings, and the position picks
//!
//! A SurrealQL `option<string>` is `none | string`. As the type of an object literal field it is
//! `nick?: string`, because the key may be absent: v2 records that as the field's `optional`.
//! Anywhere a value stands on its own (a tuple slot, an array element, a statement result) it is
//! `string | undefined`, because there is no key to omit: v2 records that as an `option` kind, or,
//! for a statement's or a parameter's top-level kind, as the entry's `optional`. The two are not
//! interchangeable: `{ nick?: string }` accepts `{}` and `{ nick: string | undefined }` does not.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::queries::{
	Certainty, ELEMENT_SEGMENT, LiveAction, LiveNotification, LiveRefusal, QueryEntry, QueryLive,
	QueryParameter, QueryStatement,
};
use crate::typegen::document::{Kind, LiteralType, LiteralValue, ObjectLiteralField, TypeDef};

/// Query parameters with this prefix are filled by the host-language template, not by the caller.
const HOST_PARAM_PREFIX: &str = "__host";

/// The SDK names the module can import, in import order.
const SDK_NAMES: &[&str] = &[
	"BoundQuery",
	"DateTime",
	"Decimal",
	"Duration",
	"FileRef",
	"Geometry",
	"GeometryCollection",
	"GeometryLine",
	"GeometryMultiLine",
	"GeometryMultiPoint",
	"GeometryMultiPolygon",
	"GeometryPoint",
	"GeometryPolygon",
	"QueryLike",
	"Range",
	"RecordId",
	"SurrealQueryable",
	"Table",
	"Uuid",
];

/// How a SurrealQL `datetime` is typed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DateStyle {
	/// The SDK's `DateTime` class, which is what it decodes to by default.
	#[default]
	Class,
	/// A native `Date`, for a client built with `useNativeDates`.
	Native,
}

/// How a kind the engine only hints at is typed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HintStyle {
	/// The kind as declared.
	#[default]
	Trust,
	/// `unknown`.
	Unknown,
}

/// What [`render`] does with the choices a query list leaves open.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RenderOptions {
	pub dates: DateStyle,
	pub hints: HintStyle,
	/// The package the module imports its SDK types from.
	pub sdk_package: String,
	/// The bindings package whose `Register` the module augments with `Queries` and
	/// `LiveQueries`; no augmentation when absent.
	pub register: Option<String>,
}

impl Default for RenderOptions {
	fn default() -> Self {
		Self {
			dates: DateStyle::default(),
			hints: HintStyle::default(),
			sdk_package: "surrealdb".to_string(),
			register: None,
		}
	}
}

/// Renders the complete module. `types` are the document's `DEFINE TYPE`s, which a `named` kind
/// is expanded from.
pub fn render(queries: &[QueryEntry], types: &[TypeDef], opts: &RenderOptions) -> String {
	module(queries, types, opts, None).0
}

/// A module split in two, for a project that writes plain JavaScript: the declarations (`x.d.ts`)
/// and the runtime value they describe (`x.js`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Declarations {
	/// The types, and `liveQueries` as `export declare const`.
	pub declarations: String,
	/// `liveQueries` as a value, typed by the declarations.
	pub runtime: String,
}

/// Renders [`render`]'s module as declarations only, and the runtime file beside it. `stem` is how
/// the runtime file names the declarations: the path of `x.d.ts` without its extension, as
/// `./x` (what an `import("./x")` in `x.js` resolves, to `x.d.ts`).
pub fn render_declarations(
	queries: &[QueryEntry],
	types: &[TypeDef],
	opts: &RenderOptions,
	stem: &str,
) -> Declarations {
	let (declarations, rows) = module(queries, types, opts, Some(stem));
	let value = if rows.is_empty() {
		"export const liveQueries = {};\n".to_string()
	} else {
		format!("export const liveQueries = {{\n{}}};\n", rows.concat())
	};
	let runtime = format!(
		"{RUNTIME_HEADER}\n/** @type {{typeof import({}).liveQueries}} */\n{value}",
		ts_string(&format!("./{stem}"))
	);
	Declarations {
		declarations,
		runtime,
	}
}

/// The module, and the `liveQueries` rows as values. With `declarations` (the stem of the
/// runtime file) the module declares `liveQueries` instead of defining it.
fn module(
	queries: &[QueryEntry],
	types: &[TypeDef],
	opts: &RenderOptions,
	declarations: Option<&str>,
) -> (String, Vec<String>) {
	let mut emitter = Emitter {
		opts,
		types,
		expanding: Vec::new(),
		used: BTreeSet::from(["BoundQuery", "QueryLike", "SurrealQueryable"]),
	};
	let registry = emitter.registry(queries);
	let names = query_names(queries);
	let (live, rows) = emitter.live(queries, declarations.is_some());

	let imports: Vec<&str> =
		SDK_NAMES.iter().copied().filter(|name| emitter.used.contains(name)).collect();
	let mut out = String::from(HEADER);
	match declarations {
		Some(stem) => out.push_str(&format!(
			"// Declarations only: `liveQueries` is a runtime value, in `{stem}.js` beside this file.\n"
		)),
		None => out.push_str(
			"// Everything here is a type except `liveQueries`, a runtime value: import it as a value.\n",
		),
	}
	out.push('\n');
	out.push_str(&format!(
		"import type {{ {} }} from {};\n",
		imports.join(", "),
		ts_string(&opts.sdk_package)
	));
	// The augmentation below needs the package in the program: without an import of it, a
	// program that includes this module but nothing that imports the package fails with TS2664.
	if let Some(package) = &opts.register {
		out.push_str(&format!("import type {{}} from {};\n", ts_string(package)));
	}
	out.push('\n');
	let mut parts = vec![registry, names, live, TYPED_CLIENT.to_string()];
	if let Some(package) = &opts.register {
		parts.push(register_block(package));
	}
	out.push_str(&parts.join("\n"));
	(out, rows)
}

const HEADER: &str = "\
// Generated by SurrealKit: the result type of each query, and each SELECT's live subscription
// facts. Do not edit.
// Source: the `queries` section of schema document v2.
//
// Type a client's `query()` by exact query text:
//
//   const db = new Surreal() as Typed<Surreal>;
//   const [people] = await db.query(\"SELECT name FROM person\");
//
";

/// The head of the runtime file written beside a declarations file.
const RUNTIME_HEADER: &str = "\
// Generated by SurrealKit: each SELECT's live subscription facts. Do not edit.
// Source: the `queries` section of schema document v2. The types are in the declarations file
// beside this one.
";

/// The live subscription types over `liveQueries`, emitted verbatim.
const LIVE_TYPES: &str = "\
export type LiveQueries = {
  [K in keyof typeof liveQueries]: (typeof liveQueries)[K] & {
    notification: K extends keyof LiveNotifications ? LiveNotifications[K] : never;
  };
};
/** The texts a live subscription can serve: every entry but a refusal LIVE cannot window. */
export type LiveQueryText = {
  [K in keyof LiveQueries]: LiveQueries[K] extends { refusal: { windowable: false } } ? never : K;
}[keyof LiveQueries];
export type LiveNotification<K extends LiveQueryText> = LiveQueries[K][\"notification\"];
";

/// The augmentation that registers the module's registries with the bindings package `package`.
fn register_block(package: &str) -> String {
	format!(
		"declare module {} {{\n  interface Register {{\n    queries: Queries;\n    liveQueries: \
		 LiveQueries;\n  }}\n}}\n",
		ts_string(package)
	)
}

/// The typed client, emitted verbatim. `Query` is not exported by the SDK, hence the `declare
/// const` probe for its type.
const TYPED_CLIENT: &str = "\
declare const queryable: SurrealQueryable;
/** The SDK's `Query<R>`, which it does not export by name. */
export type QueryOf<R extends unknown[]> = ReturnType<typeof queryable.query<R>>;
type ParamsOf<K extends keyof Queries> = Queries[K][\"params\"];
/** A query that reads no parameter takes no bindings; one whose parameters are all optional may omit them. */
type BindingsArg<K extends keyof Queries> = [keyof ParamsOf<K>] extends [never]
  ? [bindings?: Record<string, never>]
  : {} extends ParamsOf<K>
    ? [bindings?: ParamsOf<K>]
    : [bindings: ParamsOf<K>];
/** Query text that is not a key of `Queries`; a known text is `never`, so it cannot reach the untyped overload. */
type UnknownText<S extends string> = S extends keyof Queries ? never : S;
/**
 * `query()` typed by exact query text: a known text gets the registry's result type and must be
 * given its parameters' types. Any other text, and the SDK's other forms, keep the SDK's own types.
 */
export interface TypedQueryable {
  query<K extends keyof Queries>(query: K, ...bindings: BindingsArg<K>): QueryOf<Queries[K][\"result\"]>;
  query<R extends unknown[] = unknown[], S extends string = string>(
    query: UnknownText<S>,
    bindings?: Record<string, unknown>,
  ): QueryOf<R>;
  query<R extends unknown[] = unknown[]>(query: BoundQuery<R>): QueryOf<R>;
  query<R extends unknown[] = unknown[]>(queries: readonly QueryLike[]): QueryOf<R>;
}
/** A client whose `query()` returns the registry's result type for known query text. */
export type Typed<T extends SurrealQueryable> = Omit<T, \"query\"> & TypedQueryable;
";

fn query_names(queries: &[QueryEntry]) -> String {
	let mut seen = BTreeSet::new();
	let mut rows = Vec::new();
	for query in queries {
		let Some(name) = &query.name else {
			continue;
		};
		if query.error.is_some() {
			continue;
		}
		if seen.insert(name.as_str()) {
			rows.push(format!("  {}: {};\n", ts_string(name), ts_string(&query.text)));
		}
	}
	let mut out = String::from(
		"/** Each named query's text, so `Queries[QueryNames[\"name\"]]` finds its entry. */\n",
	);
	if rows.is_empty() {
		out.push_str("export type QueryNames = {};\n");
	} else {
		out.push_str("export type QueryNames = {\n");
		out.extend(rows);
		out.push_str("};\n");
	}
	out
}

/// A `liveQueries` row for the query `text`: `query`, `exit` and `watch` from the statement's
/// `live`, `refusal` from its `liveRefusal` (without the engine's code and message). A row with
/// both a query and a refusal takes a line for each field.
fn live_entry(text: &str, statement: &QueryStatement) -> String {
	let mut fields = Vec::new();
	if let Some(live) = &statement.live {
		fields.push(format!("query: {}", ts_string(&live.query)));
		if let Some(exit) = live.exit {
			fields.push(format!("exit: {}", ts_string(exit.as_str())));
		}
		if let Some(watch) = &live.watch {
			fields.push(format!("watch: {}", ts_string(watch)));
		}
	}
	if let Some(refusal) = &statement.live_refusal {
		fields.push(refusal_field(refusal));
	}
	let key = ts_string(text);
	if statement.live.is_some() && statement.live_refusal.is_some() {
		let lines: String = fields.iter().map(|field| format!("    {field},\n")).collect();
		format!("  {key}: {{\n{lines}  }},\n")
	} else {
		format!("  {key}: {{ {} }},\n", fields.join(", "))
	}
}

/// A `liveQueries` row as the type of the `as const` value [`live_entry`] writes: every property
/// `readonly`, every array a `readonly` tuple.
fn declared_entry(text: &str, statement: &QueryStatement) -> String {
	let mut fields = Vec::new();
	if let Some(live) = &statement.live {
		fields.push(format!("readonly query: {}", ts_string(&live.query)));
		if let Some(exit) = live.exit {
			fields.push(format!("readonly exit: {}", ts_string(exit.as_str())));
		}
		if let Some(watch) = &live.watch {
			fields.push(format!("readonly watch: {}", ts_string(watch)));
		}
	}
	if let Some(refusal) = &statement.live_refusal {
		let clauses: Vec<String> = refusal.clauses.iter().map(|clause| ts_string(clause)).collect();
		fields.push(format!(
			"readonly refusal: {{ readonly clauses: readonly [{}]; readonly windowable: {} }}",
			clauses.join(", "),
			refusal.windowable
		));
	}
	let key = ts_string(text);
	if statement.live.is_some() && statement.live_refusal.is_some() {
		let lines: String = fields.iter().map(|field| format!("    {field};\n")).collect();
		format!("  readonly {key}: {{\n{lines}  }};\n")
	} else {
		format!("  readonly {key}: {{ {} }};\n", fields.join("; "))
	}
}

fn refusal_field(refusal: &LiveRefusal) -> String {
	let clauses: Vec<String> = refusal.clauses.iter().map(|clause| ts_string(clause)).collect();
	format!("refusal: {{ clauses: [{}], windowable: {} }}", clauses.join(", "), refusal.windowable)
}

/// Object positions, relative to the kind being rendered, that admit undeclared keys. A path that
/// is empty is the kind itself.
type Open<'a> = [&'a [String]];

struct Emitter<'a> {
	opts: &'a RenderOptions,
	types: &'a [TypeDef],
	/// The named types being expanded, outermost first: a name already here leads back to
	/// itself.
	expanding: Vec<String>,
	/// SDK names the output uses, recorded where each is produced.
	used: BTreeSet<&'static str>,
}

impl Emitter<'_> {
	fn note(&mut self, name: &'static str) -> &'static str {
		self.used.insert(name);
		name
	}

	/// The value spelling. Recursion stays here: an array element, a union member and a tuple
	/// item are all values, and only an object literal field crosses into a key.
	fn kind(&mut self, kind: &Kind, open: &Open<'_>) -> String {
		match kind {
			Kind::Any
			| Kind::Function
			| Kind::Regex
			| Kind::Date
			| Kind::Time
			| Kind::Timestamp
			| Kind::Interval
			| Kind::BigDecimal
			| Kind::Unknown {
				..
			} => "unknown".to_string(),
			Kind::None => "undefined".to_string(),
			Kind::Null => "null".to_string(),
			Kind::Bool => "boolean".to_string(),
			Kind::Int {
				..
			}
			| Kind::Float {
				..
			}
			| Kind::Number => "number".to_string(),
			Kind::Decimal => self.note("Decimal").to_string(),
			Kind::String => "string".to_string(),
			Kind::Datetime => match self.opts.dates {
				DateStyle::Class => self.note("DateTime").to_string(),
				DateStyle::Native => "Date".to_string(),
			},
			Kind::Duration => self.note("Duration").to_string(),
			Kind::Uuid => self.note("Uuid").to_string(),
			Kind::Bytes => "Uint8Array".to_string(),
			Kind::Object => "{ [key: string]: unknown }".to_string(),
			Kind::Range => {
				let range = self.note("Range");
				format!("{range}<unknown, unknown>")
			}
			Kind::Record {
				tables,
			} => {
				let record = self.note("RecordId");
				with_tables(record, tables)
			}
			Kind::Table {
				tables,
			} => {
				let table = self.note("Table");
				with_tables(table, tables)
			}
			Kind::File {
				..
			} => self.note("FileRef").to_string(),
			Kind::Geometry {
				types,
			} => self.geometry(types),
			// An array no element fits, such as the result of a plain DELETE.
			Kind::Array {
				length: Some(0),
				..
			} => "[]".to_string(),
			Kind::Array {
				item,
				..
			} => {
				let item = self.kind(item, &into_element(open));
				format!("Array<{item}>")
			}
			Kind::Set {
				item,
				..
			} => {
				let item = self.kind(item, &into_element(open));
				format!("Set<{item}>")
			}
			// A tuple slot has no key to omit, so each item is a value, and no path leads into
			// one.
			Kind::Tuple {
				items,
			} => {
				let items: Vec<String> = items.iter().map(|item| self.kind(item, &[])).collect();
				format!("[{}]", items.join(", "))
			}
			Kind::ObjectLiteral {
				fields,
			} => self.object(fields, open),
			Kind::Literal {
				literal_type,
				value,
			} => self.literal(*literal_type, value),
			Kind::Option {
				item,
			} => {
				let item = self.kind(item, open);
				or_undefined(item)
			}
			// Repeated labels are the same string, so a repeat adds nothing to the union.
			Kind::Enum {
				labels,
			} => {
				let mut rendered: Vec<String> = Vec::new();
				for label in labels {
					let text = ts_string(label);
					if !rendered.contains(&text) {
						rendered.push(text);
					}
				}
				if rendered.is_empty() {
					"never".to_string()
				} else {
					rendered.join(" | ")
				}
			}
			Kind::Named {
				name,
			} => self.named(name, open),
			Kind::Union {
				variants,
			} => {
				let mut rendered: Vec<String> = Vec::new();
				for variant in variants {
					let text = self.kind(variant, open);
					if !rendered.contains(&text) {
						rendered.push(text);
					}
				}
				if rendered.is_empty() {
					"never".to_string()
				} else {
					rendered.join(" | ")
				}
			}
		}
	}

	fn geometry(&mut self, types: &[String]) -> String {
		let mut members: Vec<&'static str> = Vec::new();
		for name in types {
			let class = match name.to_ascii_lowercase().as_str() {
				"point" => "GeometryPoint",
				"line" => "GeometryLine",
				"polygon" => "GeometryPolygon",
				"multipoint" => "GeometryMultiPoint",
				"multiline" => "GeometryMultiLine",
				"multipolygon" => "GeometryMultiPolygon",
				"collection" => "GeometryCollection",
				_ => {
					members.clear();
					break;
				}
			};
			if !members.contains(&class) {
				members.push(class);
			}
		}
		if members.is_empty() {
			return self.note("Geometry").to_string();
		}
		members.into_iter().map(|class| self.note(class)).collect::<Vec<_>>().join(" | ")
	}

	/// A `DEFINE TYPE` name, expanded inline: the module declares no alias for it. A name `types`
	/// does not define, or one that leads back to itself, is `unknown`.
	fn named(&mut self, name: &str, open: &Open<'_>) -> String {
		let types = self.types;
		let Some(def) = types.iter().find(|def| def.name == name) else {
			return "unknown".to_string();
		};
		if self.expanding.iter().any(|n| n == name) {
			return "unknown".to_string();
		}
		self.expanding.push(name.to_string());
		let text = self.kind(&def.kind, open);
		self.expanding.pop();
		text
	}

	fn literal(&mut self, literal_type: LiteralType, value: &LiteralValue) -> String {
		let text = match value {
			LiteralValue::Bool(value) => value.to_string(),
			LiteralValue::Text(text) => text.clone(),
		};
		match literal_type {
			LiteralType::String => ts_string(&text),
			LiteralType::Int | LiteralType::Float => numeric_literal(&text),
			LiteralType::Decimal => self.note("Decimal").to_string(),
			LiteralType::Duration => self.note("Duration").to_string(),
			LiteralType::Bool => match text.as_str() {
				"true" | "false" => text,
				_ => "boolean".to_string(),
			},
		}
	}

	/// An object type: the one place a key exists, so fields render as properties.
	fn object(&mut self, fields: &[ObjectLiteralField], open: &Open<'_>) -> String {
		let mut parts = Vec::new();
		for field in fields {
			let text = self.kind(&field.kind, &into_field(open, &field.name));
			let marker = if field.optional {
				"?"
			} else {
				""
			};
			parts.push(format!("{}{marker}: {text}", object_key(&field.name)));
		}
		if is_open(open) {
			parts.push(INDEX_SIGNATURE.to_string());
		}
		object_text(&parts)
	}

	/// The query registry: one row per distinct query text.
	fn registry(&mut self, queries: &[QueryEntry]) -> String {
		let mut seen = BTreeSet::new();
		let mut rows = Vec::new();
		for query in queries {
			if query.error.is_some() || !seen.insert(query.text.as_str()) {
				continue;
			}
			let result = self.result_tuple(&query.statements);
			let params = self.query_params(&query.params);
			rows.push(format!(
				"  {}: {{ result: {result}; params: {params} }};\n",
				ts_string(&query.text)
			));
		}
		let mut out = String::from("/** Every query, keyed by its exact text. */\n");
		if rows.is_empty() {
			out.push_str("export type Queries = {};\n");
		} else {
			out.push_str("export type Queries = {\n");
			out.extend(rows);
			out.push_str("};\n");
		}
		out
	}

	/// The live subscription facts: the `liveQueries` value, and the types over it. One row per
	/// distinct query text of `Queries` whose only statement has `live` or `liveRefusal`.
	fn live(&mut self, queries: &[QueryEntry], declared: bool) -> (String, Vec<String>) {
		let mut seen = BTreeSet::new();
		let mut entries = Vec::new();
		let mut declarations = Vec::new();
		let mut notifications = Vec::new();
		for query in queries {
			if query.error.is_some() || !seen.insert(query.text.as_str()) {
				continue;
			}
			let [statement] = query.statements.as_slice() else {
				continue;
			};
			if statement.live.is_none() && statement.live_refusal.is_none() {
				continue;
			}
			entries.push(live_entry(&query.text, statement));
			declarations.push(declared_entry(&query.text, statement));
			if let Some(live) = &statement.live {
				let union = self.live_union(live);
				notifications.push(format!("  {}: {union};\n", ts_string(&query.text)));
			}
		}
		let mut out = String::from(
			"/** Each SELECT's live subscription facts, keyed by the same text as `Queries`. */\n",
		);
		if declared {
			if declarations.is_empty() {
				out.push_str("export declare const liveQueries: {};\n");
			} else {
				out.push_str("export declare const liveQueries: {\n");
				out.extend(declarations);
				out.push_str("};\n");
			}
		} else if entries.is_empty() {
			out.push_str("export const liveQueries = {} as const;\n");
		} else {
			out.push_str("export const liveQueries = {\n");
			out.extend(entries.iter().cloned());
			out.push_str("} as const;\n");
		}
		out.push_str(
			"\n/** The notification a live subscription delivers, per query, as the SDK's \
			 `LiveMessage` carries it. */\n",
		);
		if notifications.is_empty() {
			out.push_str("type LiveNotifications = {};\n");
		} else {
			out.push_str("type LiveNotifications = {\n");
			out.extend(notifications);
			out.push_str("};\n");
		}
		out.push_str(LIVE_TYPES);
		(out, entries)
	}

	/// The notifications of `live` as a union, one member per action, as the SDK's `LiveMessage`
	/// shapes them: a record action carries the record's id and the result, and `KILLED` carries
	/// neither. A notification the engine only hints at is `unknown` where hints are distrusted.
	fn live_union(&mut self, live: &QueryLive) -> String {
		let members: Vec<String> =
			live.notifications.iter().map(|notification| self.live_member(notification)).collect();
		if members.is_empty() {
			"never".to_string()
		} else {
			members.join(" | ")
		}
	}

	fn live_member(&mut self, notification: &LiveNotification) -> String {
		let uuid = self.note("Uuid");
		let action = notification.action.as_str();
		if notification.action == LiveAction::Killed {
			return format!(
				"{{ queryId: {uuid}; action: \"{action}\"; recordId?: undefined; value?: undefined }}"
			);
		}
		let distrusted =
			notification.certainty == Certainty::Hint && self.opts.hints == HintStyle::Unknown;
		let (record, value) = if distrusted {
			("unknown".to_string(), "unknown".to_string())
		} else {
			let open: Vec<&[String]> = notification.open.iter().map(Vec::as_slice).collect();
			(self.kind(&notification.record, &[]), self.kind(&notification.result, &open))
		};
		format!("{{ queryId: {uuid}; action: \"{action}\"; recordId: {record}; value: {value} }}")
	}

	/// One element per statement, in source order: every statement, `BEGIN` and `COMMIT`
	/// included, has a slot in the response.
	fn result_tuple(&mut self, statements: &[QueryStatement]) -> String {
		let slots: Vec<String> =
			statements.iter().map(|statement| self.statement(statement)).collect();
		tuple_text(&slots)
	}

	/// A statement's slot. One the engine did not type is `unknown`.
	fn statement(&mut self, statement: &QueryStatement) -> String {
		let Some(kind) = statement.kind.as_ref() else {
			return "unknown".to_string();
		};
		if statement.certainty == Certainty::Hint && self.opts.hints == HintStyle::Unknown {
			return "unknown".to_string();
		}
		let open: Vec<&[String]> = statement.open.iter().map(Vec::as_slice).collect();
		let text = self.kind(kind, &open);
		if statement.optional {
			or_undefined(text)
		} else {
			text
		}
	}

	/// The named-parameter object. This is not a property position: the `?` answers whether the
	/// query needs the argument at all, while a property's `?` answers whether the value can be
	/// absent. Folding a parameter's `option<T>` into the key as well would let callers omit a key
	/// the query requires, so an optional kind stays `T | undefined`.
	fn query_params(&mut self, params: &[QueryParameter]) -> String {
		params_text(&self.param_types(params))
	}

	/// The parameters a caller binds, in order: those the host-language template fills are
	/// left out.
	fn param_types(&mut self, params: &[QueryParameter]) -> Vec<ParamRender> {
		let mut out = Vec::new();
		for param in params {
			if param.name.starts_with(HOST_PARAM_PREFIX) {
				continue;
			}
			// A parameter's kind is inferred from how the query uses it, so it is always a hint.
			let trusted = param.kind.as_ref().filter(|_| self.opts.hints == HintStyle::Trust);
			let typescript = match trusted {
				Some(kind) => {
					let text = self.kind(kind, &[]);
					if param.optional {
						or_undefined(text)
					} else {
						text
					}
				}
				None => "unknown".to_string(),
			};
			out.push(ParamRender {
				name: param.name.clone(),
				typescript,
				required: param.required,
			});
		}
		out
	}
}

/// One parameter of a query as the module types it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParamRender {
	pub(crate) name: String,
	/// The value's type, `| undefined` included when its kind admits NONE.
	pub(crate) typescript: String,
	/// Whether the caller must bind it.
	pub(crate) required: bool,
}

/// One query rendered with the module's rules, in pieces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueryRender {
	/// One type per statement, in source order.
	pub(crate) slots: Vec<String>,
	/// The parameters a caller binds, in order.
	pub(crate) params: Vec<ParamRender>,
	/// The union of the notifications a live subscription delivers, when the query is one
	/// statement and that statement has `live`.
	pub(crate) live: Option<String>,
	/// The SDK names the types use, in import order.
	pub(crate) imports: Vec<&'static str>,
}

/// Renders `query` as [`render`] renders its registry row, and reports the SDK names it uses.
/// `types` are the document's `DEFINE TYPE`s, which a `named` kind is expanded from.
pub(crate) fn render_query(
	query: &QueryEntry,
	types: &[TypeDef],
	opts: &RenderOptions,
) -> QueryRender {
	let mut emitter = Emitter {
		opts,
		types,
		expanding: Vec::new(),
		used: BTreeSet::new(),
	};
	let slots = query.statements.iter().map(|statement| emitter.statement(statement)).collect();
	let params = emitter.param_types(&query.params);
	let live = match query.statements.as_slice() {
		[statement] => statement.live.as_ref().map(|live| emitter.live_union(live)),
		_ => None,
	};
	let imports = SDK_NAMES.iter().copied().filter(|name| emitter.used.contains(name)).collect();
	QueryRender {
		slots,
		params,
		live,
		imports,
	}
}

/// The result tuple: one element per statement.
pub(crate) fn tuple_text(slots: &[String]) -> String {
	format!("[{}]", slots.join(", "))
}

/// The named-parameter object, `{}` when there is none.
pub(crate) fn params_text(params: &[ParamRender]) -> String {
	let parts: Vec<String> = params
		.iter()
		.map(|param| {
			let marker = if param.required {
				""
			} else {
				"?"
			};
			format!("{}{marker}: {}", object_key(&param.name), param.typescript)
		})
		.collect();
	object_text(&parts)
}

/// A TypeScript double-quoted string literal for `text`, for the package a snippet imports from.
pub(crate) fn string_literal(text: &str) -> String {
	ts_string(text)
}

const INDEX_SIGNATURE: &str = "[key: string]: unknown";

fn is_open(open: &Open<'_>) -> bool {
	open.iter().any(|path| path.is_empty())
}

/// The paths that continue into the object literal field `name`.
fn into_field<'a>(open: &Open<'a>, name: &str) -> Vec<&'a [String]> {
	open.iter()
		.filter_map(|path| match path.split_first() {
			Some((segment, rest)) if segment == name => Some(rest),
			_ => None,
		})
		.collect()
}

/// The paths that continue into a collection's elements.
fn into_element<'a>(open: &Open<'a>) -> Vec<&'a [String]> {
	into_field(open, ELEMENT_SEGMENT)
}

/// `text | undefined`, unless `text` already is `undefined`.
fn or_undefined(text: String) -> String {
	if text == "undefined" {
		text
	} else {
		format!("{text} | undefined")
	}
}

/// A numeric literal type, or `number` when the text is not one TypeScript reads.
fn numeric_literal(text: &str) -> String {
	if let Ok(int) = text.parse::<i64>() {
		return int.to_string();
	}
	let plain =
		text.chars().all(|c| c.is_ascii_digit() || matches!(c, '.' | '-' | '+' | 'e' | 'E'));
	if plain && text.parse::<f64>().is_ok_and(f64::is_finite) {
		text.to_string()
	} else {
		"number".to_string()
	}
}

fn object_text(parts: &[String]) -> String {
	if parts.is_empty() {
		"{}".to_string()
	} else {
		format!("{{ {} }}", parts.join("; "))
	}
}

/// `RecordId` or `RecordId<"a" | "b">`.
fn with_tables(name: &str, tables: &[String]) -> String {
	if tables.is_empty() {
		name.to_string()
	} else {
		let tables: Vec<String> = tables.iter().map(|table| ts_string(table)).collect();
		format!("{name}<{}>", tables.join(" | "))
	}
}

/// An object key, quoted when it is not a plain identifier.
fn object_key(name: &str) -> String {
	if is_identifier(name) {
		name.to_string()
	} else {
		ts_string(name)
	}
}

fn is_identifier(name: &str) -> bool {
	let mut chars = name.chars();
	matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_' || c == '$')
		&& chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
}

/// A TypeScript double-quoted string literal. JSON's escaping is a subset of TypeScript's.
fn ts_string(text: &str) -> String {
	Value::String(text.to_string()).to_string()
}

#[cfg(test)]
mod tests {
	use serde_json::json;

	use super::*;

	fn kind_of(raw: Value) -> Kind {
		serde_json::from_value(raw).expect("v2 kind")
	}

	fn emitter(opts: &RenderOptions) -> Emitter<'_> {
		Emitter {
			opts,
			types: &[],
			expanding: Vec::new(),
			used: BTreeSet::new(),
		}
	}

	fn value_with(opts: &RenderOptions, raw: Value) -> String {
		emitter(opts).kind(&kind_of(raw), &[])
	}

	fn value(raw: Value) -> String {
		value_with(&RenderOptions::default(), raw)
	}

	fn entries(raw: Value) -> Vec<QueryEntry> {
		serde_json::from_value(raw).expect("query entries")
	}

	fn render_default(raw: Value) -> String {
		render(&entries(raw), &[], &RenderOptions::default())
	}

	/// A single statement entry's slot, rendered.
	fn slot(opts: &RenderOptions, statement: Value) -> String {
		let query = json!([{ "key": "q", "text": "q", "statements": [statement] }]);
		render(&entries(query), &[], opts)
	}

	fn int() -> Value {
		json!({ "kind": "int" })
	}

	#[test]
	fn scalars_map_to_their_typescript_types() {
		for (kind, want) in [
			("any", "unknown"),
			("function", "unknown"),
			("regex", "unknown"),
			("none", "undefined"),
			("null", "null"),
			("bool", "boolean"),
			("int", "number"),
			("float", "number"),
			("number", "number"),
			("decimal", "Decimal"),
			("string", "string"),
			("datetime", "DateTime"),
			("duration", "Duration"),
			("uuid", "Uuid"),
			("bytes", "Uint8Array"),
			("object", "{ [key: string]: unknown }"),
			("range", "Range<unknown, unknown>"),
		] {
			assert_eq!(value(json!({ "kind": kind })), want, "{kind}");
		}
		assert_eq!(value(json!({ "kind": "unknown", "source": "money" })), "unknown");
	}

	#[test]
	fn datetime_is_a_class_or_a_native_date() {
		let native = RenderOptions {
			dates: DateStyle::Native,
			..RenderOptions::default()
		};
		assert_eq!(value_with(&native, json!({ "kind": "datetime" })), "Date");
		let mut emitter = emitter(&native);
		emitter.kind(&Kind::Datetime, &[]);
		assert!(!emitter.used.contains("DateTime"));
	}

	#[test]
	fn records_tables_files_and_geometry() {
		assert_eq!(value(json!({ "kind": "table", "tables": [] })), "Table");
		assert_eq!(value(json!({ "kind": "table", "tables": ["a", "b"] })), r#"Table<"a" | "b">"#);
		assert_eq!(value(json!({ "kind": "record", "tables": [] })), "RecordId");
		assert_eq!(
			value(json!({ "kind": "record", "tables": ["a", "b"] })),
			r#"RecordId<"a" | "b">"#
		);
		assert_eq!(value(json!({ "kind": "file", "buckets": ["avatars"] })), "FileRef");
		assert_eq!(value(json!({ "kind": "geometry", "types": [] })), "Geometry");
		assert_eq!(value(json!({ "kind": "geometry", "types": ["point"] })), "GeometryPoint");
		assert_eq!(
			value(json!({ "kind": "geometry", "types": [
				"point", "line", "polygon", "multipoint", "multiline", "multipolygon", "collection"
			] })),
			"GeometryPoint | GeometryLine | GeometryPolygon | GeometryMultiPoint | GeometryMultiLine | GeometryMultiPolygon | GeometryCollection"
		);
		assert_eq!(value(json!({ "kind": "geometry", "types": ["point", "quux"] })), "Geometry");
	}

	#[test]
	fn collections_and_tuples() {
		assert_eq!(
			value(json!({ "kind": "array", "item": { "kind": "string" } })),
			"Array<string>"
		);
		assert_eq!(value(json!({ "kind": "array", "item": int(), "length": 2 })), "Array<number>");
		assert_eq!(value(json!({ "kind": "set", "item": { "kind": "string" } })), "Set<string>");
		assert_eq!(
			value(json!({ "kind": "tuple", "items": [int(), { "kind": "string" }] })),
			"[number, string]"
		);
		assert_eq!(value(json!({ "kind": "tuple", "items": [] })), "[]");
	}

	#[test]
	fn unions_dedupe_members_in_order() {
		assert_eq!(
			value(json!({ "kind": "union", "variants": [
				{ "kind": "int" }, { "kind": "float" }, { "kind": "string" }
			] })),
			"number | string"
		);
	}

	#[test]
	fn an_option_is_a_union_member_as_a_value_and_a_key_marker_on_a_field() {
		let option = json!({ "kind": "option", "item": { "kind": "string" } });
		assert_eq!(value(option.clone()), "string | undefined");
		assert_eq!(value(json!({ "kind": "array", "item": option })), "Array<string | undefined>");
		assert_eq!(
			value(json!({ "kind": "tuple", "items": [int(), option] })),
			"[number, string | undefined]"
		);
		assert_eq!(
			value(json!({ "kind": "objectLiteral", "fields": [
				{ "name": "name", "kind": { "kind": "string" }, "optional": false },
				{ "name": "nick", "kind": { "kind": "string" }, "optional": true },
				{ "name": "weird key", "kind": int(), "optional": false },
			] })),
			r#"{ name: string; nick?: string; "weird key": number }"#
		);
	}

	#[test]
	fn the_4_x_kinds() {
		assert_eq!(value(json!({ "kind": "int", "width": 64, "signed": false })), "number");
		assert_eq!(value(json!({ "kind": "float", "width": 16 })), "number");
		for kind in ["date", "time", "timestamp", "interval", "bigdecimal"] {
			assert_eq!(value(json!({ "kind": kind })), "unknown", "{kind}");
		}
		assert_eq!(
			value(json!({ "kind": "enum", "labels": ["low", "high", "low", "it's"] })),
			r#""low" | "high" | "it's""#
		);
		assert_eq!(value(json!({ "kind": "enum", "labels": [] })), "never");
	}

	#[test]
	fn a_named_kind_is_expanded_from_types() {
		let types: Vec<TypeDef> = serde_json::from_value(json!([
			{ "name": "money", "kind": { "kind": "decimal" } },
			{ "name": "tree", "kind": { "kind": "objectLiteral", "fields": [
				{ "name": "children", "optional": false,
					"kind": { "kind": "array", "item": { "kind": "named", "name": "tree" } } },
				{ "name": "price", "optional": true, "kind": { "kind": "named", "name": "money" } },
			] } },
		]))
		.expect("types");
		let opts = RenderOptions::default();
		let mut emitter = Emitter {
			types: &types,
			..emitter(&opts)
		};
		let mut render = |raw: Value| emitter.kind(&kind_of(raw), &[]);
		assert_eq!(render(json!({ "kind": "named", "name": "money" })), "Decimal");
		// A name that leads back to itself stops at `unknown`.
		assert_eq!(
			render(json!({ "kind": "named", "name": "tree" })),
			"{ children: Array<unknown>; price?: Decimal }"
		);
		assert_eq!(render(json!({ "kind": "named", "name": "absent" })), "unknown");
	}

	#[test]
	fn literals() {
		let literal = |literal_type: &str, raw: Value| {
			value(json!({ "kind": "literal", "type": literal_type, "value": raw }))
		};
		assert_eq!(literal("string", json!("say \"hi\"")), r#""say \"hi\"""#);
		assert_eq!(literal("int", json!("42")), "42");
		assert_eq!(literal("int", json!("-7")), "-7");
		assert_eq!(literal("float", json!("1.5")), "1.5");
		assert_eq!(literal("float", json!("inf")), "number");
		assert_eq!(literal("decimal", json!("1.25")), "Decimal");
		assert_eq!(literal("duration", json!("1m30s")), "Duration");
		assert_eq!(literal("bool", json!(true)), "true");
		assert_eq!(literal("bool", json!(false)), "false");
	}

	#[test]
	fn a_literal_union_of_strings() {
		assert_eq!(
			value(json!({ "kind": "union", "variants": [
				{ "kind": "literal", "type": "string", "value": "draft" },
				{ "kind": "literal", "type": "string", "value": "live" },
			] })),
			r#""draft" | "live""#
		);
	}

	#[test]
	fn open_paths_add_an_index_signature_to_the_objects_they_name() {
		let opts = RenderOptions::default();
		let row = kind_of(json!({ "kind": "array", "item": { "kind": "objectLiteral", "fields": [
			{ "name": "address", "optional": false, "kind": { "kind": "objectLiteral", "fields": [
				{ "name": "city", "kind": { "kind": "string" }, "optional": false }
			] } },
			{ "name": "id", "optional": false, "kind": { "kind": "record", "tables": ["t"] } },
		] } }));
		let at = |paths: &[&[&str]]| {
			let owned: Vec<Vec<String>> =
				paths.iter().map(|path| path.iter().map(ToString::to_string).collect()).collect();
			let open: Vec<&[String]> = owned.iter().map(Vec::as_slice).collect();
			emitter(&opts).kind(&row, &open)
		};
		assert_eq!(at(&[]), r#"Array<{ address: { city: string }; id: RecordId<"t"> }>"#);
		assert_eq!(
			at(&[&["*"]]),
			r#"Array<{ address: { city: string }; id: RecordId<"t">; [key: string]: unknown }>"#
		);
		assert_eq!(
			at(&[&["*", "address"]]),
			r#"Array<{ address: { city: string; [key: string]: unknown }; id: RecordId<"t"> }>"#
		);
		// A path that leads nowhere changes nothing.
		assert_eq!(at(&[&["address"], &["*", "gone"]]), at(&[]));
	}

	#[test]
	fn an_open_path_passes_through_an_option_and_a_union() {
		let opts = RenderOptions::default();
		let object = json!({ "kind": "objectLiteral", "fields": [
			{ "name": "a", "kind": int(), "optional": false }
		] });
		let open: [&[String]; 1] = [&[]];
		assert_eq!(
			emitter(&opts).kind(&kind_of(json!({ "kind": "option", "item": object })), &open),
			"{ a: number; [key: string]: unknown } | undefined"
		);
		assert_eq!(
			emitter(&opts).kind(
				&kind_of(json!({ "kind": "union", "variants": [{ "kind": "null" }, object] })),
				&open
			),
			"null | { a: number; [key: string]: unknown }"
		);
	}

	#[test]
	fn an_open_path_steps_into_set_items() {
		let opts = RenderOptions::default();
		let set =
			kind_of(json!({ "kind": "set", "item": { "kind": "objectLiteral", "fields": [] } }));
		let open: [&[String]; 1] = [&["*".to_string()]];
		assert_eq!(emitter(&opts).kind(&set, &open), "Set<{ [key: string]: unknown }>");
	}

	#[test]
	fn an_optional_statement_is_a_value_that_may_be_undefined() {
		let opts = RenderOptions::default();
		let statement = json!({
			"kind": { "kind": "string" }, "optional": true, "certainty": "fact", "exact": true
		});
		assert!(slot(&opts, statement).contains("result: [string | undefined]"));
		let none = json!({
			"kind": { "kind": "none" }, "optional": true, "certainty": "fact", "exact": true
		});
		assert!(slot(&opts, none).contains("result: [undefined]"));
	}

	#[test]
	fn hints_can_be_distrusted() {
		let raw = json!([{ "key": "q", "text": "q", "statements": [
			{ "kind": { "kind": "string" }, "optional": false, "certainty": "hint", "exact": true },
			{ "kind": int(), "optional": false, "certainty": "fact", "exact": true },
		] }]);
		let opts = RenderOptions {
			hints: HintStyle::Unknown,
			..RenderOptions::default()
		};
		let out = render(&entries(raw.clone()), &[], &opts);
		assert!(out.contains("result: [unknown, number]"), "{out}");
		let out = render_default(raw);
		assert!(out.contains("result: [string, number]"), "{out}");
	}

	#[test]
	fn host_params_are_skipped_and_optional_params_are_marked() {
		let out =
			render_default(json!([{ "key": "q", "text": "SELECT $a, $b, $c, $d", "statements": [],
			"params": [
				{ "name": "a", "kind": int(), "optional": false, "required": true },
				{ "name": "b", "kind": { "kind": "string" }, "optional": false, "required": false },
				{ "name": "__host0", "kind": { "kind": "string" }, "optional": false, "required": true },
				{ "name": "c", "optional": false, "required": true },
				{ "name": "d", "kind": { "kind": "string" }, "optional": true, "required": true },
			] }]));
		assert!(
			out.contains("params: { a: number; b?: string; c: unknown; d: string | undefined }"),
			"{out}"
		);
		assert!(!out.contains("__host0"), "{out}");
		let out = render_default(json!([{ "key": "q", "text": "q", "statements": [],
			"params": [{ "name": "__host0", "optional": false, "required": true }] }]));
		assert!(out.contains("params: {} }"), "{out}");
	}

	#[test]
	fn duplicate_query_text_keeps_the_first() {
		let out = render_default(json!([
			{ "key": "RETURN 1", "text": "RETURN 1", "statements": [
				{ "kind": int(), "optional": false, "certainty": "fact", "exact": true }
			] },
			{ "key": "other", "name": "other", "text": "RETURN 1", "statements": [
				{ "kind": { "kind": "string" }, "optional": false, "certainty": "fact", "exact": true }
			] },
		]));
		assert_eq!(out.matches("\"RETURN 1\":").count(), 1, "{out}");
		assert_eq!(out.matches("{ result:").count(), 1, "{out}");
		assert!(out.contains("result: [number]"), "{out}");
	}

	#[test]
	fn query_text_is_json_escaped() {
		let out = render_default(json!([{
			"key": "k", "text": "SELECT \"a\"\nFROM t\\", "statements": [],
		}]));
		assert!(out.contains(r#""SELECT \"a\"\nFROM t\\": {"#), "{out}");
	}

	#[test]
	fn an_array_of_length_zero_is_the_empty_tuple() {
		assert_eq!(value(json!({ "kind": "array", "item": { "kind": "any" }, "length": 0 })), "[]");
		assert_eq!(
			value(json!({ "kind": "array", "item": { "kind": "int" }, "length": 2 })),
			"Array<number>"
		);
	}

	#[test]
	fn a_union_parameter_kind_renders_each_variant() {
		// The engine infers `record<task> | string` for `<record<task>> $id`.
		let query = json!([{ "key": "q", "text": "q", "statements": [], "params": [
			{ "name": "id", "optional": false, "required": true, "kind": { "kind": "union",
				"variants": [{ "kind": "record", "tables": ["task"] }, { "kind": "string" }] } },
		] }]);
		let out = render(&entries(query), &[], &RenderOptions::default());
		assert!(out.contains("params: { id: RecordId<\"task\"> | string }"), "{out}");
	}

	#[test]
	fn parameter_kinds_are_hints() {
		let query = json!([{ "key": "q", "text": "q", "statements": [], "params": [
			{ "name": "min", "kind": int(), "optional": false, "required": true },
			{ "name": "nick", "kind": { "kind": "string" }, "optional": true, "required": false },
		] }]);
		let trusted = render(&entries(query.clone()), &[], &RenderOptions::default());
		assert!(
			trusted.contains("params: { min: number; nick?: string | undefined }"),
			"{trusted}"
		);
		let opts = RenderOptions {
			hints: HintStyle::Unknown,
			..RenderOptions::default()
		};
		let distrusted = render(&entries(query), &[], &opts);
		assert!(distrusted.contains("params: { min: unknown; nick?: unknown }"), "{distrusted}");
	}

	#[test]
	fn every_statement_has_a_slot_and_an_untyped_one_is_unknown() {
		let none = json!({ "kind": "none" });
		let out = render_default(json!([{
			"key": "k", "text": "BEGIN; LET $x = 1; RETURN $x; RETURN $y; COMMIT", "statements": [
				{ "kind": none, "optional": false, "certainty": "fact", "exact": true },
				{ "kind": none, "optional": false, "certainty": "fact", "exact": true },
				{ "kind": int(), "optional": false, "certainty": "fact", "exact": true },
				{ "optional": false, "certainty": "hint", "exact": false },
				{ "kind": none, "optional": false, "certainty": "fact", "exact": true },
			]
		}]));
		assert!(
			out.contains("result: [undefined, undefined, number, unknown, undefined]"),
			"{out}"
		);
	}

	#[test]
	fn named_queries_are_listed_in_query_names() {
		let out = render_default(json!([
			{ "key": "older", "name": "older", "text": "SELECT 1", "statements": [] },
			{ "key": "older", "name": "older", "text": "SELECT 3", "statements": [] },
			{ "key": "SELECT 2", "text": "SELECT 2", "statements": [] },
		]));
		assert!(
			out.contains("export type QueryNames = {\n  \"older\": \"SELECT 1\";\n};"),
			"{out}"
		);
		let out = render_default(json!([]));
		assert!(out.contains("export type QueryNames = {};"), "{out}");
		assert!(out.contains("export type Queries = {};"), "{out}");
	}

	#[test]
	fn an_empty_list_renders_empty_registries() {
		let out = render_default(json!([]));
		for want in [
			"export type Queries = {};",
			"export type QueryNames = {};",
			"import type { BoundQuery, QueryLike, SurrealQueryable } from \"surrealdb\";",
		] {
			assert!(out.contains(want), "missing `{want}` in {out}");
		}
	}

	#[test]
	fn only_the_names_the_output_produces_are_imported() {
		// A query selecting a column called `uuid` mentions `Uuid` only in its key.
		let out = render_default(json!([{ "key": "k", "text": "SELECT uuid, Decimal FROM t",
			"statements": [{
				"kind": { "kind": "array", "item": { "kind": "datetime" } },
				"optional": false, "certainty": "fact", "exact": true
			}, {
				"kind": { "kind": "record", "tables": ["t"] },
				"optional": false, "certainty": "fact", "exact": true
			}] }]));
		let import = out.lines().find(|line| line.starts_with("import type")).expect("import line");
		assert_eq!(
			import,
			"import type { BoundQuery, DateTime, QueryLike, RecordId, SurrealQueryable } from \"surrealdb\";"
		);
	}

	#[test]
	fn the_sdk_package_is_configurable() {
		let opts = RenderOptions {
			sdk_package: "@scope/surrealdb".into(),
			..RenderOptions::default()
		};
		let out = render(&[], &[], &opts);
		assert!(out.contains("from \"@scope/surrealdb\";"), "{out}");
	}

	#[test]
	fn options_read_from_config() {
		let opts: RenderOptions =
			serde_json::from_value(json!({ "dates": "native", "hints": "unknown" }))
				.expect("reads");
		assert_eq!(opts.dates, DateStyle::Native);
		assert_eq!(opts.hints, HintStyle::Unknown);
		assert_eq!(opts.sdk_package, "surrealdb");
	}

	fn live_note(action: &str, certainty: &str) -> Value {
		json!({
			"action": action,
			"record": { "kind": "record", "tables": ["t"] },
			"result": { "kind": "objectLiteral", "fields": [
				{ "name": "a", "kind": int(), "optional": false }
			] },
			"certainty": certainty, "open": []
		})
	}

	fn live_query(text: &str, statement: Value) -> Value {
		json!({ "key": text, "text": text, "statements": [statement] })
	}

	fn live_statement(extra: Value) -> Value {
		let mut statement = json!({
			"optional": false, "certainty": "fact", "exact": true, "open": []
		});
		statement
			.as_object_mut()
			.expect("object")
			.extend(extra.as_object().expect("object").clone());
		statement
	}

	#[test]
	fn a_live_row_has_query_exit_watch_and_refusal_without_code_or_message() {
		let both = live_statement(json!({
			"live": { "diff": false, "query": "LIVE SELECT * FROM t WHERE a = 1", "exit": "delete",
				"watch": "LIVE SELECT id FROM t", "notifications": [live_note("CREATE", "fact")] },
			"liveRefusal": { "code": "E4009", "message": "m", "clauses": ["LIMIT"], "windowable": true }
		}));
		let only = live_statement(json!({
			"liveRefusal": { "code": "E4009", "message": "m", "clauses": ["ONLY"], "windowable": false }
		}));
		let text = render_default(json!([
			live_query("SELECT * FROM t WHERE a = 1 LIMIT 1", both),
			live_query("SELECT * FROM ONLY t:1", only),
		]));
		assert!(text.contains(
			"  \"SELECT * FROM t WHERE a = 1 LIMIT 1\": {\n    query: \"LIVE SELECT * FROM t WHERE a = 1\",\n    \
			 exit: \"delete\",\n    watch: \"LIVE SELECT id FROM t\",\n    refusal: { clauses: \
			 [\"LIMIT\"], windowable: true },\n  },\n"
		));
		assert!(text.contains(
			"  \"SELECT * FROM ONLY t:1\": { refusal: { clauses: [\"ONLY\"], windowable: false } },\n"
		));
		assert!(!text.contains("E4009") && !text.contains("\"m\""));
		// Only the entry with a `live` has notifications.
		let notifications = text.split("type LiveNotifications = {").nth(1).expect("types");
		let notifications = notifications.split("\n};").next().expect("end");
		assert!(notifications.contains("SELECT * FROM t WHERE a = 1 LIMIT 1"));
		assert!(!notifications.contains("ONLY"));
	}

	#[test]
	fn a_notification_follows_the_sdks_live_message() {
		let statement = live_statement(json!({
			"live": { "diff": false, "query": "LIVE SELECT * FROM t", "notifications": [
				live_note("CREATE", "fact"), live_note("KILLED", "fact")
			] }
		}));
		let text = render_default(json!([live_query("SELECT * FROM t", statement)]));
		assert!(text.contains(
			"{ queryId: Uuid; action: \"CREATE\"; recordId: RecordId<\"t\">; value: { a: number } } | \
			 { queryId: Uuid; action: \"KILLED\"; recordId?: undefined; value?: undefined };"
		));
		assert!(text.contains(
			"import type { BoundQuery, QueryLike, RecordId, SurrealQueryable, Uuid } from"
		));
	}

	#[test]
	fn a_hinted_notification_is_unknown_where_hints_are_distrusted() {
		let statement = live_statement(json!({
			"live": { "diff": false, "query": "LIVE SELECT * FROM t", "notifications": [
				live_note("UPDATE", "hint")
			] }
		}));
		let queries = entries(json!([live_query("SELECT * FROM t", statement)]));
		let trusted = render(&queries, &[], &RenderOptions::default());
		assert!(trusted.contains("recordId: RecordId<\"t\">; value: { a: number } }"));
		let distrusted = render(
			&queries,
			&[],
			&RenderOptions {
				hints: HintStyle::Unknown,
				..RenderOptions::default()
			},
		);
		assert!(distrusted.contains("action: \"UPDATE\"; recordId: unknown; value: unknown }"));
	}

	#[test]
	fn a_query_the_engine_could_not_type_is_in_no_registry() {
		let text = render_default(json!([
			{ "key": "Bad", "name": "Bad", "text": "SELEC", "statements": [],
				"error": { "code": "S0001", "message": "bad" } },
			live_query("SELECT 1", live_statement(json!({}))),
		]));
		assert!(!text.contains("SELEC\""), "{text}");
		assert!(!text.contains("\"Bad\""), "{text}");
		assert!(text.contains("\"SELECT 1\": { result: [unknown]; params: {} };"));
		assert!(text.contains("export type QueryNames = {};"));
	}

	#[test]
	fn register_augments_the_named_package_and_is_json_quoted() {
		let register = |package: &str| {
			render(
				&[],
				&[],
				&RenderOptions {
					register: Some(package.to_string()),
					..RenderOptions::default()
				},
			)
		};
		assert!(register("surrealkit-query").ends_with(
			"\ndeclare module \"surrealkit-query\" {\n  interface Register {\n    queries: Queries;\n    \
			 liveQueries: LiveQueries;\n  }\n}\n"
		));
		assert!(register("a\"b").contains("declare module \"a\\\"b\" {"));
		assert!(!render_default(json!([])).contains("declare module"));
	}

	#[test]
	fn register_imports_the_package_it_augments() {
		let text = render(
			&[],
			&[],
			&RenderOptions {
				register: Some("surrealkit-query".to_string()),
				..RenderOptions::default()
			},
		);
		let import = text.find("\nimport type {} from \"surrealkit-query\";\n").expect("import");
		assert!(import < text.find("declare module").expect("augmentation"));
		assert!(!render_default(json!([])).contains("import type {} from"));
	}
}
