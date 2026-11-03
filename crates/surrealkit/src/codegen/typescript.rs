//! A [`TypesDocument`] rendered as a types-only TypeScript module for the surrealdb JS SDK.
//!
//! The module holds an `interface` per table, `Tables`, `Functions`, `Params`, a `Queries`
//! registry keyed by exact query text, `QueryNames`, and `Typed<T>`, which gives a client's
//! `query()` the registry's result type for known query text.
//!
//! `Queries` is a type alias and the tables are interfaces: an interface never satisfies an index
//! signature, so a consumer constraining a registry to `Record<string, …>` needs the alias.
//!
//! # Optionality has two spellings, and the position picks
//!
//! A SurrealQL `option<string>` is `Either([None, String])`. As the type of an object member it is
//! `nick?: string`, because the key may be absent. Anywhere a value stands on its own (a tuple
//! slot, an array element, a statement result) it is `undefined | string`, because there is no key
//! to omit. The two are not interchangeable: `{ nick?: string }` accepts `{}` and
//! `{ nick: string | undefined }` does not.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::document::{
	Certainty, FieldStep, FieldTypes, FunctionTypes, NamedType, ParamTypes, QueryParam, QueryTypes,
	StatementType, TableTypes, TypesDocument,
};
use super::kind::{KindValue, LiteralValue};

/// Query parameters with this prefix are filled by the host-language template, not by the caller.
const HOST_PARAM_PREFIX: &str = "__host";

/// The SDK names the module can import, in import order.
const SDK_NAMES: &[&str] = &[
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
	"Range",
	"RecordId",
	"SurrealQueryable",
	"Table",
	"Uuid",
];

/// Names a table's interface or a type's alias must not take: the names the module defines, the
/// SDK names it imports, and the globals its types are written in terms of. An interface named
/// `Date` or `Set` would shadow the global for every type below it, so the collision is resolved
/// by suffix.
const RESERVED_NAMES: &[&str] = &[
	"Tables",
	"Functions",
	"Params",
	"Queries",
	"QueryNames",
	"QueryOf",
	"TypedQueryable",
	"Typed",
	"BindingsArg",
	"Array",
	"Boolean",
	"Date",
	"Map",
];

/// The globals a generated type or a consumer's own signature routinely names.
const RESERVED_GLOBALS: &[&str] = &[
	"Number",
	"Object",
	"Partial",
	"Promise",
	"Readonly",
	"Record",
	"Set",
	"String",
	"Symbol",
	"Uint8Array",
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

/// What [`render`] does with the choices a document leaves open.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RenderOptions {
	pub dates: DateStyle,
	pub hints: HintStyle,
	/// The package the module imports its SDK types from.
	pub sdk_package: String,
}

impl Default for RenderOptions {
	fn default() -> Self {
		Self {
			dates: DateStyle::default(),
			hints: HintStyle::default(),
			sdk_package: "surrealdb".to_string(),
		}
	}
}

/// Renders the complete module.
pub fn render(doc: &TypesDocument, opts: &RenderOptions) -> String {
	let mut emitter = Emitter {
		opts,
		used: BTreeSet::from(["SurrealQueryable"]),
		aliases: HashMap::new(),
	};
	let tables: Vec<&TableTypes> =
		doc.tables.iter().filter(|table| !table.name.starts_with("__")).collect();
	let (table_names, type_names) = interface_names(&tables, &doc.types);
	emitter.aliases =
		type_names.iter().map(|(name, alias)| (name.as_str(), alias.clone())).collect();

	let mut blocks: Vec<String> = Vec::new();
	for table in &tables {
		let name = table_names.get(&table.name).cloned().unwrap_or_else(|| table.name.clone());
		blocks.push(emitter.table(table, &name));
	}
	blocks.push(tables_block(&tables, &table_names));
	for named in &doc.types {
		let alias = type_names.get(&named.name).cloned().unwrap_or_else(|| named.name.clone());
		blocks.push(emitter.alias(named, &alias));
	}
	blocks.push(emitter.functions(&doc.functions));
	blocks.push(emitter.params(&doc.params));
	blocks.push(emitter.queries(&doc.queries));
	blocks.push(query_names(&doc.queries));
	blocks.push(TYPED_CLIENT.to_string());

	let imports: Vec<&str> =
		SDK_NAMES.iter().copied().filter(|name| emitter.used.contains(name)).collect();
	let mut out = String::from(HEADER);
	out.push('\n');
	out.push_str(&format!(
		"import type {{ {} }} from {};\n\n",
		imports.join(", "),
		ts_string(&opts.sdk_package)
	));
	out.push_str(&blocks.join("\n"));
	out
}

const HEADER: &str = "\
// Generated by SurrealKit from the engine's types document. Do not edit.
// Regenerate with `surreal kit generate`.
//
// Type a client's `query()` by exact query text:
//
//   const db = new Surreal() as Typed<Surreal>;
//   const [people] = await db.query(\"SELECT name FROM person\");
";

/// The typed client, emitted verbatim. `Query` is not exported by the SDK, hence the `declare
/// const` probe for its type.
const TYPED_CLIENT: &str = "\
declare const queryable: SurrealQueryable;
/** The SDK's `Query<R>`, which it does not export by name. */
export type QueryOf<R extends unknown[]> = ReturnType<typeof queryable.query<R>>;
type BindingsArg<K extends keyof Queries> = {} extends Queries[K][\"params\"]
  ? [bindings?: Queries[K][\"params\"]]
  : [bindings: Queries[K][\"params\"]];
/** `query()` typed by exact query text; other text falls through to the SDK's own overloads. */
export interface TypedQueryable {
  query<K extends keyof Queries>(query: K, ...bindings: BindingsArg<K>): QueryOf<Queries[K][\"result\"]>;
}
/** A client whose `query()` returns the registry's result type for known query text. */
export type Typed<T extends SurrealQueryable> = TypedQueryable & T;
";

fn tables_block(tables: &[&TableTypes], names: &HashMap<String, String>) -> String {
	let mut out = String::from("/** Every table in the schema, keyed by its SurrealQL name. */\n");
	if tables.is_empty() {
		out.push_str("export interface Tables {}\n");
		return out;
	}
	out.push_str("export interface Tables {\n");
	for table in tables {
		let name = names.get(&table.name).map_or(table.name.as_str(), String::as_str);
		out.push_str(&format!("  {}: {};\n", object_key(&table.name), name));
	}
	out.push_str("}\n");
	out
}

fn query_names(queries: &[QueryTypes]) -> String {
	let mut seen = BTreeSet::new();
	let mut rows = Vec::new();
	for query in queries {
		let Some(name) = &query.name else {
			continue;
		};
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

/// Where a rendered type sits: standing on its own, or as the type of an object member that owns
/// a key which can be absent.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Position {
	Value,
	Property,
}

/// A rendered type, and whether the position's key may be omitted (`Property` only).
struct Rendered {
	text: String,
	optional: bool,
}

/// Object positions, relative to the kind being rendered, that admit undeclared keys. A path that
/// is empty is the kind itself.
type Open<'a> = [&'a [FieldStep]];

/// The kind itself is open.
const ROOT: &Open<'static> = &[&[]];

struct Emitter<'a> {
	opts: &'a RenderOptions,
	/// SDK names the output uses, recorded where each is produced.
	used: BTreeSet<&'static str>,
	/// `DEFINE TYPE` name to the alias that carries it.
	aliases: HashMap<&'a str, String>,
}

impl<'a> Emitter<'a> {
	fn note(&mut self, name: &'static str) -> &'static str {
		self.used.insert(name);
		name
	}

	fn ty(&mut self, kind: &KindValue, position: Position, open: &Open<'_>) -> Rendered {
		match position {
			Position::Value => Rendered {
				text: self.value(kind, open),
				optional: false,
			},
			Position::Property => {
				let (present, optional) = strip_none(kind);
				Rendered {
					text: self.value(&present, open),
					optional,
				}
			}
		}
	}

	/// The value spelling. Recursion stays here: an array element, a union member and a tuple
	/// item are all values, and only an object member crosses back into a key.
	fn value(&mut self, kind: &KindValue, open: &Open<'_>) -> String {
		match kind {
			KindValue::Any
			| KindValue::Date
			| KindValue::Time
			| KindValue::Timestamp
			| KindValue::Interval
			| KindValue::BigDecimal
			| KindValue::Regex
			| KindValue::Function(..)
			| KindValue::Unknown(_) => "unknown".to_string(),
			KindValue::None => "undefined".to_string(),
			KindValue::Null => "null".to_string(),
			KindValue::Bool => "boolean".to_string(),
			KindValue::Bytes => "Uint8Array".to_string(),
			KindValue::Datetime => match self.opts.dates {
				DateStyle::Class => self.note("DateTime").to_string(),
				DateStyle::Native => "Date".to_string(),
			},
			KindValue::Decimal => self.note("Decimal").to_string(),
			KindValue::Duration => self.note("Duration").to_string(),
			KindValue::F16
			| KindValue::F32
			| KindValue::Float
			| KindValue::I8
			| KindValue::I16
			| KindValue::I32
			| KindValue::Int
			| KindValue::U8
			| KindValue::U16
			| KindValue::U32
			| KindValue::U64
			| KindValue::Number => "number".to_string(),
			KindValue::Object => "{ [key: string]: unknown }".to_string(),
			KindValue::String => "string".to_string(),
			KindValue::Uuid => self.note("Uuid").to_string(),
			KindValue::Range => {
				let range = self.note("Range");
				format!("{range}<unknown, unknown>")
			}
			KindValue::Table(tables) => {
				let table = self.note("Table");
				with_tables(table, tables)
			}
			KindValue::Record(tables) => {
				let record = self.note("RecordId");
				with_tables(record, tables)
			}
			KindValue::Geometry(names) => self.geometry(names),
			KindValue::Either(members) => {
				let mut rendered: Vec<String> = Vec::new();
				for member in members {
					let text = self.value(member, open);
					if !rendered.contains(&text) {
						rendered.push(text);
					}
				}
				rendered.join(" | ")
			}
			KindValue::Set(inner, _) => {
				let inner = self.value(inner, &into_element(open));
				format!("Set<{inner}>")
			}
			KindValue::Array(inner, _) => {
				let inner = self.value(inner, &into_element(open));
				format!("Array<{inner}>")
			}
			KindValue::Literal(literal) => self.literal(literal, open),
			KindValue::File(_) => self.note("FileRef").to_string(),
			KindValue::Enum(labels) => {
				if labels.is_empty() {
					"never".to_string()
				} else {
					labels.iter().map(|label| ts_string(label)).collect::<Vec<_>>().join(" | ")
				}
			}
			KindValue::Named(name) => {
				self.aliases.get(name.as_str()).cloned().unwrap_or_else(|| "unknown".to_string())
			}
		}
	}

	fn geometry(&mut self, names: &[String]) -> String {
		let mut members: Vec<&'static str> = Vec::new();
		for name in names {
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

	fn literal(&mut self, literal: &LiteralValue, open: &Open<'_>) -> String {
		match literal {
			LiteralValue::String(value) => ts_string(value),
			LiteralValue::Integer(value) => value.to_string(),
			LiteralValue::Float(value) if value.is_finite() => value.to_string(),
			LiteralValue::Float(_) => "number".to_string(),
			LiteralValue::Decimal(_) => self.note("Decimal").to_string(),
			LiteralValue::Duration {
				..
			} => self.note("Duration").to_string(),
			LiteralValue::Bool(value) => value.to_string(),
			// A literal array is a tuple: each item is a value, so an `option<T>` item stays
			// `T | undefined`, since dropping the member would shorten the tuple.
			LiteralValue::Array(items) => {
				let items: Vec<String> = items.iter().map(|item| self.value(item, &[])).collect();
				format!("[{}]", items.join(", "))
			}
			LiteralValue::Object(fields) => self.object(fields, open),
		}
	}

	/// An object type: the one place a key exists, so members render as properties.
	fn object(&mut self, fields: &BTreeMap<String, KindValue>, open: &Open<'_>) -> String {
		let mut parts = Vec::new();
		for (name, kind) in fields {
			let rendered = self.ty(kind, Position::Property, &into_field(open, name));
			parts.push(member(name, &rendered));
		}
		if is_open(open) {
			parts.push(INDEX_SIGNATURE.to_string());
		}
		object_text(&parts)
	}

	fn table(&mut self, table: &TableTypes, name: &str) -> String {
		let mut members: Vec<String> = Vec::new();
		let declared = |wanted: &str| {
			table.fields.iter().any(|field| field.path.len() == 1 && field.path[0] == wanted)
		};
		if !declared("id") {
			let id = self.note("RecordId");
			members.push(format!("  id: {};", with_tables(id, std::slice::from_ref(&table.name))));
		}
		if let Some(relation) = &table.relation {
			for (key, tables) in [("in", &relation.in_), ("out", &relation.out)] {
				if !declared(key) {
					let record = self.note("RecordId");
					members.push(format!("  {key}: {};", with_tables(record, tables)));
				}
			}
		}
		let tree = build_tree(&table.fields);
		for (key, child) in &tree.fields {
			let rendered = self.node(child, Position::Property);
			members.push(format!("  {};", member(key, &rendered)));
		}
		// Only a schemafull table's declared fields are the whole row; a schemaless row may carry
		// any key, and every declared member's type is assignable to `unknown`.
		if !table.schemafull {
			members.push(format!("  {INDEX_SIGNATURE};"));
		}
		format!("export interface {name} {{\n{}\n}}\n", members.join("\n"))
	}

	/// A node's type. A node with structure below it renders from that structure, because a
	/// `DEFINE FIELD settings TYPE object` says nothing a reader can use while its subfields say
	/// everything; a leaf renders from its declared kind. A node carrying both an element step and
	/// fields (`a.b` and `a[*].c`) renders as the union of the two.
	fn node(&mut self, node: &Node<'_>, position: Position) -> Rendered {
		let field = node.field;
		let open: &Open<'_> = if field.is_some_and(|field| field.flexible) {
			ROOT
		} else {
			&[]
		};
		let own_optional = field.is_some_and(|field| {
			position == Position::Property && (field.optional || strip_none(&field.kind_value).1)
		});
		if let Some(field) = field
			&& field.certainty == Certainty::Hint
			&& self.opts.hints == HintStyle::Unknown
		{
			return Rendered {
				text: "unknown".to_string(),
				optional: own_optional,
			};
		}

		let array = node.element.as_ref().map(|element| {
			let inner = self.node(element, Position::Value).text;
			let container = match field.map(|field| strip_none(&field.kind_value).0) {
				Some(KindValue::Set(..)) => "Set",
				_ => "Array",
			};
			format!("{container}<{inner}>")
		});
		let object = if node.fields.is_empty() {
			None
		} else {
			let mut parts: Vec<String> = Vec::new();
			for (key, child) in &node.fields {
				let rendered = self.node(child, Position::Property);
				parts.push(member(key, &rendered));
			}
			if is_open(open) {
				parts.push(INDEX_SIGNATURE.to_string());
			}
			Some(object_text(&parts))
		};
		match (array, object, field) {
			(Some(array), Some(object), _) => Rendered {
				text: format!("{array} | {object}"),
				optional: own_optional,
			},
			(Some(text), None, _) | (None, Some(text), _) => Rendered {
				text,
				optional: own_optional,
			},
			(None, None, Some(field)) => {
				let mut rendered = self.ty(&field.kind_value, position, open);
				rendered.optional |= own_optional;
				rendered
			}
			(None, None, None) => Rendered {
				text: "unknown".to_string(),
				optional: false,
			},
		}
	}

	fn alias(&mut self, named: &NamedType, alias: &str) -> String {
		let text = self.value(&named.kind_value, &[]);
		format!("export type {alias} = {text};\n")
	}

	fn functions(&mut self, functions: &[FunctionTypes]) -> String {
		let mut out =
			String::from("/** Every `DEFINE FUNCTION`, with its argument and return types. */\n");
		if functions.is_empty() {
			out.push_str("export interface Functions {}\n");
			return out;
		}
		out.push_str("export interface Functions {\n");
		for function in functions {
			let args: Vec<String> = function
				.args
				.iter()
				.map(|arg| {
					let text = self.value(&arg.kind_value, &[]);
					if is_identifier(&arg.name) {
						format!("{}: {text}", arg.name)
					} else {
						text
					}
				})
				.collect();
			let returns = match &function.returns_value {
				Some(kind) => self.value(kind, &[]),
				None => "unknown".to_string(),
			};
			out.push_str(&format!(
				"  {}: {{ args: [{}]; returns: {returns} }};\n",
				ts_string(&function.name),
				args.join(", ")
			));
		}
		out.push_str("}\n");
		out
	}

	fn params(&mut self, params: &[ParamTypes]) -> String {
		let mut out =
			String::from("/** Every `DEFINE PARAM`, keyed by its name without the `$`. */\n");
		if params.is_empty() {
			out.push_str("export interface Params {}\n");
			return out;
		}
		out.push_str("export interface Params {\n");
		for param in params {
			let text = match &param.kind_value {
				Some(kind) => self.value(kind, &[]),
				None => "unknown".to_string(),
			};
			out.push_str(&format!("  {}: {text};\n", object_key(&param.name)));
		}
		out.push_str("}\n");
		out
	}

	/// The query registry: one row per distinct query text.
	fn queries(&mut self, queries: &[QueryTypes]) -> String {
		let mut seen = BTreeSet::new();
		let mut rows = Vec::new();
		for query in queries {
			if !seen.insert(query.text.as_str()) {
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

	/// One element per statement that has a slot in the response, in source order. A statement
	/// without a kind (`BEGIN`, `COMMIT`, `CANCEL`) has none, so the SDK response skips it.
	fn result_tuple(&mut self, statements: &[StatementType]) -> String {
		let slots: Vec<String> =
			statements.iter().filter_map(|statement| self.statement(statement)).collect();
		format!("[{}]", slots.join(", "))
	}

	fn statement(&mut self, statement: &StatementType) -> Option<String> {
		let kind = statement.kind_value.as_ref()?;
		if statement.certainty == Certainty::Hint && self.opts.hints == HintStyle::Unknown {
			return Some("unknown".to_string());
		}
		let open: Vec<&[FieldStep]> = statement.open.iter().map(Vec::as_slice).collect();
		Some(self.ty(kind, Position::Value, &open).text)
	}

	/// The named-parameter object. This is not a `Property` position: the `?` answers whether the
	/// query needs the argument at all, while a property's `?` answers whether the value can be
	/// absent. Folding a parameter's `option<T>` into the key as well would let callers omit a key
	/// the query requires.
	fn query_params(&mut self, params: &[QueryParam]) -> String {
		let mut parts = Vec::new();
		for param in params {
			if param.name.starts_with(HOST_PARAM_PREFIX) {
				continue;
			}
			let text = match &param.kind_value {
				Some(kind) => self.value(kind, &[]),
				None => "unknown".to_string(),
			};
			let marker = if param.required {
				""
			} else {
				"?"
			};
			parts.push(format!("{}{marker}: {text}", object_key(&param.name)));
		}
		object_text(&parts)
	}
}

const INDEX_SIGNATURE: &str = "[key: string]: unknown";

fn is_open(open: &Open<'_>) -> bool {
	open.iter().any(|path| path.is_empty())
}

/// The paths that continue into the object member `name`.
fn into_field<'a>(open: &Open<'a>, name: &str) -> Vec<&'a [FieldStep]> {
	open.iter()
		.filter_map(|path| match path.split_first() {
			Some((FieldStep::Field(field), rest)) if field == name => Some(rest),
			_ => None,
		})
		.collect()
}

/// The paths that continue into a collection's elements.
fn into_element<'a>(open: &Open<'a>) -> Vec<&'a [FieldStep]> {
	open.iter()
		.filter_map(|path| match path.split_first() {
			Some((FieldStep::Element, rest)) => Some(rest),
			_ => None,
		})
		.collect()
}

fn member(name: &str, rendered: &Rendered) -> String {
	let marker = if rendered.optional {
		"?"
	} else {
		""
	};
	format!("{}{marker}: {}", object_key(name), rendered.text)
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

/// Splits `Either[None, …]` into the present type and an optional flag. Only the property
/// position runs this: it turns an `undefined` union member into a `?` on a key.
fn strip_none(kind: &KindValue) -> (KindValue, bool) {
	let KindValue::Either(members) = kind else {
		return (kind.clone(), false);
	};
	let present: Vec<KindValue> =
		members.iter().filter(|member| **member != KindValue::None).cloned().collect();
	if present.len() == members.len() {
		return (kind.clone(), false);
	}
	let kind = match present.len() {
		0 => KindValue::None,
		1 => present.into_iter().next().unwrap_or(KindValue::None),
		_ => KindValue::Either(present),
	};
	(kind, true)
}

/// The structure a table's flat `DEFINE FIELD` list describes: `settings.timezone` and
/// `items[*].price` are separate declarations naming positions inside another field.
#[derive(Default)]
struct Node<'a> {
	field: Option<&'a FieldTypes>,
	fields: Vec<(String, Node<'a>)>,
	element: Option<Box<Node<'a>>>,
}

impl<'a> Node<'a> {
	fn child(&mut self, name: &str) -> &mut Node<'a> {
		let index = match self.fields.iter().position(|(existing, _)| existing == name) {
			Some(index) => index,
			None => {
				self.fields.push((name.to_string(), Node::default()));
				self.fields.len() - 1
			}
		};
		&mut self.fields[index].1
	}

	fn element(&mut self) -> &mut Node<'a> {
		self.element.get_or_insert_with(Box::default)
	}
}

fn build_tree(fields: &[FieldTypes]) -> Node<'_> {
	let mut root = Node::default();
	for field in fields {
		let mut node = &mut root;
		for step in &field.steps {
			node = match step {
				FieldStep::Field(name) => node.child(name),
				FieldStep::Element => node.element(),
			};
		}
		node.field = Some(field);
	}
	root
}

/// A stable name per table and per `DEFINE TYPE`: `PascalCase`, never colliding with a reserved
/// name or another's. Tables are named before types, each in name order, so a collision's suffix
/// goes to whichever comes later and does not depend on discovery order.
fn interface_names(
	tables: &[&TableTypes],
	types: &[NamedType],
) -> (HashMap<String, String>, HashMap<String, String>) {
	let mut taken: BTreeSet<String> = RESERVED_NAMES
		.iter()
		.chain(RESERVED_GLOBALS)
		.chain(SDK_NAMES)
		.map(ToString::to_string)
		.collect();
	let mut claim = |source: &str| {
		let base = pascal_case(source);
		let mut name = base.clone();
		let mut suffix = 2;
		while !taken.insert(name.clone()) {
			name = format!("{base}{suffix}");
			suffix += 1;
		}
		name
	};
	let mut table_names: Vec<&str> = tables.iter().map(|table| table.name.as_str()).collect();
	table_names.sort_unstable();
	let mut type_names: Vec<&str> = types.iter().map(|named| named.name.as_str()).collect();
	type_names.sort_unstable();
	let tables = table_names.into_iter().map(|name| (name.to_string(), claim(name))).collect();
	let types = type_names.into_iter().map(|name| (name.to_string(), claim(name))).collect();
	(tables, types)
}

/// `person` is `Person`, `team_member` is `TeamMember`, `2fa` is `T2fa`.
fn pascal_case(name: &str) -> String {
	let mut out = String::new();
	for word in name.split(|c: char| !c.is_ascii_alphanumeric()) {
		let mut chars = word.chars();
		if let Some(first) = chars.next() {
			out.extend(first.to_uppercase());
			out.push_str(chars.as_str());
		}
	}
	match out.chars().next() {
		None => "Table".to_string(),
		Some(first) if first.is_ascii_digit() => format!("T{out}"),
		Some(_) => out,
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

	fn kind(raw: Value) -> KindValue {
		KindValue::from_json(&raw)
	}

	fn emitter(opts: &RenderOptions) -> Emitter<'_> {
		Emitter {
			opts,
			used: BTreeSet::new(),
			aliases: HashMap::new(),
		}
	}

	fn value_with(opts: &RenderOptions, raw: Value) -> String {
		emitter(opts).ty(&kind(raw), Position::Value, &[]).text
	}

	fn value(raw: Value) -> String {
		value_with(&RenderOptions::default(), raw)
	}

	fn property(raw: Value) -> (String, bool) {
		let opts = RenderOptions::default();
		let rendered = emitter(&opts).ty(&kind(raw), Position::Property, &[]);
		(rendered.text, rendered.optional)
	}

	fn document(raw: Value) -> TypesDocument {
		let mut doc = json!({ "format": "surrealdb.types", "version": 1 });
		if let (Some(base), Some(extra)) = (doc.as_object_mut(), raw.as_object()) {
			base.extend(extra.clone());
		}
		TypesDocument::from_json(&doc.to_string()).expect("test document is valid")
	}

	fn field(path: &str, kind_value: Value) -> Value {
		let segments: Vec<&str> = path.split('.').collect();
		json!({
			"path": segments,
			"steps": segments.iter().map(|s| json!({ "field": s })).collect::<Vec<_>>(),
			"kind": "", "kind_value": kind_value, "certainty": "fact",
		})
	}

	fn table(name: &str, schemafull: bool, fields: Vec<Value>) -> Value {
		json!({ "name": name, "schemafull": schemafull, "fields": fields })
	}

	fn render_default(raw: Value) -> String {
		render(&document(raw), &RenderOptions::default())
	}

	#[test]
	fn scalars_map_to_their_typescript_types() {
		for (raw, want) in [
			(json!("Any"), "unknown"),
			(json!("None"), "undefined"),
			(json!("Null"), "null"),
			(json!("Bool"), "boolean"),
			(json!("Bytes"), "Uint8Array"),
			(json!("Datetime"), "DateTime"),
			(json!("Decimal"), "Decimal"),
			(json!("Duration"), "Duration"),
			(json!("String"), "string"),
			(json!("Uuid"), "Uuid"),
			(json!("Object"), "{ [key: string]: unknown }"),
			(json!("Range"), "Range<unknown, unknown>"),
		] {
			assert_eq!(value(raw.clone()), want, "{raw}");
		}
		for name in
			["F16", "F32", "Float", "I8", "I16", "I32", "Int", "U8", "U16", "U32", "U64", "Number"]
		{
			assert_eq!(value(json!(name)), "number", "{name}");
		}
		for name in ["Date", "Time", "Timestamp", "Interval", "BigDecimal", "Regex"] {
			assert_eq!(value(json!(name)), "unknown", "{name}");
		}
		assert_eq!(value(json!({ "Function": [null, null] })), "unknown");
		assert_eq!(value(json!({ "Named": "money" })), "unknown");
		assert_eq!(value(json!({ "Quux": 1 })), "unknown");
	}

	#[test]
	fn datetime_is_a_class_or_a_native_date() {
		let native = RenderOptions {
			dates: DateStyle::Native,
			..RenderOptions::default()
		};
		assert_eq!(value_with(&native, json!("Datetime")), "Date");
		let mut emitter = emitter(&native);
		emitter.ty(&kind(json!("Datetime")), Position::Value, &[]);
		assert!(!emitter.used.contains("DateTime"));
	}

	#[test]
	fn tables_records_and_geometry() {
		assert_eq!(value(json!({ "Table": [] })), "Table");
		assert_eq!(value(json!({ "Table": ["a", "b"] })), r#"Table<"a" | "b">"#);
		assert_eq!(value(json!({ "Record": [] })), "RecordId");
		assert_eq!(value(json!({ "Record": ["a", "b"] })), r#"RecordId<"a" | "b">"#);
		assert_eq!(value(json!({ "Geometry": [] })), "Geometry");
		assert_eq!(value(json!({ "Geometry": ["Point"] })), "GeometryPoint");
		assert_eq!(
			value(
				json!({ "Geometry": ["Point", "Line", "Polygon", "MultiPoint", "MultiLine", "MultiPolygon", "Collection"] })
			),
			"GeometryPoint | GeometryLine | GeometryPolygon | GeometryMultiPoint | GeometryMultiLine | GeometryMultiPolygon | GeometryCollection"
		);
		assert_eq!(value(json!({ "Geometry": ["Point", "Quux"] })), "Geometry");
	}

	#[test]
	fn collections_and_files() {
		assert_eq!(value(json!({ "Array": ["String", null] })), "Array<string>");
		assert_eq!(value(json!({ "Array": ["Int", 2] })), "Array<number>");
		assert_eq!(value(json!({ "Set": ["String", null] })), "Set<string>");
		assert_eq!(value(json!({ "File": ["avatars"] })), "FileRef");
		assert_eq!(value(json!({ "Enum": ["a", "b"] })), r#""a" | "b""#);
	}

	#[test]
	fn unions_dedupe_non_adjacent_members_in_order() {
		assert_eq!(value(json!({ "Either": ["String", "Int", "String"] })), "string | number");
		assert_eq!(value(json!({ "Either": ["String", "Int", "Null"] })), "string | number | null");
	}

	#[test]
	fn an_option_is_a_key_marker_as_a_property_and_a_union_member_as_a_value() {
		let option = json!({ "Either": ["None", "String"] });
		assert_eq!(value(option.clone()), "undefined | string");
		assert_eq!(property(option), ("string".to_string(), true));
		assert_eq!(property(json!("String")), ("string".to_string(), false));
		assert_eq!(
			property(json!({ "Either": ["None", "String", "Int"] })),
			("string | number".to_string(), true)
		);
	}

	#[test]
	fn nesting_a_value_never_makes_it_a_property() {
		let option = json!({ "Either": ["None", "String"] });
		assert_eq!(value(json!({ "Array": [option, null] })), "Array<undefined | string>");
		assert_eq!(
			value(json!({ "Literal": { "Array": ["Int", option] } })),
			"[number, undefined | string]"
		);
		assert_eq!(
			value(json!({ "Array": [{ "Literal": { "Object": { "nick": option } } }, null] })),
			"Array<{ nick?: string }>"
		);
	}

	#[test]
	fn literals() {
		assert_eq!(value(json!({ "Literal": { "String": "say \"hi\"" } })), r#""say \"hi\"""#);
		assert_eq!(value(json!({ "Literal": { "Integer": 42 } })), "42");
		assert_eq!(value(json!({ "Literal": { "Float": 1.5 } })), "1.5");
		assert_eq!(value(json!({ "Literal": { "Decimal": "1.25" } })), "Decimal");
		assert_eq!(
			value(json!({ "Literal": { "Duration": { "secs": 90, "nanos": 0 } } })),
			"Duration"
		);
		assert_eq!(value(json!({ "Literal": { "Bool": true } })), "true");
		assert_eq!(value(json!({ "Literal": { "Bool": false } })), "false");
		assert_eq!(value(json!({ "Literal": { "Array": ["String", "Int"] } })), "[string, number]");
		assert_eq!(
			value(json!({ "Literal": { "Object": {
				"name": "String",
				"nick": { "Either": ["None", "String"] },
				"weird key": "Int",
			} } })),
			r#"{ name: string; nick?: string; "weird key": number }"#
		);
	}

	#[test]
	fn a_literal_union_of_strings() {
		assert_eq!(
			value(json!({ "Either": [
				{ "Literal": { "String": "draft" } },
				{ "Literal": { "String": "live" } },
			] })),
			r#""draft" | "live""#
		);
	}

	#[test]
	fn open_paths_add_an_index_signature_to_the_objects_they_name() {
		let opts = RenderOptions::default();
		let row = kind(json!({ "Array": [{ "Literal": { "Object": {
			"id": { "Record": ["t"] },
			"address": { "Literal": { "Object": { "city": "String" } } },
		} } }, null] }));
		let at = |paths: &[Vec<FieldStep>]| {
			let open: Vec<&[FieldStep]> = paths.iter().map(Vec::as_slice).collect();
			emitter(&opts).ty(&row, Position::Value, &open).text
		};
		assert_eq!(at(&[]), r#"Array<{ address: { city: string }; id: RecordId<"t"> }>"#);
		assert_eq!(
			at(&[vec![FieldStep::Element]]),
			r#"Array<{ address: { city: string }; id: RecordId<"t">; [key: string]: unknown }>"#
		);
		assert_eq!(
			at(&[vec![FieldStep::Element, FieldStep::Field("address".into())]]),
			r#"Array<{ address: { city: string; [key: string]: unknown }; id: RecordId<"t"> }>"#
		);
	}

	#[test]
	fn an_open_path_passes_through_an_option_and_a_union() {
		let opts = RenderOptions::default();
		let row = kind(json!({ "Either": ["None", { "Literal": { "Object": { "a": "Int" } } }] }));
		let open: [&[FieldStep]; 1] = [&[]];
		assert_eq!(
			emitter(&opts).ty(&row, Position::Value, &open).text,
			"undefined | { a: number; [key: string]: unknown }"
		);
	}

	#[test]
	fn a_named_kind_refers_to_its_alias_or_is_unknown() {
		let out = render_default(json!({
			"types": [{ "name": "money", "kind": "decimal", "kind_value": "Decimal" }],
			"tables": [table("cart", true, vec![
				field("total", json!({ "Named": "money" })),
				field("other", json!({ "Named": "gone" })),
			])],
		}));
		assert!(out.contains("export type Money = Decimal;"), "{out}");
		assert!(out.contains("total: Money;"), "{out}");
		assert!(out.contains("other: unknown;"), "{out}");
	}

	#[test]
	fn a_type_alias_shares_the_collision_suffixing_with_tables() {
		let out = render_default(json!({
			"types": [{ "name": "money", "kind": "decimal", "kind_value": "Decimal" }],
			"tables": [table("money", true, vec![])],
		}));
		assert!(out.contains("export interface Money {"), "{out}");
		assert!(out.contains("export type Money2 = Decimal;"), "{out}");
	}

	#[test]
	fn a_schemaless_table_gets_an_index_signature() {
		let out = render_default(json!({ "tables": [table("event", false, vec![])] }));
		assert!(
			out.contains(
				"export interface Event {\n  id: RecordId<\"event\">;\n  [key: string]: unknown;\n}"
			),
			"{out}"
		);
		let out = render_default(json!({ "tables": [table("event", true, vec![])] }));
		assert!(!out.contains("[key: string]"), "{out}");
	}

	#[test]
	fn a_flexible_object_field_gets_an_index_signature_inside_it() {
		let mut flexible = field("meta", json!("Object"));
		flexible["flexible"] = json!(true);
		let mut literal = field("shape", json!({ "Literal": { "Object": { "a": "Int" } } }));
		literal["flexible"] = json!(true);
		let mut structured = field("settings", json!("Object"));
		structured["flexible"] = json!(true);
		let out = render_default(json!({ "tables": [table("t", true, vec![
			flexible, literal, structured, field("settings.theme", json!("String")),
		])] }));
		assert!(out.contains("meta: { [key: string]: unknown };"), "{out}");
		assert!(out.contains("shape: { a: number; [key: string]: unknown };"), "{out}");
		assert!(out.contains("settings: { theme: string; [key: string]: unknown };"), "{out}");
	}

	#[test]
	fn nested_fields_and_elements_build_from_steps() {
		let mut items = field("items", json!({ "Array": ["Object", null] }));
		items["steps"] = json!([{ "field": "items" }]);
		let price = json!({
			"path": ["items", "price"],
			"steps": [{ "field": "items" }, { "element": true }, { "field": "price" }],
			"kind": "", "kind_value": "Decimal", "certainty": "fact",
		});
		let tags = json!({
			"path": ["tags"], "steps": [{ "field": "tags" }],
			"kind": "", "kind_value": { "Set": ["String", null] }, "certainty": "fact",
		});
		let tag = json!({
			"path": ["tags"], "steps": [{ "field": "tags" }, { "element": true }],
			"kind": "", "kind_value": "String", "certainty": "fact",
		});
		let out = render_default(json!({ "tables": [table("order", true, vec![
			items, price, tags, tag,
			field("profile.name", json!("String")),
			field("profile.nick", json!({ "Either": ["None", "String"] })),
		])] }));
		assert!(out.contains("items: Array<{ price: Decimal }>;"), "{out}");
		assert!(out.contains("tags: Set<string>;"), "{out}");
		assert!(out.contains("profile: { name: string; nick?: string };"), "{out}");
	}

	#[test]
	fn a_declared_id_replaces_the_synthesized_one() {
		let out = render_default(json!({ "tables": [table("t", true, vec![
			field("id", json!({ "Record": ["t"] })),
		])] }));
		assert_eq!(out.matches("id: RecordId<\"t\">;").count(), 1, "{out}");
	}

	#[test]
	fn a_relation_leads_with_in_and_out() {
		let mut edge = table("wrote", true, vec![]);
		edge["relation"] = json!({ "in": ["user"], "out": [] });
		let out = render_default(json!({ "tables": [edge] }));
		assert!(
			out.contains(
				"  id: RecordId<\"wrote\">;\n  in: RecordId<\"user\">;\n  out: RecordId;\n"
			),
			"{out}"
		);
	}

	#[test]
	fn hints_can_be_distrusted() {
		let mut hinted = field("note", json!("String"));
		hinted["certainty"] = json!("hint");
		hinted["optional"] = json!(true);
		let raw = json!({
			"tables": [table("t", true, vec![hinted])],
			"queries": [{ "text": "q", "statements": [
				{ "kind": "string", "kind_value": "String", "certainty": "hint" },
				{ "kind": "int", "kind_value": "Int", "certainty": "fact" },
			] }],
		});
		let opts = RenderOptions {
			hints: HintStyle::Unknown,
			..RenderOptions::default()
		};
		let out = render(&document(raw.clone()), &opts);
		assert!(out.contains("note?: unknown;"), "{out}");
		assert!(out.contains("result: [unknown, number]"), "{out}");
		let out = render_default(raw);
		assert!(out.contains("note?: string;"), "{out}");
		assert!(out.contains("result: [string, number]"), "{out}");
	}

	#[test]
	fn reserved_and_colliding_names_are_suffixed() {
		let out = render_default(json!({ "tables": [
			table("date", true, vec![]),
			table("tables", true, vec![]),
			table("record_id", true, vec![]),
			table("my_thing", true, vec![]),
			table("my-thing", true, vec![]),
			table("typed", true, vec![]),
		] }));
		for want in [
			"export interface Date2 {",
			"export interface Tables2 {",
			"export interface RecordId2 {",
			"export interface MyThing {",
			"export interface MyThing2 {",
			"export interface Typed2 {",
		] {
			assert!(out.contains(want), "missing `{want}` in {out}");
		}
	}

	#[test]
	fn bookkeeping_tables_are_skipped() {
		let out = render_default(json!({ "tables": [
			table("__entity", true, vec![]),
			table("user", true, vec![]),
		] }));
		assert!(!out.contains("__entity") && !out.contains("Entity"), "{out}");
		assert!(out.contains("export interface User {"), "{out}");
	}

	#[test]
	fn host_params_are_skipped_and_optional_params_are_marked() {
		let out = render_default(json!({ "queries": [{
			"text": "SELECT $a, $b, $c",
			"statements": [],
			"params": [
				{ "name": "a", "kind_value": "Int", "required": true },
				{ "name": "b", "kind_value": "String", "required": false },
				{ "name": "__host0", "kind_value": "String", "required": true },
				{ "name": "c", "required": true },
			],
		}] }));
		assert!(out.contains("params: { a: number; b?: string; c: unknown }"), "{out}");
		assert!(!out.contains("__host0"), "{out}");
		let out = render_default(json!({ "queries": [{
			"text": "q", "statements": [],
			"params": [{ "name": "__host0", "required": true }],
		}] }));
		assert!(out.contains("params: {} }"), "{out}");
	}

	#[test]
	fn duplicate_query_text_keeps_the_first() {
		let out = render_default(json!({ "queries": [
			{ "text": "RETURN 1", "statements": [{ "kind_value": "Int", "certainty": "fact" }] },
			{ "text": "RETURN 1", "statements": [{ "kind_value": "String", "certainty": "fact" }] },
		] }));
		assert_eq!(out.matches("\"RETURN 1\":").count(), 1, "{out}");
		assert!(out.contains("result: [number]"), "{out}");
	}

	#[test]
	fn query_text_is_json_escaped() {
		let out = render_default(json!({ "queries": [{
			"text": "SELECT \"a\"\nFROM t\\", "statements": [],
		}] }));
		assert!(out.contains(r#""SELECT \"a\"\nFROM t\\": {"#), "{out}");
	}

	#[test]
	fn statements_without_a_slot_are_dropped_from_the_tuple() {
		let out = render_default(
			json!({ "queries": [{ "text": "BEGIN; LET $x = 1; RETURN $x; COMMIT", "statements": [
			{ "kind": null, "kind_value": null, "certainty": "fact" },
			{ "kind": "none", "kind_value": "None", "certainty": "fact" },
			{ "kind": "int", "kind_value": "Int", "certainty": "fact" },
			{ "kind": null, "kind_value": null, "certainty": "fact" },
		] }] }),
		);
		assert!(out.contains("result: [undefined, number]"), "{out}");
	}

	#[test]
	fn named_queries_are_listed_in_query_names() {
		let out = render_default(json!({ "queries": [
			{ "name": "older", "text": "SELECT 1", "statements": [] },
			{ "name": null, "text": "SELECT 2", "statements": [] },
		] }));
		assert!(
			out.contains("export type QueryNames = {\n  \"older\": \"SELECT 1\";\n};"),
			"{out}"
		);
		let out = render_default(json!({}));
		assert!(out.contains("export type QueryNames = {};"), "{out}");
		assert!(out.contains("export type Queries = {};"), "{out}");
	}

	#[test]
	fn functions_and_params() {
		let out = render_default(json!({
			"functions": [
				{ "name": "fn::greet", "args": [{ "name": "who", "kind": "string", "kind_value": "String" }],
					"returns": "string", "returns_value": "String" },
				{ "name": "fn::untyped", "args": [] },
			],
			"params": [
				{ "name": "tax_rate", "kind": "float", "kind_value": "Float" },
				{ "name": "mystery" },
			],
		}));
		assert!(out.contains("\"fn::greet\": { args: [who: string]; returns: string };"), "{out}");
		assert!(out.contains("\"fn::untyped\": { args: []; returns: unknown };"), "{out}");
		assert!(out.contains("tax_rate: number;"), "{out}");
		assert!(out.contains("mystery: unknown;"), "{out}");
	}

	#[test]
	fn an_empty_document_renders_empty_registries() {
		let out = render_default(json!({}));
		for want in [
			"export interface Tables {}",
			"export interface Functions {}",
			"export interface Params {}",
			"export type Queries = {};",
			"import type { SurrealQueryable } from \"surrealdb\";",
		] {
			assert!(out.contains(want), "missing `{want}` in {out}");
		}
	}

	#[test]
	fn only_the_names_the_output_produces_are_imported() {
		// A query selecting a column called `uuid` mentions `Uuid` only in its key.
		let out = render_default(json!({
			"tables": [table("t", true, vec![field("when", json!("Datetime"))])],
			"queries": [{ "text": "SELECT uuid, Decimal FROM t", "statements": [
				{ "kind_value": "String", "certainty": "fact" },
			] }],
		}));
		let import = out.lines().find(|line| line.starts_with("import type")).expect("import line");
		assert_eq!(
			import,
			"import type { DateTime, RecordId, SurrealQueryable } from \"surrealdb\";"
		);
	}

	#[test]
	fn the_sdk_package_is_configurable() {
		let opts = RenderOptions {
			sdk_package: "@scope/surrealdb".into(),
			..RenderOptions::default()
		};
		let out = render(&document(json!({})), &opts);
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
}
