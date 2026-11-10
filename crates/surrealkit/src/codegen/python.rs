//! The per-query result types of schema document v2, rendered as a Python module for the
//! `surrealdb` Python SDK.
//!
//! The module holds a row type per statement, a parameters type per query, and two clients,
//! `TypedAsyncSurreal` and `TypedSurreal`, which wrap an SDK connection (kept as `.raw`) and give
//! `query()` one `@overload` per query, keyed by the exact query text. A text the module does not
//! know has **no** overload, deliberately: a `str` fallback would accept any parameters for a
//! known text too. Such a call goes through `.raw`.
//!
//! A statement that can run as `LIVE SELECT` also gets a `live_query()` overload, keyed by the
//! same text, that yields the notifications the subscription delivers. Only a query of one
//! statement has one.
//!
//! # Optionality has two spellings, and the position picks
//!
//! A SurrealQL `option<string>` is `none | string`. As the type of an object field it is
//! `NotRequired[str]` (`str | None = None` for a model), because the SDK leaves a field that is
//! NONE out of the row. Anywhere a value stands on its own (a tuple slot, an array element, a
//! statement result) it is `str | None`.
//!
//! # Names
//!
//! A named query `ListCards` has the types `ListCardsRow` (statement 0; `ListCardsRow1`, … for the
//! later statements), `ListCardsParams` and, for each object nested in a row, `ListCardsRow` and
//! the field path in PascalCase. An inline query is named `Query_` and the first 8 hex digits of
//! the SHA-256 of its text. A name already taken gets a number.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::queries::{
	Certainty, ELEMENT_SEGMENT, LiveAction, LiveNotification, QueryEntry, QueryLive,
	QueryParameter, QueryStatement,
};
pub use super::typescript::HintStyle;
use crate::typegen::document::{Kind, LiteralType, LiteralValue, ObjectLiteralField, TypeDef};

/// Query parameters with this prefix are filled by the host-language template, not by the caller.
const HOST_PARAM_PREFIX: &str = "__host";

/// What a row is: a `TypedDict` or a pydantic model.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ModelStyle {
	/// `TypedDict` classes, which describe the dicts the SDK returns and cost nothing at runtime.
	#[default]
	TypedDict,
	/// pydantic `BaseModel` classes, which the client validates each statement's result into.
	Pydantic,
}

/// What [`render`] does with the choices a query list leaves open.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RenderOptions {
	pub hints: HintStyle,
	pub models: ModelStyle,
}

/// Python keywords: a field of one of these names cannot be written in class syntax.
const KEYWORDS: &[&str] = &[
	"False", "None", "True", "and", "as", "assert", "async", "await", "break", "class", "continue",
	"def", "del", "elif", "else", "except", "finally", "for", "from", "global", "if", "import",
	"in", "is", "lambda", "nonlocal", "not", "or", "pass", "raise", "return", "try", "while",
	"with", "yield",
];

/// Names a pydantic model cannot take for a field: the attributes of `BaseModel`.
const MODEL_ATTRIBUTES: &[&str] = &[
	"construct",
	"copy",
	"dict",
	"fields",
	"from_orm",
	"json",
	"parse_file",
	"parse_obj",
	"parse_raw",
	"schema",
	"schema_json",
	"update_forward_refs",
	"validate",
];

/// Renders the complete module. `types` are the document's `DEFINE TYPE`s, which a `named` kind
/// is expanded from; `entries` are the queries to type, which the caller has already narrowed to
/// the ones this output takes.
pub fn render(types: &[TypeDef], entries: &[QueryEntry], opts: &RenderOptions) -> String {
	let mut module = Module {
		opts,
		types,
		expanding: Vec::new(),
		imports: Imports::default(),
		taken: BTreeSet::from(["TypedAsyncSurreal".to_string(), "TypedSurreal".to_string()]),
		interned: BTreeMap::new(),
		models: BTreeSet::new(),
		defs: Vec::new(),
	};
	let mut queries = Vec::new();
	let mut lives = Vec::new();
	let mut notes = Vec::new();
	let mut seen = BTreeSet::new();
	for entry in entries {
		if let Some(error) = &entry.error {
			notes.push(format!(
				"# Not typed: {} ({}): {}",
				py_string(entry.name.as_deref().unwrap_or(&entry.key)),
				error.code,
				py_string(&entry.text)
			));
			continue;
		}
		if !seen.insert(entry.text.as_str()) {
			continue;
		}
		module.interned.clear();
		let prefix = type_prefix(entry);
		let slots: Vec<String> = entry
			.statements
			.iter()
			.enumerate()
			.map(|(at, statement)| {
				let hint = match at {
					0 => format!("{prefix}Row"),
					_ => format!("{prefix}Row{at}"),
				};
				module.statement(statement, &hint)
			})
			.collect();
		let params = module.params(&prefix, &entry.params);
		match live_of(entry) {
			Live::None => {}
			Live::Refused(clauses) => notes.push(format!(
				"# No live_query overload for {}: LIVE SELECT does not accept {clauses}",
				py_string(&entry.text)
			)),
			Live::Multi => notes.push(format!(
				"# No live_query overload for {}: live_query takes one statement",
				py_string(&entry.text)
			)),
			Live::Ok(live) => {
				if let Some(overload) = module.live(&prefix, &entry.text, live, params.clone()) {
					lives.push(overload);
				}
			}
		}
		queries.push(Overload {
			text: entry.text.clone(),
			slots,
			params,
		});
	}

	let pydantic = opts.models == ModelStyle::Pydantic;
	let mut blocks: Vec<String> = module.defs.clone();
	if !notes.is_empty() {
		blocks.push(notes.join("\n"));
	}
	if !lives.is_empty() {
		blocks.push(live_table(&lives));
	}
	if pydantic && !queries.is_empty() {
		module.imports.pydantic.extend(["ConfigDict", "TypeAdapter"]);
		module.imports.typing.insert("Any");
		module.imports.abc.insert("Sequence");
		blocks.push(adapter_table(&module, &queries, &lives));
		blocks.push(VALIDATE.to_string());
	}
	blocks.push(client(&mut module, true, &queries, &lives));
	blocks.push(client(&mut module, false, &queries, &lives));

	let mut out = String::from(HEADER);
	out.push('\n');
	out.push_str(&module.imports.render());
	// Two blank lines between top-level blocks; the imports end with a newline of their own.
	for (at, block) in blocks.iter().enumerate() {
		out.push_str(if at == 0 {
			"\n\n"
		} else {
			"\n\n\n"
		});
		out.push_str(block);
	}
	out.push('\n');
	out
}

const HEADER: &str = "\
# Generated by SurrealKit from the `queries` section of schema document v2. Do not edit.
# ruff: noqa: E501
#
# Wrap a connection to type `query()` by exact query text:
#
#   db = TypedAsyncSurreal(connection)
#   (people,) = await db.query(\"SELECT name FROM person\")
#
# A text this module does not list has no typed overload: send it through `db.raw`.
";

/// What a model's results are validated by, beside the client.
const VALIDATE: &str = "\
def _validated(text: str, result: Sequence[object]) -> tuple[object, ...]:
    adapters = _ADAPTERS.get(text)
    if adapters is None:
        return tuple(result)
    return tuple(adapter.validate_python(item) for adapter, item in zip(adapters, result))";

/// One typed `query()` overload.
struct Overload {
	text: String,
	/// One type per statement.
	slots: Vec<String>,
	params: Option<ParamsClass>,
}

impl Overload {
	fn returns(&self) -> String {
		if self.slots.is_empty() {
			"tuple[()]".to_string()
		} else {
			format!("tuple[{}]", self.slots.join(", "))
		}
	}
}

/// The parameters type of a query.
#[derive(Clone)]
struct ParamsClass {
	name: String,
	/// Whether every parameter is optional, so the argument may be left out.
	all_optional: bool,
}

/// One typed `live_query()` overload.
struct LiveOverload {
	text: String,
	/// The `LIVE SELECT` the client runs for `text`.
	query: String,
	params: Option<ParamsClass>,
	/// The alias of the union of the notifications.
	notification: String,
	/// The type the `result` of a notification validates as, for models.
	result: String,
}

/// What a query offers `live_query()`.
enum Live<'a> {
	/// Nothing: no statement has live facts.
	None,
	/// A single statement that `LIVE SELECT` can run.
	Ok(&'a QueryLive),
	/// `LIVE SELECT` cannot run the statement as written; the clauses it refuses.
	Refused(String),
	/// A statement among several has live facts.
	Multi,
}

fn live_of(entry: &QueryEntry) -> Live<'_> {
	let [statement] = entry.statements.as_slice() else {
		let any = entry.statements.iter().any(|s| s.live.is_some() || s.live_refusal.is_some());
		return if any {
			Live::Multi
		} else {
			Live::None
		};
	};
	match (&statement.live, &statement.live_refusal) {
		(_, Some(refusal)) => Live::Refused(refusal.clauses.join(", ")),
		(Some(live), None) => Live::Ok(live),
		(None, None) => Live::None,
	}
}

/// The prefix of a query's type names: its name with a capital first letter, or `Query_` and the
/// hash of its text.
fn type_prefix(entry: &QueryEntry) -> String {
	match &entry.name {
		Some(name) => {
			let mut prefix: String = name
				.chars()
				.map(|c| {
					if c.is_ascii_alphanumeric() || c == '_' {
						c
					} else {
						'_'
					}
				})
				.collect();
			match prefix.chars().next() {
				Some(first) if first.is_ascii_digit() => prefix.insert(0, 'Q'),
				Some(first) => {
					prefix = first.to_ascii_uppercase().to_string() + &prefix[first.len_utf8()..];
				}
				None => prefix.push('Q'),
			}
			prefix
		}
		None => {
			let digest = Sha256::digest(entry.text.as_bytes());
			format!("Query_{}", hex::encode(&digest[..4]))
		}
	}
}

/// The Python names the module uses, recorded where each is produced.
#[derive(Default)]
struct Imports {
	datetime: bool,
	decimal: bool,
	uuid: bool,
	/// From `collections.abc`.
	abc: BTreeSet<&'static str>,
	/// From `typing`.
	typing: BTreeSet<&'static str>,
	/// Names `typing` has from 3.11 and `typing_extensions` has before.
	compat: BTreeSet<&'static str>,
	/// From `surrealdb`.
	sdk: BTreeSet<&'static str>,
	/// From `pydantic`.
	pydantic: BTreeSet<&'static str>,
}

/// `names` in isort's order: classes before functions, each sorted without regard to case.
fn sorted(names: &BTreeSet<&'static str>) -> Vec<&'static str> {
	let mut names: Vec<&'static str> = names.iter().copied().collect();
	names.sort_by_key(|name| {
		let class = name.chars().next().is_some_and(char::is_uppercase);
		(!class, name.to_lowercase())
	});
	names
}

impl Imports {
	fn render(&self) -> String {
		let mut stdlib = Vec::new();
		if self.datetime {
			stdlib.push("import datetime".to_string());
		}
		if self.decimal {
			stdlib.push("import decimal".to_string());
		}
		if !self.compat.is_empty() {
			stdlib.push("import sys".to_string());
		}
		if self.uuid {
			stdlib.push("import uuid".to_string());
		}
		for (module, names) in [("collections.abc", &self.abc), ("typing", &self.typing)] {
			if !names.is_empty() {
				stdlib.push(from_import(module, &sorted(names)));
			}
		}
		let mut third = Vec::new();
		for (module, names) in [("pydantic", &self.pydantic), ("surrealdb", &self.sdk)] {
			if !names.is_empty() {
				third.push(from_import(module, &sorted(names)));
			}
		}
		let mut out = String::from("from __future__ import annotations\n");
		for group in [stdlib, third] {
			if !group.is_empty() {
				out.push('\n');
				out.push_str(&group.join("\n"));
				out.push('\n');
			}
		}
		if !self.compat.is_empty() {
			let names = sorted(&self.compat).join(", ");
			out.push_str(&format!(
				"\nif sys.version_info >= (3, 11):\n    from typing import {names}\nelse:\n    from \
				 typing_extensions import {names}\n"
			));
		}
		out
	}
}

/// `from module import a, b`, one name to a line with a trailing comma when it is long.
fn from_import(module: &str, names: &[&str]) -> String {
	let line = format!("from {module} import {}", names.join(", "));
	if line.len() <= 88 {
		return line;
	}
	let names: String = names.iter().map(|name| format!("    {name},\n")).collect();
	format!("from {module} import (\n{names})")
}

/// Object positions, relative to the kind being rendered, that admit undeclared keys. A path that
/// is empty is the kind itself.
type Open<'a> = [&'a [String]];

/// What a class is written as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Style {
	/// A statement's row, a notification's result: a model when the module is pydantic.
	Row,
	/// A parameters type: always a `TypedDict`, which is what the caller passes.
	Params,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClassKind {
	TypedDict,
	/// A `TypedDict` through the functional syntax, for a key that is not an identifier.
	Functional,
	Model,
}

struct Module<'a> {
	opts: &'a RenderOptions,
	types: &'a [TypeDef],
	/// The named types being expanded, outermost first: a name already here leads back to
	/// itself.
	expanding: Vec<String>,
	imports: Imports,
	/// Class and alias names in use.
	taken: BTreeSet<String>,
	/// The classes of the current query by what they hold, so one shape is one class.
	interned: BTreeMap<String, String>,
	/// The names of the models.
	models: BTreeSet<String>,
	/// The definitions, in an order that defines each name before it is used.
	defs: Vec<String>,
}

/// One field of a class.
struct Member {
	/// The name the class declares.
	ident: String,
	/// The key in the dict.
	key: String,
	type_: String,
	optional: bool,
}

impl Member {
	fn new(key: &str, type_: String, optional: bool) -> Self {
		Self {
			ident: key.to_string(),
			key: key.to_string(),
			type_,
			optional,
		}
	}
}

impl Module<'_> {
	fn any(&mut self) -> String {
		self.imports.typing.insert("Any");
		"Any".to_string()
	}

	fn value(&mut self) -> String {
		self.sdk("Value")
	}

	fn sdk(&mut self, name: &'static str) -> String {
		self.imports.sdk.insert(name);
		name.to_string()
	}

	/// `name`, or `name` and the least number from 2 that is free, which is then taken.
	fn claim(&mut self, name: &str) -> String {
		let mut chosen = name.to_string();
		let mut number = 2;
		while self.taken.contains(&chosen) {
			chosen = format!("{name}{number}");
			number += 1;
		}
		self.taken.insert(chosen.clone());
		chosen
	}

	fn class_kind(&self, style: Style, members: &[Member]) -> ClassKind {
		match (style, self.opts.models) {
			(Style::Row, ModelStyle::Pydantic) => ClassKind::Model,
			_ if members.iter().all(|m| is_identifier(&m.key)) => ClassKind::TypedDict,
			_ => ClassKind::Functional,
		}
	}

	/// The value spelling. Recursion stays here: an array element, a union member and a tuple
	/// item are all values, and only an object literal field crosses into a key. `hint` is the
	/// name an object found here is given.
	fn kind(&mut self, kind: &Kind, hint: &str, open: &Open<'_>, style: Style) -> String {
		match kind {
			Kind::Any
			| Kind::Function
			| Kind::Regex
			| Kind::Range
			| Kind::Date
			| Kind::Time
			| Kind::Timestamp
			| Kind::Interval
			| Kind::Unknown {
				..
			} => self.any(),
			Kind::BigDecimal => self.sdk("BigDecimal"),
			Kind::None => "None".to_string(),
			Kind::Null => self.sdk("NullType"),
			Kind::Bool => "bool".to_string(),
			Kind::Int {
				..
			} => "int".to_string(),
			Kind::Float {
				..
			} => "float".to_string(),
			Kind::Number => "int | float".to_string(),
			Kind::Decimal => {
				self.imports.decimal = true;
				"decimal.Decimal".to_string()
			}
			Kind::String => "str".to_string(),
			Kind::Datetime => {
				self.imports.datetime = true;
				"datetime.datetime".to_string()
			}
			Kind::Duration => self.sdk("Duration"),
			Kind::Uuid => {
				self.imports.uuid = true;
				"uuid.UUID".to_string()
			}
			Kind::Bytes => "bytes".to_string(),
			Kind::Object => {
				let value = self.value();
				format!("dict[str, {value}]")
			}
			Kind::Record {
				..
			} => self.sdk("RecordID"),
			Kind::Table {
				..
			} => self.sdk("Table"),
			Kind::File {
				..
			} => self.sdk("File"),
			Kind::Geometry {
				types,
			} => self.geometry(types),
			// An array no element fits, such as the result of a plain DELETE.
			Kind::Array {
				length: Some(0),
				..
			} => {
				self.imports.compat.insert("Never");
				"list[Never]".to_string()
			}
			Kind::Array {
				item,
				..
			} => {
				let item = self.kind(item, hint, &into_element(open), style);
				format!("list[{item}]")
			}
			// The SDK decodes a set to `SurrealSet`, a plain list subclass with no element type.
			Kind::Set {
				..
			} => self.sdk("SurrealSet"),
			// A tuple slot has no key to omit, so each item is a value, and no path leads into
			// one.
			Kind::Tuple {
				items,
			} => {
				if items.is_empty() {
					return "tuple[()]".to_string();
				}
				let items: Vec<String> =
					items.iter().map(|item| self.kind(item, hint, &[], style)).collect();
				format!("tuple[{}]", items.join(", "))
			}
			Kind::ObjectLiteral {
				fields,
			} => self.object(fields, hint, open, style),
			Kind::Literal {
				literal_type,
				value,
			} => match self.literal_args(*literal_type, value) {
				Ok(args) => self.literal_type(&args),
				Err(stand_in) => stand_in,
			},
			Kind::Option {
				item,
			} => {
				let item = self.kind(item, hint, open, style);
				or_none(item)
			}
			Kind::Enum {
				labels,
			} => {
				if labels.is_empty() {
					self.imports.compat.insert("Never");
					return "Never".to_string();
				}
				let args: Vec<String> = labels.iter().map(|label| py_string(label)).collect();
				self.literal_type(&args)
			}
			Kind::Named {
				name,
			} => self.named(name, hint, open, style),
			Kind::Union {
				variants,
			} => self.union(variants, hint, open, style),
		}
	}

	/// `Literal[…]` over `args`, which are written as Python writes them, without repeats.
	fn literal_type(&mut self, args: &[String]) -> String {
		self.imports.typing.insert("Literal");
		let mut seen: Vec<&str> = Vec::new();
		for arg in args {
			if !seen.contains(&arg.as_str()) {
				seen.push(arg);
			}
		}
		format!("Literal[{}]", seen.join(", "))
	}

	/// The argument a literal kind makes of `Literal[…]`, or, for a literal Python has no type
	/// for, the type that stands in for it.
	fn literal_args(
		&mut self,
		literal_type: LiteralType,
		value: &LiteralValue,
	) -> Result<Vec<String>, String> {
		let text = match value {
			LiteralValue::Bool(value) => value.to_string(),
			LiteralValue::Text(text) => text.clone(),
		};
		match literal_type {
			LiteralType::String => Ok(vec![py_string(&text)]),
			LiteralType::Int => match text.parse::<i64>() {
				Ok(int) => Ok(vec![int.to_string()]),
				Err(_) => Err("int".to_string()),
			},
			LiteralType::Float => Err("float".to_string()),
			LiteralType::Decimal => {
				self.imports.decimal = true;
				Err("decimal.Decimal".to_string())
			}
			LiteralType::Duration => Err(self.sdk("Duration")),
			LiteralType::Bool => match text.as_str() {
				"true" => Ok(vec!["True".to_string()]),
				"false" => Ok(vec!["False".to_string()]),
				_ => Err("bool".to_string()),
			},
		}
	}

	/// A union's members in order, without repeats. Every member that is a literal joins one
	/// `Literal[…]`, which stands where the first of them was.
	fn union(&mut self, variants: &[Kind], hint: &str, open: &Open<'_>, style: Style) -> String {
		let mut parts: Vec<String> = Vec::new();
		let mut literals: Vec<String> = Vec::new();
		let mut literal_at: Option<usize> = None;
		for variant in variants {
			let args = match variant {
				Kind::Literal {
					literal_type,
					value,
				} => self.literal_args(*literal_type, value).ok(),
				Kind::Enum {
					labels,
				} if !labels.is_empty() => Some(labels.iter().map(|label| py_string(label)).collect()),
				_ => None,
			};
			match args {
				Some(args) => {
					literal_at.get_or_insert_with(|| {
						parts.push(String::new());
						parts.len() - 1
					});
					for arg in args {
						if !literals.contains(&arg) {
							literals.push(arg);
						}
					}
				}
				None => {
					let text = self.kind(variant, hint, open, style);
					if !parts.contains(&text) {
						parts.push(text);
					}
				}
			}
		}
		if let Some(at) = literal_at {
			parts[at] = self.literal_type(&literals);
		}
		if parts.is_empty() {
			self.imports.compat.insert("Never");
			return "Never".to_string();
		}
		parts.join(" | ")
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
			members = vec!["Geometry", "GeometryCollection"];
		}
		members.into_iter().map(|class| self.sdk(class)).collect::<Vec<_>>().join(" | ")
	}

	/// A `DEFINE TYPE` name, expanded inline: the module declares no alias for it. A name `types`
	/// does not define, or one that leads back to itself, is `Any`.
	fn named(&mut self, name: &str, hint: &str, open: &Open<'_>, style: Style) -> String {
		let types = self.types;
		let Some(def) = types.iter().find(|def| def.name == name) else {
			return self.any();
		};
		if self.expanding.iter().any(|n| n == name) {
			return self.any();
		}
		self.expanding.push(name.to_string());
		let text = self.kind(&def.kind, hint, open, style);
		self.expanding.pop();
		text
	}

	/// An object type: the one place a key exists, so fields render as members of a class. The
	/// class is defined here and its name is the type.
	fn object(
		&mut self,
		fields: &[ObjectLiteralField],
		hint: &str,
		open: &Open<'_>,
		style: Style,
	) -> String {
		let mut members: Vec<Member> = Vec::new();
		for field in fields {
			// An `option<T>` field is a key that may be absent, however the document spells it.
			let (kind, optional) = match &field.kind {
				Kind::Option {
					item,
				} => (item.as_ref(), true),
				kind => (kind, field.optional),
			};
			let child = format!("{hint}{}", pascal(&field.name));
			let type_ = self.kind(kind, &child, &into_field(open, &field.name), style);
			members.push(Member::new(&field.name, type_, optional));
		}
		let class = self.class_kind(style, &members);
		self.define(hint, class, members, is_open(open))
	}

	/// Defines a class of `members` and returns its name: that of an earlier class of the same
	/// shape in this query, or `hint` (numbered if taken).
	fn define(
		&mut self,
		hint: &str,
		class: ClassKind,
		mut members: Vec<Member>,
		open: bool,
	) -> String {
		if class == ClassKind::Model {
			let mut used = BTreeSet::new();
			for member in &mut members {
				let base = model_identifier(&member.key);
				let mut ident = base.clone();
				let mut number = 2;
				while !used.insert(ident.clone()) {
					ident = format!("{base}{number}");
					number += 1;
				}
				member.ident = ident;
			}
		}
		let body = class_body(class, &members, open);
		let key = format!("{class:?}\n{body}");
		if let Some(name) = self.interned.get(&key) {
			return name.clone();
		}
		let name = self.claim(hint);
		self.interned.insert(key, name.clone());
		match class {
			ClassKind::TypedDict | ClassKind::Functional => {
				self.imports.compat.insert("TypedDict");
				if members.iter().any(|m| m.optional) {
					self.imports.compat.insert("NotRequired");
				}
			}
			ClassKind::Model => {
				self.models.insert(name.clone());
				self.imports.pydantic.extend(["BaseModel", "ConfigDict"]);
				if members.iter().any(|m| m.ident != m.key) {
					self.imports.pydantic.insert("Field");
				}
			}
		}
		self.defs.push(class_def(&name, class, &body));
		name
	}

	/// A statement's slot. One the engine did not type is `Any`.
	fn statement(&mut self, statement: &QueryStatement, hint: &str) -> String {
		let Some(kind) = statement.kind.as_ref() else {
			return self.any();
		};
		if statement.certainty == Certainty::Hint && self.opts.hints == HintStyle::Unknown {
			return self.value();
		}
		let open: Vec<&[String]> = statement.open.iter().map(Vec::as_slice).collect();
		let text = self.kind(kind, hint, &open, Style::Row);
		if statement.optional {
			or_none(text)
		} else {
			text
		}
	}

	/// The parameters type a caller binds, without those the host-language template fills.
	/// A parameter's kind is inferred from how the query uses it, so it is always a hint.
	fn params(&mut self, prefix: &str, params: &[QueryParameter]) -> Option<ParamsClass> {
		let params: Vec<&QueryParameter> =
			params.iter().filter(|p| !p.name.starts_with(HOST_PARAM_PREFIX)).collect();
		if params.is_empty() {
			return None;
		}
		let hint = format!("{prefix}Params");
		let mut members = Vec::new();
		for param in &params {
			let trusted = param.kind.as_ref().filter(|_| self.opts.hints == HintStyle::Trust);
			let type_ = match trusted {
				Some(kind) => {
					let child = format!("{hint}{}", pascal(&param.name));
					let text = self.kind(kind, &child, &[], Style::Params);
					if param.optional {
						or_none(text)
					} else {
						text
					}
				}
				None => self.value(),
			};
			members.push(Member::new(&param.name, type_, !param.required));
		}
		let class = self.class_kind(Style::Params, &members);
		let name = self.define(&hint, class, members, false);
		Some(ParamsClass {
			name,
			all_optional: params.iter().all(|p| !p.required),
		})
	}

	/// The notifications a subscription to `live` delivers, and the overload that returns them.
	/// `KILLED` ends the stream and is not delivered, so it has no type.
	fn live(
		&mut self,
		prefix: &str,
		text: &str,
		live: &QueryLive,
		params: Option<ParamsClass>,
	) -> Option<LiveOverload> {
		let mut members = Vec::new();
		let mut results: Vec<String> = Vec::new();
		for notification in &live.notifications {
			if notification.action == LiveAction::Killed {
				continue;
			}
			let (name, result) = self.notification(prefix, notification);
			if !members.contains(&name) {
				members.push(name);
			}
			if !results.contains(&result) {
				results.push(result);
			}
		}
		if members.is_empty() {
			return None;
		}
		let alias = self.claim(&format!("{prefix}Notification"));
		self.imports.typing.insert("TypeAlias");
		self.defs.push(format!("{alias}: TypeAlias = {}", members.join(" | ")));
		Some(LiveOverload {
			text: text.to_string(),
			query: live.query.clone(),
			params,
			notification: alias,
			result: results.join(" | "),
		})
	}

	/// The `TypedDict` of one notification, and the type of its `result`.
	fn notification(&mut self, prefix: &str, notification: &LiveNotification) -> (String, String) {
		let action = notification.action.as_str();
		let suffix = match notification.action {
			LiveAction::Create => "Create",
			LiveAction::Update => "Update",
			LiveAction::Delete => "Delete",
			LiveAction::Killed => "Killed",
		};
		let distrusted =
			notification.certainty == Certainty::Hint && self.opts.hints == HintStyle::Unknown;
		let (record, result) = if distrusted {
			(self.value(), self.value())
		} else {
			let open: Vec<&[String]> = notification.open.iter().map(Vec::as_slice).collect();
			let record = self.kind(&notification.record, "", &[], Style::Params);
			let result =
				self.kind(&notification.result, &format!("{prefix}Result"), &open, Style::Row);
			(record, result)
		};
		self.imports.uuid = true;
		self.imports.typing.insert("Literal");
		let members = vec![
			Member::new("action", format!("Literal[\"{action}\"]"), false),
			Member::new("id", "uuid.UUID".to_string(), false),
			Member::new("record", record, false),
			Member::new("result", result.clone(), false),
		];
		let name = self.define(&format!("{prefix}{suffix}"), ClassKind::TypedDict, members, false);
		(name, result)
	}
}

/// The lines of a class body: one per member, in the shape `class` writes them.
fn class_body(class: ClassKind, members: &[Member], open: bool) -> String {
	let typed = |m: &Member| {
		if m.optional {
			format!("NotRequired[{}]", m.type_)
		} else {
			m.type_.clone()
		}
	};
	match class {
		ClassKind::TypedDict => {
			if members.is_empty() {
				return "    pass".to_string();
			}
			members
				.iter()
				.map(|m| format!("    {}: {}", m.ident, typed(m)))
				.collect::<Vec<_>>()
				.join("\n")
		}
		ClassKind::Functional => {
			let rows: String = members
				.iter()
				.map(|m| format!("        {}: {},\n", py_string(&m.key), typed(m)))
				.collect();
			format!("    {{\n{rows}    }},")
		}
		ClassKind::Model => {
			let extra = if open {
				"allow"
			} else {
				"ignore"
			};
			let mut lines = vec![format!(
				"    model_config = ConfigDict(arbitrary_types_allowed=True, extra=\"{extra}\")"
			)];
			if !members.is_empty() {
				lines.push(String::new());
			}
			for m in members {
				let type_ = if m.optional {
					or_none(m.type_.clone())
				} else {
					m.type_.clone()
				};
				let line = match (m.ident != m.key, m.optional) {
					(false, false) => format!("    {}: {type_}", m.ident),
					(false, true) => format!("    {}: {type_} = None", m.ident),
					(true, false) => {
						format!("    {}: {type_} = Field(alias={})", m.ident, py_string(&m.key))
					}
					(true, true) => format!(
						"    {}: {type_} = Field(default=None, alias={})",
						m.ident,
						py_string(&m.key)
					),
				};
				lines.push(line);
			}
			lines.join("\n")
		}
	}
}

fn class_def(name: &str, class: ClassKind, body: &str) -> String {
	match class {
		ClassKind::TypedDict => format!("class {name}(TypedDict):\n{body}"),
		ClassKind::Functional => {
			format!("{name} = TypedDict(\n    {},\n{body}\n)", py_string(name))
		}
		ClassKind::Model => format!("class {name}(BaseModel):\n{body}"),
	}
}

/// The `_LIVE` table: each text and the `LIVE SELECT` the client runs for it.
fn live_table(lives: &[LiveOverload]) -> String {
	let rows: String = lives
		.iter()
		.map(|live| format!("    {}: {},\n", py_string(&live.text), py_string(&live.query)))
		.collect();
	format!("_LIVE: dict[str, str] = {{\n{rows}}}")
}

/// The models' validation tables: a `TypeAdapter` per statement of each query, and per live
/// result. A model carries its own configuration, which a `TypeAdapter` refuses to be given.
fn adapter_table(module: &Module<'_>, queries: &[Overload], lives: &[LiveOverload]) -> String {
	let adapter = |slot: &str| {
		if module.models.contains(slot) {
			format!("TypeAdapter({slot})")
		} else {
			format!("TypeAdapter({slot}, config=_CONFIG)")
		}
	};
	let mut out = String::from("_CONFIG = ConfigDict(arbitrary_types_allowed=True)\n\n\n");
	out.push_str("_ADAPTERS: dict[str, tuple[TypeAdapter[Any], ...]] = {\n");
	for query in queries {
		if query.slots.is_empty() {
			out.push_str(&format!("    {}: (),\n", py_string(&query.text)));
			continue;
		}
		out.push_str(&format!("    {}: (\n", py_string(&query.text)));
		for slot in &query.slots {
			out.push_str(&format!("        {},\n", adapter(slot)));
		}
		out.push_str("    ),\n");
	}
	out.push('}');
	if !lives.is_empty() {
		out.push_str("\n\n\n_LIVE_RESULTS: dict[str, TypeAdapter[Any]] = {\n");
		for live in lives {
			out.push_str(&format!("    {}: {},\n", py_string(&live.text), adapter(&live.result)));
		}
		out.push('}');
	}
	out
}

/// A method's header: the arguments after `query`, always one to a line.
struct Signature<'a> {
	/// `def` or `async def`.
	keyword: &'static str,
	method: &'static str,
	/// The type of `query`.
	query: String,
	params: Option<&'a ParamsClass>,
	returns: String,
}

impl Signature<'_> {
	/// The header and, for an overload, the `...` that is its whole body.
	fn render(&self, stub: bool) -> String {
		let params = match self.params {
			Some(params) if params.all_optional => {
				format!("        params: {} | None = None,\n", params.name)
			}
			Some(params) => format!("        params: {},\n", params.name),
			None => String::new(),
		};
		let end = if stub {
			": ..."
		} else {
			":"
		};
		format!(
			"    {} {}(\n        self,\n        query: {},\n{params}    ) -> {}{end}",
			self.keyword, self.method, self.query, self.returns
		)
	}
}

/// The code that turns the `params` argument into the variables the SDK takes. Empty when the
/// query has no parameters. `any` is the implementation of several overloads, which takes a
/// mapping that may be `None`.
fn variables(params: Option<&ParamsClass>, any: bool) -> String {
	let source = match params {
		None if !any => return String::new(),
		Some(params) if !any && !params.all_optional => "dict(params)",
		_ => "dict(params) if params is not None else None",
	};
	format!(
		"        variables = cast(\n            \"dict[str, Value] | None\",\n            {source},\n        )\n"
	)
}

/// One client class.
fn client(
	module: &mut Module<'_>,
	asynchronous: bool,
	queries: &[Overload],
	lives: &[LiveOverload],
) -> String {
	let (name, connection) = if asynchronous {
		("TypedAsyncSurreal", "AsyncSurrealConnection")
	} else {
		("TypedSurreal", "BlockingSurrealConnection")
	};
	module.sdk(connection);
	let mut out = format!(
		"class {name}:\n    def __init__(self, db: {connection}) -> None:\n        self.raw = db"
	);
	if !queries.is_empty() {
		out.push_str("\n\n");
		out.push_str(&query_methods(module, asynchronous, queries));
	}
	if !lives.is_empty() {
		out.push_str("\n\n");
		out.push_str(&live_methods(module, asynchronous, lives));
	}
	out
}

/// How a client reaches the SDK: `await` for the async one, `.execute()` for the blocking one,
/// which returns a builder.
struct Call {
	asynchronous: bool,
}

impl Call {
	fn keyword(&self) -> &'static str {
		if self.asynchronous {
			"async def"
		} else {
			"def"
		}
	}

	/// The expression that runs `query` on the connection and gives the list of results.
	fn query(&self, text: &str, with_variables: bool) -> String {
		let arguments = if with_variables {
			format!("{text}, variables")
		} else {
			text.to_string()
		};
		if self.asynchronous {
			format!("await self.raw.query({arguments})")
		} else {
			format!("self.raw.query({arguments}).execute()")
		}
	}
}

fn query_methods(module: &mut Module<'_>, asynchronous: bool, queries: &[Overload]) -> String {
	let call = Call {
		asynchronous,
	};
	let pydantic = module.opts.models == ModelStyle::Pydantic;
	module.imports.typing.insert("cast");
	module.imports.typing.insert("Literal");
	let collected = if pydantic {
		"_validated(query, result)"
	} else {
		"tuple(result)"
	};
	let keyed = |query: &Overload| format!("Literal[{}]", py_string(&query.text));

	if let [only] = queries {
		if only.params.is_some() {
			module.value();
		}
		let signature = Signature {
			keyword: call.keyword(),
			method: "query",
			query: keyed(only),
			params: only.params.as_ref(),
			returns: only.returns(),
		};
		return format!(
			"{}\n{}        result = {}\n        return cast({}, {collected})",
			signature.render(false),
			variables(only.params.as_ref(), false),
			call.query("query", only.params.is_some()),
			only.returns()
		);
	}

	module.value();
	module.imports.abc.insert("Mapping");
	module.imports.typing.insert("overload");
	let mut out = String::new();
	for query in queries {
		let signature = Signature {
			keyword: call.keyword(),
			method: "query",
			query: keyed(query),
			params: query.params.as_ref(),
			returns: query.returns(),
		};
		out.push_str("    @overload\n");
		out.push_str(&signature.render(true));
		out.push('\n');
	}
	out.push_str(&format!(
		"    {} query(\n        self,\n        query: str,\n        params: Mapping[str, object] | None = None,\n    ) -> tuple[object, ...]:\n",
		call.keyword()
	));
	out.push_str(&variables(None, true));
	out.push_str(&format!(
		"        result = {}\n        return {collected}",
		call.query("query", true)
	));
	out
}

fn live_methods(module: &mut Module<'_>, asynchronous: bool, lives: &[LiveOverload]) -> String {
	let call = Call {
		asynchronous,
	};
	let pydantic = module.opts.models == ModelStyle::Pydantic;
	module.imports.typing.insert("cast");
	module.imports.typing.insert("Literal");
	module.imports.uuid = true;
	let iterator = if asynchronous {
		"AsyncIterator"
	} else {
		"Iterator"
	};
	module.imports.abc.insert(iterator);
	let keyed = |live: &LiveOverload| format!("Literal[{}]", py_string(&live.text));
	let returns = |live: &LiveOverload| format!("{iterator}[{}]", live.notification);
	// Runs the `LIVE SELECT`, reads the query id off its result, and yields each notification.
	let body = |with_variables: bool, cast_to: Option<&str>| {
		let (for_, source) = if asynchronous {
			("async for", "await self.raw.subscribe_live(live_id)")
		} else {
			("for", "self.raw.subscribe_live(live_id)")
		};
		let yielded = match cast_to {
			Some(alias) => format!("cast({alias}, notification)"),
			None => "notification".to_string(),
		};
		let item = if pydantic {
			format!(
				"            adapter = _LIVE_RESULTS.get(query)\n            if adapter is not None and \"result\" in notification:\n                notification = {{\n                    **notification,\n                    \"result\": adapter.validate_python(notification[\"result\"]),\n                }}\n            yield {yielded}"
			)
		} else {
			format!("            yield {yielded}")
		};
		format!(
			"        result = {}\n        live_id = result[-1]\n        if not isinstance(live_id, uuid.UUID):\n            raise TypeError(\"a LIVE SELECT answers with its query id\")\n        {for_} notification in {source}:\n{item}",
			call.query("_LIVE[query]", with_variables)
		)
	};

	if let [only] = lives {
		if only.params.is_some() {
			module.value();
		}
		let signature = Signature {
			keyword: call.keyword(),
			method: "live_query",
			query: keyed(only),
			params: only.params.as_ref(),
			returns: returns(only),
		};
		return format!(
			"{}\n{}{}",
			signature.render(false),
			variables(only.params.as_ref(), false),
			body(only.params.is_some(), Some(&only.notification))
		);
	}

	module.value();
	module.imports.abc.insert("Mapping");
	module.imports.typing.insert("overload");
	let mut out = String::new();
	for live in lives {
		// A stub is a plain `def`: the implementation is a generator, which is called, not
		// awaited, and gives the iterator.
		let signature = Signature {
			keyword: "def",
			method: "live_query",
			query: keyed(live),
			params: live.params.as_ref(),
			returns: returns(live),
		};
		out.push_str("    @overload\n");
		out.push_str(&signature.render(true));
		out.push('\n');
	}
	out.push_str(&format!(
		"    {} live_query(\n        self,\n        query: str,\n        params: Mapping[str, object] | None = None,\n    ) -> {iterator}[object]:\n",
		call.keyword()
	));
	out.push_str(&variables(None, true));
	out.push_str(&body(true, None));
	out
}

/// `text | None`, unless `text` already admits `None`.
fn or_none(text: String) -> String {
	if text == "None" || text.ends_with(" | None") {
		text
	} else {
		format!("{text} | None")
	}
}

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

/// A name in PascalCase for a type name: each run of letters and digits starts with a capital.
fn pascal(name: &str) -> String {
	let mut out = String::new();
	for part in name.split(|c: char| !c.is_ascii_alphanumeric()).filter(|part| !part.is_empty()) {
		let mut chars = part.chars();
		if let Some(first) = chars.next() {
			out.push(first.to_ascii_uppercase());
			out.push_str(chars.as_str());
		}
	}
	if out.is_empty() {
		out.push_str("Field");
	}
	out
}

/// Whether a key can be written as a field of a class: an ASCII identifier that is no keyword.
fn is_identifier(name: &str) -> bool {
	let mut chars = name.chars();
	matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
		&& chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
		&& !KEYWORDS.contains(&name)
}

/// The name a pydantic model declares for the key `key`: the key itself when it can, otherwise a
/// name the key is the alias of.
fn model_identifier(key: &str) -> String {
	let usable = is_identifier(key)
		&& !key.starts_with('_')
		&& !key.starts_with("model_")
		&& !MODEL_ATTRIBUTES.contains(&key);
	if usable {
		return key.to_string();
	}
	let ident: String = key
		.chars()
		.map(|c| {
			if c.is_ascii_alphanumeric() {
				c
			} else {
				'_'
			}
		})
		.collect();
	let mut ident = ident.trim_start_matches('_').to_string();
	if ident.is_empty() || ident.starts_with(|c: char| c.is_ascii_digit()) {
		ident.insert_str(0, "field_");
	}
	if KEYWORDS.contains(&ident.as_str())
		|| MODEL_ATTRIBUTES.contains(&ident.as_str())
		|| ident.starts_with("model_")
	{
		ident.push('_');
	}
	ident
}

/// A Python double-quoted string literal for `text`.
fn py_string(text: &str) -> String {
	let mut out = String::from("\"");
	for c in text.chars() {
		match c {
			'\\' => out.push_str("\\\\"),
			'"' => out.push_str("\\\""),
			'\n' => out.push_str("\\n"),
			'\r' => out.push_str("\\r"),
			'\t' => out.push_str("\\t"),
			c if (c as u32) < 0x20 || c == '\u{7f}' => {
				out.push_str(&format!("\\x{:02x}", c as u32));
			}
			c => out.push(c),
		}
	}
	out.push('"');
	out
}

#[cfg(test)]
mod tests {
	use serde_json::{Value, json};

	use super::*;

	fn entries(raw: Value) -> Vec<QueryEntry> {
		serde_json::from_value(raw).expect("query entries")
	}

	fn render_default(raw: Value) -> String {
		render(&[], &entries(raw), &RenderOptions::default())
	}

	fn kind_of(raw: Value) -> Kind {
		serde_json::from_value(raw).expect("v2 kind")
	}

	/// The type a single statement of `kind` renders as, in a module with no types.
	fn slot(kind: Value) -> String {
		let options = RenderOptions::default();
		let mut module = Module {
			opts: &options,
			types: &[],
			expanding: Vec::new(),
			imports: Imports::default(),
			taken: BTreeSet::new(),
			interned: BTreeMap::new(),
			models: BTreeSet::new(),
			defs: Vec::new(),
		};
		module.kind(&kind_of(kind), "Row", &[], Style::Row)
	}

	fn int() -> Value {
		json!({ "kind": "int" })
	}

	fn query(key: &str, name: Option<&str>, statements: Value) -> Value {
		let mut entry = json!({ "key": key, "text": key, "statements": statements });
		if let Some(name) = name {
			entry["name"] = json!(name);
		}
		entry
	}

	fn statement(kind: Value) -> Value {
		json!({ "kind": kind, "optional": false, "certainty": "fact", "exact": true, "open": [] })
	}

	fn rows(fields: Value) -> Value {
		statement(json!({ "kind": "array", "item": { "kind": "objectLiteral", "fields": fields } }))
	}

	fn field(name: &str, kind: Value, optional: bool) -> Value {
		json!({ "name": name, "kind": kind, "optional": optional })
	}

	#[test]
	fn scalars_map_to_their_python_types() {
		for (kind, want) in [
			("any", "Any"),
			("function", "Any"),
			("regex", "Any"),
			("range", "Any"),
			("none", "None"),
			("null", "NullType"),
			("bool", "bool"),
			("int", "int"),
			("float", "float"),
			("number", "int | float"),
			("decimal", "decimal.Decimal"),
			("string", "str"),
			("datetime", "datetime.datetime"),
			("duration", "Duration"),
			("uuid", "uuid.UUID"),
			("bytes", "bytes"),
			("object", "dict[str, Value]"),
			("bigdecimal", "BigDecimal"),
			("date", "Any"),
		] {
			assert_eq!(slot(json!({ "kind": kind })), want, "{kind}");
		}
		assert_eq!(slot(json!({ "kind": "unknown", "source": "money" })), "Any");
	}

	#[test]
	fn references_collections_and_geometry() {
		assert_eq!(slot(json!({ "kind": "record", "tables": ["a"] })), "RecordID");
		assert_eq!(slot(json!({ "kind": "table", "tables": [] })), "Table");
		assert_eq!(slot(json!({ "kind": "file", "buckets": [] })), "File");
		assert_eq!(slot(json!({ "kind": "geometry", "types": ["point"] })), "GeometryPoint");
		assert_eq!(
			slot(json!({ "kind": "geometry", "types": ["point", "line"] })),
			"GeometryPoint | GeometryLine"
		);
		assert_eq!(
			slot(json!({ "kind": "geometry", "types": [] })),
			"Geometry | GeometryCollection"
		);
		assert_eq!(slot(json!({ "kind": "array", "item": int() })), "list[int]");
		assert_eq!(slot(json!({ "kind": "array", "item": int(), "length": 0 })), "list[Never]");
		assert_eq!(slot(json!({ "kind": "set", "item": int() })), "SurrealSet");
		assert_eq!(
			slot(json!({ "kind": "tuple", "items": [int(), { "kind": "string" }] })),
			"tuple[int, str]"
		);
		assert_eq!(slot(json!({ "kind": "tuple", "items": [] })), "tuple[()]");
	}

	#[test]
	fn literals_enums_and_unions() {
		assert_eq!(
			slot(json!({ "kind": "literal", "type": "string", "value": "a\"b" })),
			"Literal[\"a\\\"b\"]"
		);
		assert_eq!(slot(json!({ "kind": "literal", "type": "int", "value": "-3" })), "Literal[-3]");
		assert_eq!(
			slot(json!({ "kind": "literal", "type": "bool", "value": true })),
			"Literal[True]"
		);
		assert_eq!(slot(json!({ "kind": "literal", "type": "float", "value": "1.5" })), "float");
		assert_eq!(
			slot(json!({ "kind": "enum", "labels": ["low", "high", "low"] })),
			"Literal[\"low\", \"high\"]"
		);
		assert_eq!(slot(json!({ "kind": "enum", "labels": [] })), "Never");
		// Literal members of a union share one `Literal`, where the first stood.
		assert_eq!(
			slot(json!({ "kind": "union", "variants": [
				{ "kind": "literal", "type": "string", "value": "a" },
				{ "kind": "string" },
				{ "kind": "literal", "type": "string", "value": "b" },
				{ "kind": "none" },
			] })),
			"Literal[\"a\", \"b\"] | str | None"
		);
		assert_eq!(
			slot(json!({ "kind": "union", "variants": [int(), { "kind": "float" }, int()] })),
			"int | float"
		);
	}

	#[test]
	fn an_option_is_a_key_marker_on_a_field_and_none_elsewhere() {
		let option = json!({ "kind": "option", "item": { "kind": "string" } });
		assert_eq!(slot(option.clone()), "str | None");
		assert_eq!(slot(json!({ "kind": "array", "item": option })), "list[str | None]");
		let out = render_default(json!([query(
			"SELECT * FROM t",
			None,
			json!([rows(json!([
				field("a", json!({ "kind": "string" }), false),
				field("b", json!({ "kind": "string" }), true),
				field("c", option, false),
			]))])
		)]));
		assert!(
			out.contains("    a: str\n    b: NotRequired[str]\n    c: NotRequired[str]\n"),
			"{out}"
		);
	}

	#[test]
	fn a_statement_that_admits_none_is_a_union_with_none() {
		let mut optional = statement(json!({ "kind": "objectLiteral", "fields": [] }));
		optional["optional"] = json!(true);
		let out = render_default(json!([query("SELECT * FROM ONLY t", None, json!([optional]))]));
		assert!(out.contains("-> tuple[Query_"), "{out}");
		assert!(out.contains("Row | None]:\n"), "{out}");
	}

	#[test]
	fn names_follow_the_query_the_statement_and_the_field_path() {
		let nested = json!([
			field("id", json!({ "kind": "record", "tables": [] }), false),
			field(
				"owner_profile",
				json!({ "kind": "objectLiteral", "fields": [field("name", json!({ "kind": "string" }), false)] }),
				false
			),
			field(
				"items",
				json!({ "kind": "array", "item": { "kind": "objectLiteral",
					"fields": [field("n", int(), false)] } }),
				false
			),
		]);
		let out = render_default(json!([query(
			"SELECT 1; SELECT 2",
			Some("listCards"),
			json!([rows(nested), rows(json!([field("x", int(), false)]))])
		)]));
		for name in [
			"class ListCardsRowOwnerProfile(TypedDict):",
			"class ListCardsRowItems(TypedDict):",
			"class ListCardsRow(TypedDict):",
			"class ListCardsRow1(TypedDict):",
		] {
			assert!(out.contains(name), "{name} in\n{out}");
		}
		// A nested class is defined before the class that uses it.
		assert!(
			out.find("class ListCardsRowItems").unwrap() < out.find("class ListCardsRow(").unwrap()
		);
		assert!(out.contains("    owner_profile: ListCardsRowOwnerProfile\n"));
		assert!(out.contains("    items: list[ListCardsRowItems]\n"));
	}

	#[test]
	fn an_inline_query_is_named_by_the_hash_of_its_text() {
		let out = render_default(json!([query(
			"SELECT 1",
			None,
			json!([rows(json!([field("a", int(), false)]))])
		)]));
		// sha256("SELECT 1") begins 1b4d4b8e.
		let digest = Sha256::digest(b"SELECT 1");
		let prefix = format!("Query_{}", hex::encode(&digest[..4]));
		assert!(out.contains(&format!("class {prefix}Row(TypedDict):")), "{out}");
	}

	#[test]
	fn a_name_that_is_taken_gets_a_number() {
		// Both objects of the union want `ListRow`; the second is `ListRow2`, and the second
		// statement's `ListRow2` is numbered again.
		let union = statement(json!({ "kind": "union", "variants": [
			{ "kind": "objectLiteral", "fields": [field("a", int(), false)] },
			{ "kind": "objectLiteral", "fields": [field("b", int(), false)] },
		] }));
		let out = render_default(json!([query(
			"SELECT 1",
			Some("list"),
			json!([
				union.clone(),
				union,
				rows(json!([field("c", int(), false)])),
				rows(json!([field("d", int(), false)]))
			])
		)]));
		let classes: Vec<&str> =
			out.lines().filter_map(|line| line.strip_prefix("class ")?.split('(').next()).collect();
		let mut unique = classes.clone();
		unique.sort_unstable();
		unique.dedup();
		assert_eq!(classes.len(), unique.len(), "{classes:?}");
		assert!(classes.contains(&"ListRow") && classes.contains(&"ListRow2"), "{classes:?}");
	}

	#[test]
	fn a_key_that_is_not_an_identifier_uses_the_functional_syntax() {
		let out = render_default(json!([query(
			"SELECT 1",
			Some("keys"),
			json!([rows(json!([
				field("from", json!({ "kind": "string" }), false),
				field("weird key", int(), true),
			]))])
		)]));
		assert!(
			out.contains(
				"KeysRow = TypedDict(\n    \"KeysRow\",\n    {\n        \"from\": str,\n        \"weird key\": NotRequired[int],\n    },\n)"
			),
			"{out}"
		);
		// A keyword among identifiers keeps the others out of class syntax too.
		let models = render(
			&[],
			&entries(json!([query(
				"SELECT 1",
				Some("keys"),
				json!([rows(json!([
					field("from", json!({ "kind": "string" }), false),
					field("weird key", int(), true),
					field("model_x", int(), false),
				]))])
			)])),
			&RenderOptions {
				models: ModelStyle::Pydantic,
				..RenderOptions::default()
			},
		);
		assert!(models.contains("    from_: str = Field(alias=\"from\")\n"), "{models}");
		assert!(
			models
				.contains("    weird_key: int | None = Field(default=None, alias=\"weird key\")\n")
		);
		assert!(models.contains("    model_x_: int = Field(alias=\"model_x\")\n"));
		assert!(models.contains("from pydantic import BaseModel, ConfigDict, Field, TypeAdapter"));
	}

	#[test]
	fn the_client_has_one_overload_per_distinct_text_and_no_fallback() {
		let out = render_default(json!([
			query("SELECT 1", None, json!([statement(int())])),
			query("SELECT 2", Some("two"), json!([statement(int())])),
			query("SELECT 1", Some("again"), json!([statement(int())])),
		]));
		let client = &out[out.find("class TypedAsyncSurreal").unwrap()
			..out.find("class TypedSurreal:").unwrap()];
		assert_eq!(client.matches("@overload").count(), 2, "{client}");
		assert!(client.contains("query: Literal[\"SELECT 1\"],\n    ) -> tuple[int]: ..."));
		assert!(!client.contains("query: str,\n        params: str"));
		assert!(client.contains("    async def query(\n        self,\n        query: str,\n        params: Mapping[str, object] | None = None,\n    ) -> tuple[object, ...]:\n"));
		// The blocking client reads the builder's result.
		let blocking = &out[out.find("class TypedSurreal:").unwrap()..];
		assert!(blocking.contains("result = self.raw.query(query, variables).execute()"));
		assert!(blocking.contains("    def query(\n"));
	}

	#[test]
	fn one_entry_is_a_single_signature_and_none_is_a_bare_class() {
		let one = render_default(json!([query("SELECT 1", None, json!([statement(int())]))]));
		assert!(!one.contains("@overload"), "{one}");
		assert!(one.contains(
			"    async def query(\n        self,\n        query: Literal[\"SELECT 1\"],\n    ) -> tuple[int]:\n        result = await self.raw.query(query)\n        return cast(tuple[int], tuple(result))"
		), "{one}");
		assert!(!one.contains("Value"), "an unused name is not imported: {one}");

		let none = render_default(json!([]));
		assert!(none.contains(
			"class TypedAsyncSurreal:\n    def __init__(self, db: AsyncSurrealConnection) -> None:\n        self.raw = db\n"
		), "{none}");
		assert!(!none.contains("@overload") && !none.contains("def query"), "{none}");
		assert!(
			none.contains(
				"from surrealdb import AsyncSurrealConnection, BlockingSurrealConnection\n"
			)
		);
	}

	#[test]
	fn parameters_make_a_typed_dict_and_a_required_argument() {
		let mut entry = query("SELECT $a, $b", Some("p"), json!([statement(int())]));
		entry["params"] = json!([
			{ "name": "__host0", "kind": {"kind": "int"}, "optional": false, "required": true },
			{ "name": "a", "kind": { "kind": "string" }, "optional": false, "required": true },
			{ "name": "b", "kind": { "kind": "int" }, "optional": true, "required": false },
		]);
		let out = render_default(json!([entry]));
		assert!(
			out.contains("class PParams(TypedDict):\n    a: str\n    b: NotRequired[int | None]\n"),
			"{out}"
		);
		assert!(out.contains("        params: PParams,\n"), "{out}");
		assert!(out.contains("variables = cast(\n            \"dict[str, Value] | None\",\n            dict(params),\n        )"), "{out}");
		assert!(!out.contains("__host"));

		let mut optional = query("SELECT $b", Some("q"), json!([statement(int())]));
		optional["params"] = json!([
			{ "name": "b", "kind": { "kind": "int" }, "optional": false, "required": false },
		]);
		let out = render_default(json!([optional]));
		assert!(out.contains("        params: QParams | None = None,\n"), "{out}");
		assert!(out.contains("dict(params) if params is not None else None"), "{out}");
	}

	#[test]
	fn a_failed_query_is_a_comment_and_no_overload() {
		let mut broken = query("SELEC 1", Some("bad"), json!([]));
		broken["error"] = json!({ "code": "S0001", "message": "Unexpected token" });
		let out =
			render_default(json!([broken, query("SELECT 1", None, json!([statement(int())]))]));
		assert!(out.contains("# Not typed: \"bad\" (S0001): \"SELEC 1\"\n"), "{out}");
		assert!(!out.contains("Literal[\"SELEC 1\"]"));
	}

	fn live_entry(name: &str, text: &str, refusal: bool) -> Value {
		let notification = |action: &str| {
			json!({ "action": action, "certainty": "fact", "open": [],
				"record": { "kind": "record", "tables": ["t"] },
				"result": { "kind": "objectLiteral", "fields": [field("n", int(), false)] } })
		};
		let mut live = statement(json!({ "kind": "array", "item": { "kind": "objectLiteral",
			"fields": [field("n", int(), false)] } }));
		live["live"] = json!({ "diff": false, "query": format!("LIVE {text}"),
			"notifications": [notification("CREATE"), notification("DELETE"), notification("KILLED")] });
		if refusal {
			live["liveRefusal"] = json!({ "code": "E", "message": "m",
				"clauses": ["LIMIT", "ORDER BY"], "windowable": true });
		}
		query(text, Some(name), json!([live]))
	}

	#[test]
	fn a_live_statement_gets_notifications_and_a_generator() {
		let out = render_default(json!([
			live_entry("watch", "SELECT * FROM a", false),
			live_entry("other", "SELECT * FROM b", false),
			query("SELECT 1", None, json!([statement(int())])),
		]));
		assert!(out.contains(
			"class WatchCreate(TypedDict):\n    action: Literal[\"CREATE\"]\n    id: uuid.UUID\n    record: RecordID\n    result: WatchRow\n"
		), "{out}");
		assert!(out.contains("class WatchDelete(TypedDict):"), "{out}");
		// KILLED ends the stream and has no type.
		assert!(!out.contains("Killed") && !out.contains("KILLED"), "{out}");
		assert!(
			out.contains("WatchNotification: TypeAlias = WatchCreate | WatchDelete\n"),
			"{out}"
		);
		assert!(out.contains("_LIVE: dict[str, str] = {\n    \"SELECT * FROM a\": \"LIVE SELECT * FROM a\",\n    \"SELECT * FROM b\": \"LIVE SELECT * FROM b\",\n}"), "{out}");
		// A stub is a plain `def`; the implementation is the generator.
		assert!(out.contains("    @overload\n    def live_query(\n        self,\n        query: Literal[\"SELECT * FROM a\"],\n    ) -> AsyncIterator[WatchNotification]: ..."), "{out}");
		assert!(out.contains("    async def live_query(\n        self,\n        query: str,"));
		assert!(out.contains("async for notification in await self.raw.subscribe_live(live_id):"));
		assert!(out.contains("if not isinstance(live_id, uuid.UUID):"));
		assert!(out.contains("    ) -> Iterator[WatchNotification]: ..."), "{out}");
		assert!(out.contains("for notification in self.raw.subscribe_live(live_id):"));
	}

	#[test]
	fn one_live_statement_is_a_single_signature_that_casts() {
		let out = render_default(json!([live_entry("watch", "SELECT * FROM a", false)]));
		assert!(!out.contains("@overload"), "{out}");
		assert!(out.contains("            yield cast(WatchNotification, notification)"), "{out}");
	}

	#[test]
	fn a_refused_live_statement_is_a_comment_naming_its_clauses() {
		let out = render_default(json!([live_entry("watch", "SELECT * FROM a LIMIT 1", true)]));
		assert!(!out.contains("def live_query") && !out.contains("_LIVE"), "{out}");
		assert!(out.contains(
			"# No live_query overload for \"SELECT * FROM a LIMIT 1\": LIVE SELECT does not accept LIMIT, ORDER BY\n"
		), "{out}");
	}

	#[test]
	fn distrusted_hints_are_value() {
		let mut hinted = statement(int());
		hinted["certainty"] = json!("hint");
		let entries = entries(json!([query("SELECT 1", None, json!([hinted]))]));
		let options = RenderOptions {
			hints: HintStyle::Unknown,
			..RenderOptions::default()
		};
		let out = render(&[], &entries, &options);
		assert!(out.contains("-> tuple[Value]:\n"), "{out}");
	}
}
