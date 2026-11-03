//! Serde data model for the generated schema document.
//!
//! [`SchemaTypes`] is the top-level document emitted by `surrealkit typegen`.
//! It is intentionally self-describing (the [`FieldType`] enum is internally
//! tagged on `kind`) so the JSON can be consumed by other emitters — e.g. a
//! future TypeScript generator — without re-introspecting the database.

use serde::{Deserialize, Serialize};

/// Top-level generated schema document.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SchemaTypes {
	/// Schema-document format version. Bump on breaking shape changes.
	pub version: u32,
	/// RFC3339 timestamp of when the document was generated. Empty, and then
	/// omitted from the JSON, when generation was asked not to stamp it
	/// (`[typegen] generated_at = false`), so identical schemas produce identical
	/// bytes.
	#[serde(default, skip_serializing_if = "String::is_empty")]
	pub generated_at: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub namespace: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub database: Option<String>,
	pub tables: Vec<TableDef>,
	pub functions: Vec<FunctionDef>,
	pub params: Vec<ParamDef>,
	pub analyzers: Vec<NamedDef>,
	pub accesses: Vec<NamedDef>,
	pub apis: Vec<NamedDef>,
	pub buckets: Vec<NamedDef>,
	pub sequences: Vec<NamedDef>,
	pub configs: Vec<NamedDef>,
	pub models: Vec<NamedDef>,
	pub users: Vec<NamedDef>,
}

/// Catch-all element that preserves the name plus the raw `DEFINE` statement.
///
/// Used for kinds that are not (yet) deeply parsed, so the document stays
/// lossless and a later pass can enrich it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct NamedDef {
	pub name: String,
	pub define: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TableDef {
	pub name: String,
	pub define: String,
	/// `Some(true)` for SCHEMAFULL, `Some(false)` for SCHEMALESS, `None` if absent.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub schemafull: Option<bool>,
	/// Table kind: `NORMAL`, `RELATION`, or `ANY`, if declared.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub kind: Option<String>,
	pub fields: Vec<FieldDef>,
	pub events: Vec<NamedDef>,
	pub indexes: Vec<NamedDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FieldDef {
	/// Exact field path as reported by the database (e.g. `name`,
	/// `address.city`, `tags[*]`).
	pub name: String,
	pub define: String,
	pub r#type: FieldType,
	/// `true` when the field's top-level type was `option<...>`. The inner type
	/// is unwrapped into the `type` field.
	pub optional: bool,
	/// `FLEXIBLE` keyword present.
	pub flexible: bool,
	/// `READONLY` keyword present.
	pub readonly: bool,
	/// `DEFAULT` clause present.
	pub has_default: bool,
	/// The exact captured `TYPE` substring, for debugging / round-tripping.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub raw_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FunctionDef {
	/// Function name including the `fn::` prefix.
	pub name: String,
	pub define: String,
	pub args: Vec<FnArg>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub returns: Option<FieldType>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FnArg {
	pub name: String,
	pub r#type: FieldType,
	pub optional: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ParamDef {
	pub name: String,
	pub define: String,
}

/// A parsed SurrealQL type. Internally tagged on `kind` so the JSON is
/// self-describing and downstream emitters can switch on it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum FieldType {
	Primitive {
		name: PrimitiveType,
	},
	Option {
		inner: Box<FieldType>,
	},
	/// `array`, `array<T>` or `array<T, N>`. `max` is the `N`, which SurrealDB
	/// enforces as the *exact* length (both fewer and more elements are
	/// rejected); the name predates that finding and is kept for compatibility.
	Array {
		inner: Box<FieldType>,
		max: Option<u64>,
	},
	/// `set`, `set<T>` or `set<T, N>`; `max` is an exact length, as for arrays.
	Set {
		inner: Box<FieldType>,
		max: Option<u64>,
	},
	/// `record<a | b>`; empty `tables` means `record` / `record<any>`.
	Record {
		tables: Vec<String>,
	},
	/// `table<a | b>`, a table name value; empty `tables` means any table.
	Table {
		tables: Vec<String>,
	},
	/// `file<bucket | …>`, a file pointer; empty `buckets` means any bucket.
	File {
		buckets: Vec<String>,
	},
	/// `geometry<point | polygon | ...>`.
	Geometry {
		kinds: Vec<String>,
	},
	/// A literal type such as `"active"`, `200`, or `true`.
	///
	/// `literal_type` (serialised as `type`) distinguishes the literals a JSON
	/// value cannot: `float` (`1.5f`, value a number, or the string `NaN` /
	/// `Infinity` / `-Infinity`), `decimal` (`10dec`, value the number as a
	/// string, so it stays exact) and `duration` (`1h30m`, value the duration
	/// as a string). It is absent for strings, integers and booleans, which
	/// serialise exactly as before it existed.
	Literal {
		value: serde_json::Value,
		#[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
		literal_type: Option<LiteralType>,
	},
	/// `[A, B, C]`, a fixed-length array whose elements have these kinds in order.
	Tuple {
		items: Vec<FieldType>,
	},
	Union {
		variants: Vec<FieldType>,
	},
	/// An object literal kind such as `{ a: int, b: option<string> }`: exactly
	/// these keys. Bare `object` is [`PrimitiveType::Object`] instead. An
	/// optional key keeps its `none | T` kind as a [`FieldType::Union`]
	/// containing [`PrimitiveType::None`].
	Object {
		fields: Vec<ObjectField>,
	},
	/// Anything the parser did not recognise. Keeps the source so output is
	/// never lossy and the parser never panics. Every `Unknown` with a source
	/// comes with a `kind-unrecognized` error [`Diagnostic`](super::Diagnostic).
	/// A field without `TYPE` is `Unknown` with an empty source, as v1 has
	/// always written it (`unknown` in TypeScript), with a `field-kind-undeclared`
	/// info or, when `COMPUTED`, a `field-computed-kind-undeclared` warning.
	Unknown {
		source: String,
	},
}

/// The kinds of literal that need a tag beside their JSON value.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LiteralType {
	Float,
	Decimal,
	Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ObjectField {
	/// The key, unquoted.
	pub name: String,
	pub r#type: FieldType,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PrimitiveType {
	String,
	Int,
	Float,
	Bool,
	Number,
	Decimal,
	Datetime,
	Duration,
	Uuid,
	Bytes,
	Any,
	Null,
	/// The `none` type. SurrealDB normalises `option<T>` to `none | T`, so this
	/// usually appears as a union member that [`crate::typegen`] strips into the
	/// `optional` flag rather than a field type in its own right.
	None,
	/// Bare `object` with no inline shape.
	Object,
	/// Bare `function`.
	Function,
	/// `regex`. The SDK has no wire representation for it.
	Regex,
	/// `range`.
	Range,
}
