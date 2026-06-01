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
	/// RFC3339 timestamp of when the document was generated.
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
	/// is unwrapped into [`FieldDef::r#type`].
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
	Array {
		inner: Box<FieldType>,
		max: Option<u64>,
	},
	Set {
		inner: Box<FieldType>,
		max: Option<u64>,
	},
	/// `record<a | b>`; empty `tables` means `record` / `record<any>`.
	Record {
		tables: Vec<String>,
	},
	/// `geometry<point | polygon | ...>`.
	Geometry {
		kinds: Vec<String>,
	},
	/// A literal type such as `"active"`, `200`, or `true`.
	Literal {
		value: serde_json::Value,
	},
	Union {
		variants: Vec<FieldType>,
	},
	/// Reserved for future inline object shapes; not produced in v1.
	Object {
		fields: Vec<ObjectField>,
	},
	/// Anything the parser did not recognise. Keeps the source so output is
	/// never lossy and the parser never panics.
	Unknown {
		source: String,
	},
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ObjectField {
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
}
