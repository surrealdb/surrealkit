//! Schema document v2: the serde model.
//!
//! The normative description is `docs/typegen/schema-document-v2.md`; the machine check is
//! `schema/schema-document-v2.schema.json`. These types serialise to exactly that shape:
//! every collection is an array in a defined order, optional properties are omitted rather
//! than `null`, and every number is an integer. Decoding is strict — an unknown property is
//! an error — so a reader refuses a document it would misread.
//!
//! A document is read from the engine through the version-qualified `INFO … STRUCTURE`
//! adapter, or from plain `INFO` by the fallback reader; both feed one builder that normalises,
//! sorts, diagnoses and computes the [semantic digest](super::canonical). See
//! [`introspect_v2`](super::introspect_v2).

use serde::{Deserialize, Serialize};

use super::diagnostics::Diagnostic;

/// The version this module reads and writes.
pub const SCHEMA_DOCUMENT_VERSION: u32 = 2;

/// The JSON Schema (draft 2020-12) every [`SchemaDocument`] validates against.
pub const SCHEMA_DOCUMENT_V2_JSON_SCHEMA: &str =
	include_str!("../../schema/schema-document-v2.schema.json");

/// A schema document v2.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SchemaDocument {
	/// Always [`SCHEMA_DOCUMENT_VERSION`].
	pub version: u32,
	/// `sha256:<hex>` over the canonical JSON of the semantic content.
	pub semantic_digest: String,
	/// Where the document came from. Not part of the digest.
	pub provenance: Provenance,
	pub tables: Vec<Table>,
	pub functions: Vec<Function>,
	pub params: Vec<Param>,
	pub accesses: Vec<Access>,
	pub analyzers: Vec<Analyzer>,
	pub sequences: Vec<Sequence>,
	pub buckets: Vec<Bucket>,
	pub apis: Vec<Api>,
	pub modules: Vec<ModuleDef>,
	pub configs: Vec<Config>,
	pub users: Vec<User>,
	/// Everything not structurally understood. Not part of the digest.
	pub diagnostics: Vec<Diagnostic>,
}

impl SchemaDocument {
	/// Whether any diagnostic has error severity.
	pub fn has_errors(&self) -> bool {
		self.diagnostics.iter().any(Diagnostic::is_error)
	}
}

// ---------------------------------------------------------------------------
// Provenance
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Provenance {
	pub generator: Generator,
	pub engine: Engine,
	/// The adapter that read the engine's schema: `structure-3.2`, `structure-3.3`, or
	/// `info` for the plain-`INFO` fallback.
	pub adapter: String,
	pub source: Source,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub capabilities: Option<Capabilities>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Generator {
	/// Always `surrealkit`.
	pub name: String,
	pub version: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub commit: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Engine {
	/// `MAJOR.MINOR.PATCH`.
	pub version: String,
	/// The full version string the engine reports, e.g. `surrealdb-3.2.4+20260803.93ab219`.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub build: Option<String>,
	pub mode: EngineMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EngineMode {
	/// SurrealKit's own embedded engine (`surrealkit compile`).
	Embedded,
	/// A server SurrealKit connected to.
	Remote,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum Source {
	/// `surrealkit compile`: the modules applied, and every file's content hash.
	Compile {
		modules: Vec<String>,
		files: Vec<SourceFile>,
	},
	/// Introspection of a live database.
	Introspect {
		namespace: String,
		database: String,
	},
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SourceFile {
	/// Relative to the project root, `/`-separated.
	pub path: String,
	/// Lowercase hex SHA-256 of the bytes, after removing a UTF-8 BOM and turning CRLF into LF.
	pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Capabilities {
	/// Experimental features the compile engine allowed, sorted.
	pub experimental: Vec<String>,
	/// Whether the compile engine could reach the network. Always `false` for a compile.
	pub network: bool,
}

// ---------------------------------------------------------------------------
// Kinds
// ---------------------------------------------------------------------------

/// A SurrealQL value kind, tagged on `kind`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum Kind {
	Any,
	None,
	Null,
	Bool,
	Int,
	Float,
	Decimal,
	Number,
	String,
	Datetime,
	Duration,
	Uuid,
	Bytes,
	Regex,
	Range,
	Function,
	Object,
	/// `record<a | b>`; empty = any table.
	Record {
		tables: Vec<String>,
	},
	/// `table<a | b>`; empty = any table.
	Table {
		tables: Vec<String>,
	},
	/// `file<bucket | …>`; empty = any bucket.
	File {
		buckets: Vec<String>,
	},
	/// `geometry<point | …>`; empty = any geometry.
	Geometry {
		types: Vec<String>,
	},
	/// `array<T, N>`: `length` is the exact element count.
	Array {
		item: Box<Kind>,
		#[serde(default, skip_serializing_if = "Option::is_none")]
		length: Option<u64>,
	},
	/// `set<T, N>`: `length` is the exact distinct-element count.
	Set {
		item: Box<Kind>,
		#[serde(default, skip_serializing_if = "Option::is_none")]
		length: Option<u64>,
	},
	Tuple {
		items: Vec<Kind>,
	},
	ObjectLiteral {
		fields: Vec<ObjectLiteralField>,
	},
	Literal {
		#[serde(rename = "type")]
		literal_type: LiteralType,
		value: LiteralValue,
	},
	/// `none | T` in a nested position.
	Option {
		item: Box<Kind>,
	},
	/// Flattened, deduplicated, sorted by canonical JSON; at least two variants.
	Union {
		variants: Vec<Kind>,
	},
	/// Kind text that could not be structured; always with a `kind-unrecognized` error.
	Unknown {
		source: String,
	},
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ObjectLiteralField {
	pub name: String,
	pub kind: Kind,
	pub optional: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LiteralType {
	String,
	Int,
	Float,
	Decimal,
	Bool,
	Duration,
}

/// A literal's value: a boolean for `bool`, otherwise its text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum LiteralValue {
	Bool(bool),
	Text(String),
}

// ---------------------------------------------------------------------------
// Tables and fields
// ---------------------------------------------------------------------------

/// `true` = FULL, `false` = NONE, otherwise the `WHERE` expression.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Permission {
	Bool(bool),
	Expr(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TablePermissions {
	pub select: Permission,
	pub create: Permission,
	pub update: Permission,
	pub delete: Permission,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldPermissions {
	pub select: Permission,
	pub create: Permission,
	pub update: Permission,
}

impl FieldPermissions {
	/// The permissions of a field defined without `PERMISSIONS`: all `FULL`.
	pub fn full() -> Self {
		Self {
			select: Permission::Bool(true),
			create: Permission::Bool(true),
			update: Permission::Bool(true),
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Table {
	pub name: String,
	#[serde(rename = "type")]
	pub table_type: TableType,
	pub schemafull: bool,
	pub drop: bool,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub view: Option<View>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub changefeed: Option<Changefeed>,
	pub permissions: TablePermissions,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub id_key: Option<Kind>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub comment: Option<String>,
	pub fields: Vec<Field>,
	pub indexes: Vec<Index>,
	pub events: Vec<Event>,
	/// The plain-`INFO` `DEFINE TABLE`. Evidence only; not part of the digest.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub define: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum TableType {
	Normal,
	Any,
	Relation {
		#[serde(rename = "in")]
		from: Vec<String>,
		#[serde(rename = "out")]
		to: Vec<String>,
		enforced: bool,
		lightweight: bool,
	},
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct View {
	/// The view's `SELECT`, without the leading `AS `.
	pub select: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Changefeed {
	pub expiry: String,
	pub include_original: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Field {
	pub path: Vec<String>,
	/// Absent: no `TYPE` clause.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub kind: Option<Kind>,
	pub optional: bool,
	pub flexible: bool,
	pub readonly: bool,
	pub inline: bool,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub default: Option<DefaultClause>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub value: Option<ValueClause>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub computed: Option<ExprClause>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub assert: Option<AssertClause>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub reference: Option<Reference>,
	pub permissions: FieldPermissions,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub comment: Option<String>,
	/// The plain-`INFO` `DEFINE FIELD`. Evidence only; not part of the digest.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub define: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DefaultMode {
	/// Applied when a record is created.
	Create,
	/// Also re-applied whenever the field becomes NONE (`DEFAULT ALWAYS`).
	Always,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DefaultClause {
	pub mode: DefaultMode,
	pub expr: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ValueClause {
	pub expr: String,
	/// Derived: the expression reads `$value` or `$input`. Not part of the digest.
	pub reads_input: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExprClause {
	pub expr: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssertClause {
	pub expr: String,
	/// Derived; present only when the whole expression was recognised. Not part of the
	/// digest.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub recognized: Option<Vec<Refinement>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Reference {
	pub on_delete: OnDelete,
	/// The `ON DELETE THEN` expression.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub expr: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OnDelete {
	Reject,
	Ignore,
	Cascade,
	Unset,
	Then,
}

/// A recognised assertion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum Refinement {
	/// The value is one of these literal kinds.
	Literals {
		values: Vec<Kind>,
	},
	/// A numeric or duration range.
	#[serde(rename_all = "camelCase")]
	Range {
		value_kind: RangeValueKind,
		#[serde(default, skip_serializing_if = "Option::is_none")]
		min: Option<Bound>,
		#[serde(default, skip_serializing_if = "Option::is_none")]
		max: Option<Bound>,
	},
	/// Inclusive bounds on a string's code-point count or an array's length.
	Length {
		target: LengthTarget,
		#[serde(default, skip_serializing_if = "Option::is_none")]
		min: Option<u64>,
		#[serde(default, skip_serializing_if = "Option::is_none")]
		max: Option<u64>,
	},
	Email,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RangeValueKind {
	Int,
	Float,
	Decimal,
	Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LengthTarget {
	String,
	Array,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bound {
	pub value: String,
	pub inclusive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Index {
	pub name: String,
	pub columns: Vec<Vec<String>>,
	#[serde(rename = "type")]
	pub index_type: IndexType,
	/// The engine's rendering of the index clause, verbatim.
	pub clause: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub fulltext: Option<Fulltext>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub vector: Option<Vector>,
	#[serde(rename = "where", default, skip_serializing_if = "Option::is_none")]
	pub where_clause: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub comment: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IndexType {
	Standard,
	Unique,
	Fulltext,
	Hnsw,
	Diskann,
	Count,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fulltext {
	pub analyzer: String,
	pub highlights: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Vector {
	pub dimension: u64,
	pub distance: String,
	pub element_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Event {
	pub name: String,
	/// `true` when the event has no `WHEN`.
	pub when: String,
	pub then: Vec<String>,
	#[serde(rename = "async")]
	pub is_async: bool,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub retry: Option<u64>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_depth: Option<u64>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub comment: Option<String>,
}

// ---------------------------------------------------------------------------
// Functions and other definitions
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Function {
	/// With its `fn::` prefix.
	pub name: String,
	pub args: Vec<FunctionArg>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub returns: Option<Kind>,
	pub block: String,
	pub permissions: Permission,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FunctionArg {
	pub name: String,
	pub kind: Kind,
	/// The kind admits NONE.
	pub optional: bool,
	/// Part of the trailing run of optional arguments a caller may leave out.
	pub omittable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Param {
	/// Without the `$`.
	pub name: String,
	/// The value the engine evaluated at definition time, as rendered.
	pub value: String,
	pub permissions: Permission,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub comment: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Durations {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub grant: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub token: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub session: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AccessMethod {
	Record,
	Jwt,
	Bearer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BearerSubject {
	Record,
	User,
}

/// How a JWT is verified: an algorithm (the key itself is never copied) or a JWKS URL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Verify {
	Alg(VerifyAlg),
	Url(VerifyUrl),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifyAlg {
	pub alg: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifyUrl {
	pub url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Access {
	pub name: String,
	pub method: AccessMethod,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub signup: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub signin: Option<String>,
	/// The engine-rendered authentication expression, when the access declares one.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub authenticate: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub refresh: Option<bool>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub verify: Option<Verify>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub subject: Option<BearerSubject>,
	pub durations: Durations,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Analyzer {
	pub name: String,
	/// With its `fn::` prefix.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub function: Option<String>,
	pub tokenizers: Vec<String>,
	pub filters: Vec<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sequence {
	pub name: String,
	pub batch: String,
	pub start: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub timeout: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bucket {
	pub name: String,
	pub backend: String,
	pub readonly: bool,
	pub permissions: Permission,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Middleware {
	pub name: String,
	pub args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiAction {
	pub methods: Vec<String>,
	pub middleware: Vec<Middleware>,
	pub permissions: Permission,
	pub action: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Api {
	pub path: String,
	pub actions: Vec<ApiAction>,
	pub middleware: Vec<Middleware>,
	pub permissions: Permission,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Executable {
	#[serde(rename = "type")]
	pub executable_type: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub bucket: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModuleDef {
	/// With its `mod::` prefix.
	pub name: String,
	pub executable: Executable,
	pub permissions: Permission,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum Config {
	Graphql {
		tables: String,
		functions: String,
	},
	Api {
		middleware: Vec<Middleware>,
		permissions: Permission,
	},
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct User {
	pub name: String,
	pub roles: Vec<String>,
	pub durations: Durations,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub comment: Option<String>,
}
