//! The version-qualified `INFO … STRUCTURE` adapter (contract sections 1 and 15).
//!
//! `INFO FOR DB STRUCTURE` and `INFO FOR TABLE <t> STRUCTURE` are documented by SurrealDB as
//! internal and subject to change, so they are read only through an adapter per supported
//! engine minor. An adapter knows every key its minor returns: a key it does not know is an
//! error diagnostic (`structure-unknown-key`), never silently dropped, and an engine minor
//! with no adapter is an error (`structure-unsupported-engine`) — the caller then falls back
//! to [`super::info_v2`].
//!
//! Supported: SurrealDB 3.2.x (`structure-3.2`) and 3.3.x (`structure-3.3`). The adapters
//! differ only where the engines do (section 15): 3.3 adds `lightweight`, `inline`, a module's
//! `unsigned`, a sequence's `resume`, a user's `scram`, an access's `context` and a JWT's
//! `audience` (both reported as `definition-unsupported`), and parenthesises statements in
//! event `THEN` lists and access `SIGNUP`/`SIGNIN`/`AUTHENTICATE`, which `structure-3.3` removes.
//!
//! Plain `INFO` text, when the caller has it, is attached to tables and fields as `define`
//! evidence. It is never the source of a semantic value.

use serde_json::{Map, Value};

use super::build_v2::{RawField, RawFunction, RawIndex, RawSchema, RawTable};
use super::diagnostics::{Diagnostic, Severity, codes};
use super::document::{
	Access, AccessMethod, Analyzer, Api, ApiAction, BearerSubject, Bucket, Changefeed, Config,
	Durations, Event, Executable, FieldPermissions, Middleware, ModuleDef, Param, Permission,
	Sequence, TablePermissions, TableType, User, Verify, VerifyAlg, VerifyUrl,
};
use super::refine::strip_statement_parens;

/// A supported engine minor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Adapter {
	/// SurrealDB 3.2.x.
	Structure32,
	/// SurrealDB 3.3.x.
	Structure33,
}

impl Adapter {
	/// The adapter for an engine version (`MAJOR.MINOR.PATCH`, anything after ignored), or
	/// `None` when that minor is not supported.
	pub fn for_engine(version: &str) -> Option<Adapter> {
		let mut parts = version.split(['.', '-', '+']);
		match (parts.next(), parts.next()) {
			(Some("3"), Some("2")) => Some(Adapter::Structure32),
			(Some("3"), Some("3")) => Some(Adapter::Structure33),
			_ => None,
		}
	}

	/// The name recorded in `provenance.adapter`.
	pub fn name(self) -> &'static str {
		match self {
			Adapter::Structure32 => "structure-3.2",
			Adapter::Structure33 => "structure-3.3",
		}
	}

	fn is_33(self) -> bool {
		self == Adapter::Structure33
	}
}

/// What one introspection read: `INFO FOR DB STRUCTURE`, and per table `INFO FOR TABLE
/// STRUCTURE`, plus — optional evidence — the plain `INFO` results.
pub struct StructureInput<'a> {
	pub db: &'a Value,
	/// `(table name, INFO FOR TABLE <t> STRUCTURE)`.
	pub tables: &'a [(String, Value)],
	/// Plain `INFO FOR DB`, for `define` evidence.
	pub db_info: Option<&'a Value>,
	/// `(table name, INFO FOR TABLE <t>)`, for `define` evidence.
	pub tables_info: &'a [(String, Value)],
}

/// Reading context: the adapter, and where unknown keys are reported.
struct Reader {
	adapter: Adapter,
	diagnostics: Vec<Diagnostic>,
}

impl Reader {
	/// The object `value` is, after reporting every key outside `known` (and outside `known33`
	/// unless the adapter is 3.3).
	fn object<'v>(
		&mut self,
		value: &'v Value,
		what: &str,
		known: &[&str],
		known33: &[&str],
		table: Option<&str>,
	) -> Option<&'v Map<String, Value>> {
		let Some(map) = value.as_object() else {
			self.shape(what, value, "an object", table);
			return None;
		};
		for key in map.keys() {
			let ok = known.contains(&key.as_str())
				|| (self.adapter.is_33() && known33.contains(&key.as_str()));
			if !ok {
				let mut d = Diagnostic::new(
					Severity::Error,
					codes::STRUCTURE_UNKNOWN_KEY,
					format!("{what}.{key}"),
					format!(
						"INFO STRUCTURE returned the key `{key}` on {what}, which the {} adapter \
						 does not map",
						self.adapter.name()
					),
				);
				if let Some(t) = table {
					d = d.on_table(t);
				}
				self.diagnostics.push(d);
			}
		}
		Some(map)
	}

	/// Report a value of the wrong JSON type.
	fn shape(&mut self, what: &str, value: &Value, expected: &str, table: Option<&str>) {
		let mut d = Diagnostic::new(
			Severity::Error,
			codes::STRUCTURE_UNKNOWN_KEY,
			format!("{what}: {value}"),
			format!(
				"INFO STRUCTURE returned {value} for {what}, where the {} adapter expects {expected}",
				self.adapter.name()
			),
		);
		if let Some(t) = table {
			d = d.on_table(t);
		}
		self.diagnostics.push(d);
	}

	fn string(
		&mut self,
		map: &Map<String, Value>,
		key: &str,
		what: &str,
		table: Option<&str>,
	) -> Option<String> {
		match map.get(key) {
			None | Some(Value::Null) => None,
			Some(Value::String(s)) => Some(s.clone()),
			Some(other) => {
				self.shape(&format!("{what}.{key}"), other, "a string", table);
				None
			}
		}
	}

	fn flag(
		&mut self,
		map: &Map<String, Value>,
		key: &str,
		what: &str,
		table: Option<&str>,
	) -> bool {
		match map.get(key) {
			None | Some(Value::Null) => false,
			Some(Value::Bool(b)) => *b,
			Some(other) => {
				self.shape(&format!("{what}.{key}"), other, "a boolean", table);
				false
			}
		}
	}

	fn integer(
		&mut self,
		map: &Map<String, Value>,
		key: &str,
		what: &str,
		table: Option<&str>,
	) -> Option<u64> {
		match map.get(key) {
			None | Some(Value::Null) => None,
			Some(Value::Number(n)) if n.as_u64().is_some() => n.as_u64(),
			Some(other) => {
				self.shape(&format!("{what}.{key}"), other, "a non-negative integer", table);
				None
			}
		}
	}

	fn strings(
		&mut self,
		map: &Map<String, Value>,
		key: &str,
		what: &str,
		table: Option<&str>,
	) -> Vec<String> {
		match map.get(key) {
			None | Some(Value::Null) => Vec::new(),
			Some(Value::Array(items)) => items
				.iter()
				.filter_map(|item| match item {
					Value::String(s) => Some(s.clone()),
					other => {
						self.shape(&format!("{what}.{key}[]"), other, "a string", table);
						None
					}
				})
				.collect(),
			Some(other) => {
				self.shape(&format!("{what}.{key}"), other, "an array", table);
				Vec::new()
			}
		}
	}

	fn array<'v>(
		&mut self,
		map: &'v Map<String, Value>,
		key: &str,
		what: &str,
		table: Option<&str>,
	) -> &'v [Value] {
		match map.get(key) {
			Some(Value::Array(items)) => items,
			None | Some(Value::Null) => &[],
			Some(other) => {
				self.shape(&format!("{what}.{key}"), other, "an array", table);
				&[]
			}
		}
	}

	fn permission(
		&mut self,
		map: &Map<String, Value>,
		key: &str,
		what: &str,
		table: Option<&str>,
		absent: bool,
	) -> Permission {
		match map.get(key) {
			None | Some(Value::Null) => Permission::Bool(absent),
			Some(Value::Bool(b)) => Permission::Bool(*b),
			Some(Value::String(s)) => Permission::Expr(s.clone()),
			Some(other) => {
				self.shape(&format!("{what}.{key}"), other, "a boolean or an expression", table);
				Permission::Bool(absent)
			}
		}
	}

	fn statement(&self, text: String) -> String {
		if self.adapter.is_33() {
			strip_statement_parens(&text)
		} else {
			text
		}
	}
}

/// The `DEFINE …` string under `category.name` in a plain `INFO` result.
fn define_of(info: Option<&Value>, category: &str, name: &str) -> Option<String> {
	info?.get(category)?.get(name)?.as_str().map(str::to_string)
}

/// Read a whole schema through `adapter`.
pub(super) fn read(adapter: Adapter, input: &StructureInput<'_>) -> RawSchema {
	let mut r = Reader {
		adapter,
		diagnostics: Vec::new(),
	};
	let mut raw = RawSchema::default();
	let Some(db) = r.object(
		input.db,
		"db",
		&[
			"accesses",
			"analyzers",
			"apis",
			"buckets",
			"configs",
			"functions",
			"models",
			"modules",
			"params",
			"sequences",
			"tables",
			"users",
		],
		&[],
		None,
	) else {
		raw.diagnostics = r.diagnostics;
		return raw;
	};

	for table in r.array(db, "tables", "db", None).to_vec() {
		if let Some(t) = read_table(&mut r, &table, input) {
			raw.tables.push(t);
		}
	}
	for f in r.array(db, "functions", "db", None).to_vec() {
		if let Some(f) = read_function(&mut r, &f) {
			raw.functions.push(f);
		}
	}
	for p in r.array(db, "params", "db", None).to_vec() {
		if let Some(map) =
			r.object(&p, "param", &["name", "value", "permissions", "comment"], &[], None)
		{
			raw.params.push(Param {
				name: r.string(map, "name", "param", None).unwrap_or_default(),
				value: r.string(map, "value", "param", None).unwrap_or_default(),
				permissions: r.permission(map, "permissions", "param", None, false),
				comment: r.string(map, "comment", "param", None),
			});
		}
	}
	for a in r.array(db, "accesses", "db", None).to_vec() {
		if let Some(a) = read_access(&mut r, &a) {
			raw.accesses.push(a);
		}
	}
	for a in r.array(db, "analyzers", "db", None).to_vec() {
		if let Some(map) = r.object(
			&a,
			"analyzer",
			&["name", "function", "tokenizers", "filters", "comment"],
			&[],
			None,
		) {
			raw.analyzers.push(Analyzer {
				name: r.string(map, "name", "analyzer", None).unwrap_or_default(),
				function: r.string(map, "function", "analyzer", None).map(|f| prefixed("fn::", f)),
				tokenizers: r.strings(map, "tokenizers", "analyzer", None),
				filters: r.strings(map, "filters", "analyzer", None),
				comment: r.string(map, "comment", "analyzer", None),
			});
		}
	}
	for s in r.array(db, "sequences", "db", None).to_vec() {
		if let Some(map) =
			r.object(&s, "sequence", &["name", "batch", "start", "timeout"], &["resume"], None)
		{
			raw.sequences.push(Sequence {
				name: r.string(map, "name", "sequence", None).unwrap_or_default(),
				batch: scalar_text(map.get("batch")),
				start: scalar_text(map.get("start")),
				timeout: r.string(map, "timeout", "sequence", None),
			});
		}
	}
	for b in r.array(db, "buckets", "db", None).to_vec() {
		if let Some(map) = r.object(
			&b,
			"bucket",
			&["name", "backend", "readonly", "permissions", "comment"],
			&[],
			None,
		) {
			raw.buckets.push(Bucket {
				name: r.string(map, "name", "bucket", None).unwrap_or_default(),
				backend: r.string(map, "backend", "bucket", None).unwrap_or_default(),
				readonly: r.flag(map, "readonly", "bucket", None),
				permissions: r.permission(map, "permissions", "bucket", None, false),
				comment: r.string(map, "comment", "bucket", None),
			});
		}
	}
	for a in r.array(db, "apis", "db", None).to_vec() {
		if let Some(api) = read_api(&mut r, &a) {
			raw.apis.push(api);
		}
	}
	for m in r.array(db, "modules", "db", None).to_vec() {
		if let Some(map) = r.object(
			&m,
			"module",
			&["name", "executable", "permissions", "comment"],
			&["unsigned"],
			None,
		) {
			let executable = map
				.get("executable")
				.and_then(|e| {
					r.object(e, "module.executable", &["type", "bucket", "key"], &[], None)
				})
				.map(|e| Executable {
					executable_type: r
						.string(e, "type", "module.executable", None)
						.unwrap_or_default(),
					bucket: r.string(e, "bucket", "module.executable", None),
					key: r.string(e, "key", "module.executable", None),
				});
			raw.modules.push(ModuleDef {
				name: prefixed("mod::", r.string(map, "name", "module", None).unwrap_or_default()),
				executable: executable.unwrap_or(Executable {
					executable_type: String::new(),
					bucket: None,
					key: None,
				}),
				permissions: r.permission(map, "permissions", "module", None, false),
				comment: r.string(map, "comment", "module", None),
			});
		}
	}
	for c in r.array(db, "configs", "db", None).to_vec() {
		if let Some(config) = read_config(&mut r, &c) {
			raw.configs.push(config);
		}
	}
	for u in r.array(db, "users", "db", None).to_vec() {
		if let Some(map) = r.object(
			&u,
			"user",
			&["name", "roles", "duration", "comment", "hash"],
			&["scram"],
			None,
		) {
			raw.users.push(User {
				name: r.string(map, "name", "user", None).unwrap_or_default(),
				roles: r.strings(map, "roles", "user", None),
				durations: read_durations(&mut r, map.get("duration"), "user"),
				comment: r.string(map, "comment", "user", None),
			});
		}
	}
	for m in r.array(db, "models", "db", None).to_vec() {
		r.diagnostics.push(Diagnostic::new(
			Severity::Warning,
			codes::DEFINITION_UNSUPPORTED,
			m.to_string(),
			"INFO FOR DB category `models` is not part of schema document v2",
		));
	}

	raw.diagnostics = r.diagnostics;
	raw
}

/// `prefix` + `name`, unless `name` already starts with it.
fn prefixed(prefix: &str, name: String) -> String {
	if name.starts_with(prefix) {
		name
	} else {
		format!("{prefix}{name}")
	}
}

/// A string or number as text (sequence parameters are strings on both minors).
fn scalar_text(value: Option<&Value>) -> String {
	match value {
		Some(Value::String(s)) => s.clone(),
		Some(Value::Number(n)) => n.to_string(),
		_ => String::new(),
	}
}

fn read_table(r: &mut Reader, value: &Value, input: &StructureInput<'_>) -> Option<RawTable> {
	let map = r.object(
		value,
		"table",
		&[
			"name",
			"id",
			"kind",
			"schemafull",
			"drop",
			"view",
			"changefeed",
			"permissions",
			"comment",
		],
		&[],
		None,
	)?;
	let name = r.string(map, "name", "table", None).unwrap_or_default();
	let t = Some(name.as_str());

	let kind = map.get("kind").and_then(|k| {
		r.object(k, "table.kind", &["kind", "in", "out", "enforced"], &["lightweight"], t)
	});
	let table_type = match kind {
		Some(k) => {
			let word =
				r.string(k, "kind", "table.kind", t).unwrap_or_default().to_ascii_uppercase();
			match word.as_str() {
				"RELATION" => {
					let mut from = r.strings(k, "in", "table.kind", t);
					let mut to = r.strings(k, "out", "table.kind", t);
					from.sort();
					from.dedup();
					to.sort();
					to.dedup();
					TableType::Relation {
						from,
						to,
						enforced: r.flag(k, "enforced", "table.kind", t),
						lightweight: r.flag(k, "lightweight", "table.kind", t),
					}
				}
				"ANY" => TableType::Any,
				"NORMAL" => TableType::Normal,
				other => {
					r.shape(
						"table.kind.kind",
						&Value::String(other.to_string()),
						"NORMAL, ANY or RELATION",
						t,
					);
					TableType::Any
				}
			}
		}
		None => TableType::Any,
	};

	let changefeed = map
		.get("changefeed")
		.filter(|v| !v.is_null())
		.and_then(|c| r.object(c, "table.changefeed", &["expiry", "original"], &[], t))
		.map(|c| Changefeed {
			expiry: r.string(c, "expiry", "table.changefeed", t).unwrap_or_default(),
			include_original: r.flag(c, "original", "table.changefeed", t),
		});
	let permissions = match map.get("permissions") {
		Some(p) => match r.object(
			p,
			"table.permissions",
			&["select", "create", "update", "delete"],
			&[],
			t,
		) {
			Some(p) => TablePermissions {
				select: r.permission(p, "select", "table.permissions", t, false),
				create: r.permission(p, "create", "table.permissions", t, false),
				update: r.permission(p, "update", "table.permissions", t, false),
				delete: r.permission(p, "delete", "table.permissions", t, false),
			},
			None => none_table_permissions(),
		},
		None => none_table_permissions(),
	};
	let view = r
		.string(map, "view", "table", t)
		.map(|v| v.strip_prefix("AS ").map_or(v.clone(), str::to_string));

	let empty = Value::Null;
	let info = input.tables.iter().find(|(n, _)| *n == name).map_or(&empty, |(_, i)| i);
	let plain = input.tables_info.iter().find(|(n, _)| *n == name).map(|(_, i)| i);
	let (fields, indexes, events) = read_table_info(r, &name, info, plain);

	let schemafull = r.flag(map, "schemafull", "table", t);
	let drop = r.flag(map, "drop", "table", t);
	let comment = r.string(map, "comment", "table", t);
	Some(RawTable {
		define: define_of(input.db_info, "tables", &name),
		name,
		table_type,
		schemafull,
		drop,
		view,
		changefeed,
		permissions,
		comment,
		fields,
		indexes,
		events,
	})
}

fn none_table_permissions() -> TablePermissions {
	TablePermissions {
		select: Permission::Bool(false),
		create: Permission::Bool(false),
		update: Permission::Bool(false),
		delete: Permission::Bool(false),
	}
}

fn read_table_info(
	r: &mut Reader,
	table: &str,
	info: &Value,
	plain: Option<&Value>,
) -> (Vec<RawField>, Vec<RawIndex>, Vec<super::document::Event>) {
	let t = Some(table);
	let mut fields = Vec::new();
	let mut indexes = Vec::new();
	let mut events = Vec::new();
	if info.is_null() {
		return (fields, indexes, events);
	}
	// `lives` are running live queries and `tables` the views fed by this table: neither is
	// schema of this table.
	let Some(map) =
		r.object(info, "tableInfo", &["events", "fields", "indexes", "lives", "tables"], &[], t)
	else {
		return (fields, indexes, events);
	};
	for f in r.array(map, "fields", "tableInfo", t).to_vec() {
		let Some(f) = r.object(
			&f,
			"field",
			&[
				"name",
				"table",
				"kind",
				"flexible",
				"readonly",
				"default",
				"default_always",
				"value",
				"computed",
				"assert",
				"reference",
				"permissions",
				"comment",
			],
			&["inline"],
			t,
		) else {
			continue;
		};
		let name = r.string(f, "name", "field", t).unwrap_or_default();
		let reference = f
			.get("reference")
			.filter(|v| !v.is_null())
			.and_then(|v| r.object(v, "field.reference", &["on_delete"], &[], t))
			.map(|v| r.string(v, "on_delete", "field.reference", t).unwrap_or_default());
		let permissions = match f.get("permissions") {
			Some(p) => {
				match r.object(p, "field.permissions", &["select", "create", "update"], &[], t) {
					Some(p) => FieldPermissions {
						select: r.permission(p, "select", "field.permissions", t, true),
						create: r.permission(p, "create", "field.permissions", t, true),
						update: r.permission(p, "update", "field.permissions", t, true),
					},
					None => FieldPermissions::full(),
				}
			}
			None => FieldPermissions::full(),
		};
		let default = r
			.string(f, "default", "field", t)
			.map(|expr| (expr, r.flag(f, "default_always", "field", t)));
		fields.push(RawField {
			define: plain.and_then(|p| p.get("fields")?.get(&name)?.as_str().map(str::to_string)),
			kind: r.string(f, "kind", "field", t),
			flexible: r.flag(f, "flexible", "field", t),
			readonly: r.flag(f, "readonly", "field", t),
			inline: r.flag(f, "inline", "field", t),
			default,
			value: r.string(f, "value", "field", t),
			computed: r.string(f, "computed", "field", t),
			assert: r.string(f, "assert", "field", t),
			reference,
			permissions,
			comment: r.string(f, "comment", "field", t),
			name,
		});
	}
	for i in r.array(map, "indexes", "tableInfo", t).to_vec() {
		let Some(i) = r.object(&i, "index", &["name", "table", "cols", "index", "comment"], &[], t)
		else {
			continue;
		};
		indexes.push(RawIndex {
			name: r.string(i, "name", "index", t).unwrap_or_default(),
			columns: r.strings(i, "cols", "index", t),
			clause: r.string(i, "index", "index", t).unwrap_or_default(),
			comment: r.string(i, "comment", "index", t),
		});
	}
	for e in r.array(map, "events", "tableInfo", t).to_vec() {
		let Some(e) = r.object(
			&e,
			"event",
			&["name", "what", "when", "then", "async", "retry", "maxdepth", "comment"],
			&[],
			t,
		) else {
			continue;
		};
		let then = r.strings(e, "then", "event", t).into_iter().map(|s| r.statement(s)).collect();
		events.push(Event {
			name: r.string(e, "name", "event", t).unwrap_or_default(),
			when: r.string(e, "when", "event", t).unwrap_or_else(|| "true".to_string()),
			then,
			is_async: r.flag(e, "async", "event", t),
			retry: r.integer(e, "retry", "event", t),
			max_depth: r.integer(e, "maxdepth", "event", t),
			comment: r.string(e, "comment", "event", t),
		});
	}
	(fields, indexes, events)
}

fn read_function(r: &mut Reader, value: &Value) -> Option<RawFunction> {
	let map = r.object(
		value,
		"function",
		&["name", "args", "returns", "block", "permissions", "comment"],
		&[],
		None,
	)?;
	let name = r.string(map, "name", "function", None).unwrap_or_default();
	let mut args = Vec::new();
	for arg in r.array(map, "args", "function", None).to_vec() {
		match arg.as_array().map(Vec::as_slice) {
			Some([Value::String(n), Value::String(k)]) => args.push((n.clone(), k.clone())),
			_ => r.shape("function.args[]", &arg, "a [name, kind] pair", None),
		}
	}
	Some(RawFunction {
		returns: r.string(map, "returns", "function", None),
		block: r.string(map, "block", "function", None).unwrap_or_default(),
		permissions: r.permission(map, "permissions", "function", None, false),
		comment: r.string(map, "comment", "function", None),
		args,
		name,
	})
}

fn read_durations(r: &mut Reader, value: Option<&Value>, what: &str) -> Durations {
	let what = format!("{what}.duration");
	let Some(map) = value
		.filter(|v| !v.is_null())
		.and_then(|v| r.object(v, &what, &["grant", "token", "session"], &[], None))
	else {
		return Durations::default();
	};
	Durations {
		grant: r.string(map, "grant", &what, None),
		token: r.string(map, "token", &what, None),
		session: r.string(map, "session", &what, None),
	}
}

/// An access clause schema document v2 does not carry: a warning, as the plain-INFO reader
/// gives for the same clause.
fn unsupported_access_clause(r: &mut Reader, name: &str, clause: &str) {
	r.diagnostics.push(Diagnostic::new(
		Severity::Warning,
		codes::DEFINITION_UNSUPPORTED,
		format!("access {name}"),
		format!("{clause} is not carried by schema document v2"),
	));
}

fn read_access(r: &mut Reader, value: &Value) -> Option<Access> {
	let map = r.object(
		value,
		"access",
		&["name", "kind", "duration", "comment", "authenticate"],
		&["context"],
		None,
	)?;
	let name = map.get("name").and_then(Value::as_str).unwrap_or_default().to_string();
	// Known, and not modelled: the same warning the plain-INFO reader gives.
	if map.contains_key("context") {
		unsupported_access_clause(r, &name, "CONTEXT");
	}
	let kind = map.get("kind").and_then(|k| {
		r.object(
			k,
			"access.kind",
			&["kind", "jwt", "refresh", "signin", "signup", "subject"],
			&[],
			None,
		)
	});
	let mut access = Access {
		name: r.string(map, "name", "access", None).unwrap_or_default(),
		method: AccessMethod::Jwt,
		signup: None,
		signin: None,
		authenticate: r.string(map, "authenticate", "access", None).map(|s| r.statement(s)),
		refresh: None,
		verify: None,
		subject: None,
		durations: read_durations(r, map.get("duration"), "access"),
		comment: r.string(map, "comment", "access", None),
	};
	let Some(kind) = kind else {
		return Some(access);
	};
	let word = r.string(kind, "kind", "access.kind", None).unwrap_or_default().to_ascii_uppercase();
	access.method = match word.as_str() {
		"RECORD" => AccessMethod::Record,
		"BEARER" => AccessMethod::Bearer,
		"JWT" => AccessMethod::Jwt,
		other => {
			r.shape(
				"access.kind.kind",
				&Value::String(other.to_string()),
				"RECORD, JWT or BEARER",
				None,
			);
			AccessMethod::Jwt
		}
	};
	if access.method == AccessMethod::Record {
		access.refresh = Some(r.flag(kind, "refresh", "access.kind", None));
		access.signup = r.string(kind, "signup", "access.kind", None).map(|s| r.statement(s));
		access.signin = r.string(kind, "signin", "access.kind", None).map(|s| r.statement(s));
	}
	if access.method == AccessMethod::Bearer {
		access.subject = r.string(kind, "subject", "access.kind", None).and_then(|s| {
			match s.to_ascii_uppercase().as_str() {
				"RECORD" => Some(BearerSubject::Record),
				"USER" => Some(BearerSubject::User),
				_ => None,
			}
		});
	}
	// A bearer access signs with an engine-internal key: its JWT settings are not schema
	// (plain INFO does not show them either), so `verify` is carried for RECORD and JWT only.
	if let Some(jwt) = kind
		.get("jwt")
		.and_then(|j| r.object(j, "access.kind.jwt", &["issuer", "verify"], &["audience"], None))
	{
		if let Some(issuer) = jwt.get("issuer").filter(|v| !v.is_null()) {
			r.object(issuer, "access.kind.jwt.issuer", &["alg", "key"], &[], None);
		}
		if jwt.get("audience").is_some_and(|v| !v.is_null()) {
			unsupported_access_clause(r, &access.name, "AUDIENCE");
		}
		let verify = jwt
			.get("verify")
			.filter(|v| !v.is_null())
			.and_then(|v| r.object(v, "access.kind.jwt.verify", &["alg", "key", "url"], &[], None))
			.and_then(|v| {
				match (
					r.string(v, "url", "access.kind.jwt.verify", None),
					r.string(v, "alg", "access.kind.jwt.verify", None),
				) {
					(Some(url), _) => Some(Verify::Url(VerifyUrl {
						url,
					})),
					(None, Some(alg)) => Some(Verify::Alg(VerifyAlg {
						alg,
					})),
					(None, None) => None,
				}
			});
		if access.method != AccessMethod::Bearer {
			access.verify = verify;
		}
	}
	Some(access)
}

/// A middleware map (`{ "api::timeout": ["1s"] }`) as name/args pairs. The map's key order is
/// not the declared order, which is why `api-middleware-order` exists.
fn read_middleware(r: &mut Reader, value: Option<&Value>, what: &str) -> Vec<Middleware> {
	let Some(value) = value.filter(|v| !v.is_null()) else {
		return Vec::new();
	};
	let Some(map) = value.as_object() else {
		r.shape(what, value, "an object of middleware", None);
		return Vec::new();
	};
	map.iter()
		.map(|(name, args)| Middleware {
			name: name.clone(),
			args: match args {
				Value::Array(items) => items.iter().map(scalar_or_json).collect(),
				other => vec![scalar_or_json(other)],
			},
		})
		.collect()
}

fn scalar_or_json(value: &Value) -> String {
	match value {
		Value::String(s) => s.clone(),
		other => other.to_string(),
	}
}

fn read_api(r: &mut Reader, value: &Value) -> Option<Api> {
	let map = r.object(value, "api", &["path", "actions", "config", "comment"], &[], None)?;
	let mut actions = Vec::new();
	for a in r.array(map, "actions", "api", None).to_vec() {
		let Some(a) = r.object(&a, "api.action", &["action", "config", "methods"], &[], None)
		else {
			continue;
		};
		let config = a.get("config").and_then(|c| {
			r.object(c, "api.action.config", &["middleware", "permissions"], &[], None)
		});
		let (middleware, permissions) = match config {
			Some(c) => (
				read_middleware(r, c.get("middleware"), "api.action.config.middleware"),
				r.permission(c, "permissions", "api.action.config", None, true),
			),
			None => (Vec::new(), Permission::Bool(true)),
		};
		actions.push(ApiAction {
			methods: r
				.strings(a, "methods", "api.action", None)
				.into_iter()
				.map(|m| m.to_ascii_lowercase())
				.collect(),
			middleware,
			permissions,
			action: r.string(a, "action", "api.action", None).unwrap_or_default(),
		});
	}
	let config = map
		.get("config")
		.and_then(|c| r.object(c, "api.config", &["middleware", "permissions"], &[], None));
	let (middleware, permissions) = match config {
		Some(c) => (
			read_middleware(r, c.get("middleware"), "api.config.middleware"),
			r.permission(c, "permissions", "api.config", None, true),
		),
		None => (Vec::new(), Permission::Bool(true)),
	};
	Some(Api {
		path: r.string(map, "path", "api", None).unwrap_or_default(),
		actions,
		middleware,
		permissions,
		comment: r.string(map, "comment", "api", None),
	})
}

fn read_config(r: &mut Reader, value: &Value) -> Option<Config> {
	let map = r.object(value, "config", &["api", "graphql"], &[], None)?;
	if let Some(g) = map.get("graphql") {
		let g = r.object(g, "config.graphql", &["tables", "functions"], &[], None)?;
		let text = |r: &mut Reader, key: &str| match g.get(key) {
			Some(Value::String(s)) => s.clone(),
			Some(other) => {
				r.shape(&format!("config.graphql.{key}"), other, "a string", None);
				other.to_string()
			}
			None => String::new(),
		};
		return Some(Config::Graphql {
			tables: text(r, "tables"),
			functions: text(r, "functions"),
		});
	}
	let a = map.get("api")?;
	let a = r.object(a, "config.api", &["middleware", "permissions"], &[], None)?;
	Some(Config::Api {
		middleware: read_middleware(r, a.get("middleware"), "config.api.middleware"),
		permissions: r.permission(a, "permissions", "config.api", None, true),
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn adapters_exist_for_3_2_and_3_3_only() {
		assert_eq!(Adapter::for_engine("3.2.4"), Some(Adapter::Structure32));
		assert_eq!(Adapter::for_engine("3.3.0"), Some(Adapter::Structure33));
		assert_eq!(Adapter::for_engine("3.3.7+build"), Some(Adapter::Structure33));
		assert_eq!(Adapter::for_engine("3.4.0"), None);
		assert_eq!(Adapter::for_engine("2.3.10"), None);
		assert_eq!(Adapter::Structure33.name(), "structure-3.3");
	}

	#[test]
	fn unknown_and_version_gated_keys_are_errors() {
		let db = serde_json::json!({
			"tables": [{
				"name": "t", "id": 1, "drop": false, "schemafull": true,
				"kind": { "kind": "RELATION", "in": ["a"], "out": ["b"], "enforced": true, "lightweight": true },
				"permissions": { "select": true, "create": true, "update": true, "delete": true },
				"surprise": 1,
			}],
		});
		let input = StructureInput {
			db: &db,
			tables: &[],
			db_info: None,
			tables_info: &[],
		};
		let on_33 = read(Adapter::Structure33, &input).diagnostics;
		assert_eq!(on_33.len(), 1, "{on_33:?}");
		assert_eq!(on_33[0].code, codes::STRUCTURE_UNKNOWN_KEY);
		assert_eq!(on_33[0].source, "table.surprise");
		// `lightweight` does not exist on 3.2.
		let on_32 = read(Adapter::Structure32, &input).diagnostics;
		let sources: Vec<&str> = on_32.iter().map(|d| d.source.as_str()).collect();
		assert_eq!(sources, vec!["table.surprise", "table.kind.lightweight"]);
	}

	/// Both adapters preserve the engine-rendered authentication expression.
	#[test]
	fn access_authenticate_is_preserved() {
		let db = serde_json::json!({
			"accesses": [{
				"authenticate": "{ RETURN $auth }",
				"duration": { "session": "1h", "token": "1h" },
				"kind": { "kind": "RECORD", "signin": "SELECT * FROM user" },
				"name": "acc",
			}],
		});
		let input = StructureInput {
			db: &db,
			tables: &[],
			db_info: None,
			tables_info: &[],
		};
		for adapter in [Adapter::Structure32, Adapter::Structure33] {
			let raw = read(adapter, &input);
			assert!(raw.diagnostics.is_empty(), "{:?}", raw.diagnostics);
			assert_eq!(raw.accesses[0].authenticate.as_deref(), Some("{ RETURN $auth }"));
		}
	}

	/// What 3.3.0 returns for `CONTEXT` and a JWT `AUDIENCE`: keys the 3.3 adapter knows and
	/// reports as warnings. 3.2 has neither clause.
	#[test]
	fn access_context_and_audience_are_known_on_3_3_and_unsupported() {
		let db = serde_json::json!({
			"accesses": [{
				"context": "{ RETURN { tier: 'member' } }",
				"duration": { "grant": "4w2d", "session": "12h", "token": "15m" },
				"kind": {
					"jwt": {
						"audience": ["members"],
						"issuer": { "alg": "HS256", "key": "[REDACTED]" },
						"verify": { "alg": "HS256", "key": "[REDACTED]" },
					},
					"kind": "RECORD",
					"refresh": true,
					"signin": "(SELECT * FROM account WHERE email = $email)",
				},
				"name": "members",
			}],
		});
		let input = StructureInput {
			db: &db,
			tables: &[],
			db_info: None,
			tables_info: &[],
		};
		let on_33 = read(Adapter::Structure33, &input).diagnostics;
		let reasons: Vec<(&str, Severity, &str)> =
			on_33.iter().map(|d| (d.code.as_str(), d.severity, d.reason.as_str())).collect();
		assert_eq!(
			reasons,
			vec![
				(
					codes::DEFINITION_UNSUPPORTED,
					Severity::Warning,
					"CONTEXT is not carried by schema document v2"
				),
				(
					codes::DEFINITION_UNSUPPORTED,
					Severity::Warning,
					"AUDIENCE is not carried by schema document v2"
				),
			]
		);
		let on_32 = read(Adapter::Structure32, &input).diagnostics;
		let unknown: Vec<&str> = on_32
			.iter()
			.filter(|d| d.code == codes::STRUCTURE_UNKNOWN_KEY)
			.map(|d| d.source.as_str())
			.collect();
		assert_eq!(unknown, vec!["access.context", "access.kind.jwt.audience"]);
	}
}
