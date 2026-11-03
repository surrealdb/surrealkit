//! The plain-`INFO` fallback for schema document v2.
//!
//! When the engine minor has no `INFO … STRUCTURE` adapter (or STRUCTURE cannot be read), the
//! document is built from the `DEFINE …` strings plain `INFO FOR DB` / `INFO FOR TABLE` return,
//! split into clauses by the typegen lexer. It produces the same [`RawSchema`] the STRUCTURE
//! adapter does, so the shared builder interprets both identically; the corpus test
//! `info_and_structure_agree` holds the two readers to the same semantic digest on every
//! supported engine minor.
//!
//! The engine renders every definition in a fixed clause order, which is what makes this
//! tractable without a SurrealQL parser. Plain `INFO` parenthesises statements in event `THEN`
//! lists and access `SIGNUP`/`SIGNIN`/`AUTHENTICATE` on every version; they are normalised away as in
//! `structure-3.3`. A bearer access's JWT settings are not shown by `INFO`, and are not carried
//! by either reader.

use serde_json::Value;

use super::build_v2::{RawField, RawFunction, RawIndex, RawSchema, RawTable};
use super::define::{adjacent_run, is_clause_word, parse_field};
use super::diagnostics::{Diagnostic, Severity, codes};
use super::document::{
	Access, AccessMethod, Analyzer, Api, ApiAction, BearerSubject, Bucket, Changefeed, Config,
	Durations, Event, Executable, FieldPermissions, Middleware, ModuleDef, Param, Permission,
	Sequence, TablePermissions, TableType, User, Verify, VerifyAlg, VerifyUrl,
};
use super::refine::strip_statement_parens;
use super::scan::{Token, TokenKind, tokenize, unquote};
use super::signature::arg_list_bounds;
use super::type_parser::{parse_type_prefix, split_top_level};

/// The `INFO FOR DB` categories the fallback reads.
const DB_CATEGORIES: &[&str] = &[
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
];

/// The `INFO FOR TABLE` categories the fallback reads (`lives` and `tables` are not schema of
/// the table).
const TABLE_CATEGORIES: &[&str] = &["events", "fields", "indexes", "lives", "tables"];

/// Read a whole schema from plain `INFO` results.
pub(super) fn read(db_info: &Value, tables_info: &[(String, Value)]) -> RawSchema {
	let mut raw = RawSchema::default();
	let mut diagnostics = Vec::new();
	unsupported(db_info, DB_CATEGORIES, None, &mut diagnostics);

	for (name, define) in entries(db_info, "tables") {
		let empty = Value::Null;
		let info = tables_info.iter().find(|(n, _)| *n == name).map_or(&empty, |(_, i)| i);
		unsupported(info, TABLE_CATEGORIES, Some(&name), &mut diagnostics);
		raw.tables.push(table(&name, &define, info));
	}
	for (_, define) in entries(db_info, "functions") {
		raw.functions.push(function(&define));
	}
	for (name, define) in entries(db_info, "params") {
		raw.params.push(param(&name, &define));
	}
	for (_, define) in entries(db_info, "accesses") {
		raw.accesses.push(access(&define, &mut diagnostics));
	}
	for (_, define) in entries(db_info, "analyzers") {
		raw.analyzers.push(analyzer(&define));
	}
	for (_, define) in entries(db_info, "sequences") {
		raw.sequences.push(sequence(&define));
	}
	for (_, define) in entries(db_info, "buckets") {
		raw.buckets.push(bucket(&define));
	}
	for (_, define) in entries(db_info, "apis") {
		raw.apis.push(api(&define));
	}
	for (_, define) in entries(db_info, "modules") {
		match module(&define) {
			Some(m) => raw.modules.push(m),
			None => diagnostics.push(Diagnostic::new(
				Severity::Warning,
				codes::DEFINITION_UNSUPPORTED,
				define,
				"the plain-INFO reader understands only file-backed (surrealism) modules",
			)),
		}
	}
	for (_, define) in entries(db_info, "configs") {
		match config(&define) {
			Some(c) => raw.configs.push(c),
			None => diagnostics.push(Diagnostic::new(
				Severity::Warning,
				codes::DEFINITION_UNSUPPORTED,
				define,
				"the plain-INFO reader does not understand this DEFINE CONFIG",
			)),
		}
	}
	for (_, define) in entries(db_info, "users") {
		raw.users.push(user(&define));
	}
	for (_, define) in entries(db_info, "models") {
		diagnostics.push(Diagnostic::new(
			Severity::Warning,
			codes::DEFINITION_UNSUPPORTED,
			define,
			"INFO FOR DB category `models` is not part of schema document v2",
		));
	}
	raw.diagnostics = diagnostics;
	raw
}

/// `(name, DEFINE …)` pairs of an `INFO` category.
fn entries(info: &Value, key: &str) -> Vec<(String, String)> {
	let Some(Value::Object(map)) = info.get(key) else {
		return Vec::new();
	};
	map.iter()
		.map(|(k, v)| {
			(
				k.clone(),
				match v {
					Value::String(s) => s.clone(),
					other => other.to_string(),
				},
			)
		})
		.collect()
}

/// A `definition-unsupported` warning per entry of a category the reader does not model.
fn unsupported(
	info: &Value,
	known: &[&str],
	table: Option<&str>,
	diagnostics: &mut Vec<Diagnostic>,
) {
	let Some(map) = info.as_object() else {
		return;
	};
	for category in map.keys().filter(|c| !known.contains(&c.as_str())) {
		for (_, define) in entries(info, category) {
			let mut d = Diagnostic::new(
				Severity::Warning,
				codes::DEFINITION_UNSUPPORTED,
				define,
				format!("INFO category `{category}` is not part of schema document v2"),
			);
			if let Some(t) = table {
				d = d.on_table(t);
			}
			diagnostics.push(d);
		}
	}
}

// ---------------------------------------------------------------------------
// Clause splitting
// ---------------------------------------------------------------------------

/// One clause: its keyword (upper case, words joined by one space; empty for the text before
/// the first keyword) and its body, trimmed.
struct Clause<'a> {
	keyword: String,
	body: &'a str,
}

/// Split `stmt` from token `from` into clauses at the top-level clause keywords `keywords`.
/// A keyword may be several words (`WITH REFRESH`); it matches only as consecutive words, each
/// standing alone, outside brackets and strings. Once a `sticky` keyword starts a clause, only
/// that keyword's terminators end it (a view's `SELECT … AS …` must not be cut at `AS`).
fn clauses<'a>(
	stmt: &'a str,
	from: usize,
	keywords: &[&str],
	sticky: &[(&str, &[&str])],
) -> Vec<Clause<'a>> {
	let tokens = tokenize(stmt);
	let mut marks: Vec<(String, usize, usize)> = Vec::new(); // (keyword, keyword start, body start)
	let mut depth = 0i32;
	let mut i = from;
	let mut terminators: Option<&[&str]> = None;
	while i < tokens.len() {
		let t = tokens[i];
		match t.kind {
			TokenKind::Open => depth += 1,
			TokenKind::Close => depth -= 1,
			TokenKind::Word if depth == 0 && is_clause_word(stmt, &tokens, i) => {
				let allowed: &[&str] = terminators.unwrap_or(keywords);
				if let Some((kw, len)) = match_keyword(stmt, &tokens, i, allowed) {
					let body = tokens[i + len - 1].end;
					terminators = sticky.iter().find(|(s, _)| *s == kw).map(|(_, ends)| *ends);
					marks.push((kw.to_string(), t.start, body));
					i += len;
					continue;
				}
			}
			_ => {}
		}
		i += 1;
	}
	let start = tokens.get(from).map_or(stmt.len(), |t| t.start);
	let mut out = Vec::new();
	let first_end = marks.first().map_or(stmt.len(), |m| m.1);
	if first_end > start {
		out.push(Clause {
			keyword: String::new(),
			body: stmt[start..first_end].trim(),
		});
	}
	for (n, (kw, _, body_start)) in marks.iter().enumerate() {
		let end = marks.get(n + 1).map_or(stmt.len(), |m| m.1);
		out.push(Clause {
			keyword: kw.clone(),
			body: stmt[*body_start..end].trim().trim_end_matches(';').trim(),
		});
	}
	out
}

/// The keyword of `keywords` starting at token `i`, and how many words it spans.
fn match_keyword<'k>(
	stmt: &str,
	tokens: &[Token],
	i: usize,
	keywords: &[&'k str],
) -> Option<(&'k str, usize)> {
	let mut best: Option<(&str, usize)> = None;
	for kw in keywords {
		let words: Vec<&str> = kw.split(' ').collect();
		let matches = words
			.iter()
			.enumerate()
			.all(|(n, w)| tokens.get(i + n).is_some_and(|t| t.is_word(stmt, w)));
		if matches && best.is_none_or(|(_, len)| words.len() > len) {
			best = Some((kw, words.len()));
		}
	}
	best
}

fn clause<'c>(clauses: &'c [Clause<'_>], keyword: &str) -> Option<&'c str> {
	clauses.iter().find(|c| c.keyword == keyword).map(|c| c.body)
}

fn has(clauses: &[Clause<'_>], keyword: &str) -> bool {
	clauses.iter().any(|c| c.keyword == keyword)
}

/// The index of the token just after `DEFINE <kind> [OVERWRITE | IF NOT EXISTS] <name>`, and
/// the name as written.
fn header(stmt: &str) -> (usize, String) {
	let tokens = tokenize(stmt);
	let mut i = 2;
	if tokens.get(i).is_some_and(|t| t.is_word(stmt, "OVERWRITE")) {
		i += 1;
	} else if tokens.get(i).is_some_and(|t| t.is_word(stmt, "IF")) {
		i += 3;
	}
	match adjacent_run(stmt, &tokens, i) {
		Some((name, next)) => (next, name.to_string()),
		None => (tokens.len(), String::new()),
	}
}

/// A name without backtick or `⟨⟩` quoting.
fn bare(name: &str) -> String {
	let t = tokenize(name);
	match t.as_slice() {
		[one] if one.kind == TokenKind::QuotedIdent => unquote(one.text(name)),
		_ => name.to_string(),
	}
}

/// A `COMMENT` body: its string literal, decoded.
fn comment(body: &str) -> String {
	let t = tokenize(body);
	match t.as_slice() {
		[one] if one.kind == TokenKind::String => unquote(one.text(body)),
		_ => body.to_string(),
	}
}

/// `FULL`, `NONE` or `WHERE expr`.
fn permission(body: &str) -> Permission {
	let body = body.trim().trim_end_matches(',').trim();
	if body.eq_ignore_ascii_case("FULL") {
		Permission::Bool(true)
	} else if body.eq_ignore_ascii_case("NONE") {
		Permission::Bool(false)
	} else {
		let t = tokenize(body);
		match t.first() {
			Some(w) if w.is_word(body, "WHERE") => {
				Permission::Expr(body[w.end..].trim().to_string())
			}
			_ => Permission::Expr(body.to_string()),
		}
	}
}

/// A `PERMISSIONS` body for the operations `ops`: `FULL`, `NONE`, or `FOR op[, op] perm, …`.
/// Operations not mentioned get `absent`.
fn permissions(body: Option<&str>, ops: &[&str], absent: bool) -> Vec<Permission> {
	let mut out = vec![Permission::Bool(absent); ops.len()];
	let Some(body) = body.map(str::trim) else {
		return out;
	};
	if !body.to_ascii_uppercase().starts_with("FOR ") {
		let p = permission(body);
		return vec![p; ops.len()];
	}
	for part in clauses(body, 0, &["FOR"], &[]) {
		if part.keyword != "FOR" {
			continue;
		}
		// `select, update WHERE …` / `create FULL`
		let pc = clauses(part.body, 0, &["FULL", "NONE", "WHERE"], &[("WHERE", &[])]);
		let names = pc.iter().find(|c| c.keyword.is_empty()).map_or("", |c| c.body);
		let perm = match pc.iter().find(|c| !c.keyword.is_empty()) {
			Some(c) if c.keyword == "FULL" => Permission::Bool(true),
			Some(c) if c.keyword == "NONE" => Permission::Bool(false),
			Some(c) => Permission::Expr(c.body.trim_end_matches(',').trim().to_string()),
			None => continue,
		};
		for name in names.split(',').map(str::trim) {
			if let Some(at) = ops.iter().position(|op| op.eq_ignore_ascii_case(name)) {
				out[at] = perm.clone();
			}
		}
	}
	out
}

fn field_permissions(body: Option<&str>) -> FieldPermissions {
	let mut p = permissions(body, &["select", "create", "update"], true).into_iter();
	FieldPermissions {
		select: p.next().unwrap_or(Permission::Bool(true)),
		create: p.next().unwrap_or(Permission::Bool(true)),
		update: p.next().unwrap_or(Permission::Bool(true)),
	}
}

fn table_permissions(body: Option<&str>) -> TablePermissions {
	let mut p = permissions(body, &["select", "create", "update", "delete"], false).into_iter();
	TablePermissions {
		select: p.next().unwrap_or(Permission::Bool(false)),
		create: p.next().unwrap_or(Permission::Bool(false)),
		update: p.next().unwrap_or(Permission::Bool(false)),
		delete: p.next().unwrap_or(Permission::Bool(false)),
	}
}

/// `DURATION FOR GRANT x, FOR TOKEN y, FOR SESSION NONE`. `NONE` is omitted.
fn durations(body: Option<&str>) -> Durations {
	let mut out = Durations::default();
	let Some(body) = body else {
		return out;
	};
	for part in clauses(body, 0, &["FOR GRANT", "FOR TOKEN", "FOR SESSION"], &[]) {
		let value = part.body.trim_end_matches(',').trim();
		let value =
			(!value.eq_ignore_ascii_case("NONE") && !value.is_empty()).then(|| value.to_string());
		match part.keyword.as_str() {
			"FOR GRANT" => out.grant = value,
			"FOR TOKEN" => out.token = value,
			"FOR SESSION" => out.session = value,
			_ => {}
		}
	}
	out
}

fn list(body: &str) -> Vec<String> {
	split_top_level(body, ',')
		.into_iter()
		.map(|s| s.trim().to_string())
		.filter(|s| !s.is_empty())
		.collect()
}

// ---------------------------------------------------------------------------
// Definitions
// ---------------------------------------------------------------------------

fn table(name: &str, define: &str, info: &Value) -> RawTable {
	let (from, _) = header(define);
	let cs = clauses(
		define,
		from,
		&["TYPE", "DROP", "SCHEMAFULL", "SCHEMALESS", "AS", "CHANGEFEED", "COMMENT", "PERMISSIONS"],
		&[("AS", &["CHANGEFEED", "COMMENT", "PERMISSIONS"])],
	);
	let table_type = match clause(&cs, "TYPE") {
		Some(body) => {
			let tc = clauses(
				body,
				0,
				&[
					"NORMAL",
					"ANY",
					"RELATION",
					"IN",
					"FROM",
					"OUT",
					"TO",
					"ENFORCED",
					"LIGHTWEIGHT",
				],
				&[],
			);
			if has(&tc, "RELATION") {
				let names = |a: &str, b: &str| {
					let mut v: Vec<String> = clause(&tc, a)
						.or_else(|| clause(&tc, b))
						.map(|l| {
							l.split('|').map(|n| bare(n.trim())).filter(|n| !n.is_empty()).collect()
						})
						.unwrap_or_default();
					v.sort();
					v.dedup();
					v
				};
				TableType::Relation {
					from: names("IN", "FROM"),
					to: names("OUT", "TO"),
					enforced: has(&tc, "ENFORCED"),
					lightweight: has(&tc, "LIGHTWEIGHT"),
				}
			} else if has(&tc, "NORMAL") {
				TableType::Normal
			} else {
				TableType::Any
			}
		}
		None => TableType::Any,
	};
	let changefeed = clause(&cs, "CHANGEFEED").map(|body| {
		let cc = clauses(body, 0, &["INCLUDE ORIGINAL"], &[]);
		Changefeed {
			expiry: clause(&cc, "").unwrap_or_default().to_string(),
			include_original: has(&cc, "INCLUDE ORIGINAL"),
		}
	});

	let mut fields = Vec::new();
	for (field_name, define) in entries(info, "fields") {
		let f = parse_field(&define);
		fields.push(RawField {
			name: field_name,
			kind: f.kind.map(str::to_string),
			flexible: f.flexible,
			readonly: f.readonly,
			inline: f.inline,
			default: f.default.map(|(always, expr)| (expr.to_string(), always)),
			value: f.value.map(str::to_string),
			computed: f.computed.map(str::to_string),
			assert: f.assert.map(str::to_string),
			reference: f.reference.map(str::to_string),
			permissions: field_permissions(f.permissions),
			comment: f.comment.clone(),
			define: Some(define.clone()),
		});
	}
	let indexes = entries(info, "indexes").into_iter().map(|(_, d)| index(&d)).collect();
	let events = entries(info, "events").into_iter().map(|(_, d)| event(&d)).collect();

	RawTable {
		name: name.to_string(),
		table_type,
		schemafull: has(&cs, "SCHEMAFULL"),
		drop: has(&cs, "DROP"),
		view: clause(&cs, "AS").map(str::to_string),
		changefeed,
		permissions: table_permissions(clause(&cs, "PERMISSIONS")),
		comment: clause(&cs, "COMMENT").map(comment),
		define: Some(define.to_string()),
		fields,
		indexes,
		events,
	}
}

/// The words that end an index's column list.
const INDEX_CLAUSES: &[&str] = &[
	"UNIQUE",
	"FULLTEXT",
	"SEARCH",
	"HNSW",
	"DISKANN",
	"MTREE",
	"COUNT",
	"COMMENT",
	"CONCURRENTLY",
];

fn index(define: &str) -> RawIndex {
	let (from, name) = header(define);
	// `ON [TABLE] <table>`
	let tokens = tokenize(define);
	let mut i = from;
	if tokens.get(i).is_some_and(|t| t.is_word(define, "ON")) {
		i += 1;
		if tokens.get(i).is_some_and(|t| t.is_word(define, "TABLE")) {
			i += 1;
		}
		i = adjacent_run(define, &tokens, i).map_or(tokens.len(), |(_, next)| next);
	}
	let mut keywords = vec!["FIELDS", "COLUMNS"];
	keywords.extend_from_slice(INDEX_CLAUSES);
	let cs = clauses(define, i, &keywords, &[]);
	let columns =
		clause(&cs, "FIELDS").or_else(|| clause(&cs, "COLUMNS")).map(list).unwrap_or_default();
	// The clause is everything from the first index-kind keyword up to COMMENT.
	let start = cs.iter().position(|c| {
		INDEX_CLAUSES.contains(&c.keyword.as_str())
			&& c.keyword != "COMMENT"
			&& c.keyword != "CONCURRENTLY"
	});
	let clause_text = match start {
		Some(at) => {
			let first = &cs[at];
			// Re-join the kind keyword with its body and any following non-COMMENT clauses.
			let mut text = format!("{} {}", first.keyword, first.body).trim().to_string();
			for c in &cs[at + 1..] {
				if c.keyword == "COMMENT" || c.keyword == "CONCURRENTLY" {
					break;
				}
				text = format!("{text} {} {}", c.keyword, c.body).trim().to_string();
			}
			text
		}
		None => String::new(),
	};
	RawIndex {
		name: bare(&name),
		columns,
		clause: clause_text,
		comment: clause(&cs, "COMMENT").map(comment),
	}
}

fn event(define: &str) -> Event {
	let (from, name) = header(define);
	let cs = clauses(define, from, &["ASYNC", "RETRY", "MAXDEPTH", "WHEN", "THEN", "COMMENT"], &[]);
	let number = |kw: &str| clause(&cs, kw).and_then(|b| b.split_whitespace().next()?.parse().ok());
	Event {
		name: bare(&name),
		when: clause(&cs, "WHEN").unwrap_or("true").to_string(),
		then: clause(&cs, "THEN")
			.map(list)
			.unwrap_or_default()
			.iter()
			.map(|s| strip_statement_parens(s))
			.collect(),
		is_async: has(&cs, "ASYNC"),
		retry: number("RETRY"),
		max_depth: number("MAXDEPTH"),
		comment: clause(&cs, "COMMENT").map(comment),
	}
}

fn function(define: &str) -> RawFunction {
	let tokens = tokenize(define);
	let name = tokens
		.iter()
		.position(|t| t.is_word(define, "FUNCTION"))
		.and_then(|i| {
			let mut j = i + 1;
			if tokens.get(j).is_some_and(|t| t.is_word(define, "OVERWRITE")) {
				j += 1;
			} else if tokens.get(j).is_some_and(|t| t.is_word(define, "IF")) {
				j += 3;
			}
			adjacent_run(define, &tokens, j).map(|(n, _)| n.to_string())
		})
		.unwrap_or_default();
	let name = name.split('(').next().unwrap_or_default().to_string();
	let Some((open, close)) = arg_list_bounds(define) else {
		return RawFunction {
			name,
			args: Vec::new(),
			returns: None,
			block: String::new(),
			permissions: Permission::Bool(false),
			comment: None,
		};
	};
	let args = list(&define[open + 1..close])
		.into_iter()
		.filter_map(|part| {
			let colon = part.find(':')?;
			Some((
				part[..colon].trim().trim_start_matches('$').to_string(),
				part[colon + 1..].trim().to_string(),
			))
		})
		.collect();
	let mut rest = &define[close + 1..];
	let mut returns = None;
	if let Some(after) = rest.trim_start().strip_prefix("->") {
		let (_, used, _) = parse_type_prefix(after);
		let lead = after.len() - after.trim_start().len();
		let end = if used == 0 {
			after.find('{').unwrap_or(after.len())
		} else {
			used
		};
		returns = Some(after[lead.min(end)..end].trim().to_string()).filter(|r| !r.is_empty());
		rest = &after[end..];
	}
	// The body: the first `{ … }` block.
	let body_tokens = tokenize(rest);
	let mut depth = 0i32;
	let mut block = (0, 0);
	for t in &body_tokens {
		match t.kind {
			TokenKind::Open => {
				if depth == 0 && t.text(rest) == "{" {
					block.0 = t.start;
				}
				depth += 1;
			}
			TokenKind::Close => {
				depth -= 1;
				if depth == 0 {
					block.1 = t.end;
					break;
				}
			}
			_ => {}
		}
	}
	let tail = &rest[block.1..];
	let cs = clauses(tail, 0, &["COMMENT", "PERMISSIONS"], &[]);
	RawFunction {
		name,
		args,
		returns,
		block: rest[block.0..block.1].to_string(),
		permissions: clause(&cs, "PERMISSIONS").map_or(Permission::Bool(true), permission),
		comment: clause(&cs, "COMMENT").map(comment),
	}
}

fn param(name: &str, define: &str) -> Param {
	let (from, _) = header(define);
	let cs = clauses(define, from, &["VALUE", "COMMENT", "PERMISSIONS"], &[]);
	Param {
		name: name.trim_start_matches('$').to_string(),
		value: clause(&cs, "VALUE").unwrap_or_default().to_string(),
		permissions: clause(&cs, "PERMISSIONS").map_or(Permission::Bool(true), permission),
		comment: clause(&cs, "COMMENT").map(comment),
	}
}

fn access(define: &str, diagnostics: &mut Vec<Diagnostic>) -> Access {
	let (from, name) = header(define);
	let cs = clauses(
		define,
		from,
		&[
			"ON",
			"TYPE",
			"SIGNUP",
			"SIGNIN",
			"WITH REFRESH",
			"WITH JWT",
			"WITH ISSUER",
			"ALGORITHM",
			"KEY",
			"URL",
			"AUDIENCE",
			"FOR",
			"AUTHENTICATE",
			"CONTEXT",
			"DURATION",
			"COMMENT",
		],
		&[("DURATION", &["COMMENT"])],
	);
	let method = match clause(&cs, "TYPE").map(str::to_ascii_uppercase).as_deref() {
		Some("RECORD") => AccessMethod::Record,
		Some("BEARER") => AccessMethod::Bearer,
		_ => AccessMethod::Jwt,
	};
	// Known clauses the document does not carry; the STRUCTURE adapters give the same warnings.
	for unsupported in ["CONTEXT", "AUDIENCE"] {
		if has(&cs, unsupported) {
			diagnostics.push(Diagnostic::new(
				Severity::Warning,
				codes::DEFINITION_UNSUPPORTED,
				define,
				format!("{unsupported} is not carried by schema document v2"),
			));
		}
	}
	let verify = match (clause(&cs, "URL"), clause(&cs, "ALGORITHM")) {
		(Some(url), _) => Some(Verify::Url(VerifyUrl {
			url: comment(url),
		})),
		(None, Some(alg)) => Some(Verify::Alg(VerifyAlg {
			alg: alg.split_whitespace().next().unwrap_or_default().to_string(),
		})),
		(None, None) => None,
	};
	Access {
		name: bare(&name),
		method,
		signup: (method == AccessMethod::Record)
			.then(|| clause(&cs, "SIGNUP").map(strip_statement_parens))
			.flatten(),
		signin: (method == AccessMethod::Record)
			.then(|| clause(&cs, "SIGNIN").map(strip_statement_parens))
			.flatten(),
		refresh: (method == AccessMethod::Record).then(|| has(&cs, "WITH REFRESH")),
		authenticate: clause(&cs, "AUTHENTICATE").map(strip_statement_parens),
		verify: if method == AccessMethod::Bearer {
			None
		} else {
			verify
		},
		subject: (method == AccessMethod::Bearer)
			.then(|| match clause(&cs, "FOR").map(str::to_ascii_uppercase).as_deref() {
				Some("RECORD") => Some(BearerSubject::Record),
				Some("USER") => Some(BearerSubject::User),
				_ => None,
			})
			.flatten(),
		durations: durations(clause(&cs, "DURATION")),
		comment: clause(&cs, "COMMENT").map(comment),
	}
}

fn analyzer(define: &str) -> Analyzer {
	let (from, name) = header(define);
	let cs = clauses(define, from, &["FUNCTION", "TOKENIZERS", "FILTERS", "COMMENT"], &[]);
	Analyzer {
		name: bare(&name),
		function: clause(&cs, "FUNCTION").map(|f| {
			if f.starts_with("fn::") {
				f.to_string()
			} else {
				format!("fn::{f}")
			}
		}),
		tokenizers: clause(&cs, "TOKENIZERS").map(list).unwrap_or_default(),
		filters: clause(&cs, "FILTERS").map(list).unwrap_or_default(),
		comment: clause(&cs, "COMMENT").map(comment),
	}
}

fn sequence(define: &str) -> Sequence {
	let (from, name) = header(define);
	let cs = clauses(define, from, &["BATCH", "START", "TIMEOUT"], &[]);
	Sequence {
		name: bare(&name),
		batch: clause(&cs, "BATCH").unwrap_or_default().to_string(),
		start: clause(&cs, "START").unwrap_or_default().to_string(),
		timeout: clause(&cs, "TIMEOUT").map(str::to_string),
	}
}

fn bucket(define: &str) -> Bucket {
	let (from, name) = header(define);
	let cs = clauses(define, from, &["READONLY", "BACKEND", "PERMISSIONS", "COMMENT"], &[]);
	Bucket {
		name: bare(&name),
		backend: clause(&cs, "BACKEND").map(comment).unwrap_or_default(),
		readonly: has(&cs, "READONLY"),
		permissions: clause(&cs, "PERMISSIONS").map_or(Permission::Bool(true), permission),
		comment: clause(&cs, "COMMENT").map(comment),
	}
}

/// `api::timeout(1s), other()` as name/args pairs.
fn middleware(body: Option<&str>) -> Vec<Middleware> {
	let Some(body) = body else {
		return Vec::new();
	};
	list(body)
		.into_iter()
		.map(|item| match item.find('(') {
			Some(open) => Middleware {
				name: item[..open].trim().to_string(),
				args: list(
					item[open + 1..].trim_end().strip_suffix(')').unwrap_or(&item[open + 1..]),
				),
			},
			None => Middleware {
				name: item,
				args: Vec::new(),
			},
		})
		.collect()
}

fn api(define: &str) -> Api {
	let tokens = tokenize(define);
	// `DEFINE API [OVERWRITE | IF NOT EXISTS] '<path>'`
	let path_at = tokens.iter().position(|t| t.kind == TokenKind::String);
	let (path, from) = match path_at {
		Some(i) => (unquote(tokens[i].text(define)), i + 1),
		None => (String::new(), tokens.len()),
	};
	let top = clauses(define, from, &["FOR", "COMMENT"], &[]);
	let mut api = Api {
		path,
		actions: Vec::new(),
		middleware: Vec::new(),
		permissions: Permission::Bool(true),
		comment: clause(&top, "COMMENT").map(comment),
	};
	for part in top.iter().filter(|c| c.keyword == "FOR") {
		let pc = clauses(part.body, 0, &["MIDDLEWARE", "PERMISSIONS", "THEN"], &[("THEN", &[])]);
		let methods: Vec<String> = clause(&pc, "")
			.map(list)
			.unwrap_or_default()
			.into_iter()
			.map(|m| m.to_ascii_lowercase())
			.collect();
		let mw = middleware(clause(&pc, "MIDDLEWARE"));
		let perm = clause(&pc, "PERMISSIONS").map_or(Permission::Bool(true), permission);
		if methods == ["any"] && !has(&pc, "THEN") {
			api.middleware = mw;
			api.permissions = perm;
		} else {
			api.actions.push(ApiAction {
				methods,
				middleware: mw,
				permissions: perm,
				action: clause(&pc, "THEN").unwrap_or_default().to_string(),
			});
		}
	}
	api
}

fn module(define: &str) -> Option<ModuleDef> {
	let (from, name) = header(define);
	let cs = clauses(define, from, &["AS", "FROM", "UNSIGNED", "PERMISSIONS", "COMMENT"], &[]);
	let file = clause(&cs, "FROM").or_else(|| clause(&cs, "AS"))?;
	// `f"bucket:/key"`
	let tokens = tokenize(file);
	let literal = match tokens.as_slice() {
		[f, s] if f.is_word(file, "f") && s.kind == TokenKind::String && s.start == f.end => {
			unquote(s.text(file))
		}
		_ => return None,
	};
	let (bucket, key) = literal.split_once(':')?;
	Some(ModuleDef {
		name: if name.starts_with("mod::") {
			name
		} else {
			format!("mod::{name}")
		},
		executable: Executable {
			executable_type: "surrealism".to_string(),
			bucket: Some(bucket.to_string()),
			key: Some(key.to_string()),
		},
		permissions: clause(&cs, "PERMISSIONS").map_or(Permission::Bool(true), permission),
		comment: clause(&cs, "COMMENT").map(comment),
	})
}

fn config(define: &str) -> Option<Config> {
	let tokens = tokenize(define);
	// `INFO` renders configs without `DEFINE CONFIG`; accept both.
	let start =
		tokens.iter().position(|t| t.is_word(define, "GRAPHQL") || t.is_word(define, "API"))?;
	if tokens[start].is_word(define, "GRAPHQL") {
		let cs = clauses(define, start + 1, &["TABLES", "FUNCTIONS"], &[]);
		return Some(Config::Graphql {
			tables: clause(&cs, "TABLES").unwrap_or_default().to_string(),
			functions: clause(&cs, "FUNCTIONS").unwrap_or_default().to_string(),
		});
	}
	let cs = clauses(define, start + 1, &["MIDDLEWARE", "PERMISSIONS"], &[]);
	Some(Config::Api {
		middleware: middleware(clause(&cs, "MIDDLEWARE")),
		permissions: clause(&cs, "PERMISSIONS").map_or(Permission::Bool(true), permission),
	})
}

fn user(define: &str) -> User {
	let (from, name) = header(define);
	let cs = clauses(
		define,
		from,
		&["ON", "PASSHASH", "PASSSCRAM", "PASSWORD", "ROLES", "DURATION", "COMMENT"],
		&[],
	);
	User {
		name: bare(&name),
		roles: clause(&cs, "ROLES").map(list).unwrap_or_default(),
		durations: durations(clause(&cs, "DURATION")),
		comment: clause(&cs, "COMMENT").map(comment),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn permissions_expand_per_operation() {
		let p = table_permissions(Some(
			"FOR select FULL, FOR create, update WHERE $auth.id = id, FOR delete NONE",
		));
		assert_eq!(p.select, Permission::Bool(true));
		assert_eq!(p.create, Permission::Expr("$auth.id = id".into()));
		assert_eq!(p.update, Permission::Expr("$auth.id = id".into()));
		assert_eq!(p.delete, Permission::Bool(false));
		assert_eq!(table_permissions(Some("FULL")).delete, Permission::Bool(true));
		assert_eq!(field_permissions(None), FieldPermissions::full());
	}

	#[test]
	fn a_view_is_not_cut_at_its_own_as() {
		let t = table(
			"team_size",
			"DEFINE TABLE team_size TYPE ANY SCHEMALESS AS SELECT count() AS total, team FROM person GROUP BY team PERMISSIONS NONE",
			&Value::Null,
		);
		assert_eq!(
			t.view.as_deref(),
			Some("SELECT count() AS total, team FROM person GROUP BY team")
		);
		assert_eq!(t.table_type, TableType::Any);
		assert!(!t.schemafull);
	}

	#[test]
	fn index_clauses_and_columns() {
		let i = index(
			"DEFINE INDEX ft_body ON doc FIELDS body FULLTEXT ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS",
		);
		assert_eq!(i.columns, vec!["body"]);
		assert_eq!(i.clause, "FULLTEXT ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS");
		let i = index("DEFINE INDEX idx ON doc FIELDS owner, status");
		assert_eq!(i.columns, vec!["owner", "status"]);
		assert_eq!(i.clause, "");
		let i = index("DEFINE INDEX cnt_open ON doc COUNT WHERE status = 'open'");
		assert!(i.columns.is_empty());
		assert_eq!(i.clause, "COUNT WHERE status = 'open'");
		let i = index("DEFINE INDEX u ON doc FIELDS code UNIQUE COMMENT 'one doc per code'");
		assert_eq!(i.clause, "UNIQUE");
		assert_eq!(i.comment.as_deref(), Some("one doc per code"));
	}

	#[test]
	fn apis_split_into_config_and_actions() {
		let a = api(
			"DEFINE API '/docs' FOR any PERMISSIONS FULL FOR get, post MIDDLEWARE api::timeout(1s) PERMISSIONS FULL THEN { RETURN { status: 200 } }",
		);
		assert_eq!(a.path, "/docs");
		assert_eq!(a.permissions, Permission::Bool(true));
		assert_eq!(a.actions.len(), 1);
		assert_eq!(a.actions[0].methods, vec!["get", "post"]);
		assert_eq!(
			a.actions[0].middleware,
			vec![Middleware {
				name: "api::timeout".into(),
				args: vec!["1s".into()]
			}]
		);
		assert_eq!(a.actions[0].action, "{ RETURN { status: 200 } }");
	}

	/// 3.3.0's plain `INFO` text for access definitions with `AUDIENCE` and `CONTEXT`: each
	/// clause ends the one before it (the URL stays exact) and is reported as unsupported,
	/// as the STRUCTURE adapter reports it.
	#[test]
	fn access_audience_and_context_end_their_clauses_and_warn() {
		let reasons =
			|d: &[Diagnostic]| -> Vec<String> { d.iter().map(|d| d.reason.clone()).collect() };
		let mut diagnostics = Vec::new();
		let remote = access(
			"DEFINE ACCESS remote_audience ON DATABASE TYPE JWT URL \
			 'https://example.com/.well-known/jwks.json' AUDIENCE 'api' DURATION FOR SESSION 2h",
			&mut diagnostics,
		);
		assert_eq!(
			remote.verify,
			Some(Verify::Url(VerifyUrl {
				url: "https://example.com/.well-known/jwks.json".into(),
			}))
		);
		assert_eq!(remote.durations.session.as_deref(), Some("2h"));
		assert_eq!(reasons(&diagnostics), vec!["AUDIENCE is not carried by schema document v2"]);

		let mut diagnostics = Vec::new();
		let members = access(
			"DEFINE ACCESS members ON DATABASE TYPE RECORD SIGNUP (CREATE account SET email = \
			 $email) SIGNIN (SELECT * FROM account WHERE email = $email) WITH REFRESH WITH JWT \
			 ALGORITHM HS256 KEY '[REDACTED]' AUDIENCE 'members' WITH ISSUER KEY '[REDACTED]' \
			 AUTHENTICATE { RETURN $auth } CONTEXT { RETURN { tier: 'member' } } DURATION FOR \
			 GRANT 4w2d, FOR TOKEN 15m, FOR SESSION 12h",
			&mut diagnostics,
		);
		assert_eq!(
			members.verify,
			Some(Verify::Alg(VerifyAlg {
				alg: "HS256".into(),
			}))
		);
		assert_eq!(members.refresh, Some(true));
		assert_eq!(members.authenticate.as_deref(), Some("{ RETURN $auth }"));
		assert_eq!(members.durations.grant.as_deref(), Some("4w2d"));
		assert!(
			diagnostics
				.iter()
				.all(|d| d.severity == Severity::Warning && d.code == codes::DEFINITION_UNSUPPORTED)
		);
		assert_eq!(
			reasons(&diagnostics),
			vec![
				"CONTEXT is not carried by schema document v2",
				"AUDIENCE is not carried by schema document v2",
			]
		);
	}
}
