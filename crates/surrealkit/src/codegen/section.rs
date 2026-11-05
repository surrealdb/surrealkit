//! The `queries` section of schema document v2: collecting the project's queries, and
//! assembling the engine's typed result into entries.
//!
//! [`collect`] reads the query files and the host code `[typegen.queries]` names. The engine
//! types what [`Collected::inputs`] lists, and [`assemble`] turns its [`TypesDocument`] into the
//! section's [`QueryEntry`]s, with the places each query is written and the diagnostics about
//! them. [`attach`] does both against a database and puts the result in a document. Every path
//! in the section is relative to the same root as `provenance.source.files`, `/`-separated.

use std::collections::BTreeMap;
#[cfg(feature = "host-queries")]
use std::collections::HashMap;
#[cfg(feature = "host-queries")]
use std::sync::Arc;

use anyhow::Result;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;

use super::document::TypesDocument;
use super::queries::{QueryEntry, QueryOrigin, QuerySource, convert_query};
use super::{engine, query_files};
#[cfg(feature = "host-queries")]
use crate::typegen::Span;
use crate::typegen::document::SchemaDocument;
use crate::typegen::{Diagnostic, Severity, codes};
use crate::variables::QueriesConfig;

/// The text of the warning raised when queries are configured and the engine cannot type them.
const UNSUPPORTED_SOURCE: &str = "[typegen.queries]";

/// One query the project sends, and every place it is written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectedQuery {
	/// The name of a query file's query; `None` for an inline query.
	pub name: Option<String>,
	/// The text the engine receives.
	pub text: String,
	/// Where it is written, in the order found.
	pub sources: Vec<QuerySource>,
	/// How offsets of `text` map into each source's file, one per source in the same order. A
	/// source without one is located at its own line and column.
	pub maps: Vec<TextMap>,
}

/// How a byte offset of a query's text maps to a place in the file the query is written in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TextMap {
	/// A query file's query: its lines are the file's lines from the source's own position (the
	/// text reads CRLF as LF, which moves no line or column).
	File,
	/// Host code: the literal's own map, and the host file's text to count lines in.
	#[cfg(feature = "host-queries")]
	Host {
		query: Box<crate::host_queries::HostQuery>,
		file: Arc<str>,
	},
}

impl TextMap {
	/// The 1-based line and column (in Unicode scalar values) of `offset`, a byte offset of
	/// `text`, in the file `source` names.
	fn position(&self, source: &QuerySource, text: &str, offset: usize) -> (u32, u32) {
		let offset = offset.min(text.len());
		match self {
			TextMap::File => {
				// A query file's lines are the text's lines, from the source's own position.
				let before = text.get(..offset).unwrap_or(text);
				match before.rfind('\n') {
					None => {
						let column = source.column as usize + before.chars().count();
						(source.line, u32::try_from(column).unwrap_or(u32::MAX))
					}
					Some(at) => {
						let lines = before.bytes().filter(|byte| *byte == b'\n').count();
						let line = source.line as usize + lines;
						let column = before[at + 1..].chars().count() + 1;
						(
							u32::try_from(line).unwrap_or(u32::MAX),
							u32::try_from(column).unwrap_or(u32::MAX),
						)
					}
				}
			}
			#[cfg(feature = "host-queries")]
			TextMap::Host {
				query,
				file,
			} => {
				let at = query.host_offset(offset).min(file.len());
				let before = file.get(..at).unwrap_or(file);
				let line = before.bytes().filter(|byte| *byte == b'\n').count() + 1;
				let start = before.rfind('\n').map_or(0, |at| at + 1);
				let column = before[start..].chars().count() + 1;
				(u32::try_from(line).unwrap_or(u32::MAX), u32::try_from(column).unwrap_or(u32::MAX))
			}
		}
	}
}

/// The queries of a project, before the engine types them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Collected {
	/// Named queries from query files, in path then position order.
	pub named: Vec<CollectedQuery>,
	/// Inline queries from host code, one per distinct text, in the order first found.
	pub inline: Vec<CollectedQuery>,
	/// What reading the query files found wrong.
	pub diagnostics: Vec<Diagnostic>,
}

impl Collected {
	/// Whether no query was found. Diagnostics may remain.
	pub fn is_empty(&self) -> bool {
		self.named.is_empty() && self.inline.is_empty()
	}

	/// What the engine types, as [`engine::types_document`] takes it: named queries with their
	/// name, then inline ones without.
	pub fn inputs(&self) -> Vec<(Option<String>, String)> {
		self.named
			.iter()
			.chain(self.inline.iter().filter(|q| !self.is_a_name(&q.text)))
			.map(|q| (q.name.clone(), q.text.clone()))
			.collect()
	}

	/// Whether `text` is also a named query's name, so an inline query with that text would
	/// share its key. The engine refuses two inputs with one key, so such an inline query is
	/// not sent.
	fn is_a_name(&self, text: &str) -> bool {
		self.named.iter().any(|q| q.name.as_deref() == Some(text))
	}
}

/// Collects the project's queries as `config` names them. `root` is the directory the configured
/// paths resolve against (the current directory): every source path is written relative to it,
/// so query files and host code share one base.
///
/// # Errors
///
/// Fails when a configured directory or source root does not exist, a file cannot be read, or
/// `sources` is set in a build without the `host-queries` feature.
pub fn collect(root: &str, config: &QueriesConfig) -> Result<Collected> {
	let (files, diagnostics) = query_files::scan(&config.files)?;
	let mut named = Vec::with_capacity(files.len());
	for (path, query) in files {
		named.push(CollectedQuery {
			name: Some(query.name),
			text: query.text,
			maps: vec![TextMap::File],
			sources: vec![QuerySource {
				origin: QueryOrigin::File,
				path: relative(root, &path)?,
				span: query.span,
				line: query.line,
				column: query.column,
				method: None,
			}],
		});
	}
	Ok(Collected {
		named,
		inline: inline(root, config)?,
		diagnostics,
	})
}

fn relative(root: &str, path: &std::path::Path) -> Result<String> {
	crate::schema_state::folder_relative_key(root, path)
}

#[cfg(feature = "host-queries")]
fn inline(root: &str, config: &QueriesConfig) -> Result<Vec<CollectedQuery>> {
	if config.sources.is_empty() {
		return Ok(Vec::new());
	}
	fn names(list: &[String]) -> Vec<&str> {
		list.iter().map(String::as_str).collect()
	}
	let (methods, components) = (names(&config.methods), names(&config.components));
	let functions: Vec<crate::host_queries::QueryFunction<'_>> = config
		.functions
		.iter()
		.map(|function| crate::host_queries::QueryFunction {
			name: &function.name,
			arg: function.arg,
		})
		.collect();
	let queries = crate::host_queries::Queries {
		methods: &methods,
		functions: &functions,
		components: &components,
		attribute: &config.attribute,
	};
	let mut inline: Vec<CollectedQuery> = Vec::new();
	let mut by_text: HashMap<String, usize> = HashMap::new();
	let mut files: HashMap<std::path::PathBuf, Arc<str>> = HashMap::new();
	for found in crate::host_queries::scan_with(&config.sources, &queries)? {
		let file = match files.get(&found.file) {
			Some(file) => file.clone(),
			None => {
				let text: Arc<str> = std::fs::read_to_string(&found.file)
					.map_err(|err| anyhow::anyhow!("reading {}: {err}", found.file.display()))?
					.into();
				files.insert(found.file.clone(), text.clone());
				text
			}
		};
		let source = QuerySource {
			origin: QueryOrigin::Host,
			path: relative(root, &found.file)?,
			span: Span {
				start: found.query.host_range.start,
				end: found.query.host_range.end,
			},
			line: found.query.line,
			column: found.query.column,
			method: Some(found.query.method.clone()),
		};
		let map = TextMap::Host {
			query: Box::new(found.query.clone()),
			file,
		};
		match by_text.get(&found.query.text) {
			Some(&at) => {
				inline[at].sources.push(source);
				inline[at].maps.push(map);
			}
			None => {
				by_text.insert(found.query.text.clone(), inline.len());
				inline.push(CollectedQuery {
					name: None,
					text: found.query.text,
					sources: vec![source],
					maps: vec![map],
				});
			}
		}
	}
	Ok(inline)
}

#[cfg(not(feature = "host-queries"))]
fn inline(_root: &str, config: &QueriesConfig) -> Result<Vec<CollectedQuery>> {
	if config.sources.is_empty() {
		return Ok(Vec::new());
	}
	anyhow::bail!(
		"[typegen.queries] sources needs the `host-queries` feature, which this build of \
		 SurrealKit does not have; remove `sources`, or use a build with it"
	)
}

/// The section's entries and diagnostics for `collected`, typed by `types`.
///
/// Entries are sorted by key, each one's sources by path then `span.start` and its params by
/// name. Without a `types` document (an engine that cannot type queries) there are no entries
/// and, when anything was collected, the warning `queries-unsupported-engine`. A kind the
/// reader does not know is `query-kind-unrecognized` on its entry, a key that two entries
/// share is `query-name-duplicate`, the first one kept, and a query the engine could not type
/// is `query-invalid` on its entry, which carries the engine's `error`. The other entries are
/// unaffected. The diagnostics include those of
/// reading the query files. The document's own `types` are not used: schema document v2
/// carries its own.
pub fn assemble(
	collected: &Collected,
	types: Option<&TypesDocument>,
) -> (Vec<QueryEntry>, Vec<Diagnostic>) {
	let mut diagnostics = collected.diagnostics.clone();
	for query in collected.inline.iter().filter(|q| collected.is_a_name(&q.text)) {
		diagnostics.push(
			Diagnostic::new(
				Severity::Error,
				codes::QUERY_NAME_DUPLICATE,
				query.text.as_str(),
				format!(
					"an inline query's text is the name {:?} of a named query, so both would have \
					 the key {:?}; the named query is kept",
					query.text, query.text
				),
			)
			.on_query(&query.text),
		);
	}
	let Some(types) = types else {
		if !collected.is_empty() {
			diagnostics.push(Diagnostic::new(
				Severity::Warning,
				codes::QUERIES_UNSUPPORTED_ENGINE,
				UNSUPPORTED_SOURCE,
				"queries are configured, but the engine cannot type them (SurrealDB before 4.1), \
				 so the document has no `queries`",
			));
		}
		return (Vec::new(), diagnostics);
	};

	let sources_of = |queries: &[CollectedQuery], key: &dyn Fn(&CollectedQuery) -> String| {
		queries
			.iter()
			.map(|q| (key(q), q.sources.clone()))
			.collect::<BTreeMap<String, Vec<QuerySource>>>()
	};
	let named = sources_of(&collected.named, &|q| q.name.clone().unwrap_or_default());
	let inline = sources_of(&collected.inline, &|q| q.text.clone());

	let mut entries: Vec<QueryEntry> = Vec::with_capacity(types.queries.len());
	for query in &types.queries {
		let (mut entry, unrepresented, unknown_live) = convert_query(query);
		if entries.iter().any(|seen| seen.key == entry.key) {
			diagnostics.push(
				Diagnostic::new(
					Severity::Error,
					codes::QUERY_NAME_DUPLICATE,
					entry.key.as_str(),
					format!("two queries have the key {:?}; the first one is kept", entry.key),
				)
				.on_query(&entry.key),
			);
			continue;
		}
		for unknown in unknown_live {
			diagnostics.push(
				Diagnostic::new(
					Severity::Warning,
					codes::LIVE_UNKNOWN,
					unknown.value(),
					unknown.reason(),
				)
				.on_query(&entry.key),
			);
		}
		for spelling in unrepresented {
			diagnostics.push(
				Diagnostic::new(
					Severity::Warning,
					codes::QUERY_KIND_UNRECOGNIZED,
					spelling,
					"the engine typed the query with a kind this SurrealKit does not know; it is \
					 `unknown`",
				)
				.on_query(&entry.key),
			);
		}
		if let Some(error) = &entry.error {
			// Where the engine's span falls in the file the query is written in, so the error
			// names the line a person has to change.
			let found = match &entry.name {
				Some(name) => collected.named.iter().find(|q| q.name.as_deref() == Some(name)),
				None => collected.inline.iter().find(|q| q.text == entry.text),
			};
			let place = found.and_then(|query| {
				let source = query.sources.first()?;
				let offset = error.span.map_or(0, |span| span.start);
				let (line, column) = match query.maps.first() {
					Some(map) => map.position(source, &query.text, offset),
					None => (source.line, source.column),
				};
				let more = match query.sources.len() - 1 {
					0 => String::new(),
					1 => " (and 1 more place)".to_string(),
					n => format!(" (and {n} more places)"),
				};
				Some(format!("{}:{line}:{column}{more}: ", source.path))
			});
			let mut diagnostic = Diagnostic::new(
				Severity::Error,
				codes::QUERY_INVALID,
				entry.text.as_str(),
				format!(
					"{}the engine cannot type the query ({}): {}",
					place.unwrap_or_default(),
					error.code,
					error.message
				),
			)
			.on_query(&entry.key);
			if let Some(span) = error.span {
				diagnostic = diagnostic.with_span(span.start..span.end);
			}
			diagnostics.push(diagnostic);
		}
		let sources = match &entry.name {
			Some(name) => named.get(name),
			None => inline.get(&entry.text),
		};
		entry.sources = sources.cloned().unwrap_or_default();
		entry.sources.sort_by(|a, b| (&a.path, a.span.start).cmp(&(&b.path, b.span.start)));
		entry.params.sort_by(|a, b| a.name.cmp(&b.name));
		entries.push(entry);
	}
	entries.sort_by(|a, b| a.key.cmp(&b.key));
	(entries, diagnostics)
}

/// Types `collected` against `db` and sets `document.queries`, as [`apply`] does.
pub async fn attach(
	db: &Surreal<Any>,
	collected: &Collected,
	document: &mut SchemaDocument,
) -> Result<()> {
	let inputs = collected.inputs();
	let types = if inputs.is_empty() {
		None
	} else {
		engine::types_document(db, &inputs).await?
	};
	apply(document, collected, types.as_ref());
	Ok(())
}

/// [`assemble`]s `collected` with `types` and sets `document.queries`, adding the diagnostics
/// to `document.diagnostics`, which stay in canonical order. The digest is outside the section,
/// so it does not change. `types` is what [`engine::types_document`] returned for
/// [`Collected::inputs`], or the same assembled from earlier answers.
pub fn apply(document: &mut SchemaDocument, collected: &Collected, types: Option<&TypesDocument>) {
	let (entries, diagnostics) = assemble(collected, types);
	document.queries = entries;
	document.diagnostics.extend(diagnostics);
	document.diagnostics =
		crate::typegen::sort_diagnostics(std::mem::take(&mut document.diagnostics));
}

#[cfg(test)]
mod tests {
	use serde_json::json;

	use super::*;
	use crate::typegen::Span;
	use crate::typegen::document::LiveAction;
	use crate::typegen::document::QueryOrigin as Origin;

	fn source(origin: Origin, path: &str, start: usize, method: Option<&str>) -> QuerySource {
		QuerySource {
			origin,
			path: path.to_string(),
			span: Span {
				start,
				end: start + 10,
			},
			line: 1,
			column: 1,
			method: method.map(str::to_string),
		}
	}

	fn named(name: &str, text: &str, sources: Vec<QuerySource>) -> CollectedQuery {
		CollectedQuery {
			name: Some(name.to_string()),
			text: text.to_string(),
			sources,
			maps: Vec::new(),
		}
	}

	fn inline_query(text: &str, sources: Vec<QuerySource>) -> CollectedQuery {
		CollectedQuery {
			name: None,
			text: text.to_string(),
			sources,
			maps: Vec::new(),
		}
	}

	fn engine_document(queries: serde_json::Value) -> TypesDocument {
		let doc = json!({ "format": "surrealdb.types", "version": 1, "queries": queries });
		TypesDocument::from_json(&doc.to_string()).expect("document")
	}

	fn int_statement() -> serde_json::Value {
		json!({ "kind_value": "Int", "certainty": "fact", "support": { "exact": true } })
	}

	/// A live statement whose `exit` is `exit` and whose notifications are `actions`.
	fn live_statement(exit: Option<&str>, actions: &[&str]) -> serde_json::Value {
		let notifications: Vec<serde_json::Value> = actions
			.iter()
			.map(|action| {
				json!({ "action": action, "record": "record<user>",
					"record_value": { "Record": ["user"] }, "result": "string",
					"result_value": "String", "certainty": "fact" })
			})
			.collect();
		let mut live = json!({ "diff": false, "query": "LIVE SELECT * FROM user",
			"notifications": notifications });
		if let Some(exit) = exit {
			live["exit"] = json!(exit);
		}
		json!({ "kind_value": "String", "certainty": "fact", "support": { "exact": true },
			"live": live,
			"live_refusal": { "code": "E4009", "message": "m", "clauses": ["LIMIT"], "windowable": true } })
	}

	/// The engine's answer for `texts`: the first fails at `span` with S0001, the rest type.
	fn one_failure(texts: &[&str], span: (usize, usize)) -> TypesDocument {
		let queries: Vec<serde_json::Value> = texts
			.iter()
			.enumerate()
			.map(|(at, text)| match at {
				0 => json!({ "text": text, "statements": [], "params": [],
					"error": { "code": "S0001", "message": "Unexpected token",
						"span": { "start": span.0, "end": span.1 } } }),
				_ => json!({ "text": text, "statements": [int_statement()] }),
			})
			.collect();
		engine_document(json!(queries))
	}

	fn invalid(diagnostics: &[Diagnostic]) -> Vec<(&str, &str)> {
		diagnostics
			.iter()
			.filter(|d| d.code == codes::QUERY_INVALID)
			.map(|d| (d.query.as_deref().unwrap_or_default(), d.reason.as_str()))
			.collect()
	}

	#[test]
	fn a_failed_query_is_located_in_its_query_file() {
		// The query starts at line 2, column 1 of its file; the engine blames `nope` on its
		// second line.
		let text = "SELECT *\nFROM nope WHERE;";
		let mut query =
			named("Broken", text, vec![source(Origin::File, "queries/a.surql", 9, None)]);
		query.sources[0].line = 2;
		query.maps = vec![TextMap::File];
		let collected = Collected {
			named: vec![query],
			inline: vec![inline_query("RETURN 1", vec![])],
			diagnostics: vec![],
		};
		let at = text.find("nope").unwrap();
		let types = engine_document(json!([
			{ "name": "Broken", "text": text, "statements": [], "params": [],
				"error": { "code": "S0001", "message": "Unexpected token",
					"span": { "start": at, "end": at + 4 } } },
			{ "text": "RETURN 1", "statements": [int_statement()] },
		]));
		let (entries, diagnostics) = assemble(&collected, Some(&types));
		assert_eq!(entries.len(), 2, "the failed entry carries its error; the others type");
		assert!(entries.iter().any(|e| e.key == "RETURN 1" && e.error.is_none()));
		assert_eq!(
			invalid(&diagnostics),
			[(
				"Broken",
				"queries/a.surql:3:6: the engine cannot type the query (S0001): Unexpected token"
			)]
		);
	}

	#[cfg(feature = "host-queries")]
	#[test]
	fn a_failed_inline_query_is_located_in_its_host_file() {
		// A template literal on line 2 of the host file, with a substitution before the error.
		let host = "const db = connect();\nawait db.query(`SELECT * FROM ${table} WHERE;`);\n";
		let found = crate::host_queries::extract("a.ts", host, &["query"]);
		let query = found.into_iter().next().expect("one query");
		let text = query.text.clone();
		let source = QuerySource {
			origin: Origin::Host,
			path: "src/a.ts".to_string(),
			span: Span {
				start: query.host_range.start,
				end: query.host_range.end,
			},
			line: query.line,
			column: query.column,
			method: Some("query".to_string()),
		};
		let collected = Collected {
			named: vec![],
			inline: vec![CollectedQuery {
				name: None,
				text: text.clone(),
				sources: vec![source],
				maps: vec![TextMap::Host {
					query: Box::new(query),
					file: host.into(),
				}],
			}],
			diagnostics: vec![],
		};
		let at = text.find("WHERE").unwrap();
		let types = one_failure(&[text.as_str()], (at, at + 5));
		let (_, diagnostics) = assemble(&collected, Some(&types));
		let column = "await db.query(`SELECT * FROM ${table} ".chars().count() + 1;
		let reason = format!(
			"src/a.ts:2:{column}: the engine cannot type the query (S0001): Unexpected token"
		);
		assert_eq!(invalid(&diagnostics), [(text.as_str(), reason.as_str())]);
	}

	#[test]
	fn an_unknown_live_value_drops_only_what_it_touches_and_warns() {
		let collected = Collected {
			named: vec![],
			inline: vec![
				inline_query("SELECT a FROM user", vec![]),
				inline_query("SELECT b FROM user", vec![]),
			],
			diagnostics: vec![],
		};
		let types = engine_document(json!([
			// An action this reader does not know: that notification is left out.
			{ "text": "SELECT a FROM user",
				"statements": [live_statement(None, &["CREATE", "MOVE", "KILLED"])] },
			// An exit this reader does not know: the statement keeps no live facts at all.
			{ "text": "SELECT b FROM user",
				"statements": [live_statement(Some("bounce"), &["CREATE"])] },
		]));
		let (entries, diagnostics) = assemble(&collected, Some(&types));

		let a = &entries[0].statements[0];
		let actions: Vec<_> =
			a.live.as_ref().expect("live").notifications.iter().map(|n| n.action).collect();
		assert_eq!(actions, [LiveAction::Create, LiveAction::Killed]);
		assert!(a.live_refusal.is_some());
		assert!(a.kind.is_some());

		let b = &entries[1].statements[0];
		assert!(b.live.is_none() && b.live_refusal.is_none());
		assert!(b.kind.is_some(), "the statement's own type stays");

		let unknown: Vec<_> = diagnostics
			.iter()
			.filter(|d| d.code == codes::LIVE_UNKNOWN)
			.map(|d| (d.severity, d.source.as_str(), d.query.as_deref()))
			.collect();
		assert_eq!(
			unknown,
			[
				(Severity::Warning, "MOVE", Some("SELECT a FROM user")),
				(Severity::Warning, "bounce", Some("SELECT b FROM user")),
			]
		);
	}

	#[test]
	fn inputs_list_named_queries_with_their_name_then_inline_ones() {
		let collected = Collected {
			named: vec![named("A", "RETURN 1", vec![])],
			inline: vec![inline_query("RETURN 2", vec![])],
			diagnostics: vec![],
		};
		assert_eq!(
			collected.inputs(),
			[(Some("A".to_string()), "RETURN 1".to_string()), (None, "RETURN 2".to_string())]
		);
	}

	#[test]
	fn sources_are_attached_by_key_and_sorted_with_entries_and_params() {
		let collected = Collected {
			named: vec![named(
				"Zeta",
				"RETURN $b + $a",
				vec![source(Origin::File, "q/z.surql", 5, None)],
			)],
			inline: vec![inline_query(
				"RETURN 1",
				vec![
					source(Origin::Host, "src/b.ts", 7, Some("query")),
					source(Origin::Host, "src/a.ts", 90, Some("run")),
					source(Origin::Host, "src/a.ts", 20, Some("query")),
				],
			)],
			diagnostics: vec![],
		};
		let types = engine_document(json!([
			{ "name": "Zeta", "text": "RETURN $b + $a", "statements": [int_statement()],
				"params": [{ "name": "b", "required": true }, { "name": "a", "required": true }] },
			{ "text": "RETURN 1", "statements": [int_statement()] },
			{ "name": "Alpha", "text": "RETURN 3", "statements": [int_statement()] },
		]));
		let (entries, diagnostics) = assemble(&collected, Some(&types));
		assert!(diagnostics.is_empty(), "{diagnostics:?}");
		let keys: Vec<&str> = entries.iter().map(|e| e.key.as_str()).collect();
		assert_eq!(keys, ["Alpha", "RETURN 1", "Zeta"]);
		let inline = &entries[1];
		let places: Vec<(&str, usize)> =
			inline.sources.iter().map(|s| (s.path.as_str(), s.span.start)).collect();
		assert_eq!(places, [("src/a.ts", 20), ("src/a.ts", 90), ("src/b.ts", 7)]);
		assert_eq!(inline.sources[1].method.as_deref(), Some("run"));
		assert!(entries[0].sources.is_empty(), "an engine entry nothing collected has no sources");
		let zeta = &entries[2];
		assert_eq!(zeta.sources.len(), 1);
		assert_eq!(zeta.sources[0].origin, Origin::File);
		let params: Vec<&str> = zeta.params.iter().map(|p| p.name.as_str()).collect();
		assert_eq!(params, ["a", "b"]);
	}

	#[test]
	fn the_engines_own_types_are_not_used() {
		let types = engine_document(json!([]));
		let (entries, diagnostics) = assemble(&Collected::default(), Some(&types));
		assert!(entries.is_empty() && diagnostics.is_empty());
	}

	#[test]
	fn an_engine_that_cannot_type_queries_gives_a_warning_only_when_something_was_collected() {
		let collected = Collected {
			inline: vec![inline_query(
				"RETURN 1",
				vec![source(Origin::Host, "a.ts", 0, Some("query"))],
			)],
			..Collected::default()
		};
		let (entries, diagnostics) = assemble(&collected, None);
		assert!(entries.is_empty());
		assert_eq!(diagnostics.len(), 1);
		assert_eq!(diagnostics[0].code, codes::QUERIES_UNSUPPORTED_ENGINE);
		assert_eq!(diagnostics[0].severity, Severity::Warning);
		assert!(diagnostics[0].query.is_none());

		let (entries, diagnostics) = assemble(&Collected::default(), None);
		assert!(entries.is_empty() && diagnostics.is_empty());
	}

	#[test]
	fn reading_diagnostics_are_kept_with_or_without_an_engine() {
		let problem =
			Diagnostic::new(Severity::Error, codes::QUERY_TEXT_EMPTY, "-- name: A", "empty");
		let collected = Collected {
			diagnostics: vec![problem],
			..Collected::default()
		};
		assert_eq!(assemble(&collected, None).1, collected.diagnostics);
		assert_eq!(
			assemble(&collected, Some(&engine_document(json!([])))).1,
			collected.diagnostics
		);
	}

	#[test]
	fn an_unrepresented_kind_is_a_warning_on_its_entry() {
		let types = engine_document(json!([
			{ "name": "Odd", "text": "q1", "statements": [
				{ "kind_value": { "Hologram": 1 }, "certainty": "fact" }
			] },
			{ "text": "q2", "statements": [
				{ "kind_value": { "Quux": [] }, "certainty": "fact" }
			] },
		]));
		let (entries, diagnostics) = assemble(&Collected::default(), Some(&types));
		assert_eq!(entries.len(), 2);
		let warnings: Vec<(&str, &str, Option<&str>)> = diagnostics
			.iter()
			.map(|d| (d.code.as_str(), d.source.as_str(), d.query.as_deref()))
			.collect();
		assert_eq!(
			warnings,
			[
				("query-kind-unrecognized", r#"{"Hologram":1}"#, Some("Odd")),
				("query-kind-unrecognized", r#"{"Quux":[]}"#, Some("q2")),
			]
		);
		assert!(diagnostics.iter().all(|d| d.severity == Severity::Warning));
	}

	#[test]
	fn a_named_query_and_an_inline_text_with_the_same_key_collide() {
		let collected = Collected {
			named: vec![named("Clash", "RETURN 1", vec![source(Origin::File, "q.surql", 0, None)])],
			inline: vec![inline_query(
				"Clash",
				vec![source(Origin::Host, "a.ts", 3, Some("query"))],
			)],
			diagnostics: vec![],
		};
		// The engine refuses two inputs with one key, so the inline query is not sent.
		assert_eq!(collected.inputs(), [(Some("Clash".to_string()), "RETURN 1".to_string())]);
		let types = engine_document(json!([
			{ "name": "Clash", "text": "RETURN 1", "statements": [int_statement()] },
		]));
		let (entries, diagnostics) = assemble(&collected, Some(&types));
		assert_eq!(entries.len(), 1);
		assert_eq!(entries[0].name.as_deref(), Some("Clash"));
		assert_eq!(entries[0].sources.len(), 1, "the inline source does not join the named entry");
		assert_eq!(diagnostics.len(), 1);
		assert_eq!(diagnostics[0].code, codes::QUERY_NAME_DUPLICATE);
		assert_eq!(diagnostics[0].severity, Severity::Error);
		assert_eq!(diagnostics[0].query.as_deref(), Some("Clash"));
	}

	#[test]
	fn collect_reads_query_files_with_paths_relative_to_the_root() {
		let dir = tempfile::TempDir::new().unwrap();
		let root = dir.path().join("database");
		std::fs::create_dir_all(root.join("queries")).unwrap();
		std::fs::write(
			root.join("queries/people.surql"),
			"-- name: ListPeople\nSELECT name FROM person;\n-- name: Bad-Name\nRETURN 1;\n",
		)
		.unwrap();
		let config = QueriesConfig {
			files: vec![root.join("queries")],
			..QueriesConfig::default()
		};
		let collected = collect(root.to_str().unwrap(), &config).unwrap();
		assert_eq!(collected.named.len(), 1);
		let query = &collected.named[0];
		assert_eq!(query.name.as_deref(), Some("ListPeople"));
		assert_eq!(query.sources[0].origin, Origin::File);
		assert_eq!(query.sources[0].path, "queries/people.surql");
		assert_eq!((query.sources[0].line, query.sources[0].column), (2, 1));
		assert_eq!(collected.diagnostics.len(), 1);
		assert_eq!(collected.diagnostics[0].code, codes::QUERY_NAME_INVALID);
	}

	#[cfg(feature = "host-queries")]
	#[test]
	fn collect_reads_host_code_and_merges_identical_text() {
		let dir = tempfile::TempDir::new().unwrap();
		let root = dir.path().join("database");
		let src = dir.path().join("src");
		std::fs::create_dir_all(&root).unwrap();
		std::fs::create_dir_all(&src).unwrap();
		std::fs::write(
			src.join("a.ts"),
			"db.query(\"SELECT 1\");\ndb.run(\"SELECT 2\");\ndb.other(\"SELECT 3\");\n",
		)
		.unwrap();
		std::fs::write(src.join("b.ts"), "db.query(\"SELECT 1\");\n").unwrap();
		let config = QueriesConfig {
			sources: vec![src],
			methods: vec!["query".into(), "run".into()],
			..QueriesConfig::default()
		};
		let collected = collect(root.to_str().unwrap(), &config).unwrap();
		assert!(collected.named.is_empty());
		let texts: Vec<&str> = collected.inline.iter().map(|q| q.text.as_str()).collect();
		assert_eq!(texts, ["SELECT 1", "SELECT 2"]);
		let first = &collected.inline[0];
		assert_eq!(first.sources.len(), 2, "one query, two call sites");
		assert!(first.sources.iter().all(|s| s.origin == Origin::Host));
		assert_eq!(first.sources[0].method.as_deref(), Some("query"));
		assert_eq!(
			first.sources[0].span,
			Span {
				start: 10,
				end: 18
			}
		);
		assert_eq!((first.sources[0].line, first.sources[0].column), (1, 11));
		assert_eq!(collected.inline[1].sources[0].method.as_deref(), Some("run"));
		assert_eq!(
			collected.inputs(),
			[(None, "SELECT 1".to_string()), (None, "SELECT 2".to_string())]
		);
	}

	#[cfg(not(feature = "host-queries"))]
	#[test]
	fn host_sources_need_the_feature() {
		let config = QueriesConfig {
			sources: vec!["src".into()],
			..QueriesConfig::default()
		};
		let err = collect("database", &config).unwrap_err().to_string();
		assert!(err.contains("host-queries"), "{err}");
	}

	#[cfg(feature = "kv-mem")]
	#[tokio::test]
	async fn a_compile_on_this_engine_warns_and_keeps_the_digest() {
		use crate::compile::{CompileModule, CompileOpts, compile};
		use crate::schema_state::SchemaFile;
		let module = CompileModule {
			name: "default".into(),
			files: vec![SchemaFile {
				path: "schema/a.surql".into(),
				sql: "DEFINE TABLE person SCHEMAFULL;\nDEFINE FIELD name ON person TYPE string;"
					.into(),
				hash: String::new(),
			}],
		};
		let plain = compile(std::slice::from_ref(&module), &CompileOpts::default()).await.unwrap();
		let collected = Collected {
			inline: vec![inline_query(
				"SELECT name FROM person",
				vec![source(Origin::Host, "a.ts", 0, Some("query"))],
			)],
			..Collected::default()
		};
		// Whether the embedded engine types queries is a property of the `surrealdb` this crate
		// is built with; the test holds for either.
		let built = crate::compile::build(std::slice::from_ref(&module), &CompileOpts::default())
			.await
			.unwrap();
		let can_type =
			engine::types_document(&built.db, &collected.inputs()).await.unwrap().is_some();
		let with = compile(
			std::slice::from_ref(&module),
			&CompileOpts {
				queries: Some(collected),
				..CompileOpts::default()
			},
		)
		.await
		.unwrap();
		// The section is outside the digest either way.
		assert_eq!(with.document.semantic_digest, plain.document.semantic_digest);
		let added: Vec<&Diagnostic> = with
			.document
			.diagnostics
			.iter()
			.filter(|d| !plain.document.diagnostics.contains(d))
			.collect();
		if can_type {
			assert!(added.iter().all(|d| d.code != codes::QUERIES_UNSUPPORTED_ENGINE), "{added:?}");
			let [entry] = with.document.queries.as_slice() else {
				panic!("one typed entry: {:?}", with.document.queries);
			};
			assert_eq!(entry.key, "SELECT name FROM person");
			assert!(entry.error.is_none(), "{entry:?}");
			assert!(!entry.statements.is_empty());
		} else {
			assert!(with.document.queries.is_empty());
			assert_eq!(added.len(), 1);
			assert_eq!(added[0].code, codes::QUERIES_UNSUPPORTED_ENGINE);
			assert_eq!(with.document.without_queries(), plain.document);
		}
	}
}
