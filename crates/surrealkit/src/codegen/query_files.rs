//! Query files: `.surql` files of named queries (contract section 19).
//!
//! A query file holds queries, each introduced by a marker comment on a line of its own:
//!
//! ```surql
//! -- name: ListPeople
//! SELECT name, age FROM person WHERE age >= $min ORDER BY name;
//! ```
//!
//! The marker is `--`, optional spaces, `name:`, optional spaces and the name; nothing else
//! follows it on the line. A query's text runs from the line after its marker to the next
//! marker or the end of the file, without leading and trailing whitespace and with CRLF line
//! endings read as LF. Before the first marker a file holds only whitespace and line comments
//! (`--`, `#`, `//`).
//!
//! [`parse`] reads one file and [`scan`] every `.surql` file under some directories. A problem
//! is a [`Diagnostic`] whose `source` is the offending line and whose `span` is the offending
//! text within it, and the query it belongs to is left out.

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use walkdir::WalkDir;

use crate::typegen::{Diagnostic, Severity, Span, codes};

/// One named query of a query file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedQuery {
	/// An identifier: `[A-Za-z_][A-Za-z0-9_]*`.
	pub name: String,
	/// The query text, trimmed, with LF line endings.
	pub text: String,
	/// The UTF-8 byte range of the trimmed text in the file.
	pub span: Span,
	/// 1-based line of `span.start`.
	pub line: u32,
	/// 1-based column of `span.start`, counted in Unicode scalar values.
	pub column: u32,
}

/// A marker line: its name, and where the name is in the line.
struct Marker<'a> {
	name: &'a str,
	/// Byte offset of the name in its line.
	name_at: usize,
}

/// The marker a line is, if it is one. `line` has no line ending.
fn marker(line: &str) -> Option<Marker<'_>> {
	let body = line.trim_start_matches(|c: char| c.is_whitespace() || c == '\u{FEFF}');
	let rest = body.strip_prefix("--")?.trim_start_matches([' ', '\t']);
	let rest = rest.strip_prefix("name:")?.trim_start_matches([' ', '\t']);
	Some(Marker {
		name: rest.trim_end(),
		name_at: line.len() - rest.len(),
	})
}

fn is_identifier(name: &str) -> bool {
	let mut chars = name.chars();
	chars.next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
		&& chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Whether a trimmed line is blank or a line comment.
fn is_blank_or_comment(trimmed: &str) -> bool {
	trimmed.is_empty()
		|| trimmed.starts_with("--")
		|| trimmed.starts_with('#')
		|| trimmed.starts_with("//")
}

/// The 1-based line and the Unicode-scalar column of byte `offset` in `source`.
fn position(source: &str, offset: usize) -> (u32, u32) {
	let before = &source[..offset];
	let line = before.bytes().filter(|byte| *byte == b'\n').count() + 1;
	let line_start = before.rfind('\n').map_or(0, |at| at + 1);
	let column = before[line_start..].chars().count() + 1;
	(u32::try_from(line).unwrap_or(u32::MAX), u32::try_from(column).unwrap_or(u32::MAX))
}

/// One line of a file: its range without the line ending, and where the next line starts.
struct Line {
	start: usize,
	end: usize,
	next: usize,
}

fn lines(source: &str) -> Vec<Line> {
	let mut out = Vec::new();
	let mut start = 0;
	for raw in source.split_inclusive('\n') {
		let next = start + raw.len();
		let text = raw.strip_suffix('\n').unwrap_or(raw);
		let text = text.strip_suffix('\r').unwrap_or(text);
		out.push(Line {
			start,
			end: start + text.len(),
			next,
		});
		start = next;
	}
	out
}

/// Reads one query file. `path` is only for the diagnostics' reasons.
pub fn parse(path: &str, source: &str) -> (Vec<NamedQuery>, Vec<Diagnostic>) {
	let mut queries = Vec::new();
	let mut diagnostics = Vec::new();
	let lines = lines(source);

	// Each marker line, with its index in `lines`.
	let markers: Vec<(usize, Marker<'_>)> = lines
		.iter()
		.enumerate()
		.filter_map(|(index, line)| Some((index, marker(&source[line.start..line.end])?)))
		.collect();

	// Before the first marker: whitespace and line comments only.
	let preamble_end = markers.first().map_or(lines.len(), |(index, _)| *index);
	for line in &lines[..preamble_end] {
		let text = &source[line.start..line.end];
		if is_blank_or_comment(text.trim()) {
			continue;
		}
		let lead = text.len() - text.trim_start().len();
		let (row, column) = position(source, line.start + lead);
		diagnostics.push(
			Diagnostic::new(
				Severity::Error,
				codes::QUERY_TEXT_UNNAMED,
				text,
				format!(
					"{path}:{row}:{column}: text before the first `-- name:` marker; every query \
					 in a query file needs a name"
				),
			)
			.with_span(lead..lead + text.trim().len()),
		);
		break;
	}

	for (at, (index, marker)) in markers.iter().enumerate() {
		let line = &lines[*index];
		let marker_line = &source[line.start..line.end];
		let (row, column) = position(source, line.start + marker.name_at);
		if !is_identifier(marker.name) {
			diagnostics.push(
				Diagnostic::new(
					Severity::Error,
					codes::QUERY_NAME_INVALID,
					marker_line,
					format!(
						"{path}:{row}:{column}: {:?} is not a query name; a name is an identifier \
						 (letters, digits and `_`, not starting with a digit)",
						marker.name
					),
				)
				.with_span(marker.name_at..marker.name_at + marker.name.len()),
			);
			continue;
		}
		let body_start = line.next;
		let body_end = markers.get(at + 1).map_or(source.len(), |(next, _)| lines[*next].start);
		let body = &source[body_start..body_end];
		let text = body.trim();
		if text.is_empty() {
			let lead = marker_line.len() - marker_line.trim_start().len();
			diagnostics.push(
				Diagnostic::new(
					Severity::Error,
					codes::QUERY_TEXT_EMPTY,
					marker_line,
					format!("{path}:{row}:{column}: the query {:?} has no text", marker.name),
				)
				.with_span(lead..lead + marker_line.trim().len()),
			);
			continue;
		}
		let start = body_start + (body.len() - body.trim_start().len());
		let (line_no, column_no) = position(source, start);
		queries.push(NamedQuery {
			name: marker.name.to_string(),
			text: text.replace("\r\n", "\n"),
			span: Span {
				start,
				end: start + text.len(),
			},
			line: line_no,
			column: column_no,
		});
	}
	(queries, diagnostics)
}

/// The queries of a [`scan`] with the file each is in, and what was wrong.
pub type Scanned = (Vec<(PathBuf, NamedQuery)>, Vec<Diagnostic>);

/// Reads every `.surql` file under `dirs`, recursively, in path order; a file reachable from
/// several directories is read once. A name defined twice, in one file or across files, is
/// `query-name-duplicate` and the first definition stays.
///
/// # Errors
///
/// Fails when a directory does not exist, or a file cannot be read as UTF-8.
pub fn scan(dirs: &[PathBuf]) -> Result<Scanned> {
	let mut files = BTreeSet::new();
	for dir in dirs {
		if !dir.is_dir() {
			bail!("[typegen.queries] files: {} is not a directory", dir.display());
		}
		for entry in WalkDir::new(dir).follow_links(false) {
			let entry = entry.with_context(|| format!("listing {}", dir.display()))?;
			if entry.file_type().is_file()
				&& entry.path().extension().is_some_and(|extension| extension == "surql")
			{
				files.insert(entry.into_path());
			}
		}
	}

	let mut found: Vec<(PathBuf, NamedQuery)> = Vec::new();
	let mut diagnostics = Vec::new();
	let mut first: HashMap<String, (String, u32)> = HashMap::new();
	for file in files {
		let source = std::fs::read_to_string(&file)
			.with_context(|| format!("reading the query file {}", file.display()))?;
		let shown = slashed(&file);
		let (queries, problems) = parse(&shown, &source);
		diagnostics.extend(problems);
		for query in queries {
			if let Some((there, line)) = first.get(&query.name) {
				diagnostics.push(
					Diagnostic::new(
						Severity::Error,
						codes::QUERY_NAME_DUPLICATE,
						query.name.as_str(),
						format!(
							"{shown}:{}: the query name {:?} is already defined at {there}:{line}",
							query.line, query.name
						),
					)
					.on_query(&query.name),
				);
				continue;
			}
			first.insert(query.name.clone(), (shown.clone(), query.line));
			found.push((file.clone(), query));
		}
	}
	Ok((found, diagnostics))
}

fn slashed(path: &Path) -> String {
	path.to_string_lossy().replace('\\', "/")
}

#[cfg(test)]
mod tests {
	use super::*;

	fn codes_of(diagnostics: &[Diagnostic]) -> Vec<&str> {
		diagnostics.iter().map(|d| d.code.as_str()).collect()
	}

	#[test]
	fn two_queries_with_text_spans_and_positions() {
		let source = "-- name: ListPeople\nSELECT name FROM person;\n\n-- name: GetPerson\n-- note\nSELECT * FROM ONLY $id;\n";
		let (queries, diagnostics) = parse("q.surql", source);
		assert!(diagnostics.is_empty(), "{diagnostics:?}");
		assert_eq!(queries.len(), 2);
		let first = &queries[0];
		assert_eq!(
			(first.name.as_str(), first.text.as_str()),
			("ListPeople", "SELECT name FROM person;")
		);
		assert_eq!(&source[first.span.start..first.span.end], first.text);
		assert_eq!((first.line, first.column), (2, 1));
		let second = &queries[1];
		assert_eq!(second.text, "-- note\nSELECT * FROM ONLY $id;");
		assert_eq!(&source[second.span.start..second.span.end], second.text);
		assert_eq!((second.line, second.column), (5, 1));
	}

	#[test]
	fn comments_and_blank_lines_may_precede_the_first_marker() {
		let source = "\n-- queries for people\n# another\n// and a third\n\n-- name: A\nRETURN 1;";
		let (queries, diagnostics) = parse("q.surql", source);
		assert!(diagnostics.is_empty(), "{diagnostics:?}");
		assert_eq!(queries.len(), 1);
		assert_eq!(queries[0].text, "RETURN 1;");
		assert_eq!(queries[0].line, 7);
	}

	#[test]
	fn the_marker_tolerates_spacing() {
		let (queries, diagnostics) =
			parse("q.surql", "--name:A\nRETURN 1;\n--   name:   B   \nRETURN 2;");
		assert!(diagnostics.is_empty(), "{diagnostics:?}");
		let names: Vec<&str> = queries.iter().map(|q| q.name.as_str()).collect();
		assert_eq!(names, ["A", "B"]);
	}

	#[test]
	fn crlf_reads_as_lf_and_the_span_stays_in_original_bytes() {
		let source = "-- name: A\r\nSELECT 1;\r\nSELECT 2;\r\n\r\n-- name: B\r\nRETURN 3;\r\n";
		let (queries, diagnostics) = parse("q.surql", source);
		assert!(diagnostics.is_empty(), "{diagnostics:?}");
		assert_eq!(queries[0].text, "SELECT 1;\nSELECT 2;");
		let span = queries[0].span;
		assert_eq!(&source[span.start..span.end], "SELECT 1;\r\nSELECT 2;");
		assert_eq!(queries[1].text, "RETURN 3;");
		assert_eq!((queries[1].line, queries[1].column), (6, 1));
	}

	#[test]
	fn the_column_counts_unicode_scalar_values() {
		let (queries, _) = parse("q.surql", "-- name: A\n  é; RETURN 1;");
		assert_eq!((queries[0].line, queries[0].column), (2, 3));
		assert_eq!(queries[0].span.start, 11 + 2);
	}

	#[test]
	fn an_invalid_name_is_an_error_and_drops_the_query() {
		let source = "-- name: list-people\nRETURN 1;\n-- name: 1st\nRETURN 2;\n-- name:\nRETURN 3;\n-- name: two words\nRETURN 4;\n-- name: Ok\nRETURN 5;";
		let (queries, diagnostics) = parse("q.surql", source);
		assert_eq!(queries.len(), 1);
		assert_eq!(queries[0].name, "Ok");
		assert_eq!(codes_of(&diagnostics), ["query-name-invalid"; 4]);
		let first = &diagnostics[0];
		assert_eq!(first.source, "-- name: list-people");
		let span = first.span.expect("a span");
		assert_eq!(&first.source[span.start..span.end], "list-people");
		assert!(first.reason.starts_with("q.surql:1:"), "{}", first.reason);
		// An empty name has an empty span at the end of the line.
		let empty = diagnostics[2].span.expect("a span");
		assert_eq!(empty.start, empty.end);
	}

	#[test]
	fn text_before_the_first_marker_is_unnamed() {
		let (queries, diagnostics) =
			parse("q.surql", "-- fine\n  SELECT 1;\nSELECT 2;\n-- name: A\nRETURN 1;");
		assert_eq!(queries.len(), 1);
		assert_eq!(codes_of(&diagnostics), ["query-text-unnamed"]);
		let d = &diagnostics[0];
		assert_eq!(d.source, "  SELECT 1;");
		let span = d.span.expect("a span");
		assert_eq!(&d.source[span.start..span.end], "SELECT 1;");

		let (queries, diagnostics) = parse("q.surql", "SELECT 1;");
		assert!(queries.is_empty());
		assert_eq!(codes_of(&diagnostics), ["query-text-unnamed"]);
	}

	#[test]
	fn a_marker_without_text_is_an_error() {
		let (queries, diagnostics) =
			parse("q.surql", "-- name: A\n\n  \n-- name: B\nRETURN 1;\n-- name: C\n");
		assert_eq!(queries.iter().map(|q| q.name.as_str()).collect::<Vec<_>>(), ["B"]);
		assert_eq!(codes_of(&diagnostics), ["query-text-empty", "query-text-empty"]);
		assert_eq!(diagnostics[0].source, "-- name: A");
		assert!(diagnostics[0].reason.contains("\"A\""));
	}

	#[test]
	fn a_query_may_have_several_statements() {
		let source = "-- name: Move\nBEGIN;\nUPDATE a SET x = 1;\nCOMMIT;\n";
		let (queries, diagnostics) = parse("q.surql", source);
		assert!(diagnostics.is_empty());
		assert_eq!(queries.len(), 1);
		assert_eq!(queries[0].text, "BEGIN;\nUPDATE a SET x = 1;\nCOMMIT;");
	}

	#[test]
	fn a_plain_comment_that_mentions_name_is_not_a_marker() {
		let source = "-- name: A\n-- the name: of this\nRETURN 1;\n-- not a name: B\nRETURN 2;";
		let (queries, diagnostics) = parse("q.surql", source);
		assert!(diagnostics.is_empty(), "{diagnostics:?}");
		assert_eq!(queries.len(), 1);
		assert!(queries[0].text.starts_with("-- the name: of this"));
	}

	#[test]
	fn scan_reads_every_file_in_order_and_flags_duplicates_across_files() {
		let dir = tempfile::TempDir::new().unwrap();
		let root = dir.path();
		std::fs::create_dir_all(root.join("nested")).unwrap();
		std::fs::write(
			root.join("b.surql"),
			"-- name: Shared\nRETURN 2;\n-- name: OnlyB\nRETURN 3;",
		)
		.unwrap();
		std::fs::write(root.join("a.surql"), "-- name: Shared\nRETURN 1;").unwrap();
		std::fs::write(root.join("nested/c.surql"), "-- name: C\nRETURN 4;").unwrap();
		std::fs::write(root.join("ignored.sql"), "-- name: Ignored\nRETURN 5;").unwrap();

		let (queries, diagnostics) = scan(&[root.to_path_buf(), root.join("nested")]).unwrap();
		let seen: Vec<(String, &str)> = queries
			.iter()
			.map(|(path, q)| {
				(path.file_name().unwrap().to_string_lossy().into_owned(), q.name.as_str())
			})
			.collect();
		assert_eq!(
			seen,
			[
				("a.surql".to_string(), "Shared"),
				("b.surql".to_string(), "OnlyB"),
				("c.surql".to_string(), "C")
			]
		);
		assert_eq!(queries[0].1.text, "RETURN 1;");
		assert_eq!(codes_of(&diagnostics), ["query-name-duplicate"]);
		assert_eq!(diagnostics[0].query.as_deref(), Some("Shared"));
		assert!(diagnostics[0].reason.contains("a.surql:2"), "{}", diagnostics[0].reason);
	}

	#[test]
	fn scan_refuses_a_missing_directory() {
		let err = scan(&[PathBuf::from("no/such/queries")]).unwrap_err().to_string();
		assert!(err.contains("no/such/queries") && err.contains("not a directory"), "{err}");
	}
}
