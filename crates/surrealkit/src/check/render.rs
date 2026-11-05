//! The three ways to show a [`Report`]: for a person, as JSON and as SARIF.
//!
//! Every function is pure. Paths are shown relative to a `base` directory (the working
//! directory, for the CLI), with `/` separators on every platform, and `..` where the file is
//! not under it. Lines and columns are 1-based and columns count Unicode scalar values.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::path::{Component, Path};

use serde::Serialize;

use super::Report;
use super::finding::{Code, Finding, LineIndex, Note, NoteKind, Severity, Source, Tag, describe};

const RED: &str = "\x1b[1;31m";
const YELLOW: &str = "\x1b[1;33m";
const CYAN: &str = "\x1b[1;36m";
const BLUE: &str = "\x1b[1;34m";
const DIM: &str = "\x1b[2m";
const RESET: &str = "\x1b[0m";

/// `n` and its noun, `1 error` and `2 errors`.
fn count(n: usize, singular: &str, plural: &str) -> String {
	if n == 1 {
		format!("{n} {singular}")
	} else {
		format!("{n} {plural}")
	}
}

/// `path` as a `/`-separated path relative to `base`, with `..` where `path` is not under it.
/// A path that shares no root with `base` is shown whole.
fn display_path(base: &Path, path: &Path) -> String {
	let parts = |p: &Path| -> Vec<String> {
		p.components()
			.filter_map(|c| match c {
				Component::CurDir => None,
				Component::Normal(part) => Some(part.to_string_lossy().into_owned()),
				other => Some(other.as_os_str().to_string_lossy().into_owned()),
			})
			.collect()
	};
	let (base, path_parts) = (parts(base), parts(path));
	let shared = base.iter().zip(&path_parts).take_while(|(a, b)| a == b).count();
	if shared == 0 && path.is_absolute() {
		return path_parts.join("/").replacen("//", "/", 1);
	}
	let mut out: Vec<&str> = vec![".."; base.len() - shared];
	out.extend(path_parts[shared..].iter().map(String::as_str));
	out.join("/")
}

/// How many columns `text` takes: a tab is four.
fn width(text: &str) -> usize {
	text.chars()
		.map(|c| {
			if c == '\t' {
				4
			} else {
				1
			}
		})
		.sum()
}

fn paint(text: &str, style: &str, color: bool) -> String {
	if color {
		format!("{style}{text}{RESET}")
	} else {
		text.to_string()
	}
}

fn severity_style(severity: Severity) -> &'static str {
	match severity {
		Severity::Error => RED,
		Severity::Warning => YELLOW,
		Severity::Hint => CYAN,
	}
}

/// The style of a finding's label and carets: its severity's, and dim for a suppressed one.
fn finding_style(finding: &Finding) -> &'static str {
	if finding.suppressed.is_some() {
		DIM
	} else {
		severity_style(finding.severity)
	}
}

fn index_of<'a>(report: &'a Report, finding: &Finding) -> LineIndex<'a> {
	LineIndex::new(report.texts.get(&finding.path).map_or("", String::as_str))
}

/// The findings for a person, one block each in the style of rustc, then the summary:
///
/// ```text
/// error[K9003]: <message>
///  --> database/schema/002_bad.surql:3:1
///   |
/// 3 | DEFINE FIELD x ON nope TYPE string
///   | ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
///   = note: <note>
///   = help: <help>
/// ```
///
/// A span over several lines shows its first line, with carets to its end. ANSI colour only
/// when `color`.
///
/// A finding a directive silenced, which the report holds only when the check was asked to show
/// them, reads `suppressed[CODE]` in place of its severity and ends with
/// `= note: suppressed by the directive at <file>:<line>`. It is in none of the counts.
pub fn human(report: &Report, base: &Path, color: bool) -> String {
	let mut blocks = Vec::new();
	for finding in &report.findings {
		blocks.push(human_block(report, finding, base, color));
	}
	let mut out = blocks.join("\n");
	if !blocks.is_empty() {
		out.push('\n');
	}
	let s = &report.summary;
	out.push_str(&format!(
		"checked {} and {}: {}, {}",
		count(s.schema_files, "schema file", "schema files"),
		count(s.query_files, "query file", "query files"),
		count(s.errors, "error", "errors"),
		count(s.warnings, "warning", "warnings"),
	));
	if s.hints > 0 {
		out.push_str(&format!(", {}", count(s.hints, "hint", "hints")));
	}
	if s.suppressed > 0 {
		out.push_str(&format!(", {} suppressed", s.suppressed));
	}
	out.push('\n');
	if !s.queries_analyzed && s.query_files > 0 {
		let were = if s.query_files == 1 {
			"was"
		} else {
			"were"
		};
		out.push_str(&format!(
			"note: {} {were} found but not analyzed; query analysis needs SurrealDB 4.1\n",
			count(s.query_files, "query file", "query files"),
		));
	}
	if s.host_read {
		out.push_str(&format!(
			"read {} holding {}\n",
			count(s.host_files, "host file", "host files"),
			count(s.host_queries, "query", "queries"),
		));
	}
	if s.targets_checked > 0 {
		let targets: Vec<String> = s
			.drift
			.iter()
			.map(|(name, n)| match n {
				0 => format!("{name} (no drift)"),
				n => format!("{name} ({})", count(*n, "finding", "findings")),
			})
			.collect();
		out.push_str(&format!(
			"compared the files with {}: {}\n",
			count(s.targets_checked, "target", "targets"),
			targets.join(", ")
		));
	}
	out
}

fn human_block(report: &Report, finding: &Finding, base: &Path, color: bool) -> String {
	let index = index_of(report, finding);
	let (line, column) = index.position(finding.span.start);
	let style = finding_style(finding);
	let label = finding.suppressed.as_ref().map_or(finding.severity.as_str(), |_| "suppressed");
	let gutter = line.to_string().len();
	let pad = " ".repeat(gutter);
	let bar = paint("|", BLUE, color);

	let mut out = String::new();
	out.push_str(&paint(&format!("{label}[{}]", finding.code), style, color));
	out.push_str(&format!(": {}\n", finding.message));
	out.push_str(&format!(
		"{pad}{} {}:{line}:{column}\n",
		paint("-->", BLUE, color),
		display_path(base, &finding.path)
	));
	out.push_str(&format!("{pad} {bar}\n"));
	if report.texts.contains_key(&finding.path) {
		let text = index.line_text(line);
		let from = column - 1;
		let (end_line, end_column) = index.position(finding.span.end);
		let marked: String = if end_line == line {
			text.chars().skip(from).take(end_column.saturating_sub(column)).collect()
		} else {
			text.chars().skip(from).collect()
		};
		let before: String = text.chars().take(from).collect();
		let carets = "^".repeat(width(&marked).max(1));
		out.push_str(&format!(
			"{} {bar} {}\n",
			paint(&format!("{line:>gutter$}"), BLUE, color),
			text.replace('\t', "    ")
		));
		out.push_str(&format!(
			"{pad} {bar} {}{}\n",
			" ".repeat(width(&before)),
			paint(&carets, style, color)
		));
	}
	for note in &finding.notes {
		let word = match note.kind {
			NoteKind::Note => "note",
			NoteKind::Help => "help",
		};
		let at = note.span.as_ref().map_or(String::new(), |span| {
			let (line, column) = index.position(span.start);
			format!(" (at {line}:{column})")
		});
		out.push_str(&format!("{pad} {} {word}: {}{at}\n", paint("=", BLUE, color), note.message));
	}
	if let Some(suppressed) = &finding.suppressed {
		out.push_str(&format!(
			"{pad} {} note: suppressed by the directive at {}:{}\n",
			paint("=", BLUE, color),
			display_path(base, &finding.path),
			index.position(suppressed.directive.start).0
		));
	}
	out
}

#[derive(Debug, Serialize)]
pub struct JsonSpan {
	pub start: usize,
	pub end: usize,
	pub line: usize,
	pub column: usize,
	pub end_line: usize,
	pub end_column: usize,
}

fn span_of(index: &LineIndex<'_>, span: &Range<usize>) -> JsonSpan {
	let (line, column) = index.position(span.start);
	let (end_line, end_column) = index.position(span.end);
	JsonSpan {
		start: span.start,
		end: span.end,
		line,
		column,
		end_line,
		end_column,
	}
}

#[derive(Debug, Serialize)]
pub struct JsonNote {
	pub kind: NoteKind,
	pub message: String,
	pub span: Option<JsonSpan>,
	pub replacement: Option<String>,
}

/// What silenced a diagnostic: the directive comment, in the diagnostic's own file.
#[derive(Debug, Serialize)]
pub struct JsonSuppressed {
	pub directive: JsonSpan,
}

#[derive(Debug, Serialize)]
pub struct JsonFinding {
	pub code: Code,
	pub severity: Severity,
	pub message: String,
	pub path: String,
	pub span: JsonSpan,
	pub statement: Option<u32>,
	pub notes: Vec<JsonNote>,
	pub tags: Vec<Tag>,
	/// Which checker reported it: `kit` or `engine`.
	pub source: Source,
	/// The directive that silenced it. Present only on a finding the check was asked to show
	/// though a directive silenced it, and absent from the document otherwise.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub suppressed: Option<JsonSuppressed>,
}

#[derive(Debug, Serialize)]
pub struct JsonSummary {
	pub passed: bool,
	pub schema_files: usize,
	pub query_files: usize,
	pub queries_analyzed: bool,
	pub errors: usize,
	pub warnings: usize,
	pub hints: usize,
	pub suppressed: usize,
	pub allowed: usize,
	pub host_read: bool,
	pub host_files: usize,
	pub host_queries: usize,
	pub targets_checked: usize,
	pub drift: BTreeMap<String, usize>,
}

/// The report as JSON: `{"summary": {...}, "diagnostics": [...]}`, in the field order below.
/// A diagnostic is the engine contract's finding, plus the file's `path`. Serialise it with
/// `serde_json`; it is a struct, not a `Value`, because a `Value` would sort its keys.
#[derive(Debug, Serialize)]
pub struct JsonReport {
	pub summary: JsonSummary,
	pub diagnostics: Vec<JsonFinding>,
}

/// One finding of `report` as a diagnostic of [`json`]'s document, its path shown relative to
/// `base`. Lines and columns are read in the finding's file as `report` holds it.
pub fn json_finding(report: &Report, finding: &Finding, base: &Path) -> JsonFinding {
	let index = index_of(report, finding);
	JsonFinding {
		code: finding.code,
		severity: finding.severity,
		message: finding.message.clone(),
		path: display_path(base, &finding.path),
		span: span_of(&index, &finding.span),
		statement: finding.statement,
		notes: finding.notes.iter().map(|note| json_note(&index, note)).collect(),
		tags: finding.tags.clone(),
		source: finding.source,
		suppressed: finding.suppressed.as_ref().map(|suppressed| JsonSuppressed {
			directive: span_of(&index, &suppressed.directive),
		}),
	}
}

pub fn json(report: &Report, base: &Path) -> JsonReport {
	let s = &report.summary;
	let diagnostics =
		report.findings.iter().map(|finding| json_finding(report, finding, base)).collect();
	JsonReport {
		summary: JsonSummary {
			passed: report.passed(),
			schema_files: s.schema_files,
			query_files: s.query_files,
			queries_analyzed: s.queries_analyzed,
			errors: s.errors,
			warnings: s.warnings,
			hints: s.hints,
			suppressed: s.suppressed,
			allowed: s.allowed,
			host_read: s.host_read,
			host_files: s.host_files,
			host_queries: s.host_queries,
			targets_checked: s.targets_checked,
			drift: s.drift.clone(),
		},
		diagnostics,
	}
}

fn json_note(index: &LineIndex<'_>, note: &Note) -> JsonNote {
	JsonNote {
		kind: note.kind,
		message: note.message.clone(),
		span: note.span.as_ref().map(|span| span_of(index, span)),
		replacement: note.replacement.clone(),
	}
}

#[derive(Debug, Serialize)]
struct SarifText {
	text: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SarifRule {
	id: String,
	short_description: SarifText,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SarifDriver {
	name: &'static str,
	information_uri: &'static str,
	version: &'static str,
	rules: Vec<SarifRule>,
}

#[derive(Debug, Serialize)]
struct SarifTool {
	driver: SarifDriver,
}

#[derive(Debug, Serialize)]
struct SarifArtifact {
	uri: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SarifRegion {
	start_line: usize,
	start_column: usize,
	end_line: usize,
	end_column: usize,
	byte_offset: usize,
	byte_length: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SarifPhysical {
	artifact_location: SarifArtifact,
	region: SarifRegion,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SarifLocation {
	physical_location: SarifPhysical,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SarifRelated {
	id: usize,
	physical_location: SarifPhysical,
	message: SarifText,
}

/// SARIF's `suppression` object. A directive in the source is `inSource`.
#[derive(Debug, Serialize)]
struct SarifSuppression {
	kind: &'static str,
}

/// The result's property bag. SurrealKit sets only `source`.
#[derive(Debug, Serialize)]
struct SarifProperties {
	source: Source,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SarifResult {
	rule_id: String,
	rule_index: usize,
	level: &'static str,
	message: SarifText,
	locations: Vec<SarifLocation>,
	#[serde(skip_serializing_if = "Vec::is_empty")]
	related_locations: Vec<SarifRelated>,
	#[serde(skip_serializing_if = "Vec::is_empty")]
	suppressions: Vec<SarifSuppression>,
	properties: SarifProperties,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SarifRun {
	tool: SarifTool,
	column_kind: &'static str,
	results: Vec<SarifResult>,
}

/// The report as SARIF 2.1.0, for code scanning. Serialise it with `serde_json`.
#[derive(Debug, Serialize)]
pub struct SarifLog {
	#[serde(rename = "$schema")]
	schema: &'static str,
	version: &'static str,
	runs: Vec<SarifRun>,
}

/// `path` as a URI reference: `/` kept, everything outside the unreserved set percent-encoded.
fn uri(path: &str) -> String {
	let mut out = String::with_capacity(path.len());
	for byte in path.bytes() {
		if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~' | b'/') {
			out.push(char::from(byte));
		} else {
			out.push_str(&format!("%{byte:02X}"));
		}
	}
	out
}

pub fn sarif(report: &Report, base: &Path) -> SarifLog {
	let codes: BTreeSet<Code> = report.findings.iter().map(|f| f.code).collect();
	let codes: Vec<Code> = codes.into_iter().collect();
	let rules = codes
		.iter()
		.map(|code| SarifRule {
			id: code.to_string(),
			short_description: SarifText {
				text: describe(*code).map_or_else(|| code.to_string(), str::to_string),
			},
		})
		.collect();

	let results = report
		.findings
		.iter()
		.map(|finding| {
			let index = index_of(report, finding);
			let artifact = uri(&display_path(base, &finding.path));
			let physical = |span: &Range<usize>| {
				let (line, column) = index.position(span.start);
				let (end_line, end_column) = index.position(span.end);
				SarifPhysical {
					artifact_location: SarifArtifact {
						uri: artifact.clone(),
					},
					region: SarifRegion {
						start_line: line,
						start_column: column,
						end_line,
						end_column,
						byte_offset: span.start,
						byte_length: span.end - span.start,
					},
				}
			};
			let mut text = finding.message.clone();
			let mut related = Vec::new();
			for note in &finding.notes {
				let word = match note.kind {
					NoteKind::Note => "note",
					NoteKind::Help => "help",
				};
				text.push_str(&format!("\n{word}: {}", note.message));
				if let Some(span) = &note.span {
					related.push(SarifRelated {
						id: related.len(),
						physical_location: physical(span),
						message: SarifText {
							text: note.message.clone(),
						},
					});
				}
			}
			SarifResult {
				rule_id: finding.code.to_string(),
				rule_index: codes.iter().position(|code| *code == finding.code).unwrap_or(0),
				level: match finding.severity {
					Severity::Error => "error",
					Severity::Warning => "warning",
					Severity::Hint => "note",
				},
				message: SarifText {
					text,
				},
				locations: vec![SarifLocation {
					physical_location: physical(&finding.span),
				}],
				related_locations: related,
				suppressions: finding
					.suppressed
					.iter()
					.map(|_| SarifSuppression {
						kind: "inSource",
					})
					.collect(),
				properties: SarifProperties {
					source: finding.source,
				},
			}
		})
		.collect();

	SarifLog {
		schema: "https://json.schemastore.org/sarif-2.1.0.json",
		version: "2.1.0",
		runs: vec![SarifRun {
			tool: SarifTool {
				driver: SarifDriver {
					name: "surreal kit check",
					information_uri: "https://github.com/surrealdb/surrealkit",
					version: env!("CARGO_PKG_VERSION"),
					rules,
				},
			},
			column_kind: "unicodeCodePoints",
			results,
		}],
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;
	use std::path::PathBuf;

	use super::*;
	use crate::check::Summary;
	use crate::check::finding::{K9001, K9003, L7013};
	use crate::check::policy::Level;

	const SCHEMA: &str = "DEFINE TABLE user SCHEMAFULL;\n\
		DEFINE FIELD prénom ON user TYPE string COMMENT 'né à 日本';\n\
		DEFINE FIELD age ON user\n\
		\tTYPE nonsense\n\
		\tDEFAULT 0;\n\
		-- surreal: allow(\n";
	const QUERY: &str = "SELECT * FROM user;\n\t\tSELECT nickname FROM user;\n";

	fn schema_path() -> PathBuf {
		PathBuf::from("/proj/database/schema/001 user.surql")
	}

	fn query_path() -> PathBuf {
		PathBuf::from("/proj/queries/list.surql")
	}

	/// A report with every severity, a multibyte line, a multi-line span, a tab-indented line,
	/// notes with and without a span, and help.
	fn report() -> Report {
		let at = |text: &str, needle: &str| {
			let start = text.find(needle).expect("needle");
			start..start + needle.len()
		};
		let mut error = Finding::kit(
			K9003,
			"Parse error: unexpected token `nonsense`",
			schema_path(),
			at(SCHEMA, "DEFINE FIELD age ON user\n\tTYPE nonsense\n\tDEFAULT 0"),
		)
		.in_statement(2);
		error.notes.push(Note::context("expected a kind such as `string` or `int`"));
		error.notes.push(Note {
			kind: NoteKind::Note,
			message: "the table is defined here".to_string(),
			span: Some(at(SCHEMA, "DEFINE TABLE user SCHEMAFULL")),
			replacement: None,
		});
		error.notes.push(Note {
			kind: NoteKind::Help,
			message: "write `TYPE string`".to_string(),
			span: None,
			replacement: Some("TYPE string".to_string()),
		});

		let mut warning = Finding::new(
			L7013,
			Severity::Warning,
			Level::Warn,
			"malformed suppression directive: the `)` after the code is missing",
			schema_path(),
			at(SCHEMA, "-- surreal: allow("),
		);
		warning.severity = Severity::Warning;

		let mut hint = Finding::new(
			Code::from_number(7005),
			Severity::Hint,
			Level::Warn,
			"`prénom` is never read",
			schema_path(),
			at(SCHEMA, "prénom"),
		)
		.in_statement(1);
		hint.tags.push(Tag::Unnecessary);

		let mut tabbed = Finding::kit(
			Code::from_number(1002),
			"no field `nickname` on `user`",
			query_path(),
			at(QUERY, "nickname"),
		)
		.in_statement(1);
		tabbed.severity = Severity::Error;

		let empty = Finding::kit(K9001, "a finding with an empty span", query_path(), 0..0);

		Report {
			root: "/proj".into(),
			findings: vec![hint, error, warning, empty, tabbed],
			texts: BTreeMap::from([
				(schema_path(), SCHEMA.to_string()),
				(query_path(), QUERY.to_string()),
			]),
			summary: Summary {
				schema_files: 1,
				query_files: 2,
				queries_analyzed: false,
				errors: 3,
				warnings: 1,
				hints: 1,
				suppressed: 2,
				allowed: 1,
				..Summary::default()
			},
		}
	}

	fn pretty(value: &impl Serialize) -> String {
		serde_json::to_string_pretty(value).expect("serialises")
	}

	#[test]
	fn human_without_colour() {
		insta::assert_snapshot!("human_plain", human(&report(), Path::new("/proj"), false));
	}

	#[test]
	fn human_with_colour() {
		insta::assert_snapshot!(
			"human_colour",
			human(&report(), Path::new("/proj"), true).replace('\x1b', "<ESC>")
		);
	}

	#[test]
	fn json_report() {
		insta::assert_snapshot!("json", pretty(&json(&report(), Path::new("/proj"))));
	}

	#[test]
	fn sarif_report() {
		let text = pretty(&sarif(&report(), Path::new("/proj")))
			.replace(env!("CARGO_PKG_VERSION"), "<version>");
		insta::assert_snapshot!("sarif", text);
	}

	#[test]
	fn human_from_a_subdirectory_uses_dotdot() {
		let text = human(&report(), Path::new("/proj/queries"), false);
		assert!(text.contains("--> ../database/schema/001 user.surql:3:1"), "{text}");
		assert!(text.contains("--> list.surql:2:10"), "{text}");
	}

	/// A report with a warning and, after it, a finding the directive on the line above it
	/// silenced.
	fn report_with_a_suppressed_finding() -> Report {
		let text = "-- surreal: allow(K9003) reason=\"x\"\nBAD;\n\tBAD;\n";
		let directive = 0..text.find('\n').expect("newline");
		let (bad, tab_bad) = (text.find("BAD").expect("first"), text.rfind("BAD").expect("last"));
		let silenced =
			Finding::kit(K9003, "silenced", schema_path(), bad..bad + 3).with_suppressed(directive);
		let mut reported = Finding::kit(K9003, "reported", schema_path(), tab_bad..tab_bad + 3);
		reported.severity = Severity::Warning;
		Report {
			root: "/proj".into(),
			findings: vec![silenced, reported],
			texts: BTreeMap::from([(schema_path(), text.to_string())]),
			summary: Summary {
				schema_files: 1,
				warnings: 1,
				suppressed: 1,
				..Summary::default()
			},
		}
	}

	#[test]
	fn a_suppressed_finding_says_which_directive_silenced_it() {
		let report = report_with_a_suppressed_finding();
		let base = Path::new("/proj");

		let plain = human(&report, base, false);
		assert!(
			plain.starts_with(
				"suppressed[K9003]: silenced\n --> database/schema/001 user.surql:2:1\n"
			),
			"{plain}"
		);
		assert!(
			plain.contains(
				"  = note: suppressed by the directive at database/schema/001 user.surql:1\n"
			),
			"{plain}"
		);
		assert!(plain.contains("warning[K9003]: reported"), "{plain}");
		assert!(!plain.contains("error["), "{plain}");
		assert!(plain.ends_with("0 errors, 1 warning, 1 suppressed\n"), "{plain}");
		assert!(human(&report, base, true).contains("\x1b[2msuppressed[K9003]\x1b[0m"));

		let json = serde_json::to_value(json(&report, base)).expect("json");
		let [silenced, reported] = json["diagnostics"].as_array().expect("array").as_slice() else {
			panic!("two diagnostics: {json}");
		};
		assert_eq!(
			silenced["suppressed"],
			serde_json::json!({"directive": {
				"start": 0, "end": 35, "line": 1, "column": 1, "end_line": 1, "end_column": 36
			}})
		);
		assert_eq!(silenced["severity"], "error");
		assert!(reported.get("suppressed").is_none(), "absent, not null");

		let sarif = serde_json::to_value(sarif(&report, base)).expect("sarif");
		let [silenced, reported] =
			sarif["runs"][0]["results"].as_array().expect("array").as_slice()
		else {
			panic!("two results: {sarif}");
		};
		assert_eq!(silenced["suppressions"], serde_json::json!([{"kind": "inSource"}]));
		assert_eq!(silenced["level"], "error", "it keeps its level");
		assert!(reported.get("suppressions").is_none());
	}

	#[test]
	fn a_clean_report_is_one_line() {
		let report = Report {
			root: "/proj".into(),
			findings: Vec::new(),
			texts: BTreeMap::new(),
			summary: Summary {
				schema_files: 1,
				query_files: 0,
				queries_analyzed: true,
				..Summary::default()
			},
		};
		assert_eq!(
			human(&report, Path::new("/proj"), false),
			"checked 1 schema file and 0 query files: 0 errors, 0 warnings\n"
		);
	}

	#[test]
	fn the_unanalysed_note_agrees_in_number() {
		let mut report = Report {
			root: "/proj".into(),
			findings: Vec::new(),
			texts: BTreeMap::new(),
			summary: Summary {
				query_files: 1,
				..Summary::default()
			},
		};
		let text = human(&report, Path::new("/proj"), false);
		assert!(
			text.ends_with(
				"note: 1 query file was found but not analyzed; query analysis needs SurrealDB 4.1\n"
			),
			"{text}"
		);
		report.summary.query_files = 3;
		assert!(human(&report, Path::new("/proj"), false).contains("3 query files were found"));
	}

	#[test]
	fn the_summary_says_what_host_code_and_targets_added() {
		let mut report = Report {
			root: "/proj".into(),
			findings: Vec::new(),
			texts: BTreeMap::new(),
			summary: Summary::default(),
		};
		let plain = human(&report, Path::new("/proj"), false);
		assert_eq!(plain, "checked 0 schema files and 0 query files: 0 errors, 0 warnings\n");
		report.summary.host_read = true;
		report.summary.host_files = 2;
		report.summary.host_queries = 1;
		report.summary.targets_checked = 2;
		report.summary.drift = BTreeMap::from([("prod".to_string(), 3), ("stage".to_string(), 0)]);
		let text = human(&report, Path::new("/proj"), false);
		assert!(text.contains("read 2 host files holding 1 query\n"), "{text}");
		assert!(
			text.contains(
				"compared the files with 2 targets: prod (3 findings), stage (no drift)\n"
			),
			"{text}"
		);
		let value = serde_json::to_value(json(&report, Path::new("/proj"))).expect("json");
		let summary = &value["summary"];
		assert_eq!(summary["host_files"], 2);
		assert_eq!(summary["host_queries"], 1);
		assert_eq!(summary["targets_checked"], 2);
		assert_eq!(summary["drift"]["prod"], 3);
	}

	#[test]
	fn uris_encode_what_a_uri_cannot_hold() {
		assert_eq!(uri("a b/ü#.surql"), "a%20b/%C3%BC%23.surql");
		assert_eq!(uri("../x-y_z~.surql"), "../x-y_z~.surql");
	}

	#[test]
	fn paths_are_relative_with_forward_slashes() {
		assert_eq!(display_path(Path::new("/a/b"), Path::new("/a/b/c/d.surql")), "c/d.surql");
		assert_eq!(display_path(Path::new("/a/b"), Path::new("/a/x/d.surql")), "../x/d.surql");
		assert_eq!(display_path(Path::new("/a/b"), Path::new("/z/d.surql")), "../../z/d.surql");
	}
}
