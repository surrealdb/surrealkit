//! Query extraction over the Python grammar.
//!
//! A query is a call `receiver.method(…)` whose method is one of the configured names and whose
//! first positional argument, or `query=` keyword, is a single string literal: plain or
//! triple-quoted, with an optional `r`, `R`, `u` or `U` prefix. Its text is the literal's runtime
//! value, with the escapes decoded and a line break read as `\n`, as Python reads the file.
//!
//! A call whose argument is not such a literal is counted under the reason it was skipped
//! ([`Skip`]), not reported as a diagnostic.

use std::collections::BTreeMap;
use std::ops::Range;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail};
use tree_sitter::{Node, Parser};
use walkdir::WalkDir;

use super::{FoundQuery, HostQuery, HostQuerySource, Segment};

/// Directories [`scan`] never descends into, besides hidden ones.
pub const SKIPPED_DIRECTORIES: &[&str] =
	&[".venv", "venv", "__pycache__", "site-packages", "node_modules"];

/// Why a call that looks like a query was not read as one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Skip {
	/// An f-string: its substitutions have no value at generation time.
	FString,
	/// A `bytes` literal.
	Bytes,
	/// Adjacent literals or a `+`: the text is built, not written.
	Concatenation,
	/// A name, a call, a template string or any other expression.
	NotALiteral,
	/// A literal with an escape this reader does not decode (`\N{…}`, a lone surrogate).
	UnsupportedEscape,
}

impl Skip {
	/// The reason as the summary words it.
	pub fn label(self) -> &'static str {
		match self {
			Skip::FString => "f-string",
			Skip::Bytes => "bytes",
			Skip::Concatenation => "concatenation",
			Skip::NotALiteral => "not a literal",
			Skip::UnsupportedEscape => "unsupported escape",
		}
	}
}

/// How many calls were skipped, by reason.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SkipCounts(BTreeMap<Skip, usize>);

impl SkipCounts {
	pub fn add(&mut self, skip: Skip) {
		*self.0.entry(skip).or_default() += 1;
	}

	/// The count of `skip`.
	pub fn get(&self, skip: Skip) -> usize {
		self.0.get(&skip).copied().unwrap_or(0)
	}

	/// The count of every reason.
	pub fn total(&self) -> usize {
		self.0.values().sum()
	}

	/// `f-string 2, concatenation 1`: each reason with a count, in the order of [`Skip`].
	pub fn describe(&self) -> String {
		self.0
			.iter()
			.map(|(skip, count)| format!("{} {count}", skip.label()))
			.collect::<Vec<_>>()
			.join(", ")
	}

	fn merge(&mut self, other: &SkipCounts) {
		for (skip, count) in &other.0 {
			*self.0.entry(*skip).or_default() += count;
		}
	}
}

/// What one Python source holds.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Extracted {
	/// The queries, in document order.
	pub queries: Vec<HostQuery>,
	/// The calls that carry no readable query.
	pub skipped: SkipCounts,
}

/// What [`scan`] found in a project.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PythonScan {
	/// The queries, sorted by file, then position.
	pub found: Vec<FoundQuery>,
	/// The number of files that hold at least one query.
	pub files: usize,
	pub skipped: SkipCounts,
}

impl PythonScan {
	/// The line the compile summary shows: `python: 12 queries in 4 files; skipped 3 (f-string 2,
	/// concatenation 1)`.
	pub fn summary(&self) -> String {
		let plural = |count: usize, word: &str| {
			if count == 1 {
				format!("{count} {word}")
			} else {
				format!("{count} {word}s")
			}
		};
		let mut line = format!(
			"python: {} in {}",
			plural(self.found.len(), "query").replace("querys", "queries"),
			plural(self.files, "file")
		);
		if self.skipped.total() > 0 {
			line.push_str(&format!(
				"; skipped {} ({})",
				self.skipped.total(),
				self.skipped.describe()
			));
		}
		line
	}
}

/// Extracts the queries passed to the method calls named by `methods` from one Python source.
pub fn extract(source: &str, methods: &[&str]) -> Extracted {
	let mut extracted = Extracted::default();
	let mut parser = Parser::new();
	if parser.set_language(&tree_sitter_python::LANGUAGE.into()).is_err() {
		return extracted;
	}
	let Some(tree) = parser.parse(source, None) else {
		return extracted;
	};
	let mut cursor = tree.walk();
	loop {
		let node = cursor.node();
		if node.kind() == "call" {
			match query_of(node, source, methods) {
				Some(Ok(query)) => extracted.queries.push(query),
				Some(Err(skip)) => extracted.skipped.add(skip),
				None => {}
			}
		}
		if cursor.goto_first_child() {
			continue;
		}
		while !cursor.goto_next_sibling() {
			if !cursor.goto_parent() {
				return extracted;
			}
		}
	}
}

/// What a call carries: nothing when it is not `receiver.method(…)` with a configured method or
/// has no query argument, the query, or the reason its argument is not one.
fn query_of(
	call: Node<'_>,
	source: &str,
	methods: &[&str],
) -> Option<std::result::Result<HostQuery, Skip>> {
	let function = call.child_by_field_name("function")?;
	if function.kind() != "attribute" {
		return None;
	}
	let method = text(function.child_by_field_name("attribute")?, source);
	if !methods.contains(&method) {
		return None;
	}
	let arguments = call.child_by_field_name("arguments")?;
	if arguments.kind() != "argument_list" {
		return None;
	}
	let mut positional = None;
	let mut keyword = None;
	let mut walker = arguments.walk();
	for argument in arguments.named_children(&mut walker) {
		match argument.kind() {
			"comment" => {}
			"keyword_argument" => {
				let named = argument.child_by_field_name("name").map(|name| text(name, source));
				if named == Some("query") && keyword.is_none() {
					keyword = argument.child_by_field_name("value");
				}
			}
			_ => {
				positional.get_or_insert(argument);
			}
		}
	}
	let argument = positional.or(keyword)?;
	Some(literal(argument, source, method))
}

fn text<'s>(node: Node<'_>, source: &'s str) -> &'s str {
	&source[node.start_byte()..node.end_byte()]
}

/// The query `node` is, or why it is not one.
fn literal(node: Node<'_>, source: &str, method: &str) -> std::result::Result<HostQuery, Skip> {
	match node.kind() {
		"string" => string_query(node, source, method),
		"concatenated_string" => Err(Skip::Concatenation),
		"binary_operator" => {
			let plus = node
				.child_by_field_name("operator")
				.is_some_and(|operator| text(operator, source) == "+");
			let literal_side = ["left", "right"].iter().any(|side| {
				node.child_by_field_name(side).is_some_and(|operand| is_stringy(operand))
			});
			Err(if plus && literal_side {
				Skip::Concatenation
			} else {
				Skip::NotALiteral
			})
		}
		"parenthesized_expression" => {
			let mut walker = node.walk();
			let mut parts = node.named_children(&mut walker).filter(|n| n.kind() != "comment");
			match (parts.next(), parts.next()) {
				(Some(inner), None) => literal(inner, source, method),
				_ => Err(Skip::NotALiteral),
			}
		}
		_ => Err(Skip::NotALiteral),
	}
}

/// Whether `node` is a string or builds one from strings.
fn is_stringy(node: Node<'_>) -> bool {
	matches!(node.kind(), "string" | "concatenated_string")
}

fn string_query(
	node: Node<'_>,
	source: &str,
	method: &str,
) -> std::result::Result<HostQuery, Skip> {
	let mut walker = node.walk();
	let children: Vec<Node<'_>> = node.children(&mut walker).collect();
	let start = children.iter().find(|child| child.kind() == "string_start");
	let end = children.iter().rev().find(|child| child.kind() == "string_end");
	let (Some(start), Some(end)) = (start, end) else {
		return Err(Skip::NotALiteral);
	};
	let opening = text(*start, source);
	let quote_at = opening.find(['"', '\'']).ok_or(Skip::NotALiteral)?;
	let prefix = opening[..quote_at].to_ascii_lowercase();
	if prefix.contains('f') {
		return Err(Skip::FString);
	}
	if prefix.contains('b') {
		return Err(Skip::Bytes);
	}
	if prefix.chars().any(|c| !matches!(c, 'r' | 'u')) {
		return Err(Skip::NotALiteral);
	}
	let host_range = start.end_byte()..end.start_byte();
	let mut decoded = String::new();
	let mut segments = Vec::new();
	decode(source, host_range.clone(), prefix.contains('r'), &mut decoded, &mut segments)
		.ok_or(Skip::UnsupportedEscape)?;
	Ok(HostQuery::new(
		source,
		decoded,
		host_range,
		HostQuerySource::Method(method.to_string()),
		Vec::new(),
		segments,
	))
}

/// Appends the runtime value of `source[range]` to `text` and its span map to `segments`.
///
/// Copied runs map byte for byte; an escape sequence or a normalised line break is one segment
/// covering the whole host sequence. `None` when the range holds an escape this reader does not
/// decode.
fn decode(
	source: &str,
	range: Range<usize>,
	raw: bool,
	text: &mut String,
	segments: &mut Vec<Segment>,
) -> Option<()> {
	let mut at = range.start;
	while at < range.end {
		let rest = &source[at..range.end];
		let run = rest.find(|c| (c == '\\' && !raw) || c == '\r').unwrap_or(rest.len());
		if run > 0 {
			segments.push(Segment {
				text_start: text.len(),
				text_len: run,
				host_start: at,
				host_len: run,
			});
			text.push_str(&rest[..run]);
			at += run;
			continue;
		}
		let (decoded, consumed) = if rest.starts_with('\r') {
			// Python reads a file with universal newlines: CRLF and CR are LF.
			(
				Some('\n'),
				if rest.starts_with("\r\n") {
					2
				} else {
					1
				},
			)
		} else {
			escape(rest)?
		};
		if let Some(decoded) = decoded {
			segments.push(Segment {
				text_start: text.len(),
				text_len: decoded.len_utf8(),
				host_start: at,
				host_len: consumed,
			});
			text.push(decoded);
		}
		at += consumed;
	}
	Some(())
}

/// Decodes the escape sequence at the start of `rest` (which begins with a backslash).
///
/// Returns the decoded char, `None` inside the `Some` for a line continuation, and the number of
/// host bytes consumed. A backslash before a char Python does not escape is itself, and consumes
/// only the backslash.
fn escape(rest: &str) -> Option<(Option<char>, usize)> {
	let c = rest[1..].chars().next()?;
	let after = 1 + c.len_utf8();
	let tail = &rest[after..];
	let one = |decoded: char| Some((Some(decoded), after));
	match c {
		'n' => one('\n'),
		't' => one('\t'),
		'r' => one('\r'),
		'a' => one('\u{7}'),
		'b' => one('\u{8}'),
		'f' => one('\u{c}'),
		'v' => one('\u{b}'),
		'\\' | '\'' | '"' => one(c),
		'\n' => Some((None, after)),
		'\r' => Some((None, after + usize::from(tail.starts_with('\n')))),
		'0'..='7' => {
			let more = tail.bytes().take(2).take_while(|byte| (b'0'..=b'7').contains(byte)).count();
			let value = u32::from_str_radix(&rest[1..after + more], 8).ok()?;
			Some((Some(char::from_u32(value)?), after + more))
		}
		'x' => Some((Some(char::from_u32(hex(tail.get(..2)?)?)?), after + 2)),
		'u' => Some((Some(char::from_u32(hex(tail.get(..4)?)?)?), after + 4)),
		'U' => Some((Some(char::from_u32(hex(tail.get(..8)?)?)?), after + 8)),
		// `\N{name}` needs the Unicode name table.
		'N' => None,
		_ => Some((Some('\\'), 1)),
	}
}

/// Parses `digits` as hexadecimal, rejecting signs and the empty string.
fn hex(digits: &str) -> Option<u32> {
	if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_hexdigit()) {
		return None;
	}
	u32::from_str_radix(digits, 16).ok()
}

/// Whether the walk reads files from the directory at `entry`: not hidden, not a
/// [`SKIPPED_DIRECTORIES`] name.
fn is_skipped_directory(entry: &walkdir::DirEntry) -> bool {
	if entry.depth() == 0 || !entry.file_type().is_dir() {
		return false;
	}
	let name = entry.file_name().to_string_lossy();
	name.starts_with('.') || SKIPPED_DIRECTORIES.contains(&name.as_ref())
}

/// `path` without `.` components, to compare two spellings of one file.
fn lexical(path: &Path) -> PathBuf {
	path.components().filter(|component| !matches!(component, Component::CurDir)).collect()
}

/// Extracts the queries passed to the method calls named by `methods` from every `.py` file under
/// `root` whose path, relative to `root`, matches one of `globs`.
///
/// `.pyi` files, symlinks, hidden directories and [`SKIPPED_DIRECTORIES`] are not read, and
/// neither is a file in `exclude` (the generated module). A file that is not UTF-8 is skipped with
/// a warning. Results are sorted by file, then position.
///
/// # Errors
///
/// Fails when `root` does not exist, a glob is not one, or a source file cannot be read.
pub fn scan(
	root: &Path,
	globs: &[String],
	methods: &[&str],
	exclude: &[PathBuf],
) -> Result<PythonScan> {
	if !root.exists() {
		bail!("path does not exist: {}", root.display());
	}
	let patterns = globs
		.iter()
		.map(|glob| {
			glob::Pattern::new(glob).with_context(|| format!("{glob:?} is not a glob pattern"))
		})
		.collect::<Result<Vec<_>>>()?;
	let excluded: Vec<PathBuf> = exclude.iter().map(|path| lexical(path)).collect();
	let walker = WalkDir::new(root)
		.follow_links(false)
		.sort_by_file_name()
		.into_iter()
		.filter_entry(|entry| !is_skipped_directory(entry));
	let mut scan = PythonScan::default();
	for entry in walker {
		let entry = match entry {
			Ok(entry) => entry,
			Err(error) => {
				log::warn!("skipping unreadable path under {}: {error}", root.display());
				continue;
			}
		};
		let path = entry.path();
		if !entry.file_type().is_file() || path.extension().is_none_or(|ext| ext != "py") {
			continue;
		}
		let relative = path.strip_prefix(root).unwrap_or(path);
		if excluded.contains(&lexical(path)) || excluded.contains(&lexical(relative)) {
			continue;
		}
		let slashed = relative.to_string_lossy().replace('\\', "/");
		if !patterns.iter().any(|pattern| pattern.matches(&slashed)) {
			continue;
		}
		let bytes =
			std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
		let Ok(source) = String::from_utf8(bytes) else {
			log::warn!("skipping {}: not valid UTF-8", path.display());
			continue;
		};
		let extracted = extract(&source, methods);
		scan.skipped.merge(&extracted.skipped);
		if !extracted.queries.is_empty() {
			scan.files += 1;
		}
		for query in extracted.queries {
			scan.found.push(FoundQuery {
				file: path.to_path_buf(),
				query,
			});
		}
	}
	Ok(scan)
}
