//! What a check reports: [`Finding`], its [`Code`], and the positions in the file it points at.
//!
//! The shape is the engine contract's finding plus the `path` of the file: a code, a severity,
//! a message, a byte span into the file as read from disk, the index of the top-level
//! statement, notes and tags. Every backend converts what it finds into this type.

use std::fmt;
use std::ops::Range;
use std::path::PathBuf;

use serde::Serialize;

use super::policy::Level;

/// How serious a finding is. Policy resolves it from the code's intrinsic severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
	/// The contract is definitely violated.
	Error,
	/// The contract is probably violated.
	Warning,
	/// Advisory.
	Hint,
}

impl Severity {
	/// `error`, `warning` or `hint`.
	pub fn as_str(self) -> &'static str {
		match self {
			Self::Error => "error",
			Self::Warning => "warning",
			Self::Hint => "hint",
		}
	}
}

/// A finding's code: a letter and a number, `E1002` or `C10001`.
///
/// The prefix follows the engine's catalog: `0xxx` is `S` (syntax), `1xxx` to `6xxx` are `E`,
/// `7xxx` is `L` (lints), `10xxx` is `C` (certainty), `11xxx` is `A` (access), `12xxx` is `M`
/// (migration), `13xxx` is `D` (SQL dialects) and `14xxx` is `P` (performance). `8xxx` is
/// retired. SurrealKit's own findings are `K9xxx`. Two codes with the same number are the same
/// code.
///
/// A code below 10000 renders as its prefix and four digits; from 10000 up, as its prefix and the
/// number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Code {
	prefix: char,
	number: u16,
}

impl Code {
	/// The code the engine's catalog gives `number`: `S` for `0xxx`, `E` for `1xxx` to `6xxx`,
	/// `L` for `7xxx`, `K` for `9xxx`, then `C`, `A`, `M`, `D` and `P` for `10xxx` to `14xxx`.
	/// A number outside the catalog is `E`.
	pub const fn from_number(number: u16) -> Self {
		let prefix = match number / 1000 {
			0 => 'S',
			1..=6 => 'E',
			7 => 'L',
			9 => 'K',
			10 => 'C',
			11 => 'A',
			12 => 'M',
			13 => 'D',
			14 => 'P',
			_ => 'E',
		};
		Self {
			prefix,
			number,
		}
	}

	/// The code `prefix` and `number` spell, as written: a suppression names a rendered code,
	/// and `L1002` is not `E1002`. It does not check that the prefix suits the number.
	pub const fn new(prefix: char, number: u16) -> Self {
		Self {
			prefix,
			number,
		}
	}

	/// A SurrealKit code, `K` and `number`.
	pub const fn kit(number: u16) -> Self {
		Self {
			prefix: 'K',
			number,
		}
	}

	/// The number, `1002` for `E1002`.
	pub const fn number(self) -> u16 {
		self.number
	}

	/// The family, the number's thousands: `1` for `E1002`, `10` for `C10001`.
	pub const fn family(self) -> u16 {
		self.number / 1000
	}

	/// The prefix letter.
	pub const fn prefix(self) -> char {
		self.prefix
	}

	/// Whether this is one of SurrealKit's own codes.
	pub const fn is_kit(self) -> bool {
		self.prefix == 'K'
	}
}

impl fmt::Display for Code {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}{:04}", self.prefix, self.number)
	}
}

impl Serialize for Code {
	fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		serializer.collect_str(self)
	}
}

/// A file SurrealKit cannot split into statements: an unterminated string, an unmatched
/// bracket, a `DEFINE` inside another statement.
pub const K9001: Code = Code::kit(9001);
/// A template variable a statement uses is not set.
pub const K9002: Code = Code::kit(9002);
/// A schema statement the engine rejected when applying it.
pub const K9003: Code = Code::kit(9003);
/// Drift: a table the files define is missing on the target.
pub const K9101: Code = Code::kit(9101);
/// Drift: a table's definition differs between the files and the target (schemafull or
/// schemaless, a relation's `in` and `out`, a view, `DROP`, and so on).
pub const K9102: Code = Code::kit(9102);
/// Drift: a field the files define is missing on the target.
pub const K9103: Code = Code::kit(9103);
/// Drift: a field differs between the files and the target (its kind, optionality, default or
/// value, `READONLY`, a computed expression, `FLEXIBLE`, a reference, and so on).
pub const K9104: Code = Code::kit(9104);
/// Drift: on the target and recorded as managed by SurrealKit, but no longer in the files:
/// removed from the schema and not yet synced. A warning.
pub const K9105: Code = Code::kit(9105);
/// Drift: on the target and never managed by SurrealKit, as in a shared database or one
/// another writer changes. A warning.
pub const K9106: Code = Code::kit(9106);
/// Drift: the target runs another SurrealDB 4 minor than the one SurrealKit embeds, so only
/// normalised facts were compared. A hint. A 3.x target is refused when it connects.
pub const K9107: Code = Code::kit(9107);
/// Host code: an untagged template literal with `${…}` passed to a query method, which splices
/// the value into the SurrealQL text.
pub const K9201: Code = Code::kit(9201);
/// A suppression directive that is malformed, or that has no reason when one is required.
///
/// This is the engine's own code, not a SurrealKit one: it is a warning by default.
pub const L7013: Code = Code::from_number(7013);

/// The one-line rule description of a SurrealKit code, for the SARIF rule table.
pub fn describe(code: Code) -> Option<&'static str> {
	match code {
		K9001 => Some("The file cannot be split into statements"),
		K9002 => Some("A template variable the statement uses is not set"),
		K9003 => Some("The engine rejected a schema statement"),
		K9101 => Some("A table the files define is missing on the target"),
		K9102 => Some("A table's definition differs between the files and the target"),
		K9103 => Some("A field the files define is missing on the target"),
		K9104 => Some("A field differs between the files and the target"),
		K9105 => Some("On the target and managed by SurrealKit, but no longer in the files"),
		K9106 => Some("On the target and not managed by SurrealKit"),
		K9107 => {
			Some("The target runs another engine minor, so only normalised facts were compared")
		}
		K9201 => Some("A value is interpolated into a query string"),
		_ => None,
	}
}

/// How a [`Note`] reads under its finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum NoteKind {
	/// Context, such as where a field is declared.
	Note,
	/// A suggestion.
	Help,
}

/// A note or suggestion attached to a [`Finding`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Note {
	pub kind: NoteKind,
	pub message: String,
	/// Where it points in the same file, when it does.
	pub span: Option<Range<usize>>,
	/// For a help: the text that replaces the finding's span.
	pub replacement: Option<String>,
}

impl Note {
	/// A note with no position.
	pub fn context(message: impl Into<String>) -> Self {
		Self {
			kind: NoteKind::Note,
			message: message.into(),
			span: None,
			replacement: None,
		}
	}
}

/// A tag an editor renders: faded for `Unnecessary`, struck through for `Deprecated`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Tag {
	Unnecessary,
	Deprecated,
}

/// One thing a check found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
	pub code: Code,
	/// The effective severity, after policy. A backend returns the intrinsic one.
	pub severity: Severity,
	pub message: String,
	/// The absolute path of the source file.
	pub path: PathBuf,
	/// Byte range into the file's text as read from disk.
	pub span: Range<usize>,
	/// The index of the top-level statement in the file; `None` for the file as a whole.
	pub statement: Option<u32>,
	pub notes: Vec<Note>,
	pub tags: Vec<Tag>,
	intrinsic: Severity,
	default_level: Level,
}

impl Finding {
	/// A finding with its intrinsic severity and its code's default level, which policy reads.
	pub fn new(
		code: Code,
		intrinsic: Severity,
		default_level: Level,
		message: impl Into<String>,
		path: PathBuf,
		span: Range<usize>,
	) -> Self {
		Self {
			code,
			severity: intrinsic,
			message: message.into(),
			path,
			span,
			statement: None,
			notes: Vec::new(),
			tags: Vec::new(),
			intrinsic,
			default_level,
		}
	}

	/// One of SurrealKit's own findings: an error, denied by default.
	pub fn kit(code: Code, message: impl Into<String>, path: PathBuf, span: Range<usize>) -> Self {
		Self::new(code, Severity::Error, Level::Deny, message, path, span)
	}

	/// The index of the statement this finding is in.
	#[must_use]
	pub fn in_statement(mut self, index: usize) -> Self {
		self.statement = u32::try_from(index).ok();
		self
	}

	/// With `note` added.
	#[must_use]
	pub fn with_note(mut self, note: Note) -> Self {
		self.notes.push(note);
		self
	}

	/// The severity the code carries before policy.
	pub fn intrinsic(&self) -> Severity {
		self.intrinsic
	}

	/// The level the code has when no policy entry names it.
	pub fn default_level(&self) -> Level {
		self.default_level
	}

	/// A message of several lines as a finding's message (the first line) and notes (the
	/// rest), the way an engine error with a source snippet reads.
	#[must_use]
	pub fn with_message_lines(mut self, text: &str) -> Self {
		let mut lines = text.lines().map(str::trim_end);
		self.message = lines.next().unwrap_or_default().to_string();
		self.notes.extend(lines.filter(|line| !line.trim().is_empty()).map(Note::context));
		self
	}
}

/// The lines of a text, for turning byte offsets into line and column and back.
///
/// Lines are separated by `\n`; a `\r` before it belongs to the line it ends but is left out
/// of [`line_text`](Self::line_text). Columns count Unicode scalar values.
#[derive(Debug, Clone)]
pub struct LineIndex<'a> {
	text: &'a str,
	starts: Vec<usize>,
}

impl<'a> LineIndex<'a> {
	pub fn new(text: &'a str) -> Self {
		let mut starts = vec![0];
		starts.extend(text.match_indices('\n').map(|(at, _)| at + 1));
		Self {
			text,
			starts,
		}
	}

	/// The 1-based line and column of byte offset `at`, which is clamped to the text and to a
	/// character boundary.
	pub fn position(&self, at: usize) -> (usize, usize) {
		let at = self.floor(at);
		let line = match self.starts.binary_search(&at) {
			Ok(line) => line,
			Err(line) => line - 1,
		};
		let column = self.text[self.starts[line]..at].chars().count() + 1;
		(line + 1, column)
	}

	/// The byte offset of 1-based `line` and `column`, clamped to the end of the line.
	pub fn offset(&self, line: usize, column: usize) -> usize {
		let line = line.clamp(1, self.starts.len());
		let start = self.starts[line - 1];
		let end = self.line_end(line);
		self.text[start..end]
			.char_indices()
			.nth(column.saturating_sub(1))
			.map_or(end, |(at, _)| start + at)
	}

	/// How many lines the text has; an empty text has one.
	pub fn line_count(&self) -> usize {
		self.starts.len()
	}

	/// The text of 1-based `line`, without its line break.
	pub fn line_text(&self, line: usize) -> &'a str {
		let line = line.clamp(1, self.starts.len());
		&self.text[self.starts[line - 1]..self.line_end(line)]
	}

	/// The byte offset where 1-based `line` starts.
	pub fn line_start(&self, line: usize) -> usize {
		self.starts[line.clamp(1, self.starts.len()) - 1]
	}

	fn line_end(&self, line: usize) -> usize {
		let end = match self.starts.get(line) {
			Some(next) => next - 1,
			None => self.text.len(),
		};
		if self.text[..end].ends_with('\r') {
			end - 1
		} else {
			end
		}
	}

	fn floor(&self, at: usize) -> usize {
		let mut at = at.min(self.text.len());
		while !self.text.is_char_boundary(at) {
			at -= 1;
		}
		at
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn codes_render_with_the_catalogs_prefix() {
		assert_eq!(Code::from_number(1).to_string(), "S0001");
		assert_eq!(Code::from_number(1002).to_string(), "E1002");
		assert_eq!(Code::from_number(6004).to_string(), "E6004");
		assert_eq!(Code::from_number(7001).to_string(), "L7001");
		assert_eq!(Code::from_number(9003), K9003);
		assert_eq!(Code::from_number(10001).to_string(), "C10001");
		assert_eq!(Code::from_number(11042).to_string(), "A11042");
		assert_eq!(Code::from_number(12001).to_string(), "M12001");
		assert_eq!(Code::from_number(13001).to_string(), "D13001");
		assert_eq!(Code::from_number(14001).to_string(), "P14001");
		assert_eq!(Code::from_number(8001).to_string(), "E8001", "8xxx is retired: the fallback");
		assert_eq!(Code::from_number(15001).to_string(), "E15001", "outside the catalog");
		assert_eq!(Code::kit(9001).to_string(), "K9001");
		assert_eq!(
			[K9101, K9102, K9103, K9104, K9105, K9106, K9107, K9201].map(|code| code.to_string()),
			["K9101", "K9102", "K9103", "K9104", "K9105", "K9106", "K9107", "K9201"]
		);
		assert!(K9201.is_kit() && K9201.family() == 9, "9xxx is SurrealKit's family");
		assert_eq!(L7013.to_string(), "L7013");
	}

	#[test]
	fn a_code_has_a_family() {
		assert_eq!(Code::from_number(1002).family(), 1);
		assert_eq!(Code::from_number(7013).family(), 7);
		assert_eq!(Code::from_number(1).family(), 0);
		assert_eq!(Code::from_number(10001).family(), 10);
		assert_eq!(Code::from_number(14999).family(), 14);
	}

	#[test]
	fn only_the_kit_codes_have_a_description() {
		assert!(describe(K9001).is_some());
		assert!(describe(K9002).is_some());
		assert!(describe(K9003).is_some());
		for code in [K9101, K9102, K9103, K9104, K9105, K9106, K9107, K9201] {
			assert!(describe(code).is_some(), "{code}");
		}
		assert!(describe(L7013).is_none());
	}

	#[test]
	fn a_multi_line_message_becomes_a_message_and_notes() {
		let finding = Finding::kit(K9003, "", PathBuf::from("/p/a.surql"), 0..1)
			.with_message_lines("Parse error\n  --> here\n\n  | text");
		assert_eq!(finding.message, "Parse error");
		let notes: Vec<_> = finding.notes.iter().map(|note| note.message.as_str()).collect();
		assert_eq!(notes, vec!["  --> here", "  | text"]);
	}

	#[test]
	fn positions_count_characters_not_bytes() {
		// `é` is two bytes, `😀` four.
		let text = "ab\nécd😀e\nlast";
		let index = LineIndex::new(text);
		assert_eq!(index.position(0), (1, 1));
		assert_eq!(index.position(2), (1, 3));
		assert_eq!(index.position(3), (2, 1));
		// `c` follows `é`: byte 5, the second character of the line.
		assert_eq!(index.position(5), (2, 2));
		// `e` follows the emoji: bytes 3 + 2 + 1 + 1 + 4.
		assert_eq!(index.position(11), (2, 5));
		assert_eq!(index.position(text.len()), (3, 5));
		// Inside a multi-byte character reads as the character's start.
		assert_eq!(index.position(4), (2, 1));
		// Past the end clamps.
		assert_eq!(index.position(1000), (3, 5));
	}

	#[test]
	fn offsets_invert_positions() {
		let text = "ab\nécd😀e\nlast";
		let index = LineIndex::new(text);
		for at in [0, 2, 3, 5, 6, 7, 11, 12, 16] {
			let (line, column) = index.position(at);
			assert_eq!(index.offset(line, column), at, "{at}");
		}
		// A column past the end of a line is the end of that line.
		assert_eq!(index.offset(1, 99), 2);
	}

	#[test]
	fn a_tab_is_one_column() {
		let index = LineIndex::new("\t\tx");
		assert_eq!(index.position(2), (1, 3));
	}

	#[test]
	fn line_text_leaves_out_the_line_break() {
		let index = LineIndex::new("one\r\ntwo\n\nfour");
		assert_eq!(index.line_count(), 4);
		assert_eq!(index.line_text(1), "one");
		assert_eq!(index.line_text(2), "two");
		assert_eq!(index.line_text(3), "");
		assert_eq!(index.line_text(4), "four");
		assert_eq!(index.line_start(2), 5);
	}

	#[test]
	fn an_empty_text_has_one_empty_line() {
		let index = LineIndex::new("");
		assert_eq!(index.line_count(), 1);
		assert_eq!(index.position(0), (1, 1));
		assert_eq!(index.line_text(1), "");
	}
}
