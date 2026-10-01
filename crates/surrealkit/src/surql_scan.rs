//! A single-pass SurrealQL scanner: splits a file into statements and tokens.
//!
//! SurrealKit reads `.surql` files itself, to find statement boundaries, build its
//! entity catalog and make `DEFINE` statements idempotent. It does not need to
//! parse SurrealQL, only to tokenise it well enough that a `;`, quote, `#` or
//! brace inside a literal is never mistaken for structure.
//!
//! The rules follow SurrealDB 3.3's lexer (`surrealdb/syn`):
//!
//! - `--`, `//` and `#` start a comment that runs to the end of the line;
//!   `/* */` does not nest. Comments are lexed before anything else, so `//` and
//!   `/*` can never start a regex.
//! - Strings are `'...'` or `"..."`, optionally prefixed with one of `s d u b f r`.
//!   A backslash escapes the next character, inside strings only.
//! - Identifiers can be quoted with backticks or `⟨...⟩`, and params with either
//!   after the `$`.
//! - A regex literal runs from `/` to the first unescaped `/`. Character classes
//!   are not special: `/[/]/` ends inside the class, exactly as SurrealDB reads it.
//!   SurrealDB decides between regex and division in its parser, by whether an
//!   operand or an operator is expected. [`regex_allowed`](Scanner::regex_allowed)
//!   approximates that from the two preceding tokens, and only accepts a regex
//!   whose closing `/` is on the same line.
//! - The body of an embedded `function(...) { ... }` is JavaScript, which has its
//!   own strings and comments (`i--` is not a comment there).
//!
//! What it adds over SurrealDB is a refusal to guess. An unterminated string, an
//! unmatched bracket, or a `DEFINE` that turns up in the middle of another
//! statement is an error with a line and column. The previous splitter glued such
//! statements together, and sync then pruned the definitions it had lost.

use std::fmt;
use std::ops::Range;

/// What a token is. Only the distinctions the callers need are made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TokKind {
	/// A bare identifier or keyword: `[A-Za-z_][A-Za-z0-9_]*`.
	Word,
	/// A backtick- or `⟨⟩`-quoted identifier, delimiters included.
	QuotedIdent,
	/// `$name`, `` $`name` `` or `$⟨name⟩`.
	Param,
	/// A string literal, prefix and quotes included.
	Str,
	/// A number, duration or version-like run starting with a digit.
	Number,
	/// A regex literal, slashes included.
	Regex,
	/// The `{ ... }` body of an embedded JavaScript function.
	Js,
	/// A SurrealKit template variable, `${NAME}` or the escape `$${NAME}`.
	Placeholder,
	/// The `>` that closes a type's generic argument, as in `option<string>`.
	/// It ends an operand, unlike a comparison `>` or the end of a cast.
	GenericClose,
	/// Any other single character: operators, brackets, separators.
	Punct,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Tok {
	pub kind: TokKind,
	/// Byte offsets into the scanned source.
	pub span: Range<usize>,
	/// How many brackets enclose the token. An opening bracket counts the
	/// brackets outside it; a closing one, likewise, those outside the pair.
	pub depth: usize,
}

/// One top-level statement, without its terminating `;`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Stmt {
	/// From the start of the first token to the end of the last.
	pub span: Range<usize>,
	/// 1-based line of the first token.
	pub line: u32,
	pub tokens: Vec<Tok>,
	/// Comments that fall inside `span`.
	pub comments: Vec<Range<usize>>,
}

impl Stmt {
	/// The statement exactly as written, inner comments included.
	pub fn text<'a>(&self, src: &'a str) -> &'a str {
		&src[self.span.clone()]
	}

	/// The statement with each inner comment replaced by a single space.
	///
	/// This is what the catalog hashes. Any comment counts as whitespace, so after
	/// whitespace is collapsed it matches what the previous comment stripper
	/// produced, which kept entity hashes stable across the change.
	pub fn stripped(&self, src: &str) -> String {
		let mut out = String::with_capacity(self.span.len());
		let mut at = self.span.start;
		for comment in &self.comments {
			out.push_str(&src[at..comment.start]);
			out.push(' ');
			at = comment.end;
		}
		out.push_str(&src[at..self.span.end]);
		out
	}

	/// The text of token `idx`, or `None` past the end.
	pub fn tok_text<'a>(&self, src: &'a str, idx: usize) -> Option<&'a str> {
		self.tokens.get(idx).map(|tok| &src[tok.span.clone()])
	}

	/// Whether token `idx` is the bare word `word`, ignoring case.
	pub fn is_word(&self, src: &str, idx: usize, word: &str) -> bool {
		self.tokens.get(idx).is_some_and(|tok| {
			tok.kind == TokKind::Word && src[tok.span.clone()].eq_ignore_ascii_case(word)
		})
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ScanErrorKind {
	UnterminatedString,
	UnterminatedIdent,
	UnterminatedPlaceholder,
	UnterminatedJs,
	UnexpectedCloser {
		found: char,
		expected: Option<char>,
	},
	UnclosedBracket {
		open: char,
	},
	LostStatementBoundary {
		statement_line: u32,
	},
}

/// A scan failure, located in the source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScanError {
	pub line: u32,
	pub column: u32,
	pub kind: ScanErrorKind,
	/// Where the construct that caused the error opened, when that differs from
	/// where the error was found.
	pub opened_at: Option<(u32, u32)>,
	pub hint: Option<&'static str>,
}

impl fmt::Display for ScanError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "line {}, column {}: ", self.line, self.column)?;
		match &self.kind {
			ScanErrorKind::UnterminatedString => f.write_str("string is never closed")?,
			ScanErrorKind::UnterminatedIdent => f.write_str("quoted identifier is never closed")?,
			ScanErrorKind::UnterminatedPlaceholder => {
				f.write_str("template variable `${` is never closed with `}`")?
			}
			ScanErrorKind::UnterminatedJs => f.write_str("function body is never closed")?,
			ScanErrorKind::UnexpectedCloser {
				found,
				expected: Some(expected),
			} => write!(f, "found `{found}` where `{expected}` was expected")?,
			ScanErrorKind::UnexpectedCloser {
				found,
				expected: None,
			} => write!(f, "found `{found}` with nothing open to close")?,
			ScanErrorKind::UnclosedBracket {
				open,
			} => write!(f, "`{open}` is never closed")?,
			ScanErrorKind::LostStatementBoundary {
				statement_line,
			} => write!(
				f,
				"`DEFINE` inside the statement that began on line {statement_line}. A `;` is \
				 missing, or a string, regex or bracket opened earlier runs further than intended"
			)?,
		}
		if let Some((line, column)) = self.opened_at {
			write!(f, " (opened at line {line}, column {column})")?;
		}
		if let Some(hint) = self.hint {
			write!(f, ". {hint}")?;
		}
		Ok(())
	}
}

impl std::error::Error for ScanError {}

const MULTILINE_REGEX_HINT: &str = "SurrealKit only reads a regex literal whose closing `/` is on \
	the same line; write a multi-line pattern as <regex> \"...\" instead";

const DIVISION_HINT: &str = "If this statement has a regex literal, SurrealKit may have read its \
	opening `/` as division; write it as <regex> \"...\" instead";

const SLASH_IN_REGEX_HINT: &str = "A `/` inside a regex literal ends it, even inside `[...]`; \
	escape it as `\\/`";

/// Split `src` into statements.
pub(crate) fn scan(src: &str) -> Result<Vec<Stmt>, ScanError> {
	Scanner::new(src).run()
}

/// Words after which a `/` starts a regex, when the word is used as a keyword.
const REGEX_KEYWORDS: &[&str] = &[
	"ALLINSIDE",
	"ALWAYS",
	"AND",
	"ANYINSIDE",
	"ASSERT",
	"AUDIENCE",
	"AUTHENTICATE",
	"BATCH",
	"COMMENT",
	"COMPUTED",
	"CONTAINS",
	"CONTAINSALL",
	"CONTAINSANY",
	"CONTAINSNONE",
	"CONTAINSNOT",
	"CONTENT",
	"CONTEXT",
	"DEFAULT",
	"ELSE",
	"IF",
	"IN",
	"INSIDE",
	"INTERSECTS",
	"IS",
	"KEY",
	"LIMIT",
	"MERGE",
	"NONEINSIDE",
	"NOT",
	"NOTINSIDE",
	"OR",
	"OUTSIDE",
	"PATCH",
	"REPLACE",
	"RETURN",
	"SIGNIN",
	"SIGNUP",
	"START",
	"THEN",
	"THROW",
	"TIMEOUT",
	"URL",
	"VALUE",
	"WHEN",
	"WHERE",
];

/// The kinds a `DEFINE` can introduce. Used to tell a lost statement boundary
/// apart from a field or param that happens to be called `define`.
const DEFINE_KINDS: &[&str] = &[
	"ACCESS",
	"ANALYZER",
	"API",
	"BUCKET",
	"CONFIG",
	"DATABASE",
	"EVENT",
	"FIELD",
	"FUNCTION",
	"INDEX",
	"MODEL",
	"MODULE",
	"NAMESPACE",
	"PARAM",
	"SEQUENCE",
	"TABLE",
	"USER",
];

fn is_regex_keyword(word: &str) -> bool {
	REGEX_KEYWORDS.iter().any(|kw| kw.eq_ignore_ascii_case(word))
}

/// Punctuation after which an operand is expected, so a `/` starts a regex.
fn expects_operand_after(punct: &str) -> bool {
	matches!(
		punct,
		"(" | "["
			| "{" | ","
			| ":" | ";"
			| "=" | "!"
			| "<" | ">"
			| "+" | "-"
			| "*" | "%"
			| "?" | "|"
			| "&" | "~"
			| "^" | "@"
			| "∋" | "∌"
			| "∈" | "∉"
			| "⊇" | "⊃"
			| "⊅" | "⊆"
			| "⊂" | "⊄"
			| "×" | "÷"
	)
}

/// Types that take a generic argument, so a `<` straight after one opens it.
const GENERIC_TYPES: &[&str] = &[
	"array",
	"either",
	"file",
	"geometry",
	"literal",
	"option",
	"range",
	"record",
	"references",
	"set",
	"table",
];

/// Keywords that start a statement inside a block, so they stay keywords right
/// after `{` or `;`.
const BLOCK_STATEMENT_KEYWORDS: &[&str] = &["IF", "RETURN", "THROW"];

fn is_ident_start(c: char) -> bool {
	c.is_ascii_alphabetic() || c == '_'
}

fn is_ident_continue(c: char) -> bool {
	c.is_ascii_alphanumeric() || c == '_'
}

fn is_line_end(c: char) -> bool {
	matches!(c, '\n' | '\r' | '\u{2028}' | '\u{2029}' | '\u{0085}')
}

fn closer_for(open: char) -> char {
	match open {
		'(' => ')',
		'[' => ']',
		_ => '}',
	}
}

/// Tracks an embedded JavaScript function, `function(args) { body }`, whose body
/// must be lexed with JavaScript's rules rather than SurrealQL's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsState {
	Idle,
	/// Saw the word `function` in expression position; its `(` comes next.
	AwaitArgs,
	/// Inside the argument list, which opened at this depth.
	InArgs(usize),
	/// The argument list closed; a `{` now starts a JavaScript body.
	AwaitBody,
}

struct Scanner<'a> {
	src: &'a str,
	pos: usize,
	line_starts: Vec<usize>,
	stmts: Vec<Stmt>,
	cur: Vec<Tok>,
	cur_comments: Vec<Range<usize>>,
	brackets: Vec<(char, usize)>,
	/// A `/` in this statement that looked like a regex but had no closing `/`
	/// on its line, kept to explain a later error.
	abandoned_regex: Option<usize>,
	js: JsState,
	/// How many type generics (`option<...>`) are open.
	generics: usize,
}

impl<'a> Scanner<'a> {
	fn new(src: &'a str) -> Self {
		let mut line_starts = vec![0];
		line_starts.extend(src.match_indices('\n').map(|(i, _)| i + 1));
		Self {
			src,
			pos: 0,
			line_starts,
			stmts: Vec::new(),
			cur: Vec::new(),
			cur_comments: Vec::new(),
			brackets: Vec::new(),
			abandoned_regex: None,
			js: JsState::Idle,
			generics: 0,
		}
	}

	fn peek(&self) -> Option<char> {
		self.src[self.pos..].chars().next()
	}

	fn peek_nth(&self, n: usize) -> Option<char> {
		self.src[self.pos..].chars().nth(n)
	}

	fn bump(&mut self) -> Option<char> {
		let c = self.peek()?;
		self.pos += c.len_utf8();
		Some(c)
	}

	fn line_col(&self, offset: usize) -> (u32, u32) {
		let line_idx = match self.line_starts.binary_search(&offset) {
			Ok(idx) => idx,
			Err(idx) => idx - 1,
		};
		let line_start = self.line_starts[line_idx];
		let column = self.src[line_start..offset].chars().count() + 1;
		(line_idx as u32 + 1, column as u32)
	}

	fn error(&self, at: usize, kind: ScanErrorKind) -> ScanError {
		let (line, column) = self.line_col(at);
		ScanError {
			line,
			column,
			kind,
			opened_at: None,
			hint: None,
		}
	}

	fn run(mut self) -> Result<Vec<Stmt>, ScanError> {
		while let Some(c) = self.peek() {
			let start = self.pos;
			match c {
				c if c.is_whitespace() => {
					self.bump();
				}
				'-' if self.peek_nth(1) == Some('-') => self.line_comment(start),
				'/' if self.peek_nth(1) == Some('/') => self.line_comment(start),
				'#' => self.line_comment(start),
				'/' if self.peek_nth(1) == Some('*') => self.block_comment(start),
				'\'' | '"' => {
					self.string(start)?;
					self.push(TokKind::Str, start);
				}
				'`' => {
					self.quoted(start, '`')?;
					self.push(TokKind::QuotedIdent, start);
				}
				'⟨' => {
					self.quoted(start, '⟩')?;
					self.push(TokKind::QuotedIdent, start);
				}
				'$' => self.dollar(start)?,
				';' if self.brackets.is_empty() => {
					self.bump();
					self.end_statement()?;
				}
				'{' if self.js == JsState::AwaitBody => {
					self.js_body(start)?;
					self.push(TokKind::Js, start);
				}
				'(' | '[' | '{' => {
					self.bump();
					self.push(TokKind::Punct, start);
					self.brackets.push((c, start));
				}
				')' | ']' | '}' => self.closer(start, c)?,
				// `regex` consumes the literal only when it finds one.
				'/' if self.regex_allowed() && self.regex(start) => {
					self.push(TokKind::Regex, start);
				}
				'/' => {
					self.bump();
					self.push(TokKind::Punct, start);
				}
				c if is_ident_start(c) => {
					self.bump();
					let is_prefix =
						self.pos - start == 1 && matches!(c, 's' | 'd' | 'u' | 'b' | 'f' | 'r');
					if is_prefix && matches!(self.peek(), Some('\'' | '"')) {
						self.string(self.pos)?;
						self.push(TokKind::Str, start);
					} else {
						while self.peek().is_some_and(is_ident_continue) {
							self.bump();
						}
						self.push(TokKind::Word, start);
					}
				}
				'<' => {
					let opens_generic = self.cur.last().is_some_and(|prev| {
						prev.kind == TokKind::Word
							&& GENERIC_TYPES
								.iter()
								.any(|ty| self.src[prev.span.clone()].eq_ignore_ascii_case(ty))
					});
					if opens_generic {
						self.generics += 1;
					}
					self.bump();
					self.push(TokKind::Punct, start);
				}
				'>' if self.generics > 0 => {
					self.generics -= 1;
					self.bump();
					self.push(TokKind::GenericClose, start);
				}
				c if c.is_ascii_digit() => {
					while self.peek().is_some_and(|c| is_ident_continue(c) || c == '.') {
						// `1..5` is a range, not a number with two points. Keep the
						// dots out so the range operator stays punctuation.
						if self.peek() == Some('.') && self.peek_nth(1) == Some('.') {
							break;
						}
						self.bump();
					}
					self.push(TokKind::Number, start);
				}
				_ => {
					self.bump();
					self.push(TokKind::Punct, start);
				}
			}
		}

		if let Some(&(open, at)) = self.brackets.last() {
			let mut err = self.error(
				at,
				ScanErrorKind::UnclosedBracket {
					open,
				},
			);
			if self.abandoned_regex.is_some() {
				err.hint = Some(MULTILINE_REGEX_HINT);
			}
			return Err(err);
		}
		self.end_statement()?;
		Ok(self.stmts)
	}

	fn push(&mut self, kind: TokKind, start: usize) {
		let tok = Tok {
			kind,
			span: start..self.pos,
			depth: self.brackets.len(),
		};
		self.advance_js(&tok);
		self.cur.push(tok);
	}

	fn advance_js(&mut self, tok: &Tok) {
		let text = &self.src[tok.span.clone()];
		self.js = match self.js {
			JsState::Idle | JsState::AwaitBody => {
				let defines_function = self.cur.last().is_some_and(|prev| {
					prev.kind == TokKind::Word
						&& ["DEFINE", "REMOVE", "ALTER", "INFO"]
							.iter()
							.any(|kw| self.src[prev.span.clone()].eq_ignore_ascii_case(kw))
				});
				if tok.kind == TokKind::Word
					&& text.eq_ignore_ascii_case("function")
					&& !defines_function
				{
					JsState::AwaitArgs
				} else {
					JsState::Idle
				}
			}
			JsState::AwaitArgs if tok.kind == TokKind::Punct && text == "(" => {
				JsState::InArgs(self.brackets.len())
			}
			JsState::AwaitArgs => JsState::Idle,
			JsState::InArgs(depth)
				if tok.kind == TokKind::Punct && text == ")" && self.brackets.len() == depth =>
			{
				JsState::AwaitBody
			}
			state @ JsState::InArgs(_) => state,
		};
	}

	fn end_statement(&mut self) -> Result<(), ScanError> {
		let tokens = std::mem::take(&mut self.cur);
		let comments = std::mem::take(&mut self.cur_comments);
		self.abandoned_regex = None;
		self.js = JsState::Idle;
		self.generics = 0;
		let (Some(first), Some(last)) = (tokens.first(), tokens.last()) else {
			return Ok(());
		};
		let span = first.span.start..last.span.end;
		let comments = comments.into_iter().filter(|c| c.start < span.end).collect();
		let (line, _) = self.line_col(span.start);
		let stmt = Stmt {
			span,
			line,
			tokens,
			comments,
		};
		self.check_boundary(&stmt)?;
		self.stmts.push(stmt);
		Ok(())
	}

	/// Refuse a `DEFINE <kind>` at the top level of a statement it does not start.
	/// That is never valid SurrealQL, and it is exactly what a statement looks like
	/// once a missing `;` or a misread literal has glued two together.
	fn check_boundary(&self, stmt: &Stmt) -> Result<(), ScanError> {
		for idx in 1..stmt.tokens.len() {
			let tok = &stmt.tokens[idx];
			if tok.depth != 0 || !stmt.is_word(self.src, idx, "DEFINE") {
				continue;
			}
			let next_is_kind = stmt.tokens.get(idx + 1).is_some_and(|next| {
				next.kind == TokKind::Word
					&& DEFINE_KINDS
						.iter()
						.any(|kind| self.src[next.span.clone()].eq_ignore_ascii_case(kind))
			});
			if !next_is_kind {
				continue;
			}
			// In 3.x statements are expressions, so `THEN DEFINE ...` is legal.
			let prev = &stmt.tokens[idx - 1];
			let prev_text = &self.src[prev.span.clone()];
			let introduces_expression = prev.kind == TokKind::Word
				&& ["THEN", "ELSE", "RETURN"].iter().any(|kw| prev_text.eq_ignore_ascii_case(kw));
			let is_member = prev.kind == TokKind::Punct && matches!(prev_text, "." | ":");
			if introduces_expression || is_member {
				continue;
			}
			let mut err = self.error(
				tok.span.start,
				ScanErrorKind::LostStatementBoundary {
					statement_line: stmt.line,
				},
			);
			if stmt.tokens[..idx]
				.iter()
				.any(|t| t.kind == TokKind::Punct && &self.src[t.span.clone()] == "/")
			{
				err.hint = Some(DIVISION_HINT);
			}
			return Err(err);
		}
		Ok(())
	}

	fn line_comment(&mut self, start: usize) {
		while self.peek().is_some_and(|c| !is_line_end(c)) {
			self.bump();
		}
		self.record_comment(start);
	}

	fn block_comment(&mut self, start: usize) {
		self.pos += 2;
		// Unterminated, it runs to the end of the input, which is how the previous
		// stripper treated it and what `schema_handles_unterminated_block_comment`
		// pins.
		match self.src[self.pos..].find("*/") {
			Some(end) => self.pos += end + 2,
			None => self.pos = self.src.len(),
		}
		self.record_comment(start);
	}

	fn record_comment(&mut self, start: usize) {
		if !self.cur.is_empty() {
			self.cur_comments.push(start..self.pos);
		}
	}

	/// Consume a quoted string starting at `start`, which holds the quote.
	fn string(&mut self, start: usize) -> Result<(), ScanError> {
		let quote = self.bump().expect("caller saw the quote");
		while let Some(c) = self.bump() {
			if c == '\\' {
				self.bump();
			} else if c == quote {
				return Ok(());
			}
		}
		let mut err = self.error(start, ScanErrorKind::UnterminatedString);
		err.hint = Some(if self.abandoned_regex.is_some() {
			MULTILINE_REGEX_HINT
		} else {
			DIVISION_HINT
		});
		Err(err)
	}

	fn quoted(&mut self, start: usize, close: char) -> Result<(), ScanError> {
		self.bump();
		while let Some(c) = self.bump() {
			if c == '\\' {
				self.bump();
			} else if c == close {
				return Ok(());
			}
		}
		Err(self.error(start, ScanErrorKind::UnterminatedIdent))
	}

	fn dollar(&mut self, start: usize) -> Result<(), ScanError> {
		let placeholder = match (self.peek_nth(1), self.peek_nth(2)) {
			(Some('{'), _) => Some(1),
			(Some('$'), Some('{')) => Some(2),
			_ => None,
		};
		if let Some(skip) = placeholder {
			self.pos += skip;
			match self.src[self.pos..].find('}') {
				Some(end) => self.pos += end + 1,
				None => return Err(self.error(start, ScanErrorKind::UnterminatedPlaceholder)),
			}
			self.push(TokKind::Placeholder, start);
			return Ok(());
		}
		self.bump();
		match self.peek() {
			Some('`') => self.quoted(self.pos, '`')?,
			Some('⟨') => self.quoted(self.pos, '⟩')?,
			Some(c) if is_ident_continue(c) => {
				while self.peek().is_some_and(is_ident_continue) {
					self.bump();
				}
			}
			_ => {
				self.push(TokKind::Punct, start);
				return Ok(());
			}
		}
		self.push(TokKind::Param, start);
		Ok(())
	}

	fn closer(&mut self, start: usize, found: char) -> Result<(), ScanError> {
		match self.brackets.last().copied() {
			Some((open, _)) if closer_for(open) == found => {
				self.brackets.pop();
				self.bump();
				self.push(TokKind::Punct, start);
				Ok(())
			}
			top => {
				let mut err = self.error(
					start,
					ScanErrorKind::UnexpectedCloser {
						found,
						expected: top.map(|(open, _)| closer_for(open)),
					},
				);
				err.opened_at = top.map(|(_, at)| self.line_col(at));
				if self.cur.last().is_some_and(|t| t.kind == TokKind::Regex) {
					err.hint = Some(SLASH_IN_REGEX_HINT);
				}
				Err(err)
			}
		}
	}

	/// Whether a `/` here starts a regex rather than dividing.
	///
	/// SurrealDB decides in its parser: a regex where an operand is expected,
	/// division where an operator is. From the two previous tokens, `P` then `Q`
	/// before it:
	///
	/// - at the start of a statement, an operand is expected;
	/// - after an opener, separator or operator (`( [ { , : = < > + -` ...), too;
	/// - after an expression keyword (`VALUE`, `WHERE`, `THEN`, `AND` ...), too,
	///   unless that keyword is really a name. It is a name when `Q` expected an
	///   operand (`VALUE value / 2`, `= in / 2`) or was a member access (`$o.value`),
	///   except that `IF`, `RETURN` and `THROW` right after `{` or `;` start a block
	///   statement;
	/// - after anything else (an identifier, number, string, param, a closing
	///   bracket or the `>` of `option<string>`) an operator is expected, so `/`
	///   divides.
	fn regex_allowed(&self) -> bool {
		let Some(p) = self.cur.last() else {
			return true;
		};
		let p_text = &self.src[p.span.clone()];
		match p.kind {
			TokKind::Punct => expects_operand_after(p_text),
			TokKind::Word if is_regex_keyword(p_text) => {
				if p_text.eq_ignore_ascii_case("ALWAYS") {
					return true;
				}
				let Some(q) = self.cur.len().checked_sub(2).map(|i| &self.cur[i]) else {
					return true;
				};
				let q_text = &self.src[q.span.clone()];
				let p_is_name = match q.kind {
					// Right after `{` or `;` a block statement may begin.
					TokKind::Punct if matches!(q_text, "{" | ";") => {
						!BLOCK_STATEMENT_KEYWORDS.iter().any(|kw| p_text.eq_ignore_ascii_case(kw))
					}
					TokKind::Punct => expects_operand_after(q_text) || q_text == ".",
					TokKind::Word => {
						is_regex_keyword(q_text) || q_text.eq_ignore_ascii_case("SELECT")
					}
					_ => false,
				};
				!p_is_name
			}
			_ => false,
		}
	}

	/// Try to consume a regex literal starting at `start`. On failure nothing is
	/// consumed and the caller treats the `/` as division.
	fn regex(&mut self, start: usize) -> bool {
		let mut chars = self.src[start + 1..].char_indices();
		while let Some((i, c)) = chars.next() {
			match c {
				'\\' => match chars.next() {
					Some((_, next)) if !is_line_end(next) => {}
					_ => break,
				},
				'/' => {
					self.pos = start + 1 + i + 1;
					return true;
				}
				c if is_line_end(c) => break,
				_ => {}
			}
		}
		self.abandoned_regex = Some(start);
		false
	}

	/// Consume a JavaScript function body, braces included, using JavaScript's
	/// strings and comments.
	fn js_body(&mut self, start: usize) -> Result<(), ScanError> {
		self.bump();
		let mut depth = 1usize;
		while let Some(c) = self.peek() {
			match c {
				'\'' | '"' | '`' => {
					let quote_at = self.pos;
					self.bump();
					loop {
						match self.bump() {
							Some('\\') => {
								self.bump();
							}
							Some(q) if q == c => break,
							Some(_) => {}
							None => {
								return Err(self.error(quote_at, ScanErrorKind::UnterminatedString));
							}
						}
					}
				}
				'/' if self.peek_nth(1) == Some('/') => {
					while self.peek().is_some_and(|c| !is_line_end(c)) {
						self.bump();
					}
				}
				'/' if self.peek_nth(1) == Some('*') => {
					self.pos += 2;
					match self.src[self.pos..].find("*/") {
						Some(end) => self.pos += end + 2,
						None => return Err(self.error(start, ScanErrorKind::UnterminatedJs)),
					}
				}
				'{' => {
					depth += 1;
					self.bump();
				}
				'}' => {
					depth -= 1;
					self.bump();
					if depth == 0 {
						return Ok(());
					}
				}
				_ => {
					self.bump();
				}
			}
		}
		Err(self.error(start, ScanErrorKind::UnterminatedJs))
	}
}

#[cfg(test)]
mod tests {
	use test_case::test_case;

	use super::*;

	fn texts(src: &str) -> Vec<String> {
		scan(src)
			.unwrap_or_else(|e| panic!("scan failed: {e}\n{src}"))
			.iter()
			.map(|s| s.text(src).to_string())
			.collect()
	}

	fn count(src: &str) -> usize {
		texts(src).len()
	}

	fn kinds(src: &str) -> Vec<TokKind> {
		let stmts = scan(src).unwrap_or_else(|e| panic!("scan failed: {e}"));
		stmts.into_iter().flat_map(|s| s.tokens).map(|t| t.kind).collect()
	}

	fn regexes(src: &str) -> Vec<String> {
		let stmts = scan(src).unwrap_or_else(|e| panic!("scan failed: {e}"));
		stmts
			.iter()
			.flat_map(|s| s.tokens.iter())
			.filter(|t| t.kind == TokKind::Regex)
			.map(|t| src[t.span.clone()].to_string())
			.collect()
	}

	fn err(src: &str) -> ScanError {
		scan(src).expect_err(&format!("expected a scan error for:\n{src}"))
	}

	// The exact statement from #92, followed by another definition.
	const ISSUE_92: &str = "DEFINE PARAM OVERWRITE $PRE_FILTER VALUE /[ \\-_().,\\\\\\/$&+,:;=?@#|'<>^*%!]/;\n\
		DEFINE FUNCTION OVERWRITE fn::preFilter($str: option<string>) {\n\
		\tRETURN IF $str {\n\
		\t\tstring::replace($str, $PRE_FILTER, '')\n\
		\t};\n\
		};\n";

	#[test]
	fn issue_92_regex_param_and_following_function_split_in_two() {
		let stmts = texts(ISSUE_92);
		assert_eq!(stmts.len(), 2, "{stmts:#?}");
		assert!(stmts[0].starts_with("DEFINE PARAM OVERWRITE $PRE_FILTER VALUE /["));
		assert!(stmts[0].ends_with("%!]/"), "regex must be kept whole: {}", stmts[0]);
		assert!(stmts[1].starts_with("DEFINE FUNCTION OVERWRITE fn::preFilter"));
		assert_eq!(regexes(ISSUE_92), vec!["/[ \\-_().,\\\\\\/$&+,:;=?@#|'<>^*%!]/".to_string()]);
	}

	#[test_case("DEFINE PARAM $re VALUE /a#b/;\nDEFINE TABLE t;" ; "hash")]
	#[test_case("DEFINE PARAM $re VALUE /a--b/;\nDEFINE TABLE t;" ; "double dash")]
	#[test_case("DEFINE PARAM $re VALUE /a\\/\\/b/;\nDEFINE TABLE t;" ; "escaped double slash")]
	#[test_case("DEFINE PARAM $re VALUE /it's/;\nDEFINE TABLE t;" ; "lone single quote")]
	#[test_case("DEFINE PARAM $re VALUE /say \"hi/;\nDEFINE TABLE t;" ; "lone double quote")]
	#[test_case("DEFINE PARAM $re VALUE /a;{b/;\nDEFINE TABLE t;" ; "semicolon and brace")]
	#[test_case("DEFINE PARAM $re VALUE /`x/;\nDEFINE TABLE t;" ; "lone backtick")]
	#[test_case("DEFINE PARAM $re VALUE /[(]/;\nDEFINE TABLE t;" ; "unbalanced paren")]
	fn regex_bodies_hide_structure(src: &str) {
		assert_eq!(count(src), 2);
		assert_eq!(regexes(src).len(), 1);
	}

	#[test]
	fn slash_inside_a_character_class_ends_the_regex_like_surrealdb() {
		// SurrealDB reads `/[/` as the regex and then trips on `]`. So do we, and
		// the error says why.
		let e = err("DEFINE PARAM $re VALUE /[/]/;");
		assert!(
			matches!(
				e.kind,
				ScanErrorKind::UnexpectedCloser {
					found: ']',
					..
				}
			),
			"{e:?}"
		);
		assert_eq!(e.hint, Some(SLASH_IN_REGEX_HINT));
	}

	#[test_case("DEFINE PARAM $re VALUE /a\\/b/;", "/a\\/b/" ; "escaped slash")]
	#[test_case("DEFINE PARAM $re VALUE /a\\\\/;", "/a\\\\/" ; "escaped backslash ends")]
	#[test_case("DEFINE PARAM $re VALUE /a\\\\\\/b/;", "/a\\\\\\/b/" ; "backslash then escaped slash")]
	fn regex_escapes(src: &str, expected: &str) {
		assert_eq!(regexes(src), vec![expected.to_string()]);
	}

	#[test_case("DEFINE FIELD half ON t VALUE $value / 2;" ; "param")]
	#[test_case("DEFINE FIELD q ON t VALUE a / b / c;" ; "chained")]
	#[test_case("DEFINE FIELD half ON t VALUE value / 2;" ; "keyword named field")]
	#[test_case("DEFINE FIELD half ON t VALUE math::floor($v) / 2;" ; "after call")]
	#[test_case("DEFINE FIELD half ON t VALUE [1,2][0] / 2;" ; "after index")]
	#[test_case("DEFINE FIELD half ON t VALUE count() / 2;" ; "after empty call")]
	#[test_case("DEFINE FIELD half ON t VALUE 10 / 2;" ; "after number")]
	#[test_case("DEFINE FIELD half ON t VALUE $o.value / 2;" ; "member named value")]
	#[test_case("DEFINE FIELD half ON t VALUE { a: in / 2 };" ; "object value named in")]
	#[test_case("DEFINE FIELD half ON t PERMISSIONS FOR select WHERE value / 2 > 1;" ; "where name")]
	#[test_case("SELECT value / 2 FROM t;" ; "select name")]
	#[test_case("DEFINE FIELD half ON t VALUE 'a' / 2;" ; "after string")]
	fn division_is_not_a_regex(src: &str) {
		assert!(regexes(src).is_empty(), "{:?}", regexes(src));
	}

	#[test]
	fn keyword_named_field_divides_on_one_line_with_a_following_statement() {
		let src = "DEFINE FIELD x ON t VALUE value / 2; DEFINE FIELD y ON t VALUE 1 / 2;";
		assert_eq!(count(src), 2);
		assert!(regexes(src).is_empty());
	}

	#[test_case("DEFINE FIELD f ON t TYPE option<string> DEFAULT /x;y/;" ; "after generic and default")]
	#[test_case("DEFINE FIELD f ON t VALUE <string> /x'/;" ; "after cast")]
	#[test_case("DEFINE FIELD f ON t ASSERT string::matches($value, /^[a-z#']+$/);" ; "function argument")]
	#[test_case("DEFINE PARAM $r VALUE [/a'/, /b#/];" ; "array items")]
	#[test_case("DEFINE PARAM $r VALUE { re: /c;/ };" ; "object value")]
	#[test_case("DEFINE FIELD f ON t DEFAULT ALWAYS /x;/;" ; "default always")]
	#[test_case("DEFINE FIELD f ON t PERMISSIONS FOR select WHERE name = /a'b/;" ; "comparison")]
	#[test_case("LET $x = /a;b/;" ; "let statement")]
	#[test_case("DEFINE FIELD f ON t ASSERT $value ~ /a'/;" ; "fuzzy match")]
	#[test_case("DEFINE FIELD f ON t ASSERT $value CONTAINS /a'/;" ; "contains keyword")]
	#[test_case("DEFINE FIELD f ON t ASSERT $value != /a'/ AND $value = /b'/;" ; "two in one")]
	fn regex_where_an_operand_is_expected(src: &str) {
		assert!(!regexes(src).is_empty(), "no regex found in {src}");
		assert_eq!(count(src), 1);
	}

	#[test_case("DEFINE FIELD f ON t TYPE option<array<string>> DEFAULT /x;/;" ; "nested generic")]
	#[test_case("DEFINE FIELD f ON t TYPE record<user> VALUE /x;/;" ; "record generic")]
	#[test_case("DEFINE FIELD f ON t VALUE <option<string>> /x;/;" ; "cast with generic")]
	#[test_case("DEFINE FUNCTION fn::f() { RETURN /x;/; };" ; "return in a block")]
	#[test_case("DEFINE FUNCTION fn::f() { LET $a = 1; THROW /x;/; };" ; "throw after semicolon")]
	fn regex_after_generics_and_block_keywords(src: &str) {
		assert_eq!(regexes(src).len(), 1, "{src}");
		assert_eq!(count(src), 1);
	}

	#[test_case("DEFINE FIELD f ON t VALUE <array<int>> value / 2;" ; "cast then name")]
	#[test_case("DEFINE FUNCTION fn::f() { value / 2 };" ; "block expression")]
	#[test_case("DEFINE FIELD f ON t VALUE $a > value / 2;" ; "comparison then name")]
	fn division_after_generics_and_blocks(src: &str) {
		assert!(regexes(src).is_empty(), "{:?}", regexes(src));
	}

	#[test]
	fn if_then_else_regexes() {
		let src = "DEFINE FIELD f ON t VALUE IF $v = /a'/ THEN /b#/ ELSE /c;/ END;";
		assert_eq!(regexes(src), vec!["/a'/", "/b#/", "/c;/"]);
		assert_eq!(count(src), 1);
	}

	#[test]
	fn value_value_divides_but_value_regex_does_not() {
		assert!(regexes("DEFINE FIELD f ON t VALUE <string> value / 2;").is_empty());
		assert_eq!(regexes("DEFINE PARAM $x VALUE /v/;"), vec!["/v/"]);
	}

	#[test]
	fn comments_carry_no_string_state() {
		let src = "-- don't\nDEFINE TABLE a;\n# it's\nDEFINE TABLE b;\n/* \"x */ DEFINE TABLE c;\n// it's\nDEFINE TABLE d;";
		assert_eq!(
			texts(src),
			vec!["DEFINE TABLE a", "DEFINE TABLE b", "DEFINE TABLE c", "DEFINE TABLE d"]
		);
	}

	#[test]
	fn block_comments_do_not_nest() {
		assert_eq!(texts("/* a /* b */ DEFINE TABLE t;"), vec!["DEFINE TABLE t"]);
	}

	#[test]
	fn unterminated_block_comment_runs_to_the_end() {
		assert_eq!(texts("DEFINE TABLE a;\n/* never closes"), vec!["DEFINE TABLE a"]);
	}

	#[test]
	fn arrow_dash_comment() {
		assert_eq!(texts("DEFINE TABLE a; --> note\nDEFINE TABLE b;").len(), 2);
	}

	#[test_case("DEFINE FIELD a ON t DEFAULT 'it\\'s';" ; "escaped single")]
	#[test_case("DEFINE FIELD a ON t DEFAULT \"a\\\"b\";" ; "escaped double")]
	#[test_case("DEFINE FIELD a ON t DEFAULT 'a\\\\';" ; "trailing backslash")]
	#[test_case("DEFINE FIELD a ON t DEFAULT '; DEFINE TABLE x';" ; "statement inside string")]
	#[test_case("DEFINE FIELD a ON t DEFAULT '#';" ; "hash inside string")]
	#[test_case("DEFINE FIELD a ON t DEFAULT 'line one\nline two; still';" ; "multi line string")]
	fn strings_hide_structure(src: &str) {
		assert_eq!(count(src), 1);
	}

	#[test]
	fn prefixed_strings() {
		let src = "DEFINE PARAM $x VALUE [d\"2024-01-01T00:00:00Z\", r\"person:1\", u'018f2b1c-0000-7000-8000-000000000000', b\"00ff\", s\"a;b\", f\"b:/a;b\"];";
		assert_eq!(count(src), 1);
		assert_eq!(kinds(src).iter().filter(|k| **k == TokKind::Str).count(), 6);
	}

	#[test]
	fn a_word_that_merely_starts_with_a_prefix_letter_is_a_word() {
		let src = "DEFINE FIELD bar ON t TYPE string;";
		assert_eq!(kinds(src).iter().filter(|k| **k == TokKind::Str).count(), 0);
	}

	#[test]
	fn quoted_identifiers_hide_structure() {
		let src = "DEFINE FIELD `a;b#c` ON `t'x` TYPE string;\nDEFINE TABLE ⟨my;table⟩;\nDEFINE PARAM $`we;ird` VALUE 1;\nDEFINE PARAM $⟨a#b⟩ VALUE 2;";
		assert_eq!(count(src), 4);
	}

	#[test_case("DEFINE PARAM $p VALUE person:⟨a;b⟩;" ; "angle key")]
	#[test_case("DEFINE PARAM $p VALUE person:[1, 'x;'];" ; "array key")]
	#[test_case("DEFINE PARAM $p VALUE person:{ a: '#' };" ; "object key")]
	#[test_case("DEFINE PARAM $p VALUE person:`a;b`;" ; "backtick key")]
	fn complex_record_ids(src: &str) {
		assert_eq!(count(src), 1);
	}

	#[test]
	fn function_bodies_keep_their_statements() {
		let src = "DEFINE FUNCTION fn::f() { LET $x = { a: 1 }; IF true { RETURN /}/; }; RETURN $x; };\nDEFINE TABLE t;";
		assert_eq!(count(src), 2);
	}

	#[test]
	fn javascript_function_bodies_use_javascript_rules() {
		let src = "DEFINE FIELD f ON t VALUE function($a) { for (let i = 3; i > 0; i--) { /* x */ } return \"#\" + '}'; };\nDEFINE TABLE u;";
		assert_eq!(count(src), 2);
		assert!(kinds(src).contains(&TokKind::Js));
	}

	#[test]
	fn javascript_template_strings() {
		let src = "DEFINE FIELD f ON t VALUE function() { return `a${1}b}`; };";
		assert_eq!(count(src), 1);
	}

	#[test]
	fn define_function_body_is_surrealql_not_javascript() {
		let src = "DEFINE FUNCTION fn::f() { RETURN 1; -- i--\n };";
		assert!(!kinds(src).contains(&TokKind::Js));
		assert_eq!(count(src), 1);
	}

	#[test]
	fn a_field_named_function_is_not_javascript() {
		let src = "DEFINE FIELD function ON t TYPE object DEFAULT {};";
		assert!(!kinds(src).contains(&TokKind::Js));
	}

	#[test]
	fn whitespace_and_comments_between_tokens() {
		let src = "DEFINE\n  TABLE\tperson;\nDEFINE /*c*/ TABLE p;\nDEFINE\u{00A0}TABLE n;";
		let stmts = scan(src).unwrap();
		assert_eq!(stmts.len(), 3);
		for stmt in &stmts {
			assert!(stmt.is_word(src, 0, "DEFINE"));
			assert!(stmt.is_word(src, 1, "TABLE"));
		}
		assert_eq!(stmts[1].stripped(src), "DEFINE   TABLE p");
	}

	#[test]
	fn stripped_replaces_each_inner_comment_with_a_space() {
		let src = "DEFINE TABLE t -- trailing\n  SCHEMAFULL # more\n PERMISSIONS NONE /* tail */;";
		let stmts = scan(src).unwrap();
		assert_eq!(stmts.len(), 1);
		assert_eq!(stmts[0].stripped(src), "DEFINE TABLE t  \n  SCHEMAFULL  \n PERMISSIONS NONE");
		assert_eq!(
			stmts[0].text(src),
			"DEFINE TABLE t -- trailing\n  SCHEMAFULL # more\n PERMISSIONS NONE"
		);
	}

	#[test]
	fn empty_statements_and_a_missing_final_semicolon() {
		assert_eq!(texts(";;DEFINE TABLE a;;"), vec!["DEFINE TABLE a"]);
		assert_eq!(texts("DEFINE TABLE a"), vec!["DEFINE TABLE a"]);
		assert!(texts("").is_empty());
		assert!(texts("-- only a comment\n").is_empty());
	}

	#[test]
	fn crlf_line_endings() {
		assert_eq!(texts("DEFINE TABLE a; -- x\r\nDEFINE TABLE b;\r\n").len(), 2);
	}

	#[test]
	fn template_placeholders_are_opaque() {
		let src = "DEFINE TABLE ${PREFIX}_users;\nDEFINE PARAM $p VALUE '$${KEEP}';\nDEFINE PARAM $q VALUE $${RAW};";
		assert_eq!(count(src), 3);
		let stmts = scan(src).unwrap();
		assert_eq!(stmts[0].tokens[2].kind, TokKind::Placeholder);
		assert_eq!(stmts[2].tokens[4].kind, TokKind::Placeholder);
	}

	#[test]
	fn info_output_without_a_semicolon() {
		assert_eq!(count("DEFINE PARAM $re VALUE /x\\/y/ PERMISSIONS FULL"), 1);
	}

	#[test]
	fn then_define_is_an_expression_not_a_lost_boundary() {
		assert_eq!(count("DEFINE EVENT e ON t WHEN true THEN DEFINE TABLE u;"), 1);
	}

	#[test_case("DEFINE FIELD define ON t TYPE string;" ; "field named define")]
	#[test_case("DEFINE FIELD x ON t VALUE $o.define;" ; "member named define")]
	#[test_case("DEFINE PARAM $define VALUE 1;" ; "param named define")]
	fn define_as_a_name_is_fine(src: &str) {
		assert_eq!(count(src), 1);
	}

	#[test]
	fn missing_semicolon_is_a_lost_boundary_not_a_silent_glue() {
		let e = err("DEFINE TABLE a\nDEFINE TABLE b;");
		assert_eq!(
			e.kind,
			ScanErrorKind::LostStatementBoundary {
				statement_line: 1
			}
		);
		assert_eq!((e.line, e.column), (2, 1));
	}

	#[test]
	fn unterminated_string_points_at_its_opening_quote() {
		let e = err("DEFINE PARAM $x VALUE 'abc;\nDEFINE TABLE t;");
		assert_eq!(e.kind, ScanErrorKind::UnterminatedString);
		assert_eq!((e.line, e.column), (1, 23));
	}

	#[test]
	fn a_regex_whose_close_is_on_another_line_explains_itself() {
		let e = err("DEFINE PARAM $x VALUE /a\n'b/;");
		assert_eq!(e.kind, ScanErrorKind::UnterminatedString);
		assert_eq!(e.hint, Some(MULTILINE_REGEX_HINT));
	}

	#[test]
	fn unmatched_closer_and_unclosed_bracket() {
		let closer = err("DEFINE TABLE t);");
		assert!(matches!(
			closer.kind,
			ScanErrorKind::UnexpectedCloser {
				found: ')',
				expected: None
			}
		));
		let unclosed = err("DEFINE FUNCTION fn::f() { RETURN 1;");
		assert_eq!(
			unclosed.kind,
			ScanErrorKind::UnclosedBracket {
				open: '{'
			}
		);
		assert_eq!((unclosed.line, unclosed.column), (1, 25));
		let mismatched = err("DEFINE PARAM $x VALUE [1, 2);");
		assert!(matches!(
			mismatched.kind,
			ScanErrorKind::UnexpectedCloser {
				found: ')',
				expected: Some(']')
			}
		));
		assert_eq!(mismatched.opened_at, Some((1, 23)));
	}

	#[test]
	fn unterminated_identifiers_and_placeholders() {
		assert_eq!(err("DEFINE TABLE `abc;").kind, ScanErrorKind::UnterminatedIdent);
		assert_eq!(err("DEFINE TABLE ⟨abc;").kind, ScanErrorKind::UnterminatedIdent);
		assert_eq!(err("DEFINE TABLE ${PREFIX;").kind, ScanErrorKind::UnterminatedPlaceholder);
		assert_eq!(
			err("DEFINE FIELD f ON t VALUE function() { return 1;").kind,
			ScanErrorKind::UnterminatedJs
		);
	}

	#[test]
	fn columns_count_characters_not_bytes() {
		let e = err("DEFINE TABLE ⟨a⟩ ⟨b;");
		assert_eq!((e.line, e.column), (1, 18));
	}

	#[test]
	fn ranges_are_not_numbers() {
		let src = "DEFINE PARAM $r VALUE 1..5;";
		let stmts = scan(src).unwrap();
		let tokens: Vec<&str> = stmts[0].tokens.iter().map(|t| &src[t.span.clone()]).collect();
		assert_eq!(tokens[4..], ["1", ".", ".", "5"]);
	}

	#[test]
	fn the_error_display_reads_as_one_sentence() {
		let e = err("DEFINE PARAM $x VALUE [1, 2);");
		assert_eq!(
			e.to_string(),
			"line 1, column 28: found `)` where `]` was expected (opened at line 1, column 23)"
		);
	}
}
