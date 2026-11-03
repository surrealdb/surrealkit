//! A small lexer for the `DEFINE …` text that `INFO` returns.
//!
//! Typegen does not parse SurrealQL in general; it only needs to find clause
//! keywords, bracket pairs and the boundaries of names in engine-rendered
//! definitions. Doing that by scanning characters went wrong in ways this lexer
//! exists to rule out: an escaped quote (`'it\'s'`, `"say \"hi\""`) ended a
//! string early, a comparison (`$value >= 0`) was counted as a closing `>`, and
//! a backtick-quoted name containing a dot was split in two.
//!
//! The lexer is total: any input produces tokens, and an unterminated string,
//! quoted identifier or regex simply runs to the end of the text.

/// What a [`Token`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TokenKind {
	/// A run of ASCII letters, digits and underscores: a keyword, an
	/// identifier, or a number such as `1` or `0f`.
	Word,
	/// A `'…'` or `"…"` string literal, quotes included.
	String,
	/// A `` `…` `` or `⟨…⟩` quoted identifier, quotes included.
	QuotedIdent,
	/// A `/…/` regex literal.
	Regex,
	/// `(`, `[` or `{`.
	Open,
	/// `)`, `]` or `}`.
	Close,
	/// Any other single character.
	Punct,
}

/// One lexical unit: a kind and its byte range in the text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Token {
	pub kind: TokenKind,
	pub start: usize,
	pub end: usize,
}

impl Token {
	pub fn text<'a>(&self, src: &'a str) -> &'a str {
		&src[self.start..self.end]
	}

	/// The first byte of the token, for single-character tokens.
	pub fn byte(&self, src: &str) -> u8 {
		src.as_bytes()[self.start]
	}

	/// Whether this is the word `kw`, compared ASCII-case-insensitively.
	pub fn is_word(&self, src: &str, kw: &str) -> bool {
		self.kind == TokenKind::Word && self.text(src).eq_ignore_ascii_case(kw)
	}
}

const LANGLE: &str = "\u{27E8}";
const RANGLE: &str = "\u{27E9}";

/// Split `src` into tokens, skipping whitespace.
pub(super) fn tokenize(src: &str) -> Vec<Token> {
	let bytes = src.as_bytes();
	let mut tokens: Vec<Token> = Vec::new();
	let mut i = 0;
	while i < bytes.len() {
		let b = bytes[i];
		if b.is_ascii_whitespace() {
			i += 1;
			continue;
		}
		let start = i;
		let kind = match b {
			b'\'' | b'"' => {
				i = skip_quoted(bytes, i, b);
				TokenKind::String
			}
			b'`' => {
				i = skip_quoted(bytes, i, b'`');
				TokenKind::QuotedIdent
			}
			b'(' | b'[' | b'{' => {
				i += 1;
				TokenKind::Open
			}
			b')' | b']' | b'}' => {
				i += 1;
				TokenKind::Close
			}
			b'/' if regex_may_start(src, &tokens) => {
				i = skip_quoted(bytes, i, b'/');
				TokenKind::Regex
			}
			_ if is_word_byte(b) => {
				while i < bytes.len() && is_word_byte(bytes[i]) {
					i += 1;
				}
				TokenKind::Word
			}
			_ if src[i..].starts_with(LANGLE) => {
				i = src[i..].find(RANGLE).map_or(bytes.len(), |end| i + end + RANGLE.len());
				TokenKind::QuotedIdent
			}
			_ => {
				i += utf8_len(b);
				TokenKind::Punct
			}
		};
		tokens.push(Token {
			kind,
			start,
			end: i,
		});
	}
	tokens
}

pub(super) fn is_word_byte(b: u8) -> bool {
	b.is_ascii_alphanumeric() || b == b'_'
}

/// The length of the UTF-8 sequence that starts with `lead`.
fn utf8_len(lead: u8) -> usize {
	match lead {
		0xF0..=0xFF => 4,
		0xE0..=0xEF => 3,
		0xC0..=0xDF => 2,
		_ => 1,
	}
}

/// From the opening `quote` at `i`, return the index just past the closing one.
/// A backslash escapes the next character. Unterminated: the end of the text.
fn skip_quoted(bytes: &[u8], i: usize, quote: u8) -> usize {
	let mut j = i + 1;
	while j < bytes.len() {
		match bytes[j] {
			b'\\' => j += 2,
			b if b == quote => return j + 1,
			_ => j += 1,
		}
	}
	bytes.len()
}

/// Whether a `/` here starts a regex literal rather than dividing: only where an
/// operand is expected, i.e. at the start or after an operator or opener.
fn regex_may_start(src: &str, tokens: &[Token]) -> bool {
	match tokens.last() {
		None => true,
		Some(t) if t.kind == TokenKind::Open => true,
		Some(t) if t.kind == TokenKind::Punct => {
			matches!(
				t.byte(src),
				b'=' | b',' | b'!' | b'~' | b'?' | b':' | b'|' | b'&' | b'>' | b'<'
			)
		}
		_ => false,
	}
}

/// The decoded contents of a quoted string or identifier token (quotes
/// removed, escapes resolved). Escapes are those the engine writes: `\\`,
/// `\'`, `\"`, `` \` ``, `\n`, `\r`, `\t`, `\0`, `\f`, `\b` and `\u{…}`; any
/// other escaped character stands for itself.
pub(super) fn unquote(token_text: &str) -> String {
	if let Some(inner) = token_text.strip_prefix(LANGLE) {
		return inner.strip_suffix(RANGLE).unwrap_or(inner).to_string();
	}
	let mut chars = token_text.chars();
	let Some(quote) = chars.next() else {
		return String::new();
	};
	let body = chars.as_str();
	let body = body.strip_suffix(quote).unwrap_or(body);
	unescape(body)
}

/// Resolve backslash escapes in the body of a quoted string.
pub(super) fn unescape(body: &str) -> String {
	let mut out = String::with_capacity(body.len());
	let mut chars = body.chars();
	while let Some(c) = chars.next() {
		if c != '\\' {
			out.push(c);
			continue;
		}
		match chars.next() {
			Some('n') => out.push('\n'),
			Some('r') => out.push('\r'),
			Some('t') => out.push('\t'),
			Some('0') => out.push('\0'),
			Some('f') => out.push('\u{0C}'),
			Some('b') => out.push('\u{08}'),
			Some('u') => {
				let rest = chars.as_str();
				let decoded = rest
					.strip_prefix('{')
					.and_then(|r| r.find('}').map(|end| (&r[..end], end)))
					.and_then(|(hex, end)| {
						u32::from_str_radix(hex, 16)
							.ok()
							.and_then(char::from_u32)
							.map(|ch| (ch, end))
					});
				match decoded {
					Some((ch, end)) => {
						out.push(ch);
						// Skip `{`, the hex digits and `}`.
						chars = rest[end + 2..].chars();
					}
					None => out.push('u'),
				}
			}
			Some(other) => out.push(other),
			None => out.push('\\'),
		}
	}
	out
}

/// Split a field name as `INFO` reports it into path segments: on `.` outside
/// backtick or `⟨⟩` quoting, unquoting quoted segments, with `[*]` and `*`
/// becoming the segment `"*"`. `` `odd.name` `` is one segment; `phones[*].number`
/// is `["phones", "*", "number"]`.
pub(super) fn field_path(name: &str) -> Vec<String> {
	split_field_name(name).into_iter().map(|(segment, _)| segment).collect()
}

/// [`field_path`], also reporting which segments were quoted. A quoted segment
/// is a literal name, never `*` or an array marker.
pub(super) fn split_field_name(name: &str) -> Vec<(String, bool)> {
	let mut segments = Vec::new();
	let mut current = String::new();
	let mut quoted = false;
	for token in tokenize(name) {
		let text = token.text(name);
		match token.kind {
			TokenKind::Punct if text == "." => {
				push_segment(&mut segments, &mut current, quoted);
				quoted = false;
			}
			TokenKind::Open if text == "[" => {
				push_segment(&mut segments, &mut current, quoted);
				quoted = false;
			}
			TokenKind::Close if text == "]" => {}
			TokenKind::QuotedIdent => {
				current.push_str(&unquote(text));
				quoted = true;
			}
			_ => current.push_str(text),
		}
	}
	push_segment(&mut segments, &mut current, quoted);
	segments
}

fn push_segment(segments: &mut Vec<(String, bool)>, current: &mut String, quoted: bool) {
	if !current.is_empty() || quoted {
		segments.push((std::mem::take(current), quoted));
	}
}

/// Whether `stmt` contains the keyword `kw` as a clause keyword: a whole word,
/// outside strings, quoted identifiers and regexes, and not nested in `()`,
/// `[]` or `{}`. `<` and `>` are not brackets here: in an expression they are
/// comparisons.
pub(super) fn has_top_level_keyword(stmt: &str, kw: &str) -> bool {
	top_level_words(stmt).any(|t| t.is_word(stmt, kw))
}

/// The word following the top-level keyword `kw`, if any.
pub(super) fn word_after_keyword<'a>(stmt: &'a str, kw: &str) -> Option<&'a str> {
	let mut words = top_level_words(stmt);
	words.find(|t| t.is_word(stmt, kw))?;
	words.next().map(|t| t.text(stmt))
}

/// Words at bracket depth zero.
fn top_level_words(stmt: &str) -> impl Iterator<Item = Token> + '_ {
	let mut depth = 0i32;
	tokenize(stmt).into_iter().filter(move |t| match t.kind {
		TokenKind::Open => {
			depth += 1;
			false
		}
		TokenKind::Close => {
			depth -= 1;
			false
		}
		TokenKind::Word => depth == 0,
		_ => false,
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	fn kinds(src: &str) -> Vec<(TokenKind, &str)> {
		tokenize(src).into_iter().map(|t| (t.kind, t.text(src))).collect()
	}

	#[test]
	fn escaped_quotes_do_not_end_a_string() {
		use TokenKind::*;
		assert_eq!(
			kinds(r#"COMMENT "semi; \"and\" 'q'" PERMISSIONS"#),
			vec![(Word, "COMMENT"), (String, r#""semi; \"and\" 'q'""#), (Word, "PERMISSIONS")]
		);
		assert_eq!(kinds(r"'it\'s' x"), vec![(String, r"'it\'s'"), (Word, "x")]);
		assert_eq!(unquote(r#""semi; \"and\" 'q'""#), r#"semi; "and" 'q'"#);
		assert_eq!(unquote(r"'it\'s'"), "it's");
	}

	#[test]
	fn unescapes_everything_the_engine_writes() {
		assert_eq!(unescape(r"a\nb\tc\rd\0e\\f\u{8}g\u{1F600}"), "a\nb\tc\rd\0e\\f\u{8}g😀");
		assert_eq!(unescape(r"\x\u{zz}"), "xu{zz}");
		assert_eq!(unescape("trailing\\"), "trailing\\");
	}

	#[test]
	fn quoted_identifiers_and_regexes_are_single_tokens() {
		use TokenKind::*;
		assert_eq!(
			kinds("`odd.name` ⟨a b⟩ `sequence`::nextval"),
			vec![
				(QuotedIdent, "`odd.name`"),
				(QuotedIdent, "⟨a b⟩"),
				(QuotedIdent, "`sequence`"),
				(Punct, ":"),
				(Punct, ":"),
				(Word, "nextval"),
			]
		);
		assert_eq!(
			kinds("$value = /^[a-z]+$/ OR"),
			vec![(Punct, "$"), (Word, "value"), (Punct, "="), (Regex, "/^[a-z]+$/"), (Word, "OR")]
		);
		// Division is not a regex.
		assert_eq!(kinds("a / 2 / 3").iter().filter(|(k, _)| *k == Regex).count(), 0);
	}

	#[test]
	fn unterminated_text_runs_to_the_end() {
		assert_eq!(kinds("'abc"), vec![(TokenKind::String, "'abc")]);
		assert_eq!(kinds("`abc"), vec![(TokenKind::QuotedIdent, "`abc")]);
		assert_eq!(kinds("⟨abc"), vec![(TokenKind::QuotedIdent, "⟨abc")]);
	}

	#[test]
	fn field_paths_respect_quoting() {
		assert_eq!(field_path("address.city"), vec!["address", "city"]);
		assert_eq!(field_path("`odd.name`"), vec!["odd.name"]);
		assert_eq!(field_path("phones[*].number"), vec!["phones", "*", "number"]);
		assert_eq!(field_path("phones.*.kind"), vec!["phones", "*", "kind"]);
		assert_eq!(field_path("a.`b.c`.d"), vec!["a", "b.c", "d"]);
		assert_eq!(field_path("⟨x.y⟩"), vec!["x.y"]);
		assert_eq!(
			split_field_name("`*`.x"),
			vec![("*".to_string(), true), ("x".to_string(), false)]
		);
	}

	#[test]
	fn top_level_keywords_ignore_strings_brackets_and_comparisons() {
		let stmt = r#"DEFINE TABLE t TYPE NORMAL SCHEMAFULL COMMENT "a \"RELATION\" SCHEMALESS" PERMISSIONS NONE"#;
		assert!(has_top_level_keyword(stmt, "SCHEMAFULL"));
		assert!(!has_top_level_keyword(stmt, "RELATION"));
		assert!(!has_top_level_keyword(stmt, "SCHEMALESS"));
		assert_eq!(word_after_keyword(stmt, "TYPE"), Some("NORMAL"));
		// A comparison does not unbalance anything that follows it.
		let view =
			"DEFINE TABLE v TYPE ANY SCHEMALESS AS SELECT * FROM p WHERE a >= 18 PERMISSIONS NONE";
		assert!(has_top_level_keyword(view, "PERMISSIONS"));
		assert!(!has_top_level_keyword("x { READONLY } y", "READONLY"));
		assert!(!has_top_level_keyword("readonly_at", "READONLY"));
	}
}
